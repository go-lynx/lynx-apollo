package apollo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ApolloHTTPClient talks to Apollo's HTTP config/notification API. It resolves
// the config server address from the meta server once and caches it. Safe for
// concurrent use: configServer is guarded by mu and closed is atomic.
type ApolloHTTPClient struct {
	metaServer   string
	appId        string
	cluster      string
	namespace    string
	token        string
	httpClient   *http.Client
	configServer string // resolved config server address, cached after first lookup
	mu           sync.RWMutex
	closed       int32
}

// ApolloConfigResponse is the payload returned by the configs endpoint.
type ApolloConfigResponse struct {
	AppId          string            `json:"appId"`
	Cluster        string            `json:"cluster"`
	NamespaceName  string            `json:"namespaceName"`
	Configurations map[string]string `json:"configurations"`
	ReleaseKey     string            `json:"releaseKey"`
}

// ApolloNotificationResponse is one entry from the long-poll notifications endpoint.
type ApolloNotificationResponse struct {
	NamespaceName  string `json:"namespaceName"`
	NotificationId int64  `json:"notificationId"`
}

// NewApolloHTTPClient creates a new Apollo HTTP client
func NewApolloHTTPClient(metaServer, appId, cluster, namespace, token string, timeout time.Duration) *ApolloHTTPClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return &ApolloHTTPClient{
		metaServer: metaServer,
		appId:      appId,
		cluster:    cluster,
		namespace:  namespace,
		token:      token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// getConfigServer returns the cached config server address, resolving it from
// the meta server on first call.
func (c *ApolloHTTPClient) getConfigServer(ctx context.Context) (string, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return "", fmt.Errorf("apollo HTTP client is closed")
	}
	c.mu.RLock()
	if c.configServer != "" {
		server := c.configServer
		c.mu.RUnlock()
		return server, nil
	}
	c.mu.RUnlock()

	// Query meta server for config server address
	metaURL := fmt.Sprintf("%s/services/config?appId=%s&ip=%s", c.metaServer, c.appId, c.getClientIP())

	req, err := http.NewRequestWithContext(ctx, "GET", metaURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to query meta server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("meta server returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	// Parse config server addresses (can be multiple, comma-separated)
	servers := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(servers) == 0 {
		return "", fmt.Errorf("no config server found")
	}

	// Use the first server, prefer HTTPS if available
	configServer := strings.TrimSpace(servers[0])
	for _, s := range servers {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "https://") {
			configServer = s
			break
		}
	}

	// Ensure it has a scheme
	if !strings.HasPrefix(configServer, "http://") && !strings.HasPrefix(configServer, "https://") {
		configServer = "http://" + configServer
	}

	c.mu.Lock()
	c.configServer = configServer
	c.mu.Unlock()

	return configServer, nil
}

// getClientIP returns the client IP used for grey-release routing. Empty means
// Apollo falls back to the request's source IP.
func (c *ApolloHTTPClient) getClientIP() string {
	return ""
}

// GetConfig fetches the full configuration for a namespace. A 404 is treated as
// an empty namespace rather than an error.
func (c *ApolloHTTPClient) GetConfig(ctx context.Context, namespace string) (*ApolloConfigResponse, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, fmt.Errorf("apollo HTTP client is closed")
	}
	configServer, err := c.getConfigServer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get config server: %w", err)
	}

	configURL := fmt.Sprintf("%s/configs/%s/%s/%s", configServer, c.appId, c.cluster, namespace)
	if c.token != "" {
		configURL += "?token=" + url.QueryEscape(c.token)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", configURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &ApolloConfigResponse{
			AppId:          c.appId,
			Cluster:        c.cluster,
			NamespaceName:  namespace,
			Configurations: make(map[string]string),
			ReleaseKey:     "",
		}, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("config server returned status %d: %s", resp.StatusCode, string(body))
	}

	var configResp ApolloConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&configResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &configResp, nil
}

// GetConfigValue returns a single key's value, erroring if the key is absent.
func (c *ApolloHTTPClient) GetConfigValue(ctx context.Context, namespace, key string) (string, error) {
	config, err := c.GetConfig(ctx, namespace)
	if err != nil {
		return "", err
	}

	value, exists := config.Configurations[key]
	if !exists {
		return "", fmt.Errorf("config key %s not found in namespace %s", key, namespace)
	}

	return value, nil
}

// WatchNotifications watches for configuration changes using long polling
func (c *ApolloHTTPClient) WatchNotifications(ctx context.Context, namespace string, notificationId int64, timeout time.Duration) ([]ApolloNotificationResponse, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, fmt.Errorf("apollo HTTP client is closed")
	}
	configServer, err := c.getConfigServer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get config server: %w", err)
	}

	notification := fmt.Sprintf(`{"namespaceName":"%s","notificationId":%d}`, namespace, notificationId)
	notificationURL := fmt.Sprintf("%s/notifications/v2?appId=%s&cluster=%s&notifications=%s",
		configServer, c.appId, c.cluster, url.QueryEscape(notification))

	if c.token != "" {
		notificationURL += "&token=" + url.QueryEscape(c.token)
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", notificationURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Timeout is expected in long polling, check if it's a timeout
		if reqCtx.Err() == context.DeadlineExceeded {
			// No changes, return empty list
			return []ApolloNotificationResponse{}, nil
		}
		return nil, fmt.Errorf("failed to watch notifications: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		// No changes
		return []ApolloNotificationResponse{}, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("notification server returned status %d: %s", resp.StatusCode, string(body))
	}

	var notifications []ApolloNotificationResponse
	if err := json.NewDecoder(resp.Body).Decode(&notifications); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return notifications, nil
}

// Close closes the HTTP client
func (c *ApolloHTTPClient) Close() {
	atomic.StoreInt32(&c.closed, 1)
	// HTTP client doesn't need explicit close, but we can clear cached config server
	c.mu.Lock()
	c.configServer = ""
	c.mu.Unlock()
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}
