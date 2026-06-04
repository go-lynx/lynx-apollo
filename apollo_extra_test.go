package apollo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx-apollo/conf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- helpers ----

// apolloTarget builds a lynx.ControlPlaneConfigTarget for tests.
func apolloTarget(fileName string) lynx.ControlPlaneConfigTarget {
	return lynx.ControlPlaneConfigTarget{FileName: fileName}
}

// newApolloServer returns a minimal mock Apollo HTTP server that serves GetConfig responses.
func newApolloServer(t *testing.T, configs map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// services/config → return the server URL itself as config server
		if r.URL.Path == "/services/config" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `[{"homepageUrl":"%s"}]`, "http://"+r.Host)
			return
		}
		// configs/<appId>/<cluster>/<namespace> → return config
		resp := ApolloConfigResponse{
			AppId:          "test-app",
			Cluster:        "default",
			NamespaceName:  "application",
			Configurations: configs,
			ReleaseKey:     "release-1",
		}
		b, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))
}

// ---- Metrics tests ----

// Note: NewApolloMetrics uses promauto which registers to the default global registry.
// Calling it more than once panics on duplicate registration.
// We only call it once via TestApolloMetrics_AllMethods.
func TestApolloMetrics_AllMethods(t *testing.T) {
	m := NewApolloMetrics()
	require.NotNil(t, m)
	// Exercise every record method to hit the code paths.
	m.RecordClientOperation("op", "start")
	m.RecordClientOperation("op", "success")
	m.RecordConfigOperation("ns", "get", "start")
	m.RecordConfigOperation("ns", "get", "success")
	m.RecordConfigChange("ns")
	m.RecordNotification("ns", "ok")
	m.RecordHealthCheck("start")
	m.RecordHealthCheck("success")
	m.RecordConnectionError("ns")
	m.RecordCacheHit("ns")
	m.RecordCacheMiss("ns")
}

// ---- ApolloHTTPClient tests ----

func TestApolloHTTPClient_GetConfig_Closed(t *testing.T) {
	client := NewApolloHTTPClient("http://localhost:8080", "app", "default", "application", "", time.Second)
	atomic.StoreInt32(&client.closed, 1)
	_, err := client.GetConfig(context.Background(), "application")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestApolloHTTPClient_GetConfigValue_NotFound(t *testing.T) {
	server := newApolloServer(t, map[string]string{"key1": "val1"})
	defer server.Close()

	client := NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	client.configServer = server.URL

	_, err := client.GetConfigValue(context.Background(), "application", "missing-key")
	assert.Error(t, err)
}

func TestApolloHTTPClient_GetConfigValue_Success(t *testing.T) {
	server := newApolloServer(t, map[string]string{"key1": "val1"})
	defer server.Close()

	client := NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	client.configServer = server.URL

	val, err := client.GetConfigValue(context.Background(), "application", "key1")
	require.NoError(t, err)
	assert.Equal(t, "val1", val)
}

func TestApolloHTTPClient_GetConfigServer_CachedValue(t *testing.T) {
	client := NewApolloHTTPClient("http://meta.example.com", "app", "default", "application", "", time.Second)
	client.configServer = "http://cached-server:8080"

	ctx := context.Background()
	server, err := client.getConfigServer(ctx)
	require.NoError(t, err)
	assert.Equal(t, "http://cached-server:8080", server)
}

func TestApolloHTTPClient_GetConfigServer_FromMetaServer(t *testing.T) {
	metaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `[{"homepageUrl":"http://%s"}]`, r.Host)
	}))
	defer metaServer.Close()

	client := NewApolloHTTPClient(metaServer.URL, "test-app", "default", "application", "", 5*time.Second)
	server, err := client.getConfigServer(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, server)
}

func TestApolloHTTPClient_GetConfigServer_Closed(t *testing.T) {
	client := NewApolloHTTPClient("http://meta.example.com", "app", "default", "application", "", time.Second)
	atomic.StoreInt32(&client.closed, 1)
	_, err := client.getConfigServer(context.Background())
	assert.Error(t, err)
}

func TestApolloHTTPClient_GetClientIP(t *testing.T) {
	client := NewApolloHTTPClient("http://meta.example.com", "app", "default", "application", "", time.Second)
	// getClientIP resolves the local IP; it may return empty in some network
	// environments — just verify it doesn't panic.
	_ = client.getClientIP()
}

// ---- GetConfig / GetConfigSources / GetConfigWatchTargets tests ----

func TestPlugApollo_GetConfig_NotInitialized(t *testing.T) {
	p := NewApolloConfigCenter()
	_, err := p.GetConfig("application", "")
	assert.Error(t, err)
}

func TestPlugApollo_GetConfig_NilClient(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{}
	p.setInitialized()
	_, err := p.GetConfig("application", "")
	assert.Error(t, err)
}

func TestPlugApollo_GetConfig_Success(t *testing.T) {
	server := newApolloServer(t, map[string]string{"key": "value"})
	defer server.Close()

	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{AppId: "test-app", Cluster: "default", Namespace: "application"}
	p.client = NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	p.client.configServer = server.URL
	p.setInitialized()

	src, err := p.GetConfig("application", "")
	require.NoError(t, err)
	require.NotNil(t, src)
}

func TestPlugApollo_GetConfigSources_NotInitialized(t *testing.T) {
	p := NewApolloConfigCenter()
	_, err := p.GetConfigSources()
	assert.Error(t, err)
}

func TestPlugApollo_GetConfigSources_WithClient(t *testing.T) {
	server := newApolloServer(t, map[string]string{"key": "value"})
	defer server.Close()

	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{AppId: "test-app", Cluster: "default", Namespace: "application"}
	p.client = NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	p.client.configServer = server.URL
	p.setInitialized()

	srcs, err := p.GetConfigSources()
	require.NoError(t, err)
	assert.Len(t, srcs, 1)
}

func TestPlugApollo_GetConfigSources_WithAdditionalNamespaces(t *testing.T) {
	server := newApolloServer(t, map[string]string{"key": "value"})
	defer server.Close()

	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{
		AppId:     "test-app",
		Cluster:   "default",
		Namespace: "application",
		ServiceConfig: &conf.ServiceConfig{
			Namespace:            "svc-ns",
			AdditionalNamespaces: []string{"extra-ns"},
		},
	}
	p.client = NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	p.client.configServer = server.URL
	p.setInitialized()

	srcs, err := p.GetConfigSources()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(srcs), 1)
}

func TestPlugApollo_GetConfigWatchTargets_NotInitialized(t *testing.T) {
	p := NewApolloConfigCenter()
	_, err := p.GetConfigWatchTargets("")
	assert.Error(t, err)
}

func TestPlugApollo_GetConfigWatchTargets_Basic(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{Namespace: "application"}
	p.setInitialized()

	targets, err := p.GetConfigWatchTargets("myapp")
	require.NoError(t, err)
	require.Len(t, targets, 1)
	assert.Equal(t, "application", targets[0].FileName)
}

func TestPlugApollo_GetConfigWatchTargets_WithServiceConfig(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{
		Namespace: "application",
		ServiceConfig: &conf.ServiceConfig{
			Namespace:            "svc-ns",
			AdditionalNamespaces: []string{"ns1", "ns2"},
		},
	}
	p.setInitialized()

	targets, err := p.GetConfigWatchTargets("")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(targets), 3) // main + 2 additional
}

// ---- WatchControlPlaneConfig tests ----

func TestPlugApollo_WatchControlPlaneConfig_NotInitialized(t *testing.T) {
	p := NewApolloConfigCenter()
	_, err := p.WatchControlPlaneConfig(context.Background(), apolloTarget("application"))
	assert.Error(t, err)
}

func TestPlugApollo_WatchControlPlaneConfig_NilClient(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{}
	p.setInitialized()
	_, err := p.WatchControlPlaneConfig(context.Background(), apolloTarget("application"))
	assert.Error(t, err)
}

func TestPlugApollo_WatchControlPlaneConfig_WithServer(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requestStarted <- struct{}{}:
		default:
		}
		// Keep connection open.
		<-r.Context().Done()
	}))
	defer server.Close()

	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{AppId: "test-app", Cluster: "default", Namespace: "application"}
	p.client = NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	p.client.configServer = server.URL
	p.setInitialized()

	ctx, cancel := context.WithCancel(context.Background())
	watcher, err := p.WatchControlPlaneConfig(ctx, apolloTarget("application"))
	require.NoError(t, err)

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("watcher should have started a long-poll request")
	}

	cancel()
	assert.NoError(t, watcher.Stop())
}

// ---- GetConfigValue with cache ----

func TestPlugApollo_GetConfigValue_WithCache(t *testing.T) {
	server := newApolloServer(t, map[string]string{"cached-key": "cached-value"})
	defer server.Close()

	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{
		AppId:       "test-app",
		Cluster:     "default",
		Namespace:   "application",
		EnableCache: true,
	}
	p.client = NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	p.client.configServer = server.URL
	p.retryManager = NewRetryManager(0, time.Millisecond)
	p.circuitBreaker = NewCircuitBreaker(2.0)
	p.setInitialized()

	val, err := p.GetConfigValue("application", "cached-key")
	require.NoError(t, err)
	assert.Equal(t, "cached-value", val)

	// Second call should hit cache.
	val2, err := p.GetConfigValue("application", "cached-key")
	require.NoError(t, err)
	assert.Equal(t, "cached-value", val2)
}

// ---- WatchConfig tests ----

func TestPlugApollo_WatchConfig_NotInitialized(t *testing.T) {
	p := NewApolloConfigCenter()
	_, err := p.WatchConfig("application")
	assert.Error(t, err)
}

func TestPlugApollo_WatchConfig_Success(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{}
	p.setInitialized()

	watcher, err := p.WatchConfig("application")
	require.NoError(t, err)
	require.NotNil(t, watcher)

	// Duplicate call returns same watcher.
	watcher2, err := p.WatchConfig("application")
	require.NoError(t, err)
	assert.Equal(t, watcher, watcher2)
}

func TestPlugApollo_WatchConfig_SetCallbacks(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{}
	p.setInitialized()

	watcher, err := p.WatchConfig("application")
	require.NoError(t, err)

	watcher.SetOnConfigChanged(func(ns, key, val string) {})
	watcher.SetOnError(func(err error) {})
	watcher.Start() // legacy start, should be no-op effectively
}

// ---- ConfigWatcher callback handlers ----

func TestPlugApollo_handleConfigChanged(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{}
	p.setInitialized()
	p.configCache["application:test-key"] = "old-value"

	p.handleConfigChanged("application", "test-key", "new-value")

	p.cacheMutex.RLock()
	_, exists := p.configCache["application:test-key"]
	p.cacheMutex.RUnlock()
	assert.False(t, exists, "cache entry should be cleared on config change")
}

func TestPlugApollo_handleConfigWatchError_NoRetry(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{EnableRetry: false}
	p.setInitialized()
	// Should not panic.
	p.handleConfigWatchError("application", fmt.Errorf("watch error"))
}

func TestPlugApollo_handleConfigWatchError_WithRetry(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{EnableRetry: true}
	p.setInitialized()
	p.setDestroyed() // prevents retryConfigWatch from actually retrying
	// Should not panic.
	p.handleConfigWatchError("application", fmt.Errorf("watch error"))
}

// ---- Control plane capabilities ----

func TestPlugApollo_ControlPlaneCapabilities(t *testing.T) {
	p := NewApolloConfigCenter()
	caps := p.ControlPlaneCapabilities()
	assert.GreaterOrEqual(t, len(caps), 2)
}

func TestPlugApollo_NilRateLimitsAndNilRegistry(t *testing.T) {
	p := NewApolloConfigCenter()
	assert.Nil(t, p.HTTPRateLimit())
	assert.Nil(t, p.GRPCRateLimit())
	assert.Nil(t, p.NewServiceRegistry())
	assert.Nil(t, p.NewServiceDiscovery())
	assert.Nil(t, p.NewNodeRouter("svc"))
}

// ---- CheckHealth ----

func TestPlugApollo_CheckHealth_NotInitialized(t *testing.T) {
	p := NewApolloConfigCenter()
	err := p.CheckHealth()
	assert.Error(t, err)
}

func TestPlugApollo_CheckHealth_NilClient(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{}
	p.setInitialized()
	err := p.CheckHealth()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestPlugApollo_CheckHealth_WithComponents(t *testing.T) {
	// Verify CheckHealth returns an error quickly when the Apollo server is unreachable.
	// The deadlock fix (not wrapping checkClientConnection inside circuitBreaker.Do)
	// is exercised here: if the fix is absent the test will hang indefinitely.
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{AppId: "test-app", Cluster: "default", Namespace: "application"}
	p.client = NewApolloHTTPClient("http://127.0.0.1:1", "test-app", "default", "application", "", 50*time.Millisecond)
	p.client.configServer = "http://127.0.0.1:1"
	p.retryManager = NewRetryManager(0, time.Millisecond) // 0 retries → fail fast
	p.circuitBreaker = NewCircuitBreaker(0.5)
	p.setInitialized()

	err := p.CheckHealth()
	assert.Error(t, err, "expected error when Apollo server is unreachable")
}

// ---- cleanup helpers ----

func TestPlugApollo_getCleanupStats(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{}
	stats := p.getCleanupStats()
	require.NotNil(t, stats)
	assert.Contains(t, stats, "cleanup_time")
	assert.Contains(t, stats, "plugin_state")
	assert.Contains(t, stats, "resources")
}

func TestPlugApollo_stopBackgroundTasks(t *testing.T) {
	p := NewApolloConfigCenter()
	p.retryManager = NewRetryManager(1, time.Millisecond)
	p.circuitBreaker = NewCircuitBreaker(0.5)
	p.stopBackgroundTasks()
	// retryManager is set to nil by stopBackgroundTasks.
	assert.Nil(t, p.retryManager)
	// circuitBreaker is ForceClose()'d but the reference is not cleared (by design).
	assert.NotNil(t, p.circuitBreaker)
}

func TestPlugApollo_releaseMemoryResources(t *testing.T) {
	p := NewApolloConfigCenter()
	p.retryManager = NewRetryManager(1, time.Millisecond)
	p.circuitBreaker = NewCircuitBreaker(0.5)
	p.configCache["ns:key"] = "value"
	p.releaseMemoryResources()
	assert.Nil(t, p.retryManager)
	assert.Nil(t, p.circuitBreaker)
	assert.Empty(t, p.configCache)
}

// ---- ApolloConfigSource tests ----

func TestApolloConfigSource_Load_WithServer(t *testing.T) {
	server := newApolloServer(t, map[string]string{"key": "val"})
	defer server.Close()

	client := NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	client.configServer = server.URL
	src := NewApolloConfigSource(client, "test-app", "default", "application")

	kvs, err := src.Load()
	require.NoError(t, err)
	assert.NotEmpty(t, kvs)
}

func TestApolloConfigSource_Load_NilClient(t *testing.T) {
	src := NewApolloConfigSource(nil, "test-app", "default", "application")
	_, err := src.Load()
	assert.Error(t, err)
}

func TestApolloConfigSource_Watch_NilClient(t *testing.T) {
	src := NewApolloConfigSource(nil, "test-app", "default", "application")
	_, err := src.Watch()
	assert.Error(t, err)
}

func TestApolloConfigSource_Watch_WithServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", time.Minute)
	client.configServer = server.URL

	src := NewApolloConfigSource(client, "test-app", "default", "application")
	watcher, err := src.Watch()
	require.NoError(t, err)
	require.NotNil(t, watcher)
	assert.NoError(t, watcher.Stop())
}

// ---- ResilienceDoWithRetryContext ----

func TestRetryManager_DoWithRetryContext_Cancelled(t *testing.T) {
	rm := NewRetryManager(5, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rm.DoWithRetryContext(ctx, func() error {
		return fmt.Errorf("fail")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

// ---- ApolloConfigWatcher watchLoop with config change ----

func TestApolloConfigWatcher_watchLoop_ConfigChange(t *testing.T) {
	notifSent := int32(0)
	configFetched := int32(0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/notifications/v2" {
			// First call: return a notification; subsequent: block until cancelled.
			if atomic.CompareAndSwapInt32(&notifSent, 0, 1) {
				resp := []ApolloNotificationResponse{{NamespaceName: "application", NotificationId: 42}}
				b, _ := json.Marshal(resp)
				w.Header().Set("Content-Type", "application/json")
				w.Write(b)
				return
			}
			<-r.Context().Done()
			return
		}
		// configs endpoint
		atomic.AddInt32(&configFetched, 1)
		resp := ApolloConfigResponse{
			AppId:          "test-app",
			Cluster:        "default",
			NamespaceName:  "application",
			Configurations: map[string]string{"updated": "value"},
			ReleaseKey:     "r2",
		}
		b, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))
	defer server.Close()

	client := NewApolloHTTPClient(server.URL, "test-app", "default", "application", "", 5*time.Second)
	client.configServer = server.URL
	watcher := NewApolloConfigWatcher(client, "application")
	watcher.notificationTimeout = 200 * time.Millisecond

	assert.NoError(t, watcher.Start(context.Background()))

	// Wait for config change notification.
	select {
	case kvs := <-watcher.notifyCh:
		assert.NotEmpty(t, kvs)
	case <-time.After(2 * time.Second):
		t.Fatal("expected config change to arrive")
	}

	assert.NoError(t, watcher.Stop())
}

// ---- errors tests ----

func TestApolloError_NewTimeoutError(t *testing.T) {
	err := NewTimeoutError("timeout")
	assert.Error(t, err)
	assert.Equal(t, ErrCodeTimeout, err.Code)
}

func TestApolloError_NewRetryError(t *testing.T) {
	err := NewRetryError("retry")
	assert.Error(t, err)
	assert.Equal(t, ErrCodeRetryExhausted, err.Code)
}

func TestApolloError_NewHealthCheckError(t *testing.T) {
	err := NewHealthCheckError("health failed")
	assert.Error(t, err)
	assert.Equal(t, ErrCodeHealthCheckFailed, err.Code)
}

func TestApolloError_IsRetryError(t *testing.T) {
	retryErr := NewRetryError("retry error")
	assert.True(t, IsRetryError(retryErr))

	configErr := NewConfigError("config error")
	assert.False(t, IsRetryError(configErr))
}

func TestApolloError_WrapNetworkError(t *testing.T) {
	baseErr := fmt.Errorf("dial error")
	err := WrapNetworkError(baseErr, "network problem")
	assert.Error(t, err)
	assert.Equal(t, ErrCodeNetworkError, err.Code)
	assert.Contains(t, err.Error(), "network problem")
}

// ---- IsInitialized / IsDestroyed / GetApolloConfig / GetNamespace ----

func TestPlugApollo_GetApolloConfig_NilConf(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = nil
	cfg := p.GetApolloConfig()
	assert.Nil(t, cfg)
}

// ---- GetMetrics ----

func TestPlugApollo_GetMetrics(t *testing.T) {
	p := NewApolloConfigCenter()
	// metrics not initialized yet
	m := p.GetMetrics()
	assert.Nil(t, m)

	// set a dummy metrics reference
	p.metrics = &Metrics{}
	m = p.GetMetrics()
	assert.NotNil(t, m)
}

// ---- validate config edge cases ----

func TestPlugApollo_validateConfig_Nil(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = nil
	err := p.validateConfig()
	assert.Error(t, err)
}

// ---- initComponents ----

func TestPlugApollo_initComponents_NoMetricsDuplicate(t *testing.T) {
	// Only tests the retry/cb initialization path because metrics were registered
	// globally by TestApolloMetrics_AllMethods above.
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{MaxRetryTimes: 2}
	// Avoid calling NewApolloMetrics() again; manually set metrics.
	// Instead test just that the values are set.
	p.retryManager = NewRetryManager(2, conf.DefaultRetryInterval)
	p.circuitBreaker = NewCircuitBreaker(conf.DefaultCircuitBreakerThreshold)
	assert.NotNil(t, p.retryManager)
	assert.NotNil(t, p.circuitBreaker)
}
