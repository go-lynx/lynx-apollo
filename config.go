package apollo

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx-apollo/conf"
	"github.com/go-lynx/lynx/log"
)

// GetConfig returns a config.Source for a single namespace. Apollo addresses
// config by app_id+cluster+namespace, so fileName is treated as the namespace
// and group is ignored. An empty fileName falls back to the configured namespace.
func (p *PlugApollo) GetConfig(fileName string, group string) (config.Source, error) {
	if err := p.checkInitialized(); err != nil {
		return nil, err
	}

	// Snapshot the client under the lock so a concurrent shutdown cannot race
	// or nil it out from under us.
	p.mu.RLock()
	client := p.client
	p.mu.RUnlock()
	if client == nil {
		return nil, NewInitError("Apollo client is nil")
	}

	namespace := fileName
	if namespace == "" {
		namespace = p.conf.Namespace
	}

	log.Infof("Getting config from Apollo - Namespace: [%s]", namespace)

	source := NewApolloConfigSource(client, p.conf.AppId, p.conf.Cluster, namespace)

	return source, nil
}

// GetConfigSources returns the main namespace source plus any additional
// namespace sources, implementing the MultiConfigControlPlane interface.
func (p *PlugApollo) GetConfigSources() ([]config.Source, error) {
	if err := p.checkInitialized(); err != nil {
		return nil, err
	}

	var sources []config.Source

	mainSource, err := p.getMainConfigSource()
	if err != nil {
		return nil, fmt.Errorf("failed to get main config source: %w", err)
	}
	if mainSource != nil {
		sources = append(sources, mainSource)
	}

	additionalSources, err := p.getAdditionalConfigSources()
	if err != nil {
		return nil, fmt.Errorf("failed to get additional config sources: %w", err)
	}
	sources = append(sources, additionalSources...)

	return sources, nil
}

// GetConfigWatchTargets returns the remote config entries that should feed the global config snapshot.
func (p *PlugApollo) GetConfigWatchTargets(appName string) ([]lynx.ControlPlaneConfigTarget, error) {
	if err := p.checkInitialized(); err != nil {
		return nil, err
	}

	targets := make([]lynx.ControlPlaneConfigTarget, 0, 4)
	mainNamespace := p.conf.Namespace
	mainPriority := 0
	mainStrategy := ""
	if p.conf.ServiceConfig != nil {
		if p.conf.ServiceConfig.Namespace != "" {
			mainNamespace = p.conf.ServiceConfig.Namespace
		}
		mainPriority = int(p.conf.ServiceConfig.Priority)
		mainStrategy = p.conf.ServiceConfig.MergeStrategy
	}
	if mainNamespace == "" {
		mainNamespace = conf.DefaultNamespace
	}
	targets = append(targets, lynx.ControlPlaneConfigTarget{
		FileName:      mainNamespace,
		Priority:      mainPriority,
		MergeStrategy: mainStrategy,
	})

	if p.conf.ServiceConfig == nil {
		return targets, nil
	}

	for _, namespace := range p.conf.ServiceConfig.AdditionalNamespaces {
		if namespace == "" {
			namespace = p.conf.ServiceConfig.Namespace
		}
		if namespace == "" {
			namespace = p.conf.Namespace
		}
		if namespace == "" {
			continue
		}
		targets = append(targets, lynx.ControlPlaneConfigTarget{
			FileName:      namespace,
			Priority:      int(p.conf.ServiceConfig.Priority),
			MergeStrategy: p.conf.ServiceConfig.MergeStrategy,
		})
	}

	return targets, nil
}

// WatchControlPlaneConfig opens a running watcher for a remote config target.
func (p *PlugApollo) WatchControlPlaneConfig(ctx context.Context, target lynx.ControlPlaneConfigTarget) (config.Watcher, error) {
	if err := p.checkInitialized(); err != nil {
		return nil, err
	}
	p.mu.RLock()
	client := p.client
	p.mu.RUnlock()
	if client == nil {
		return nil, NewInitError("Apollo client is nil")
	}
	watcher := NewApolloConfigWatcher(client, target.FileName)
	if err := lynx.StartControlPlaneWatcher(ctx, watcher); err != nil {
		_ = watcher.Stop()
		return nil, err
	}
	return watcher, nil
}

// getMainConfigSource gets the main configuration source based on service_config
func (p *PlugApollo) getMainConfigSource() (config.Source, error) {
	if p.conf.ServiceConfig == nil {
		// Fallback to default behavior if service_config is not configured
		namespace := p.conf.Namespace
		if namespace == "" {
			namespace = conf.DefaultNamespace
		}

		log.Infof("Loading main configuration - Namespace: [%s]", namespace)

		return p.GetConfig(namespace, "")
	}

	serviceConfig := p.conf.ServiceConfig

	namespace := serviceConfig.Namespace
	if namespace == "" {
		namespace = p.conf.Namespace
	}

	log.Infof("Loading main configuration - Namespace: [%s]", namespace)

	return p.GetConfig(namespace, "")
}

// getAdditionalConfigSources gets additional configuration sources
func (p *PlugApollo) getAdditionalConfigSources() ([]config.Source, error) {
	if p.conf.ServiceConfig == nil || len(p.conf.ServiceConfig.AdditionalNamespaces) == 0 {
		return nil, nil
	}

	serviceConfig := p.conf.ServiceConfig

	var sources []config.Source
	for _, namespace := range serviceConfig.AdditionalNamespaces {
		// Use service_config namespace as default if not specified
		if namespace == "" {
			namespace = serviceConfig.Namespace
		}
		if namespace == "" {
			namespace = p.conf.Namespace
		}

		log.Infof("Loading additional configuration - Namespace: [%s] Priority: [%d] Strategy: [%s]",
			namespace, serviceConfig.Priority, serviceConfig.MergeStrategy)

		source, err := p.GetConfig(namespace, "")
		if err != nil {
			log.Errorf("Failed to load additional configuration - Namespace: [%s] Error: %v",
				namespace, err)
			return nil, fmt.Errorf("failed to load additional config %s: %w", namespace, err)
		}

		sources = append(sources, source)
	}

	// Sources are appended in the order they appear in AdditionalNamespaces.
	// Priority-based merging is delegated to the framework's config layer.
	return sources, nil
}

// GetConfigValue gets configuration value from Apollo
func (p *PlugApollo) GetConfigValue(namespace, key string) (string, error) {
	if err := p.checkInitialized(); err != nil {
		return "", err
	}

	// Snapshot the shared resilience components and metrics under the lock so
	// concurrent shutdown (which may nil them) cannot cause a data race or nil
	// dereference.
	p.mu.RLock()
	circuitBreaker := p.circuitBreaker
	retryManager := p.retryManager
	metrics := p.metrics
	p.mu.RUnlock()

	success := false
	if metrics != nil {
		metrics.RecordConfigOperation(namespace, "get", "start")
		defer func() {
			if success {
				metrics.RecordConfigOperation(namespace, "get", "success")
			}
		}()
	}

	log.Infof("Getting config value - Namespace: [%s], Key: [%s]", namespace, key)

	if circuitBreaker == nil || retryManager == nil {
		return "", NewInitError("Apollo resilience components are not initialized")
	}

	// Execute with circuit breaker and retry mechanism.
	//
	// The retry loop wraps the circuit breaker so that the breaker only gates a
	// single attempt; backoff sleeps happen between breaker calls rather than
	// while the breaker lock is held. This keeps config reads from serialising
	// behind one another.
	var value string
	var lastErr error

	err := retryManager.DoWithRetry(func() error {
		return circuitBreaker.Do(func() error {
			val, err := p.getConfigValueFromApollo(context.Background(), namespace, key)
			if err != nil {
				lastErr = err
				return err
			}
			value = val
			return nil
		})
	})

	if err != nil {
		log.Errorf("Failed to get config value %s:%s after retries: %v", namespace, key, err)
		if metrics != nil {
			metrics.RecordConfigOperation(namespace, "get", "error")
		}
		return "", WrapClientError(lastErr, ErrCodeConfigGetFailed, "failed to get config value")
	}

	success = true
	log.Infof("Successfully got config value - Namespace: [%s], Key: [%s]", namespace, key)
	return value, nil
}

// getConfigValueFromApollo gets configuration value from Apollo client.
// The supplied context is propagated to the underlying HTTP request so the
// call honours caller deadlines and shutdown cancellation.
func (p *PlugApollo) getConfigValueFromApollo(ctx context.Context, namespace, key string) (string, error) {
	// Snapshot the client under the lock so a concurrent shutdown (which nils
	// p.client) cannot cause a data race or nil dereference.
	p.mu.RLock()
	client := p.client
	metrics := p.metrics
	p.mu.RUnlock()
	if client == nil {
		return "", fmt.Errorf("Apollo client not initialized")
	}

	// Check cache first if enabled
	if p.conf.EnableCache {
		cacheKey := fmt.Sprintf("%s:%s", namespace, key)
		p.cacheMutex.RLock()
		if cached, exists := p.configCache[cacheKey]; exists {
			if value, ok := cached.(string); ok {
				p.cacheMutex.RUnlock()
				if metrics != nil {
					metrics.RecordCacheHit(p.GetNamespace())
				}
				return value, nil
			}
		}
		p.cacheMutex.RUnlock()
		if metrics != nil {
			metrics.RecordCacheMiss(p.GetNamespace())
		}
	}

	value, err := client.GetConfigValue(ctx, namespace, key)
	if err != nil {
		return "", err
	}

	if p.conf.EnableCache {
		cacheKey := fmt.Sprintf("%s:%s", namespace, key)
		p.cacheMutex.Lock()
		if p.configCache == nil {
			p.configCache = make(map[string]any)
		}
		p.configCache[cacheKey] = value
		p.cacheMutex.Unlock()
	}

	return value, nil
}

// ApolloConfigSource implements config.Source for Apollo
type ApolloConfigSource struct {
	client    *ApolloHTTPClient
	appId     string
	cluster   string
	namespace string
	// ctx is the base context used for Load/Watch operations.  The Kratos
	// config.Source interface methods do not accept a context, so callers that
	// want shutdown/deadline propagation set it via WithContext.  When unset it
	// defaults to context.Background().
	ctx context.Context
}

// NewApolloConfigSource creates a new Apollo config source
func NewApolloConfigSource(client *ApolloHTTPClient, appId, cluster, namespace string) *ApolloConfigSource {
	return &ApolloConfigSource{
		client:    client,
		appId:     appId,
		cluster:   cluster,
		namespace: namespace,
		ctx:       context.Background(),
	}
}

// WithContext returns the source configured to use ctx as the base context for
// Load and Watch operations, so the caller's deadline/cancellation propagates
// into the underlying HTTP requests.
func (s *ApolloConfigSource) WithContext(ctx context.Context) *ApolloConfigSource {
	if ctx != nil {
		s.ctx = ctx
	}
	return s
}

// context returns the source's base context, defaulting to background.
func (s *ApolloConfigSource) context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

// Load implements config.Source interface
func (s *ApolloConfigSource) Load() ([]*config.KeyValue, error) {
	if s.client == nil {
		return nil, fmt.Errorf("apollo HTTP client is nil")
	}
	configResp, err := s.client.GetConfig(s.context(), s.namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to load config from Apollo: %w", err)
	}

	var kvs []*config.KeyValue
	for key, value := range configResp.Configurations {
		kvs = append(kvs, &config.KeyValue{
			Key:   key,
			Value: []byte(value),
		})
	}

	return kvs, nil
}

// Watch implements config.Source.Watch.  Because the Kratos interface does not
// accept a context, the source's base context (background unless set via
// WithContext) is used to drive the watcher.  Callers are responsible for
// calling watcher.Stop() when the source is no longer needed.
func (s *ApolloConfigSource) Watch() (config.Watcher, error) {
	if s.client == nil {
		return nil, fmt.Errorf("apollo HTTP client is nil")
	}
	watcher := NewApolloConfigWatcher(s.client, s.namespace)
	if err := watcher.Start(s.context()); err != nil {
		return nil, err
	}
	return watcher, nil
}
