// Package apollo provides an Apollo configuration-centre plugin for the Lynx framework.
// It connects to an Apollo Config Service, reads namespaces on startup, and watches for
// configuration changes via long-polling, propagating updates to the Lynx runtime.
package apollo

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx-apollo/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
)

const (
	pluginName        = "apollo.config.center"
	pluginVersion     = "v1.6.3"
	pluginDescription = "apollo configuration center plugin for lynx framework"
	// confPrefix is the config key prefix under which this plugin's settings are read.
	confPrefix = "lynx.apollo"
)

// PlugApollo is the Apollo configuration center plugin instance.
type PlugApollo struct {
	*plugins.BasePlugin
	conf *conf.Apollo
	rt   plugins.Runtime

	client *ApolloHTTPClient

	metrics        *Metrics
	retryManager   *RetryManager
	circuitBreaker *CircuitBreaker

	// initialized/destroyed are int32 so they can be read without holding mu
	// (via atomic) from health checks and config reads running concurrently
	// with startup/shutdown.
	mu                   sync.RWMutex
	initialized          int32
	destroyed            int32
	healthCheckCh        chan struct{}
	healthCheckCloseOnce sync.Once // guards close(healthCheckCh) against double-close

	// configWatchers maps namespace -> watcher; guarded by watcherMutex.
	configWatchers map[string]*ConfigWatcher
	watcherMutex   sync.RWMutex

	// configCache caches resolved values keyed by "namespace:key"; guarded by cacheMutex.
	configCache map[string]any
	cacheMutex  sync.RWMutex
}

// NewApolloConfigCenter returns a PlugApollo with its base metadata set.
// Weight is math.MaxInt so the config center loads before plugins that depend
// on it for configuration.
func NewApolloConfigCenter() *PlugApollo {
	return &PlugApollo{
		BasePlugin: plugins.NewBasePlugin(
			plugins.GeneratePluginID("", pluginName, pluginVersion),
			pluginName,
			pluginDescription,
			pluginVersion,
			confPrefix,
			math.MaxInt,
		),
		healthCheckCh:  make(chan struct{}),
		configWatchers: make(map[string]*ConfigWatcher),
		configCache:    make(map[string]any),
	}
}

// InitializeResources loads, defaults, and validates Apollo configuration and
// builds the resilience components (metrics, retry, circuit breaker).
func (p *PlugApollo) InitializeResources(rt plugins.Runtime) error {
	if err := p.BasePlugin.InitializeResources(rt); err != nil {
		return err
	}
	p.rt = rt
	atomic.StoreInt32(&p.destroyed, 0)

	p.conf = &conf.Apollo{}
	err := rt.GetConfig().Value(confPrefix).Scan(p.conf)
	if err != nil {
		return WrapInitError(err, "failed to scan apollo configuration")
	}

	p.setDefaultConfig()

	if err := p.validateConfig(); err != nil {
		return WrapInitError(err, "configuration validation failed")
	}

	if err := p.initComponents(); err != nil {
		return WrapInitError(err, "failed to initialize components")
	}

	return nil
}

// setDefaultConfig fills unset fields with the package defaults (cluster
// "default", namespace "application", 10s request timeout, 30s notification
// long-poll timeout, etc.) so validation runs against complete config.
func (p *PlugApollo) setDefaultConfig() {
	if p.conf == nil {
		return
	}
	if p.conf.Cluster == "" {
		p.conf.Cluster = conf.DefaultCluster
	}
	if p.conf.Namespace == "" {
		p.conf.Namespace = conf.DefaultNamespace
	}
	if p.conf.Timeout == nil {
		p.conf.Timeout = conf.GetDefaultTimeout()
	}
	if p.conf.NotificationTimeout == nil {
		p.conf.NotificationTimeout = conf.GetDefaultNotificationTimeout()
	}
	if p.conf.CacheDir == "" {
		p.conf.CacheDir = conf.DefaultCacheDir
	}
	if p.conf.CircuitBreakerThreshold == 0 {
		p.conf.CircuitBreakerThreshold = conf.DefaultCircuitBreakerThreshold
	}
}

func (p *PlugApollo) validateConfig() error {
	if p.conf == nil {
		return NewConfigError("configuration is required")
	}

	validator := NewValidator(p.conf)
	result := validator.Validate()
	if !result.IsValid {
		return NewConfigError(result.Errors[0].Error())
	}

	return nil
}

// initComponents builds the metrics, retry manager, and circuit breaker,
// honouring configured overrides and falling back to package defaults.
func (p *PlugApollo) initComponents() error {
	p.metrics = NewApolloMetrics()

	maxRetries := int(conf.DefaultMaxRetryTimes)
	if p.conf != nil && p.conf.MaxRetryTimes > 0 {
		maxRetries = int(p.conf.MaxRetryTimes)
	}
	retryInterval := conf.DefaultRetryInterval
	if p.conf != nil && p.conf.RetryInterval != nil && p.conf.RetryInterval.AsDuration() > 0 {
		retryInterval = p.conf.RetryInterval.AsDuration()
	}
	p.retryManager = NewRetryManager(maxRetries, retryInterval)

	threshold := conf.DefaultCircuitBreakerThreshold
	if p.conf != nil && p.conf.CircuitBreakerThreshold > 0 {
		threshold = float64(p.conf.CircuitBreakerThreshold)
	}
	p.circuitBreaker = NewCircuitBreaker(threshold)

	return nil
}

// checkInitialized returns an error unless the plugin is initialized and not
// yet destroyed. Safe to call concurrently (reads atomic state).
func (p *PlugApollo) checkInitialized() error {
	if atomic.LoadInt32(&p.initialized) == 0 {
		return NewInitError("Apollo plugin not initialized")
	}
	if atomic.LoadInt32(&p.destroyed) == 1 {
		return NewInitError("Apollo plugin has been destroyed")
	}
	return nil
}

func (p *PlugApollo) setInitialized() {
	atomic.StoreInt32(&p.initialized, 1)
}

func (p *PlugApollo) clearInitialized() {
	atomic.StoreInt32(&p.initialized, 0)
}

func (p *PlugApollo) setDestroyed() {
	atomic.StoreInt32(&p.destroyed, 1)
}

func (p *PlugApollo) publishRuntimeResources() error {
	if p.rt == nil {
		return nil
	}
	if err := lynx.RegisterControlPlaneCapabilityResources(p.rt, pluginName, p); err != nil {
		return err
	}
	if err := p.rt.RegisterPrivateResource("http_client", p.client); err != nil && p.client != nil {
		log.Warnf("failed to register Apollo private http client resource: %v", err)
	}
	if err := p.rt.RegisterPrivateResource("client", p.client); err != nil && p.client != nil {
		log.Warnf("failed to register Apollo private client resource: %v", err)
	}
	if p.metrics != nil {
		if err := p.rt.RegisterPrivateResource("metrics", p.metrics); err != nil {
			log.Warnf("failed to register Apollo private metrics resource: %v", err)
		}
	}
	if p.retryManager != nil {
		if err := p.rt.RegisterPrivateResource("retry_manager", p.retryManager); err != nil {
			log.Warnf("failed to register Apollo private retry manager resource: %v", err)
		}
	}
	if p.circuitBreaker != nil {
		if err := p.rt.RegisterPrivateResource("circuit_breaker", p.circuitBreaker); err != nil {
			log.Warnf("failed to register Apollo private circuit breaker resource: %v", err)
		}
	}
	if p.configWatchers != nil {
		if err := p.rt.RegisterPrivateResource("config_watchers", p.configWatchers); err != nil {
			log.Warnf("failed to register Apollo private config watchers resource: %v", err)
		}
	}
	if p.configCache != nil {
		if err := p.rt.RegisterPrivateResource("config_cache", p.configCache); err != nil {
			log.Warnf("failed to register Apollo private config cache resource: %v", err)
		}
	}
	return nil
}

// StartupTasks builds the Apollo client, registers this plugin as the Lynx
// control plane, publishes runtime resources, and loads dependent plugins from
// the control-plane config. On any failure it rolls back the client and the
// initialized flag so the plugin is left in a clean uninitialized state.
func (p *PlugApollo) StartupTasks() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt32(&p.initialized) == 1 {
		return NewInitError("Apollo plugin already initialized")
	}
	if p.conf == nil {
		return NewConfigError("configuration is required")
	}

	started := false
	if p.metrics != nil {
		p.metrics.RecordClientOperation("startup", "start")
		defer func() {
			if started && p.metrics != nil {
				p.metrics.RecordClientOperation("startup", "success")
			}
		}()
	}

	log.Infof("Initializing apollo plugin with app_id: %s, cluster: %s, namespace: %s", p.conf.AppId, p.conf.Cluster, p.conf.Namespace)

	client, err := p.initApolloClient()
	if err != nil {
		log.Errorf("Failed to initialize Apollo client: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to initialize Apollo client")
	}

	p.client = client
	p.setInitialized()

	// Roll back the client and initialized flag if startup does not reach the end.
	defer func() {
		if started {
			return
		}
		p.clearInitialized()
		if p.client != nil {
			p.client.Close()
			p.client = nil
		}
	}()

	err = lynx.Lynx().SetControlPlane(p)
	if err != nil {
		log.Errorf("Failed to set control plane: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to set control plane")
	}

	cfg, err := lynx.Lynx().InitControlPlaneConfig()
	if err != nil {
		log.Errorf("Failed to init control plane config: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to init control plane config")
	}

	if err := p.publishRuntimeResources(); err != nil {
		log.Errorf("Failed to publish Apollo runtime resources: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to publish runtime resources")
	}

	if err := lynx.Lynx().GetPluginManager().LoadPlugins(cfg); err != nil {
		log.Errorf("Failed to load dependent plugins from Apollo config: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to load dependent plugins")
	}

	started = true
	log.Infof("Apollo plugin initialized successfully")
	return nil
}

// initApolloClient builds the Apollo HTTP client. MetaServer and AppId are
// required; cluster, namespace, and timeout fall back to defaults.
func (p *PlugApollo) initApolloClient() (*ApolloHTTPClient, error) {
	if p.conf.MetaServer == "" {
		return nil, fmt.Errorf("meta server address is required")
	}
	if p.conf.AppId == "" {
		return nil, fmt.Errorf("app ID is required")
	}

	cluster := p.conf.Cluster
	if cluster == "" {
		cluster = conf.DefaultCluster
	}

	namespace := p.conf.Namespace
	if namespace == "" {
		namespace = conf.DefaultNamespace
	}

	timeout := 10 * time.Second
	if p.conf.Timeout != nil {
		timeout = p.conf.Timeout.AsDuration()
	}

	client := NewApolloHTTPClient(
		p.conf.MetaServer,
		p.conf.AppId,
		cluster,
		namespace,
		p.conf.Token,
		timeout,
	)

	log.Infof("Apollo HTTP client initialized - MetaServer: %s, AppId: %s, Cluster: %s, Namespace: %s",
		p.conf.MetaServer, p.conf.AppId, cluster, namespace)

	return client, nil
}

// GetMetrics gets monitoring metrics.
// Snapshot under the lock so a concurrent shutdown (which nils p.metrics)
// cannot cause a data race.
func (p *PlugApollo) GetMetrics() *Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metrics
}

// IsInitialized reports whether startup completed. Safe for concurrent use.
func (p *PlugApollo) IsInitialized() bool {
	return atomic.LoadInt32(&p.initialized) == 1
}

// IsDestroyed reports whether cleanup has run. Safe for concurrent use.
func (p *PlugApollo) IsDestroyed() bool {
	return atomic.LoadInt32(&p.destroyed) == 1
}

// GetApolloConfig returns the resolved plugin configuration.
func (p *PlugApollo) GetApolloConfig() *conf.Apollo {
	return p.conf
}

// GetNamespace returns the configured namespace, or the package default if unset.
func (p *PlugApollo) GetNamespace() string {
	if p.conf != nil {
		return p.conf.Namespace
	}
	return conf.DefaultNamespace
}
