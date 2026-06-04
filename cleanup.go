package apollo

import (
	"sync/atomic"
	"time"

	"github.com/go-lynx/lynx/log"
)

// stopHealthCheck closes the health-check channel. sync.Once guards against a
// double-close panic if cleanup runs more than once.
func (p *PlugApollo) stopHealthCheck() {
	p.mu.Lock()
	ch := p.healthCheckCh
	p.mu.Unlock()

	if ch != nil {
		p.healthCheckCloseOnce.Do(func() {
			close(ch)
			p.mu.Lock()
			p.healthCheckCh = nil
			p.mu.Unlock()
			log.Infof("Stopping health check")
		})
	}
}

// cleanupWatchers stops every registered config watcher and resets the map.
func (p *PlugApollo) cleanupWatchers() {
	log.Infof("Cleaning up watchers")

	p.watcherMutex.Lock()
	configWatcherCount := len(p.configWatchers)
	for configKey, watcher := range p.configWatchers {
		log.Infof("Stopping config watcher for: %s", configKey)
		if watcher != nil {
			watcher.Stop()
		}
	}
	p.configWatchers = make(map[string]*ConfigWatcher)
	p.watcherMutex.Unlock()

	log.Infof("Cleaned up %d config watchers", configWatcherCount)
}

// closeClientConnection closes client connection.
// p.client is read concurrently by CheckHealth/GetConfig/getConfigValueFromApollo,
// so the swap-to-nil must be performed under p.mu to avoid a data race and a
// nil dereference during shutdown.
func (p *PlugApollo) closeClientConnection() {
	p.mu.Lock()
	client := p.client
	p.client = nil
	p.mu.Unlock()

	if client != nil {
		log.Infof("Closing Apollo HTTP client connection")

		clientInfo := map[string]any{
			"client_type": "ApolloHTTPClient",
		}
		if p.conf != nil {
			clientInfo["app_id"] = p.conf.AppId
			clientInfo["cluster"] = p.conf.Cluster
			clientInfo["namespace"] = p.conf.Namespace
		}

		client.Close()

		log.Infof("Apollo HTTP client connection closed: %+v", clientInfo)
	}
}

// releaseMemoryResources nils out the resilience components and clears the
// config cache during shutdown.
func (p *PlugApollo) releaseMemoryResources() {
	log.Infof("Releasing memory resources")

	if p.conf != nil {
		log.Infof("Clearing configuration")
		// Kept non-nil; clear sensitive fields here if that becomes necessary.
	}

	// These fields are read concurrently by
	// CheckHealth/GetConfigValue, so swap them to nil under p.mu to avoid a
	// data race and nil dereference during shutdown.  ForceClose() is called on
	// the local reference outside the lock.
	p.mu.Lock()
	metrics := p.metrics
	retryManager := p.retryManager
	circuitBreaker := p.circuitBreaker
	p.metrics = nil
	p.retryManager = nil
	p.circuitBreaker = nil
	p.mu.Unlock()

	if metrics != nil {
		log.Infof("Clearing metrics")
	}
	if retryManager != nil {
		log.Infof("Clearing retry manager")
	}
	if circuitBreaker != nil {
		log.Infof("Clearing circuit breaker")
		circuitBreaker.ForceClose()
	}

	p.clearConfigCache()

	log.Infof("Memory resources released")
}

// clearConfigCache replaces the config cache with an empty map. Concurrency-safe via cacheMutex.
func (p *PlugApollo) clearConfigCache() {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()
	p.configCache = make(map[string]any)
	log.Infof("Configuration cache cleared")
}

// stopBackgroundTasks stops background tasks.
// retryManager/metrics are read concurrently, so swap them to nil under p.mu.
// The circuit breaker reference is intentionally kept (just ForceClose()'d).
func (p *PlugApollo) stopBackgroundTasks() {
	log.Infof("Stopping background tasks")

	p.mu.Lock()
	retryManager := p.retryManager
	circuitBreaker := p.circuitBreaker
	metrics := p.metrics
	p.retryManager = nil
	p.metrics = nil
	p.mu.Unlock()

	if retryManager != nil {
		log.Infof("Stopping retry manager background tasks")
	}

	if circuitBreaker != nil {
		log.Infof("Stopping circuit breaker background tasks")
		circuitBreaker.ForceClose()
	}

	if metrics != nil {
		log.Infof("Stopping metrics collection tasks")
	}

	log.Infof("Stopping health check tasks")
	log.Infof("Stopping monitoring tasks")
	log.Infof("Stopping audit log tasks")

	log.Infof("Background tasks stopped")
}

// getCleanupStats snapshots which resources have been released, for diagnostics.
func (p *PlugApollo) getCleanupStats() map[string]any {
	p.mu.RLock()
	clientNil := p.client == nil
	metricsNil := p.metrics == nil
	retryNil := p.retryManager == nil
	breakerNil := p.circuitBreaker == nil
	p.mu.RUnlock()

	stats := map[string]any{
		"cleanup_time": time.Now().Unix(),
		"plugin_state": map[string]any{
			"initialized": p.IsInitialized(),
			"destroyed":   p.IsDestroyed(),
		},
		"resources": map[string]any{
			"client_closed":   clientNil,
			"metrics_cleared": metricsNil,
			"retry_cleared":   retryNil,
			"breaker_cleared": breakerNil,
		},
	}

	return stats
}

// CleanupTasks tears the plugin down in dependency order: health check,
// watchers, client connection, in-memory resources, then background tasks. The
// CompareAndSwap on destroyed makes it idempotent under concurrent shutdown.
func (p *PlugApollo) CleanupTasks() error {
	if !p.IsInitialized() {
		return nil
	}

	if !atomic.CompareAndSwapInt32(&p.destroyed, 0, 1) {
		return nil
	}

	if p.metrics != nil {
		p.metrics.RecordClientOperation("cleanup", "start")
		defer func() {
			if p.metrics != nil {
				p.metrics.RecordClientOperation("cleanup", "success")
			}
		}()
	}

	log.Infof("Destroying Apollo plugin")

	// 1. Stop health check
	p.stopHealthCheck()

	// 2. Clean up watchers
	p.cleanupWatchers()

	// 3. Close client connection
	p.closeClientConnection()

	// 4. Release memory resources
	p.releaseMemoryResources()

	// 5. Stop background tasks
	p.stopBackgroundTasks()

	p.clearInitialized()
	log.Infof("Apollo plugin destroyed successfully")
	return nil
}
