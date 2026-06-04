package apollo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
)

// ConfigWatcher watches Apollo configuration changes.
//
// It is a thin lifecycle wrapper around a real ApolloConfigWatcher: Start()
// kicks off a tracked long-poll watcher (when a client is available) plus a
// dispatch goroutine that fans notifications out to the onChange callback.
// Stop() tears both down.  This replaces the previous dead no-op Start().
type ConfigWatcher struct {
	namespace string
	onChange  func(namespace string, key string, value string)
	onError   func(err error)
	stopCh    chan struct{}
	mu        sync.RWMutex
	metrics   *Metrics

	// client and ctx back the real ApolloConfigWatcher.  client may be nil
	// (e.g. in tests / before the plugin client is initialised), in which case
	// no long-poll watcher is started but Start()/Stop() remain valid no-ops.
	client *ApolloHTTPClient
	ctx    context.Context

	apolloWatcher *ApolloConfigWatcher
	wg            sync.WaitGroup
	startOnce     sync.Once
	stopOnce      sync.Once
}

// NewConfigWatcher creates a new configuration watcher
func NewConfigWatcher(namespace string) *ConfigWatcher {
	return &ConfigWatcher{
		namespace: namespace,
		stopCh:    make(chan struct{}),
		ctx:       context.Background(),
	}
}

// SetOnConfigChanged sets the callback for configuration changes
func (w *ConfigWatcher) SetOnConfigChanged(callback func(namespace string, key string, value string)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onChange = callback
}

// SetOnError sets the error callback
func (w *ConfigWatcher) SetOnError(callback func(err error)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onError = callback
}

// Start starts watching configuration changes.
//
// When backed by a client it spins up a real, tracked ApolloConfigWatcher and
// a dispatch goroutine that forwards each reloaded key/value to the onChange
// callback.  Both are torn down by Stop().  When no client is configured this
// is effectively a no-op (but still safe to call and Stop()).
func (w *ConfigWatcher) Start() {
	w.startOnce.Do(func() {
		w.mu.RLock()
		client := w.client
		ctx := w.ctx
		w.mu.RUnlock()

		if client == nil {
			log.Infof("Config watcher started for namespace: %s (no client; dispatch disabled)", w.namespace)
			return
		}
		if ctx == nil {
			ctx = context.Background()
		}

		aw := NewApolloConfigWatcher(client, w.namespace)
		w.mu.Lock()
		w.apolloWatcher = aw
		w.mu.Unlock()

		if err := aw.Start(ctx); err != nil {
			w.mu.RLock()
			onError := w.onError
			w.mu.RUnlock()
			if onError != nil {
				onError(err)
			}
			log.Errorf("Failed to start Apollo config watcher for namespace %s: %v", w.namespace, err)
			return
		}

		w.wg.Add(1)
		go w.dispatchLoop(aw)
		log.Infof("Config watcher started for namespace: %s (backed by ApolloConfigWatcher)", w.namespace)
	})
}

// dispatchLoop pulls config changes from the underlying ApolloConfigWatcher and
// fans them out to the onChange/onError callbacks until the watcher is stopped.
func (w *ConfigWatcher) dispatchLoop(aw *ApolloConfigWatcher) {
	defer w.wg.Done()
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		kvs, err := aw.Next()
		if err != nil {
			// Next returns an error once the watcher is stopped.
			select {
			case <-w.stopCh:
				return
			default:
			}
			w.mu.RLock()
			onError := w.onError
			w.mu.RUnlock()
			if onError != nil {
				onError(err)
			}
			return
		}

		w.mu.RLock()
		onChange := w.onChange
		w.mu.RUnlock()
		if onChange != nil {
			for _, kv := range kvs {
				onChange(w.namespace, kv.Key, string(kv.Value))
			}
		}
	}
}

// Stop stops watching configuration changes
func (w *ConfigWatcher) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)

		w.mu.RLock()
		aw := w.apolloWatcher
		w.mu.RUnlock()
		if aw != nil {
			_ = aw.Stop()
		}
		w.wg.Wait()
		log.Infof("Stopped config watcher for namespace: %s", w.namespace)
	})
}

// WatchConfig watches configuration changes
func (p *PlugApollo) WatchConfig(namespace string) (*ConfigWatcher, error) {
	if !p.IsInitialized() {
		return nil, NewInitError("Apollo plugin not initialized")
	}

	// Snapshot the shared metrics under the lock so a concurrent shutdown
	// (which may nil it) cannot cause a data race or nil dereference.
	p.mu.RLock()
	metrics := p.metrics
	p.mu.RUnlock()

	// Record configuration watch operation metrics
	if metrics != nil {
		metrics.RecordConfigOperation(namespace, "watch", "start")
		defer func() {
			metrics.RecordConfigOperation(namespace, "watch", "success")
		}()
	}

	log.Infof("Watching config - Namespace: %s", namespace)

	// Check if the configuration is already being watched
	p.watcherMutex.RLock()
	if existingWatcher, exists := p.configWatchers[namespace]; exists {
		p.watcherMutex.RUnlock()
		log.Infof("Config %s is already being watched", namespace)
		return existingWatcher, nil
	}
	p.watcherMutex.RUnlock()

	// Create configuration watcher backed by a real ApolloConfigWatcher.
	// Snapshot the shared client under the lock so a concurrent shutdown
	// cannot race it.
	p.mu.RLock()
	client := p.client
	p.mu.RUnlock()

	watcher := NewConfigWatcher(namespace)
	watcher.metrics = metrics // Pass metrics reference
	watcher.client = client   // Back the watcher with the real Apollo client

	// Set event handling callbacks
	watcher.SetOnConfigChanged(func(ns string, key string, value string) {
		p.handleConfigChanged(ns, key, value)
	})

	watcher.SetOnError(func(err error) {
		p.handleConfigWatchError(namespace, err)
	})

	// Register watcher
	p.watcherMutex.Lock()
	if existingWatcher, exists := p.configWatchers[namespace]; exists {
		p.watcherMutex.Unlock()
		watcher.Stop()
		log.Infof("Config %s is already being watched", namespace)
		return existingWatcher, nil
	}
	p.configWatchers[namespace] = watcher
	p.watcherMutex.Unlock()

	// Start watching. ConfigWatcher.Start() is a fire-and-forget; lifecycle
	// cleanup is handled by CleanupTasks closing the watcher's stop channel.
	watcher.Start()

	return watcher, nil
}

// handleConfigChanged handles configuration change events
func (p *PlugApollo) handleConfigChanged(namespace, key, value string) {
	log.Infof("Config changed - Namespace: [%s], Key: [%s]", namespace, key)

	// Record configuration change metrics. Snapshot the metrics pointer under
	// the lock so a concurrent shutdown cannot race or nil it out.
	p.mu.RLock()
	metrics := p.metrics
	p.mu.RUnlock()
	if metrics != nil {
		metrics.RecordConfigChange(namespace)
	}

	// Clear cache for this namespace
	p.cacheMutex.Lock()
	delete(p.configCache, fmt.Sprintf("%s:%s", namespace, key))
	p.cacheMutex.Unlock()
}

// handleConfigWatchError handles configuration watch errors
func (p *PlugApollo) handleConfigWatchError(namespace string, err error) {
	log.Errorf("Config watch error - Namespace: [%s], Error: %v", namespace, err)

	// Record error metrics. Snapshot the metrics pointer under the lock so a
	// concurrent shutdown cannot race or nil it out.
	p.mu.RLock()
	metrics := p.metrics
	p.mu.RUnlock()
	if metrics != nil {
		metrics.RecordConfigOperation(namespace, "watch", "error")
	}

	// Retry watching if needed
	if p.conf.EnableRetry {
		go p.retryConfigWatch(namespace)
	}
}

// retryConfigWatch retries configuration watching after an error.
// It runs in a separate goroutine; panic recovery prevents silent failure.
func (p *PlugApollo) retryConfigWatch(namespace string) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Unexpected panic in retryConfigWatch for namespace %s: %v", namespace, r)
		}
	}()
	log.Infof("Retrying config watch for %s", namespace)

	// Wait for a period before retrying, but allow cancellation on plugin stop
	if p.healthCheckCh != nil {
		select {
		case <-p.healthCheckCh:
			log.Infof("Config watch retry canceled due to plugin shutdown: %s", namespace)
			return
		case <-time.After(5 * time.Second):
		}
	} else {
		// Fallback when channel is not available
		if p.IsDestroyed() {
			return
		}
		time.Sleep(5 * time.Second)
	}

	if p.IsDestroyed() {
		return
	}

	// Recreate watcher
	if _, err := p.WatchConfig(namespace); err == nil {
		log.Infof("Successfully recreated config watcher for %s", namespace)
	} else {
		log.Errorf("Failed to recreate config watcher for %s: %v", namespace, err)
	}
}
