package apollo

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx/log"
)

// IsContextAware asserts that the plugin's lifecycle genuinely observes context
// cancellation: the core BasePlugin drives StartContext/StopContext and routes
// into the context-aware step hooks below.
func (p *PlugApollo) IsContextAware() bool {
	return true
}

// StartupTasksContext is the context-aware startup hook. The core BasePlugin
// drives the lifecycle state machine (status transitions, events, health check)
// and passes the caller's context straight through so cancellation is real.
func (p *PlugApollo) StartupTasksContext(ctx context.Context) error {
	return p.startupTasksContext(ctx)
}

// CleanupTasksContext is the context-aware cleanup hook driven by the core BasePlugin.
func (p *PlugApollo) CleanupTasksContext(ctx context.Context) error {
	return p.cleanupTasksContext(ctx)
}

// startupTasksContext builds the Apollo client, registers this plugin as the
// Lynx control plane, publishes runtime resources, and loads dependent plugins
// from the control-plane config. ctx is checked at every phase boundary; on any
// failure (including cancellation) the client and the initialized flag are
// rolled back so the plugin is left in a clean uninitialized state.
func (p *PlugApollo) startupTasksContext(ctx context.Context) (startErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("apollo startup canceled before execution: %w", err)
	}
	if atomic.LoadInt32(&p.initialized) == 1 {
		return NewInitError("Apollo plugin already initialized")
	}
	if p.conf == nil {
		return NewConfigError("configuration is required")
	}

	if p.metrics != nil {
		p.metrics.RecordClientOperation("startup", "start")
		defer func() {
			if p.metrics == nil {
				return
			}
			if startErr != nil {
				p.metrics.RecordClientOperation("startup", "error")
				return
			}
			p.metrics.RecordClientOperation("startup", "success")
		}()
	}

	log.Infof("Initializing apollo plugin with app_id: %s, cluster: %s, namespace: %s", p.conf.AppId, p.conf.Cluster, p.conf.Namespace)

	client, err := p.initApolloClient()
	if err != nil {
		log.Errorf("Failed to initialize Apollo client: %v", err)
		return WrapInitError(err, "failed to initialize Apollo client")
	}

	p.client = client
	p.setInitialized()

	// Roll back the client and initialized flag if startup does not reach the end.
	defer func() {
		if startErr == nil {
			return
		}
		p.clearInitialized()
		if p.client != nil {
			p.client.Close()
			p.client = nil
		}
	}()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("apollo startup canceled before setting control plane: %w", err)
	}
	if err := lynx.Lynx().SetControlPlane(p); err != nil {
		log.Errorf("Failed to set control plane: %v", err)
		return WrapInitError(err, "failed to set control plane")
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("apollo startup canceled before initializing control plane config: %w", err)
	}
	cfg, err := lynx.Lynx().InitControlPlaneConfig()
	if err != nil {
		log.Errorf("Failed to init control plane config: %v", err)
		return WrapInitError(err, "failed to init control plane config")
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("apollo startup canceled before publishing runtime resources: %w", err)
	}
	if err := p.publishRuntimeResources(); err != nil {
		log.Errorf("Failed to publish Apollo runtime resources: %v", err)
		return WrapInitError(err, "failed to publish runtime resources")
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("apollo startup canceled before loading dependent plugins: %w", err)
	}
	if err := lynx.Lynx().GetPluginManager().LoadPlugins(cfg); err != nil {
		log.Errorf("Failed to load dependent plugins from Apollo config: %v", err)
		return WrapInitError(err, "failed to load dependent plugins")
	}

	log.Infof("Apollo plugin initialized successfully")
	return nil
}

// cleanupTasksContext tears the plugin down in dependency order: health check,
// watchers, client connection, in-memory resources, then background tasks. The
// CompareAndSwap on destroyed makes it idempotent under concurrent shutdown.
// Every step is local and non-blocking, so teardown always runs to completion;
// a cancelled ctx is reported in the returned error so the caller knows the
// stop deadline was not honoured.
func (p *PlugApollo) cleanupTasksContext(ctx context.Context) error {
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

	var errs []error
	if err := ctx.Err(); err != nil {
		errs = append(errs, fmt.Errorf("apollo cleanup canceled: %w", err))
	}
	if joined := errors.Join(errs...); joined != nil {
		return joined
	}

	log.Infof("Apollo plugin destroyed successfully")
	return nil
}
