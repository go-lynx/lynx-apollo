package apollo

import (
	"fmt"
	"strings"

	"github.com/go-lynx/lynx-apollo/conf"
	"github.com/go-lynx/lynx/log"
)

// CheckHealth performs a health check.
func (p *PlugApollo) CheckHealth() error {
	if err := p.checkInitialized(); err != nil {
		return err
	}

	// Check Apollo client under the lock so a concurrent shutdown (which nils
	// p.client) cannot cause a data race or nil dereference.
	p.mu.RLock()
	client := p.client
	p.mu.RUnlock()
	if client == nil {
		return NewInitError("Apollo client is nil")
	}

	// Perform actual health check of the Apollo configuration center
	return p.checkApolloHealth()
}

// checkApolloHealth checks the health of the Apollo configuration center.
func (p *PlugApollo) checkApolloHealth() error {
	// Snapshot shared components under the lock so a concurrent shutdown cannot
	// race or nil them out from under us.
	p.mu.RLock()
	metrics := p.metrics
	retryManager := p.retryManager
	circuitBreaker := p.circuitBreaker
	p.mu.RUnlock()

	// Record the start of the health check
	success := false
	if metrics != nil {
		metrics.RecordHealthCheck("start")
		defer func() {
			if success && metrics != nil {
				metrics.RecordHealthCheck("success")
			}
		}()
	}

	log.Infof("Checking Apollo configuration center health")
	if circuitBreaker == nil || retryManager == nil {
		return NewHealthCheckError("Apollo resilience components are not initialized")
	}

	// checkClientConnection internally calls GetConfigValue which goes through the
	// circuit breaker — wrapping it in another circuitBreaker.Do would cause a
	// deadlock (the channel-based mutex is not re-entrant).  Run the checks
	// directly via the retry manager only.
	var healthErr error
	err := retryManager.DoWithRetry(func() error {
		// 1) Check client connection status
		if err := p.checkClientConnection(); err != nil {
			healthErr = err
			return err
		}

		// 2) Check configuration management functionality
		if err := p.checkConfigManagementHealth(); err != nil {
			healthErr = err
			return err
		}

		return nil
	})

	if err != nil {
		log.Errorf("Apollo configuration center health check failed: %v", healthErr)
		if metrics != nil {
			metrics.RecordHealthCheck("error")
		}
		return WrapClientError(healthErr, ErrCodeHealthCheckFailed, "Apollo configuration center health check failed")
	}

	success = true
	log.Infof("Apollo configuration center health check passed")
	return nil
}

// checkClientConnection verifies client connection status.
func (p *PlugApollo) checkClientConnection() error {
	// Try to get a configuration value to validate connectivity
	testNamespace := p.conf.Namespace
	if testNamespace == "" {
		testNamespace = conf.DefaultNamespace
	}

	// Try to get a test configuration key
	// If the key doesn't exist, that's fine - we just need to verify we can connect
	_, err := p.GetConfigValue(testNamespace, "health.check.key")
	if err != nil {
		// If the error indicates the key is not found, connectivity is fine
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "not exist") {
			log.Debugf("Client connection test passed (key not found is expected)")
			return nil
		}
		// Other errors indicate connection problems
		return fmt.Errorf("client connection test failed: %v", err)
	}

	// Key exists and was retrieved successfully
	return nil
}

// checkConfigManagementHealth checks configuration management functionality.
func (p *PlugApollo) checkConfigManagementHealth() error {
	// Check status of components related to configuration management
	if p.configWatchers == nil {
		return fmt.Errorf("config watchers not initialized")
	}

	// Check whether there are active config watchers
	p.watcherMutex.RLock()
	configWatcherCount := len(p.configWatchers)
	p.watcherMutex.RUnlock()
	log.Debugf("Config management health: %d active config watchers", configWatcherCount)

	return nil
}
