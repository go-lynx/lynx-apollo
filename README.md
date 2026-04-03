# Apollo Plugin for Lynx Framework

This plugin provides Apollo configuration center integration for the Lynx framework, offering features such as configuration management, dynamic configuration updates, and configuration change notifications.

## Features

- **Configuration Management**: Dynamic configuration loading from Apollo
- **Configuration Watching**: Real-time configuration change monitoring
- **Multi-Namespace Support**: Load configurations from multiple namespaces
- **Local Cache**: Optional local caching to reduce network requests
- **Health Checking**: Service health monitoring
- **Metrics**: Prometheus metrics integration
- **Retry Management**: Configurable retry policies
- **Circuit Breaker**: Fault tolerance with circuit breaker pattern

## Installation

```bash
go get github.com/go-lynx/lynx-apollo
```

## Configuration

The plugin can be configured through the Lynx configuration system. Here's an example configuration:

```yaml
lynx:
  apollo:
    # Basic Configuration
    app_id: "your-app-id"                      # Application ID (required)
    cluster: "default"                         # Cluster name (default: "default")
    namespace: "application"                   # Namespace (default: "application")
    meta_server: "http://localhost:8080"       # Apollo Meta Server address (required)
    token: "your-apollo-token"                 # Authentication token (optional)
    timeout: "10s"                             # Operation timeout
    
    # Notification Configuration
    enable_notification: true                  # Enable configuration change notification
    notification_timeout: "30s"                 # Notification timeout
    
    # Cache Configuration
    enable_cache: true                          # Enable local cache
    cache_dir: "/tmp/apollo-cache"             # Cache directory
    
    # Advanced Feature Configuration
    enable_metrics: true                        # Enable monitoring metrics
    enable_retry: true                          # Enable retry mechanism
    max_retry_times: 3                          # Maximum retry times
    retry_interval: "1s"                       # Retry interval
    enable_circuit_breaker: true               # Enable circuit breaker
    circuit_breaker_threshold: 0.5              # Circuit breaker threshold
    enable_graceful_shutdown: true             # Enable graceful shutdown
    shutdown_timeout: "30s"                    # Graceful shutdown timeout
    enable_logging: true                        # Enable detailed logging
    log_level: "info"                          # Log level (debug, info, warn, error)
    
    # Service Configuration for remote configuration loading
    service_config:
      namespace: "application"                   # Main namespace
      additional_namespaces:                    # Additional namespaces to load
        - "shared-config"
        - "feature-flags"
      priority: 0                               # Merge priority
      merge_strategy: "override"                # Merge strategy (override, merge, append)
```

Complete example: [conf/example_config.yml](./conf/example_config.yml).
If you rely on retry, circuit breaker, graceful shutdown, or logging-related behavior, set those fields explicitly in config instead of assuming omitted values will always be defaulted at runtime.

### Configuration Options

#### Basic Options
- `app_id` (string, required): Apollo application ID. Example: `"my-app"`
- `cluster` (string, default: `"default"`): Cluster name. Example: `"default"`
- `namespace` (string, default: `"application"`): Namespace name. Example: `"application"`
- `meta_server` (string, required): Apollo Meta Server address. Example: `"http://localhost:8080"`
- `token` (string, optional): Authentication token. Example: `"your-token"`
- `timeout` (duration, default: `"10s"`): Operation timeout. Example: `"15s"`
- `release_key` (string, optional): Release key for the configuration. Example: `"20230510-120000-abcdef"`
- `ip` (string, optional): Client IP address. Example: `"192.168.1.10"`

#### Notification & Cache
- `enable_notification` (bool, default: `true`): Enable configuration change notification.
- `notification_timeout` (duration, default: `"30s"`): Notification timeout. Example: `"60s"`
- `enable_cache` (bool, default: `true`): Enable local cache.
- `cache_dir` (string, default: `"/tmp/apollo-cache"`): Cache directory.

#### Advanced Features
- `enable_metrics` (bool, default: `true`): Enable monitoring metrics.
- `enable_retry` (bool, default: `true`): Enable retry mechanism.
- `max_retry_times` (int32, default: `3`): Maximum retry times.
- `retry_interval` (duration, default: `"1s"`): Retry interval. Example: `"500ms"`
- `enable_circuit_breaker` (bool, default: `true`): Enable circuit breaker.
- `circuit_breaker_threshold` (float, default: `0.5`): Circuit breaker threshold.
- `enable_graceful_shutdown` (bool, default: `true`): Enable graceful shutdown.
- `shutdown_timeout` (duration, default: `"30s"`): Graceful shutdown timeout.
- `enable_logging` (bool, default: `true`): Enable detailed logging.
- `log_level` (string, default: `"info"`): Log level (debug, info, warn, error).

#### Service Configuration
Used for loading configurations from multiple namespaces.
- `service_config.namespace` (string, optional): Main namespace. Defaults to the top-level `namespace`.
- `service_config.additional_namespaces` (repeated string, optional): List of additional namespaces to load.
- `service_config.priority` (int32, default: `0`): Merge priority (higher number = higher priority).
- `service_config.merge_strategy` (string, default: `"override"`): How to handle conflicts (`override`, `merge`, `append`).

## Usage

### Basic Usage

The plugin automatically registers itself when imported. You can access it through the plugin manager:

```go
import (
    "github.com/go-lynx/lynx"
    apollo "github.com/go-lynx/lynx-apollo"
)

// Get Apollo plugin
raw := lynx.Lynx().GetPluginManager().GetPlugin("apollo.config.center")
if raw != nil {
    apolloPlugin, ok := raw.(*apollo.PlugApollo)
    if ok && apolloPlugin != nil {
        // Use the plugin
    }
}
```

### Configuration Management

The Apollo plugin supports both single and multiple namespace loading:

#### Single Namespace Loading

```go
// Get configuration value
value, err := plugin.GetConfigValue("application", "config.key")
if err != nil {
    log.Errorf("Failed to get config: %v", err)
}
```

#### Multiple Namespace Loading

When `service_config` is configured, the plugin automatically loads multiple namespaces:

1. **Main Configuration**: Loaded based on `service_config.namespace`
   - If `namespace` is not specified, uses main apollo namespace

2. **Additional Configurations**: Loaded from `additional_namespaces` list
   - Each entry specifies a separate namespace to load
   - Namespace defaults to `service_config.namespace` if not specified

The plugin implements the `MultiConfigControlPlane` interface to support this functionality, allowing the Lynx framework to load and merge multiple configuration sources automatically.

### Configuration Watching

```go
// Watch configuration changes
watcher, err := plugin.WatchConfig("application")
if err != nil {
    log.Errorf("Failed to watch config: %v", err)
}

// Set up callbacks for configuration changes
watcher.SetOnConfigChanged(func(namespace, key, value string) {
    log.Infof("Config changed - Namespace: %s, Key: %s, Value: %s", namespace, key, value)
})

watcher.SetOnError(func(err error) {
    log.Errorf("Config watch error: %v", err)
})

// Start watching
watcher.Start()
defer watcher.Stop()
```

## Implementation Notes

This plugin currently uses an HTTP-based Apollo client plus long polling for config-source watching.

Current boundaries to keep in mind:

1. `WatchConfig()` returns the legacy callback-style `*ConfigWatcher`, while the lower-level `config.Watcher` integration lives in `watcher_adapter.go`.
2. Advanced feature fields such as retry / circuit breaker / graceful shutdown / logging should be set explicitly in config when you depend on them.
3. The current automated validation baseline is red; see [VALIDATION.md](./VALIDATION.md) before treating the examples here as CI-passing coverage.

## Metrics

The plugin exposes the following Prometheus metrics:

- `lynx_apollo_client_operations_total` - Total number of client operations
- `lynx_apollo_client_operations_duration_seconds` - Duration of client operations
- `lynx_apollo_client_errors_total` - Total number of client errors
- `lynx_apollo_config_operations_total` - Total number of configuration operations
- `lynx_apollo_config_operations_duration_seconds` - Duration of configuration operations
- `lynx_apollo_config_changes_total` - Total number of configuration changes
- `lynx_apollo_notification_total` - Total number of notifications
- `lynx_apollo_notification_duration_seconds` - Duration of notification operations
- `lynx_apollo_health_check_total` - Total number of health checks
- `lynx_apollo_health_check_duration_seconds` - Duration of health checks
- `lynx_apollo_cache_hits_total` - Total number of cache hits
- `lynx_apollo_cache_misses_total` - Total number of cache misses

## Validation

Current automated baseline in this workspace is red. See [VALIDATION.md](./VALIDATION.md) for the exact failing tests and current `go test ./...` output summary.

## License

This plugin is part of the Lynx framework and follows the same license.
