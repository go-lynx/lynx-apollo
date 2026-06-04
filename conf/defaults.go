package conf

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

// Default configuration constants
const (
	// Namespace related
	DefaultNamespace = "application"
	DefaultCluster   = "default"

	// Timeout related
	DefaultTimeoutSeconds = 10
	MinTimeoutSeconds     = 1
	MaxTimeoutSeconds     = 60

	// Notification related
	DefaultNotificationTimeoutSeconds = 30
	MinNotificationTimeoutSeconds     = 5
	MaxNotificationTimeoutSeconds     = 300

	// Retry related
	DefaultMaxRetryTimes = 3
	MinRetryTimes        = 0
	MaxRetryTimes        = 10
	DefaultRetryInterval = 1 * time.Second
	MinRetryInterval     = 100 * time.Millisecond
	MaxRetryInterval     = 30 * time.Second

	// Circuit breaker related
	DefaultCircuitBreakerThreshold = 0.5
	MinCircuitBreakerThreshold     = 0.1
	MaxCircuitBreakerThreshold     = 0.9

	// Graceful shutdown related
	DefaultShutdownTimeout = 30 * time.Second
	MinShutdownTimeout     = 5 * time.Second
	MaxShutdownTimeout     = 300 * time.Second

	// Cache related
	DefaultCacheDir = "/tmp/apollo-cache"

	// Log levels
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"

	// Merge strategies
	MergeStrategyOverride = "override"
	MergeStrategyMerge    = "merge"
	MergeStrategyAppend   = "append"
)

// Supported log levels
var SupportedLogLevels = []string{
	LogLevelDebug,
	LogLevelInfo,
	LogLevelWarn,
	LogLevelError,
}

// Supported merge strategies
var SupportedMergeStrategies = []string{
	MergeStrategyOverride,
	MergeStrategyMerge,
	MergeStrategyAppend,
}

// GetDefaultTimeout returns the default request timeout (10s) as a protobuf Duration.
func GetDefaultTimeout() *durationpb.Duration {
	return &durationpb.Duration{Seconds: DefaultTimeoutSeconds}
}

// GetDefaultNotificationTimeout returns the default long-poll timeout (30s).
func GetDefaultNotificationTimeout() *durationpb.Duration {
	return &durationpb.Duration{Seconds: DefaultNotificationTimeoutSeconds}
}

// GetDefaultRetryInterval returns the default base retry interval (1s).
func GetDefaultRetryInterval() *durationpb.Duration {
	return &durationpb.Duration{Seconds: int64(DefaultRetryInterval.Seconds())}
}

// GetDefaultShutdownTimeout returns the default graceful-shutdown timeout (30s).
func GetDefaultShutdownTimeout() *durationpb.Duration {
	return &durationpb.Duration{Seconds: int64(DefaultShutdownTimeout.Seconds())}
}
