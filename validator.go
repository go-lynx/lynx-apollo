package apollo

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/go-lynx/lynx-apollo/conf"
)

// ValidationError records one failed field check.
type ValidationError struct {
	Field   string
	Message string
	Value   any
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s (value: %v)", e.Field, e.Message, e.Value)
}

// ValidationResult accumulates field errors; IsValid is false once any is added.
type ValidationResult struct {
	IsValid bool
	Errors  []*ValidationError
}

func NewValidationResult() *ValidationResult {
	return &ValidationResult{
		IsValid: true,
		Errors:  make([]*ValidationError, 0),
	}
}

// AddError appends a field error and marks the result invalid.
func (r *ValidationResult) AddError(field, message string, value any) {
	r.IsValid = false
	r.Errors = append(r.Errors, &ValidationError{
		Field:   field,
		Message: message,
		Value:   value,
	})
}

// Error returns error message
func (r *ValidationResult) Error() string {
	if r.IsValid {
		return ""
	}

	var messages []string
	for _, err := range r.Errors {
		messages = append(messages, err.Error())
	}
	return strings.Join(messages, "; ")
}

// Validator checks an Apollo config against required fields, value ranges,
// enums, time bounds, and cross-field dependencies.
type Validator struct {
	config *conf.Apollo
}

func NewValidator(config *conf.Apollo) *Validator {
	return &Validator{
		config: config,
	}
}

// Validate runs all checks and returns the accumulated result.
func (v *Validator) Validate() *ValidationResult {
	result := NewValidationResult()
	if v == nil || v.config == nil {
		result.AddError("config", "configuration cannot be nil", nil)
		return result
	}
	v.applyImplicitDefaults()

	v.validateBasicFields(result)
	v.validateNumericRanges(result)
	v.validateEnumValues(result)
	v.validateTimeConfigs(result)
	v.validateDependencies(result)
	v.validateSecurityConfigs(result)
	v.validateNetworkConfigs(result)

	return result
}

func (v *Validator) applyImplicitDefaults() {
	if v.config == nil {
		return
	}
	if v.config.CircuitBreakerThreshold == 0 {
		v.config.CircuitBreakerThreshold = conf.DefaultCircuitBreakerThreshold
	}
}

func (v *Validator) validateBasicFields(result *ValidationResult) {
	// app_id is required; max 128 chars; only [a-zA-Z0-9_-].
	if v.config.AppId == "" {
		result.AddError("app_id", "app_id cannot be empty", v.config.AppId)
	} else if len(v.config.AppId) > 128 {
		result.AddError("app_id", "app_id length must not exceed 128 characters", v.config.AppId)
	} else {
		appIdRegex := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
		if !appIdRegex.MatchString(v.config.AppId) {
			result.AddError("app_id", "app_id can only contain letters, numbers, underscores, and hyphens", v.config.AppId)
		}
	}

	// meta_server is required and must be a parseable URL.
	if v.config.MetaServer == "" {
		result.AddError("meta_server", "meta_server cannot be empty", v.config.MetaServer)
	} else {
		_, err := url.Parse(v.config.MetaServer)
		if err != nil {
			result.AddError("meta_server", fmt.Sprintf("meta_server must be a valid URL: %v", err), v.config.MetaServer)
		}
	}

	if v.config.Cluster != "" && len(v.config.Cluster) > 64 {
		result.AddError("cluster", "cluster length must not exceed 64 characters", v.config.Cluster)
	}

	if v.config.Namespace != "" && len(v.config.Namespace) > 128 {
		result.AddError("namespace", "namespace length must not exceed 128 characters", v.config.Namespace)
	}

	if v.config.Token != "" && len(v.config.Token) > 1024 {
		result.AddError("token", "token length must not exceed 1024 characters", v.config.Token)
	}
	if v.config.Token != "" && len(v.config.Token) < 8 {
		result.AddError("token", "token must be at least 8 characters long", v.config.Token)
	}

	if v.config.CacheDir != "" && len(v.config.CacheDir) > 512 {
		result.AddError("cache_dir", "cache_dir length must not exceed 512 characters", v.config.CacheDir)
	}
}

func (v *Validator) validateNumericRanges(result *ValidationResult) {
	if v.config.MaxRetryTimes < conf.MinRetryTimes || v.config.MaxRetryTimes > conf.MaxRetryTimes {
		result.AddError("max_retry_times", fmt.Sprintf("max_retry_times must be between %d and %d", conf.MinRetryTimes, conf.MaxRetryTimes), v.config.MaxRetryTimes)
	}

	if v.config.CircuitBreakerThreshold < conf.MinCircuitBreakerThreshold || v.config.CircuitBreakerThreshold > conf.MaxCircuitBreakerThreshold {
		result.AddError("circuit_breaker_threshold", fmt.Sprintf("circuit_breaker_threshold must be between %.1f and %.1f", conf.MinCircuitBreakerThreshold, conf.MaxCircuitBreakerThreshold), v.config.CircuitBreakerThreshold)
	}
}

func (v *Validator) validateEnumValues(result *ValidationResult) {
	if v.config.LogLevel != "" {
		valid := false
		for _, level := range conf.SupportedLogLevels {
			if v.config.LogLevel == level {
				valid = true
				break
			}
		}
		if !valid {
			result.AddError("log_level", fmt.Sprintf("log_level must be one of: %v", conf.SupportedLogLevels), v.config.LogLevel)
		}
	}

	if v.config.ServiceConfig != nil && v.config.ServiceConfig.MergeStrategy != "" {
		valid := false
		for _, strategy := range conf.SupportedMergeStrategies {
			if v.config.ServiceConfig.MergeStrategy == strategy {
				valid = true
				break
			}
		}
		if !valid {
			result.AddError("service_config.merge_strategy", fmt.Sprintf("merge_strategy must be one of: %v", conf.SupportedMergeStrategies), v.config.ServiceConfig.MergeStrategy)
		}
	}
}

func (v *Validator) validateTimeConfigs(result *ValidationResult) {
	if v.config.Timeout != nil {
		timeout := time.Duration(v.config.Timeout.Seconds) * time.Second
		if timeout < time.Duration(conf.MinTimeoutSeconds)*time.Second || timeout > time.Duration(conf.MaxTimeoutSeconds)*time.Second {
			result.AddError("timeout", fmt.Sprintf("timeout must be between %d and %d seconds", conf.MinTimeoutSeconds, conf.MaxTimeoutSeconds), timeout)
		}
	}

	if v.config.NotificationTimeout != nil {
		timeout := time.Duration(v.config.NotificationTimeout.Seconds) * time.Second
		if timeout < time.Duration(conf.MinNotificationTimeoutSeconds)*time.Second || timeout > time.Duration(conf.MaxNotificationTimeoutSeconds)*time.Second {
			result.AddError("notification_timeout", fmt.Sprintf("notification_timeout must be between %d and %d seconds", conf.MinNotificationTimeoutSeconds, conf.MaxNotificationTimeoutSeconds), timeout)
		}
	}

	if v.config.RetryInterval != nil {
		interval := time.Duration(v.config.RetryInterval.Seconds) * time.Second
		if interval < conf.MinRetryInterval || interval > conf.MaxRetryInterval {
			result.AddError("retry_interval", fmt.Sprintf("retry_interval must be between %v and %v", conf.MinRetryInterval, conf.MaxRetryInterval), interval)
		}
	}

	if v.config.ShutdownTimeout != nil {
		timeout := time.Duration(v.config.ShutdownTimeout.Seconds) * time.Second
		if timeout < conf.MinShutdownTimeout || timeout > conf.MaxShutdownTimeout {
			result.AddError("shutdown_timeout", fmt.Sprintf("shutdown_timeout must be between %v and %v", conf.MinShutdownTimeout, conf.MaxShutdownTimeout), timeout)
		}
	}
}

func (v *Validator) validateDependencies(result *ValidationResult) {
	// timeout must be shorter than notification_timeout: a per-request timeout
	// at or above the long-poll window would cut off notifications prematurely.
	if v.config.Timeout != nil && v.config.NotificationTimeout != nil {
		timeout := time.Duration(v.config.Timeout.Seconds) * time.Second
		notificationTimeout := time.Duration(v.config.NotificationTimeout.Seconds) * time.Second

		if timeout >= notificationTimeout {
			result.AddError("timeout", "timeout should be less than notification_timeout to ensure proper operation", timeout)
		}
	}

	if v.config.EnableCache && v.config.CacheDir == "" {
		result.AddError("cache_dir", "cache_dir must be set when enable_cache is true", v.config.CacheDir)
	}
}

func (v *Validator) validateSecurityConfigs(result *ValidationResult) {
	if v.config.Token != "" {
		if len(v.config.Token) < 8 {
			result.AddError("token", "token must be at least 8 characters long for security", v.config.Token)
		}
	}

	// Flag plain-HTTP meta servers: production deployments should use HTTPS.
	if v.config.MetaServer != "" {
		parsedURL, err := url.Parse(v.config.MetaServer)
		if err == nil && parsedURL.Scheme == "http" {
			result.AddError("meta_server", "meta_server should use HTTPS in production environments", v.config.MetaServer)
		}
	}
}

func (v *Validator) validateNetworkConfigs(result *ValidationResult) {
	// Network timeout sanity bounds: 100ms .. 30s.
	if v.config.Timeout != nil {
		timeout := v.config.Timeout.AsDuration()
		if timeout < 100*time.Millisecond {
			result.AddError("timeout", "timeout should be at least 100ms for network operations", timeout)
		}
		if timeout > 30*time.Second {
			result.AddError("timeout", "timeout should not exceed 30s for network operations", timeout)
		}
	}

	if v.config.MaxRetryTimes < 0 {
		result.AddError("max_retry_times", "max_retry_times cannot be negative", v.config.MaxRetryTimes)
	}
	if v.config.MaxRetryTimes > 10 {
		result.AddError("max_retry_times", "max_retry_times should not exceed 10 to prevent excessive retries", v.config.MaxRetryTimes)
	}
}

// ValidateConfig validates config and returns the first failure as an error,
// or nil if the config is valid.
func ValidateConfig(config *conf.Apollo) error {
	validator := NewValidator(config)
	result := validator.Validate()

	if !result.IsValid {
		return fmt.Errorf("configuration validation failed: %s", result.Error())
	}

	return nil
}
