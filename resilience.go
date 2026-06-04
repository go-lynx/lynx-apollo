package apollo

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-lynx/lynx/log"
)

// RetryManager retries an operation with exponential backoff, up to maxRetries
// times, capped at 30s per backoff (see calculateBackoff).
type RetryManager struct {
	maxRetries    int
	retryInterval time.Duration
	backoffFactor float64
}

func NewRetryManager(maxRetries int, retryInterval time.Duration) *RetryManager {
	return &RetryManager{
		maxRetries:    maxRetries,
		retryInterval: retryInterval,
		backoffFactor: 2.0,
	}
}

// DoWithRetry runs operation, retrying with backoff until it succeeds or
// maxRetries is exhausted. The backoff sleep blocks; use DoWithRetryContext for
// cancellable waits.
func (r *RetryManager) DoWithRetry(operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if err := operation(); err == nil {
			if attempt > 0 {
				log.Infof("Operation succeeded after %d retries", attempt)
			}
			return nil
		} else {
			lastErr = err
			if attempt < r.maxRetries {
				backoffTime := r.calculateBackoff(attempt)
				log.Warnf("Operation failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, r.maxRetries+1, err, backoffTime)
				time.Sleep(backoffTime)
			}
		}
	}

	return fmt.Errorf("operation failed after %d attempts, last error: %w", r.maxRetries+1, lastErr)
}

// DoWithRetryContext executes operation with retry (supports context)
func (r *RetryManager) DoWithRetryContext(ctx context.Context, operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled: %w", ctx.Err())
		default:
		}

		if err := operation(); err == nil {
			if attempt > 0 {
				log.Infof("Operation succeeded after %d retries", attempt)
			}
			return nil
		} else {
			lastErr = err
			if attempt < r.maxRetries {
				backoffTime := r.calculateBackoff(attempt)
				log.Warnf("Operation failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, r.maxRetries+1, err, backoffTime)

				select {
				case <-time.After(backoffTime):
				case <-ctx.Done():
					return fmt.Errorf("operation cancelled during retry: %w", ctx.Err())
				}
			}
		}
	}

	return fmt.Errorf("operation failed after %d attempts, last error: %w", r.maxRetries+1, lastErr)
}

// calculateBackoff returns retryInterval * backoffFactor^attempt, capped at 30s.
func (r *RetryManager) calculateBackoff(attempt int) time.Duration {
	backoffSeconds := float64(r.retryInterval) * math.Pow(r.backoffFactor, float64(attempt))

	maxBackoff := 30 * time.Second
	if time.Duration(backoffSeconds) > maxBackoff {
		return maxBackoff
	}

	return time.Duration(backoffSeconds)
}

// circuitWindow is the rolling time window over which the circuit breaker
// evaluates the failure rate.  Events older than this window are discarded so
// that the breaker can recover automatically as failures age out instead of
// accumulating over the lifetime of the process.
const circuitWindow = 60 * time.Second

// CircuitBreaker trips open when the failure rate over a rolling window reaches
// threshold, blocking calls until a half-open probe succeeds. The mu channel is
// used as a non-reentrant mutex (see Do).
type CircuitBreaker struct {
	threshold float64
	// failures/successes hold the timestamps of recent outcomes within the
	// rolling window.  They decay over time so the breaker can recover.
	failures    []time.Time
	successes   []time.Time
	lastFailure time.Time
	state       CircuitState
	mu          chan struct{} // Used as mutex lock
}

// CircuitState circuit breaker state
type CircuitState int

const (
	CircuitStateClosed   CircuitState = iota // Closed state: normal
	CircuitStateOpen                         // Open state: circuit broken
	CircuitStateHalfOpen                     // Half-open state: attempting recovery
)

// NewCircuitBreaker returns a closed breaker that opens at the given failure-rate threshold.
func NewCircuitBreaker(threshold float64) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		state:     CircuitStateClosed,
		mu:        make(chan struct{}, 1), // capacity-1 channel used as a mutex
	}
}

// Do executes operation with circuit breaker protection.
//
// The breaker lock is held only while inspecting/mutating breaker state — it is
// released before invoking operation().  This is critical because callers run
// retry logic with backoff sleeps inside operation(); holding the lock there
// would serialise every config read behind the slowest one.  The breaker only
// gates a single attempt.
func (cb *CircuitBreaker) Do(operation func() error) error {
	// Decide whether the attempt is allowed under the breaker lock.
	if err := cb.beforeAttempt(); err != nil {
		return err
	}

	// Execute the operation WITHOUT holding the breaker lock.
	err := operation()

	// Record the outcome under the breaker lock.
	cb.afterAttempt(err)

	return err
}

// beforeAttempt inspects breaker state and returns an error if the attempt
// should be rejected. It runs under the breaker lock.
func (cb *CircuitBreaker) beforeAttempt() error {
	cb.mu <- struct{}{}
	defer func() { <-cb.mu }()

	switch cb.state {
	case CircuitStateOpen:
		// Probe for recovery once the open period (30s) has elapsed.
		if time.Since(cb.lastFailure) > 30*time.Second {
			cb.state = CircuitStateHalfOpen
			log.Infof("Circuit breaker transitioning to half-open state")
		} else {
			return fmt.Errorf("circuit breaker is open")
		}
	case CircuitStateHalfOpen:
		log.Infof("Circuit breaker in half-open state, allowing one attempt")
	case CircuitStateClosed:
	default:
		return fmt.Errorf("invalid circuit breaker state: %v", cb.state)
	}
	return nil
}

// afterAttempt records the outcome of an attempt under the breaker lock.
func (cb *CircuitBreaker) afterAttempt(err error) {
	cb.mu <- struct{}{}
	defer func() { <-cb.mu }()

	if err != nil {
		cb.recordFailure()
	} else {
		cb.recordSuccess()
	}
}

// pruneLocked drops events that have aged out of the rolling window.
// Caller must hold the breaker lock.
func (cb *CircuitBreaker) pruneLocked(now time.Time) {
	cutoff := now.Add(-circuitWindow)
	cb.failures = pruneBefore(cb.failures, cutoff)
	cb.successes = pruneBefore(cb.successes, cutoff)
}

// pruneBefore returns the slice with all timestamps before cutoff removed.
func pruneBefore(ts []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(ts) && ts[i].Before(cutoff) {
		i++
	}
	if i == 0 {
		return ts
	}
	return ts[i:]
}

// failureRateLocked computes the failure rate over the rolling window.
// Caller must hold the breaker lock.
func (cb *CircuitBreaker) failureRateLocked() float64 {
	total := len(cb.failures) + len(cb.successes)
	if total == 0 {
		return 0
	}
	return float64(len(cb.failures)) / float64(total)
}

// recordFailure records failure. Caller must hold the breaker lock.
func (cb *CircuitBreaker) recordFailure() {
	now := time.Now()
	cb.failures = append(cb.failures, now)
	cb.lastFailure = now
	cb.pruneLocked(now)

	failureRate := cb.failureRateLocked()
	if cb.state == CircuitStateClosed && failureRate >= cb.threshold {
		cb.state = CircuitStateOpen
		log.Warnf("Circuit breaker opened: failure rate %.2f >= threshold %.2f",
			failureRate, cb.threshold)
	} else if cb.state == CircuitStateHalfOpen {
		cb.state = CircuitStateOpen
		log.Warnf("Circuit breaker reopened after failed attempt")
	}
}

// recordSuccess records success. Caller must hold the breaker lock.
func (cb *CircuitBreaker) recordSuccess() {
	cb.successes = append(cb.successes, time.Now())
	cb.pruneLocked(time.Now())

	if cb.state == CircuitStateHalfOpen {
		// A successful half-open probe closes the breaker and clears the window.
		cb.state = CircuitStateClosed
		cb.resetCounters()
		log.Infof("Circuit breaker closed after successful attempt")
	}
}

// resetCounters resets counters. Caller must hold the breaker lock.
func (cb *CircuitBreaker) resetCounters() {
	cb.failures = nil
	cb.successes = nil
}

// GetState returns the current breaker state.
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mu <- struct{}{}
	defer func() { <-cb.mu }()
	return cb.state
}

// GetFailureRate gets failure rate over the rolling window
func (cb *CircuitBreaker) GetFailureRate() float64 {
	cb.mu <- struct{}{}
	defer func() { <-cb.mu }()

	cb.pruneLocked(time.Now())
	return cb.failureRateLocked()
}

// ForceOpen trips the breaker open regardless of the current failure rate.
func (cb *CircuitBreaker) ForceOpen() {
	cb.mu <- struct{}{}
	defer func() { <-cb.mu }()
	cb.state = CircuitStateOpen
	cb.lastFailure = time.Now()
	log.Warnf("Circuit breaker forced open")
}

// ForceClose resets the breaker to closed and clears the failure window.
func (cb *CircuitBreaker) ForceClose() {
	cb.mu <- struct{}{}
	defer func() { <-cb.mu }()
	cb.state = CircuitStateClosed
	cb.resetCounters()
	log.Infof("Circuit breaker forced closed")
}
