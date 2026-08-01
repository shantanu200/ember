package quelon

import (
	"errors"
	"math/rand/v2"
	"time"
)

// PermanentError wraps an error to signal that a task must not be retried.
// A ProcessFunc or BatchProcessFunc that returns (or wraps, via errors.Is
// semantics) a *PermanentError is dead-lettered on the first failure,
// bypassing the pool's RetryPolicy entirely. Use NewPermanentError to
// construct one; use IsPermanent to test for one.
type PermanentError struct {
	Err error
}

// RetryPolicy controls how many times a failed task is attempted and the
// backoff between attempts. Delay grows exponentially from BaseDelay,
// doubling per attempt, capped at MaxDelay. DefaultRetryPolicy is used when
// no RetryPolicy is supplied via WithRetryPolicy.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (not retries) before a task
	// is dead-lettered. Values < 1 are clamped to 1 at pool construction, so a
	// task is always attempted at least once.
	MaxAttempts int
	// BaseDelay is the backoff before the second attempt; later attempts
	// double it, up to MaxDelay.
	BaseDelay time.Duration
	// MaxDelay caps the backoff regardless of attempt count. A zero or
	// negative BaseDelay<<attempt overflow also falls back to MaxDelay.
	MaxDelay time.Duration
	// JitterFactor randomizes the computed delay to avoid thundering-herd
	// retries across many tasks. 0 = no jitter (default); 1.0 = full jitter,
	// subtracting a random amount in [0, delay) from the computed delay.
	JitterFactor float64
}

// Error implements the error interface, delegating to the wrapped error's
// message.
func (e *PermanentError) Error() string {
	return e.Err.Error()
}

// Unwrap exposes the wrapped error so errors.Is/errors.As can see through a
// *PermanentError to the underlying cause.
func (e *PermanentError) Unwrap() error {
	return e.Err
}

// NewPermanentError wraps err so the pool treats it as non-retryable: the
// task that produced it is dead-lettered on the first failure instead of
// being retried per the configured RetryPolicy.
func NewPermanentError(err error) *PermanentError {
	return &PermanentError{
		Err: err,
	}
}

// IsPermanent reports whether err is, or wraps, a *PermanentError.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// ErrBufferFull is returned by Submit when the jobs buffer (or, in
// partitioned mode, the task's lane) has no room and the task was rejected.
var ErrBufferFull = errors.New("quelon: job buffer full")

// ErrPoolClosed is returned by Submit when CloseAndWait has begun: either the
// call arrived after shutdown started, or it was waiting for buffer room (see
// WithBlockOnFull) when shutdown began and was released. The task was not
// enqueued and will not be processed.
var ErrPoolClosed = errors.New("quelon: pool closed")

// DefaultRetryPolicy is used by NewPool/NewPoolWithBatch when no
// WithRetryPolicy option is supplied: 3 attempts, 200ms base delay doubling
// up to a 10s cap, no jitter.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 3,
	BaseDelay:   200 * time.Millisecond,
	MaxDelay:    10 * time.Second,
}

// Delay computes the backoff before the attempt following the given
// (zero-based) attempt index, i.e. Delay(0) is the wait before the second
// attempt. It doubles BaseDelay per attempt, caps at MaxDelay, and applies
// JitterFactor if set. Negative attempt values are treated as 0.
func (r RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	d := r.BaseDelay << uint(attempt)
	if d <= 0 || (r.MaxDelay > 0 && d > r.MaxDelay) {
		d = r.MaxDelay
	}

	if r.JitterFactor > 0 {
		jitter := time.Duration(rand.Float64() * float64(d) * r.JitterFactor)
		d -= jitter
	}

	return d
}
