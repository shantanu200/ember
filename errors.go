package ember

import (
	"errors"
	"math/rand/v2"
	"time"
)

type PermanentError struct {
	Err error
}

type RetryPolicy struct {
	MaxAttempts  int
	BaseDelay    time.Duration
	MaxDelay     time.Duration
	JitterFactor float64 // 0 = no jitter (default), 1.0 = full jitter (random 0..delay)
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

func NewPermanentError(err error) *PermanentError {
	return &PermanentError{
		Err: err,
	}
}

func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

var ErrBufferFull = errors.New("ember: job buffer full")

var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 3,
	BaseDelay:   200 * time.Millisecond,
	MaxDelay:    10 * time.Second,
}

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
