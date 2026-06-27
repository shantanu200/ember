package ember

import (
	"errors"
	"time"
)

type PermanentError struct {
	Err error
}

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

func NewPermanantError(err error) *PermanentError {
	return &PermanentError{
		Err: err,
	}
}

func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 3,
	BaseDelay:   200 * time.Millisecond,
	MaxDelay:    10 * time.Second,
}

func (r RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	d := r.BaseDelay << time.Duration(attempt)
	if d <= 0 || d > r.MaxDelay {
		return r.MaxDelay
	}

	return d
}
