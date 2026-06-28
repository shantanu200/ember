package ember

import (
	"testing"
	"time"
)

func TestDelayNoJitter(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
	}
	for _, c := range cases {
		if got := p.Delay(c.attempt); got != c.want {
			t.Errorf("attempt %d: got %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestDelayCappedAtMaxDelay(t *testing.T) {
	p := RetryPolicy{BaseDelay: 1 * time.Second, MaxDelay: 3 * time.Second}

	for attempt := 3; attempt <= 10; attempt++ {
		if got := p.Delay(attempt); got > p.MaxDelay {
			t.Errorf("attempt %d: delay %v exceeds MaxDelay %v", attempt, got, p.MaxDelay)
		}
	}
}

func TestDelayNegativeAttemptClampedToZero(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	if got := p.Delay(-1); got != p.Delay(0) {
		t.Errorf("negative attempt should equal attempt 0: got %v", got)
	}
}

func TestDelayJitterWithinBounds(t *testing.T) {
	p := RetryPolicy{
		BaseDelay:    1 * time.Second,
		MaxDelay:     10 * time.Second,
		JitterFactor: 1.0, // full jitter: result in [0, base]
	}

	base := p.BaseDelay // attempt 0, no cap
	for i := range 1000 {
		d := p.Delay(0)
		if d < 0 || d > base {
			t.Fatalf("iteration %d: jittered delay %v out of bounds [0, %v]", i, d, base)
		}
	}
}

func TestDelayPartialJitterWithinBounds(t *testing.T) {
	p := RetryPolicy{
		BaseDelay:    1 * time.Second,
		MaxDelay:     10 * time.Second,
		JitterFactor: 0.5, // result in [base/2, base]
	}

	base := p.BaseDelay
	low := time.Duration(float64(base) * 0.5)
	for i := range 1000 {
		d := p.Delay(0)
		if d < low || d > base {
			t.Fatalf("iteration %d: delay %v out of bounds [%v, %v]", i, d, low, base)
		}
	}
}

func TestDelayJitterZeroMeansNoJitter(t *testing.T) {
	p := RetryPolicy{BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second, JitterFactor: 0}
	want := 200 * time.Millisecond
	for range 100 {
		if got := p.Delay(0); got != want {
			t.Fatalf("JitterFactor=0 should produce deterministic delay %v, got %v", want, got)
		}
	}
}

func TestDelayJitterCappedAtMaxDelay(t *testing.T) {
	p := RetryPolicy{
		BaseDelay:    1 * time.Second,
		MaxDelay:     2 * time.Second,
		JitterFactor: 1.0,
	}

	// attempt 10 would be 1024s without cap; after cap it's 2s, jitter brings it down
	for i := range 1000 {
		d := p.Delay(10)
		if d < 0 || d > p.MaxDelay {
			t.Fatalf("iteration %d: delay %v out of bounds [0, %v]", i, d, p.MaxDelay)
		}
	}
}
