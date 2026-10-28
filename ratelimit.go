// Package ratelimit defines the common types shared by every limiter in this
// module. Concrete algorithms live in the memory and redislimit packages;
// transport adapters live in httplimit and grpclimit.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	// ErrInvalidLimit is returned when a Limit cannot be enforced.
	ErrInvalidLimit = errors.New("ratelimit: invalid limit")
	// ErrExceedsCapacity is returned when a single request asks for more
	// units than the limiter can ever grant at once. Retrying will not help.
	ErrExceedsCapacity = errors.New("ratelimit: n exceeds limiter capacity")
	// ErrInvalidN is returned when n is zero or negative.
	ErrInvalidN = errors.New("ratelimit: n must be positive")
	// ErrWaitExceedsDeadline is returned by Wait when the context deadline
	// would expire before the request could be admitted.
	ErrWaitExceedsDeadline = errors.New("ratelimit: wait would exceed context deadline")
)

// Algorithm identifies a limiting algorithm.
type Algorithm string

const (
	TokenBucket          Algorithm = "token_bucket"
	SlidingWindowLog     Algorithm = "sliding_window_log"
	SlidingWindowCounter Algorithm = "sliding_window_counter"
	GCRA                 Algorithm = "gcra"
)

// Algorithms lists every supported algorithm.
var Algorithms = []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter, GCRA}

// ParseAlgorithm converts a configuration string into an Algorithm.
func ParseAlgorithm(s string) (Algorithm, error) {
	for _, a := range Algorithms {
		if string(a) == s {
			return a, nil
		}
	}
	return "", fmt.Errorf("ratelimit: unknown algorithm %q", s)
}

// Limit describes Rate events per Period.
//
// Burst is the maximum number of events admitted at once by the token bucket
// and GCRA algorithms; it defaults to Rate. Window algorithms ignore Burst:
// their capacity is Rate events in any window of length Period.
type Limit struct {
	Rate   int
	Period time.Duration
	Burst  int
}

// PerSecond returns a limit of n events per second.
func PerSecond(n int) Limit { return Limit{Rate: n, Period: time.Second} }

// PerMinute returns a limit of n events per minute.
func PerMinute(n int) Limit { return Limit{Rate: n, Period: time.Minute} }

// PerHour returns a limit of n events per hour.
func PerHour(n int) Limit { return Limit{Rate: n, Period: time.Hour} }

// WithBurst returns a copy of l with the given burst.
func (l Limit) WithBurst(b int) Limit {
	l.Burst = b
	return l
}

// Validate reports whether the limit is usable.
func (l Limit) Validate() error {
	switch {
	case l.Rate <= 0:
		return fmt.Errorf("%w: rate must be positive, got %d", ErrInvalidLimit, l.Rate)
	case l.Period <= 0:
		return fmt.Errorf("%w: period must be positive, got %s", ErrInvalidLimit, l.Period)
	case l.Burst < 0:
		return fmt.Errorf("%w: burst must not be negative, got %d", ErrInvalidLimit, l.Burst)
	case l.Period/time.Duration(l.Rate) < time.Microsecond:
		return fmt.Errorf("%w: rate %d per %s exceeds one event per microsecond", ErrInvalidLimit, l.Rate, l.Period)
	}
	return nil
}

// EffectiveBurst returns Burst, or Rate when Burst is unset.
func (l Limit) EffectiveBurst() int {
	if l.Burst > 0 {
		return l.Burst
	}
	return l.Rate
}

// Interval is the steady-state spacing between events (Period / Rate).
func (l Limit) Interval() time.Duration { return l.Period / time.Duration(l.Rate) }

func (l Limit) String() string {
	return fmt.Sprintf("%d/%s (burst %d)", l.Rate, l.Period, l.EffectiveBurst())
}

// Result is the outcome of an Allow or AllowN call.
type Result struct {
	// Allowed reports whether the request was admitted and its cost recorded.
	Allowed bool
	// Limit is the capacity of the limiter: burst for token bucket and GCRA,
	// Rate for the window algorithms.
	Limit int
	// Remaining is how many more single-unit requests would be admitted right now.
	Remaining int
	// RetryAfter is how long to wait before the same request would be
	// admitted. Zero when Allowed.
	RetryAfter time.Duration
	// ResetAfter is how long until the limiter returns to full capacity for
	// this key, assuming no further requests.
	ResetAfter time.Duration
}

// Reservation is the outcome of a Reserve call.
type Reservation struct {
	// OK reports whether the reservation was granted. When false nothing was
	// consumed.
	OK bool
	// Delay is how long the caller must wait before acting.
	Delay time.Duration
	Limit int
}

// Limiter admits or rejects requests per key.
//
// Implementations are safe for concurrent use. AllowN is all-or-nothing: a
// rejected call consumes nothing.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
	AllowN(ctx context.Context, key string, n int) (Result, error)
}

// Reserver is implemented by limiters that can grant capacity in the future
// (token bucket and GCRA). Reserve consumes n units now and reports how long
// the caller must wait before using them. If that delay would exceed maxWait
// nothing is consumed and OK is false.
type Reserver interface {
	Limiter
	Reserve(ctx context.Context, key string, n int, maxWait time.Duration) (Reservation, error)
}

// Wait blocks until n units are available for key or ctx is done. If ctx has
// a deadline that is shorter than the required delay, Wait returns
// ErrWaitExceedsDeadline immediately without consuming anything.
//
// Units reserved before ctx is cancelled are not returned to the limiter.
func Wait(ctx context.Context, r Reserver, key string, n int) error {
	maxWait := time.Duration(math.MaxInt64)
	if dl, ok := ctx.Deadline(); ok {
		maxWait = time.Until(dl)
		if maxWait <= 0 {
			return ctx.Err()
		}
	}
	res, err := r.Reserve(ctx, key, n, maxWait)
	if err != nil {
		return err
	}
	if !res.OK {
		return ErrWaitExceedsDeadline
	}
	if res.Delay <= 0 {
		return nil
	}
	t := time.NewTimer(res.Delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
