// Package memory implements every algorithm with process-local state. Use it
// for single-instance services or as a first tier in front of a shared
// limiter.
package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/MahyarNemati/ratelimit"
	"github.com/MahyarNemati/ratelimit/clock"
	"github.com/MahyarNemati/ratelimit/internal/algo"
)

type options struct {
	clock      clock.Clock
	sweepEvery time.Duration
}

// Option configures a memory limiter.
type Option func(*options)

// WithClock sets the time source. Defaults to clock.Real.
func WithClock(c clock.Clock) Option { return func(o *options) { o.clock = c } }

// WithSweepInterval sets how often each shard evicts idle keys. Defaults to
// one minute.
func WithSweepInterval(d time.Duration) Option { return func(o *options) { o.sweepEvery = d } }

func buildOptions(opts []Option) options {
	o := options{clock: clock.Real{}, sweepEvery: time.Minute}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// New returns a limiter for the named algorithm.
func New(alg ratelimit.Algorithm, limit ratelimit.Limit, opts ...Option) (ratelimit.Limiter, error) {
	switch alg {
	case ratelimit.TokenBucket:
		return NewTokenBucket(limit, opts...)
	case ratelimit.SlidingWindowLog:
		return NewSlidingWindowLog(limit, opts...)
	case ratelimit.SlidingWindowCounter:
		return NewSlidingWindowCounter(limit, opts...)
	case ratelimit.GCRA:
		return NewGCRA(limit, opts...)
	default:
		return nil, fmt.Errorf("memory: unsupported algorithm %q", alg)
	}
}

func checkN(n, capacity int) error {
	if n <= 0 {
		return ratelimit.ErrInvalidN
	}
	if n > capacity {
		return fmt.Errorf("%w: n=%d capacity=%d", ratelimit.ErrExceedsCapacity, n, capacity)
	}
	return nil
}

func toResult(d algo.Decision, limit int) ratelimit.Result {
	return ratelimit.Result{
		Allowed:    d.Allowed,
		Limit:      limit,
		Remaining:  int(d.Remaining),
		RetryAfter: time.Duration(d.RetryAfter),
		ResetAfter: time.Duration(d.ResetAfter),
	}
}

// TokenBucket refills Rate tokens per Period up to Burst.
type TokenBucket struct {
	limit ratelimit.Limit
	burst int
	clock clock.Clock
	store *store[algo.Bucket]
}

var _ ratelimit.Reserver = (*TokenBucket)(nil)

// NewTokenBucket returns an in-memory token bucket limiter.
func NewTokenBucket(limit ratelimit.Limit, opts ...Option) (*TokenBucket, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	o := buildOptions(opts)
	return &TokenBucket{limit: limit, burst: limit.EffectiveBurst(), clock: o.clock, store: newStore[algo.Bucket](o.sweepEvery)}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *TokenBucket) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN takes n tokens if available.
func (l *TokenBucket) AllowN(_ context.Context, key string, n int) (ratelimit.Result, error) {
	if err := checkN(n, l.burst); err != nil {
		return ratelimit.Result{Limit: l.burst}, err
	}
	d := l.take(key, n, false, 0)
	return toResult(d, l.burst), nil
}

// Reserve takes n tokens, borrowing against future refills if needed.
func (l *TokenBucket) Reserve(_ context.Context, key string, n int, maxWait time.Duration) (ratelimit.Reservation, error) {
	if err := checkN(n, l.burst); err != nil {
		return ratelimit.Reservation{Limit: l.burst}, err
	}
	d := l.take(key, n, true, int64(maxWait))
	return ratelimit.Reservation{OK: d.Allowed, Delay: time.Duration(d.Delay), Limit: l.burst}, nil
}

func (l *TokenBucket) take(key string, n int, reserve bool, maxWait int64) algo.Decision {
	now := l.clock.Now().UnixNano()
	var d algo.Decision
	l.store.update(key, now, func(b *algo.Bucket) int64 {
		d = b.Take(now, int64(l.limit.Rate), int64(l.limit.Period), int64(l.burst), int64(n), reserve, maxWait)
		return d.ResetAfter
	})
	return d
}

// GCRA tracks a theoretical arrival time per key. It behaves like a token
// bucket but stores a single integer.
type GCRA struct {
	burst    int
	interval int64
	clock    clock.Clock
	store    *store[algo.GCRA]
}

var _ ratelimit.Reserver = (*GCRA)(nil)

// NewGCRA returns an in-memory GCRA limiter.
func NewGCRA(limit ratelimit.Limit, opts ...Option) (*GCRA, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	o := buildOptions(opts)
	return &GCRA{burst: limit.EffectiveBurst(), interval: int64(limit.Interval()), clock: o.clock, store: newStore[algo.GCRA](o.sweepEvery)}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *GCRA) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN admits n units if they conform.
func (l *GCRA) AllowN(_ context.Context, key string, n int) (ratelimit.Result, error) {
	if err := checkN(n, l.burst); err != nil {
		return ratelimit.Result{Limit: l.burst}, err
	}
	return toResult(l.take(key, n, false, 0), l.burst), nil
}

// Reserve admits n units at the earliest conforming time.
func (l *GCRA) Reserve(_ context.Context, key string, n int, maxWait time.Duration) (ratelimit.Reservation, error) {
	if err := checkN(n, l.burst); err != nil {
		return ratelimit.Reservation{Limit: l.burst}, err
	}
	d := l.take(key, n, true, int64(maxWait))
	return ratelimit.Reservation{OK: d.Allowed, Delay: time.Duration(d.Delay), Limit: l.burst}, nil
}

func (l *GCRA) take(key string, n int, reserve bool, maxWait int64) algo.Decision {
	now := l.clock.Now().UnixNano()
	var d algo.Decision
	l.store.update(key, now, func(g *algo.GCRA) int64 {
		d = g.Take(now, l.interval, int64(l.burst), int64(n), reserve, maxWait)
		return d.ResetAfter
	})
	return d
}

// SlidingWindowLog records every admitted request and counts those inside
// the trailing window. Exact, but memory grows with Rate.
type SlidingWindowLog struct {
	limit ratelimit.Limit
	clock clock.Clock
	store *store[algo.Log]
}

var _ ratelimit.Limiter = (*SlidingWindowLog)(nil)

// NewSlidingWindowLog returns an in-memory sliding window log limiter.
func NewSlidingWindowLog(limit ratelimit.Limit, opts ...Option) (*SlidingWindowLog, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	o := buildOptions(opts)
	return &SlidingWindowLog{limit: limit, clock: o.clock, store: newStore[algo.Log](o.sweepEvery)}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *SlidingWindowLog) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN admits n requests if the trailing window has room.
func (l *SlidingWindowLog) AllowN(_ context.Context, key string, n int) (ratelimit.Result, error) {
	if err := checkN(n, l.limit.Rate); err != nil {
		return ratelimit.Result{Limit: l.limit.Rate}, err
	}
	now := l.clock.Now().UnixNano()
	var d algo.Decision
	l.store.update(key, now, func(s *algo.Log) int64 {
		d = s.Take(now, int64(l.limit.Period), int64(l.limit.Rate), int64(n))
		return d.ResetAfter
	})
	return toResult(d, l.limit.Rate), nil
}

// SlidingWindowCounter approximates a sliding window from two fixed-window
// counters. Constant memory per key.
type SlidingWindowCounter struct {
	limit ratelimit.Limit
	clock clock.Clock
	store *store[algo.Counter]
}

var _ ratelimit.Limiter = (*SlidingWindowCounter)(nil)

// NewSlidingWindowCounter returns an in-memory sliding window counter limiter.
func NewSlidingWindowCounter(limit ratelimit.Limit, opts ...Option) (*SlidingWindowCounter, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	o := buildOptions(opts)
	return &SlidingWindowCounter{limit: limit, clock: o.clock, store: newStore[algo.Counter](o.sweepEvery)}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *SlidingWindowCounter) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN admits n requests if the weighted estimate leaves room.
func (l *SlidingWindowCounter) AllowN(_ context.Context, key string, n int) (ratelimit.Result, error) {
	if err := checkN(n, l.limit.Rate); err != nil {
		return ratelimit.Result{Limit: l.limit.Rate}, err
	}
	now := l.clock.Now().UnixNano()
	var d algo.Decision
	l.store.update(key, now, func(s *algo.Counter) int64 {
		d = s.Take(now, int64(l.limit.Period), int64(l.limit.Rate), int64(n))
		return d.ResetAfter
	})
	return toResult(d, l.limit.Rate), nil
}
