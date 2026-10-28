// Package redislimit implements every algorithm on Redis. Each decision is a
// single EVALSHA of an atomic Lua script touching exactly one key, so it is
// safe across any number of application instances and works on Redis
// Cluster.
package redislimit

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MahyarNemati/ratelimit"
	"github.com/MahyarNemati/ratelimit/clock"
)

var (
	//go:embed scripts/prelude.lua
	prelude string
	//go:embed scripts/token_bucket.lua
	tokenBucketSrc string
	//go:embed scripts/gcra.lua
	gcraSrc string
	//go:embed scripts/sliding_window_log.lua
	slidingLogSrc string
	//go:embed scripts/sliding_window_counter.lua
	slidingCounterSrc string

	tokenBucketScript    = redis.NewScript(prelude + tokenBucketSrc)
	gcraScript           = redis.NewScript(prelude + gcraSrc)
	slidingLogScript     = redis.NewScript(prelude + slidingLogSrc)
	slidingCounterScript = redis.NewScript(prelude + slidingCounterSrc)
)

// Client is the subset of go-redis used here. *redis.Client,
// *redis.ClusterClient and *redis.Ring all satisfy it.
type Client = redis.Scripter

type options struct {
	prefix string
	clock  clock.Clock
}

// Option configures a Redis limiter.
type Option func(*options)

// WithPrefix sets the key prefix. Defaults to "rl:". Keys are
// prefix + algorithm + ":" + key; wrap key in {braces} to control cluster
// slot placement.
func WithPrefix(p string) Option { return func(o *options) { o.prefix = p } }

// WithClientClock makes the limiter send timestamps from c instead of
// reading the Redis server clock. Useful for deterministic tests; in
// production it exposes decisions to clock skew between app instances.
func WithClientClock(c clock.Clock) Option { return func(o *options) { o.clock = c } }

func buildOptions(opts []Option) options {
	o := options{prefix: "rl:"}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// New returns a limiter for the named algorithm.
func New(rdb Client, alg ratelimit.Algorithm, limit ratelimit.Limit, opts ...Option) (ratelimit.Limiter, error) {
	switch alg {
	case ratelimit.TokenBucket:
		return NewTokenBucket(rdb, limit, opts...)
	case ratelimit.SlidingWindowLog:
		return NewSlidingWindowLog(rdb, limit, opts...)
	case ratelimit.SlidingWindowCounter:
		return NewSlidingWindowCounter(rdb, limit, opts...)
	case ratelimit.GCRA:
		return NewGCRA(rdb, limit, opts...)
	default:
		return nil, fmt.Errorf("redislimit: unsupported algorithm %q", alg)
	}
}

// base holds what every algorithm shares.
type base struct {
	rdb      Client
	script   *redis.Script
	ns       string
	clock    clock.Clock
	capacity int
}

func newBase(rdb Client, script *redis.Script, alg ratelimit.Algorithm, capacity int, opts []Option) (base, error) {
	if rdb == nil {
		return base{}, errors.New("redislimit: nil client")
	}
	o := buildOptions(opts)
	return base{rdb: rdb, script: script, ns: o.prefix + string(alg) + ":", clock: o.clock, capacity: capacity}, nil
}

func (b *base) nowArg() int64 {
	if b.clock == nil {
		return -1
	}
	return b.clock.Now().UnixMicro()
}

func (b *base) check(n int) error {
	if n <= 0 {
		return ratelimit.ErrInvalidN
	}
	if n > b.capacity {
		return fmt.Errorf("%w: n=%d capacity=%d", ratelimit.ErrExceedsCapacity, n, b.capacity)
	}
	return nil
}

// reply is the common script return shape:
// {allowed, remaining, retry_us, reset_us, delay_us}.
type reply struct {
	allowed   bool
	remaining int64
	retry     time.Duration
	reset     time.Duration
	delay     time.Duration
}

func (b *base) run(ctx context.Context, key string, args ...any) (reply, error) {
	argv := append([]any{b.nowArg()}, args...)
	vals, err := b.script.Run(ctx, b.rdb, []string{b.ns + key}, argv...).Int64Slice()
	if err != nil {
		return reply{}, fmt.Errorf("redislimit: %w", err)
	}
	if len(vals) != 5 {
		return reply{}, fmt.Errorf("redislimit: unexpected script reply length %d", len(vals))
	}
	return reply{
		allowed:   vals[0] == 1,
		remaining: vals[1],
		retry:     time.Duration(vals[2]) * time.Microsecond,
		reset:     time.Duration(vals[3]) * time.Microsecond,
		delay:     time.Duration(vals[4]) * time.Microsecond,
	}, nil
}

func (r reply) result(capacity int) ratelimit.Result {
	return ratelimit.Result{
		Allowed:    r.allowed,
		Limit:      capacity,
		Remaining:  int(r.remaining),
		RetryAfter: r.retry,
		ResetAfter: r.reset,
	}
}

func micros(d time.Duration) int64 { return int64(d / time.Microsecond) }

func boolArg(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// TokenBucket is a Redis-backed token bucket.
type TokenBucket struct {
	base
	limit ratelimit.Limit
}

var _ ratelimit.Reserver = (*TokenBucket)(nil)

// NewTokenBucket returns a Redis token bucket limiter.
func NewTokenBucket(rdb Client, limit ratelimit.Limit, opts ...Option) (*TokenBucket, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	b, err := newBase(rdb, tokenBucketScript, ratelimit.TokenBucket, limit.EffectiveBurst(), opts)
	if err != nil {
		return nil, err
	}
	return &TokenBucket{base: b, limit: limit}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *TokenBucket) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN takes n tokens if available.
func (l *TokenBucket) AllowN(ctx context.Context, key string, n int) (ratelimit.Result, error) {
	if err := l.check(n); err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	r, err := l.exec(ctx, key, n, false, 0)
	if err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	return r.result(l.capacity), nil
}

// Reserve takes n tokens, borrowing against future refills if needed.
func (l *TokenBucket) Reserve(ctx context.Context, key string, n int, maxWait time.Duration) (ratelimit.Reservation, error) {
	if err := l.check(n); err != nil {
		return ratelimit.Reservation{Limit: l.capacity}, err
	}
	r, err := l.exec(ctx, key, n, true, maxWait)
	if err != nil {
		return ratelimit.Reservation{Limit: l.capacity}, err
	}
	return ratelimit.Reservation{OK: r.allowed, Delay: r.delay, Limit: l.capacity}, nil
}

func (l *TokenBucket) exec(ctx context.Context, key string, n int, reserve bool, maxWait time.Duration) (reply, error) {
	return l.run(ctx, key, l.limit.Rate, micros(l.limit.Period), l.capacity, n, boolArg(reserve), micros(maxWait))
}

// GCRA is a Redis-backed generic cell rate algorithm limiter. State is one
// integer per key.
type GCRA struct {
	base
	interval int64
}

var _ ratelimit.Reserver = (*GCRA)(nil)

// NewGCRA returns a Redis GCRA limiter.
func NewGCRA(rdb Client, limit ratelimit.Limit, opts ...Option) (*GCRA, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	b, err := newBase(rdb, gcraScript, ratelimit.GCRA, limit.EffectiveBurst(), opts)
	if err != nil {
		return nil, err
	}
	return &GCRA{base: b, interval: micros(limit.Interval())}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *GCRA) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN admits n units if they conform.
func (l *GCRA) AllowN(ctx context.Context, key string, n int) (ratelimit.Result, error) {
	if err := l.check(n); err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	r, err := l.run(ctx, key, l.interval, l.capacity, n, "0", 0)
	if err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	return r.result(l.capacity), nil
}

// Reserve admits n units at the earliest conforming time.
func (l *GCRA) Reserve(ctx context.Context, key string, n int, maxWait time.Duration) (ratelimit.Reservation, error) {
	if err := l.check(n); err != nil {
		return ratelimit.Reservation{Limit: l.capacity}, err
	}
	r, err := l.run(ctx, key, l.interval, l.capacity, n, "1", micros(maxWait))
	if err != nil {
		return ratelimit.Reservation{Limit: l.capacity}, err
	}
	return ratelimit.Reservation{OK: r.allowed, Delay: r.delay, Limit: l.capacity}, nil
}

// SlidingWindowLog is a Redis-backed sliding window log using a sorted set.
type SlidingWindowLog struct {
	base
	window int64
}

var _ ratelimit.Limiter = (*SlidingWindowLog)(nil)

// NewSlidingWindowLog returns a Redis sliding window log limiter.
func NewSlidingWindowLog(rdb Client, limit ratelimit.Limit, opts ...Option) (*SlidingWindowLog, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	b, err := newBase(rdb, slidingLogScript, ratelimit.SlidingWindowLog, limit.Rate, opts)
	if err != nil {
		return nil, err
	}
	return &SlidingWindowLog{base: b, window: micros(limit.Period)}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *SlidingWindowLog) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN admits n requests if the trailing window has room.
func (l *SlidingWindowLog) AllowN(ctx context.Context, key string, n int) (ratelimit.Result, error) {
	if err := l.check(n); err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	id, err := requestID()
	if err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	r, err := l.run(ctx, key, l.window, l.capacity, n, id)
	if err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	return r.result(l.capacity), nil
}

// SlidingWindowCounter is a Redis-backed sliding window counter.
type SlidingWindowCounter struct {
	base
	window int64
}

var _ ratelimit.Limiter = (*SlidingWindowCounter)(nil)

// NewSlidingWindowCounter returns a Redis sliding window counter limiter.
func NewSlidingWindowCounter(rdb Client, limit ratelimit.Limit, opts ...Option) (*SlidingWindowCounter, error) {
	if err := limit.Validate(); err != nil {
		return nil, err
	}
	b, err := newBase(rdb, slidingCounterScript, ratelimit.SlidingWindowCounter, limit.Rate, opts)
	if err != nil {
		return nil, err
	}
	return &SlidingWindowCounter{base: b, window: micros(limit.Period)}, nil
}

// Allow is AllowN(ctx, key, 1).
func (l *SlidingWindowCounter) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN admits n requests if the weighted estimate leaves room.
func (l *SlidingWindowCounter) AllowN(ctx context.Context, key string, n int) (ratelimit.Result, error) {
	if err := l.check(n); err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	r, err := l.run(ctx, key, l.window, l.capacity, n)
	if err != nil {
		return ratelimit.Result{Limit: l.capacity}, err
	}
	return r.result(l.capacity), nil
}

// requestID makes sorted-set members unique when several requests land on
// the same microsecond.
func requestID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("redislimit: request id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
