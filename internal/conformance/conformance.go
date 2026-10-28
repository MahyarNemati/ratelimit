// Package conformance is a backend-independent test suite. Every backend
// runs the same scenarios against a fake clock, which guarantees the memory
// and Redis implementations make identical decisions.
package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MahyarNemati/ratelimit"
	"github.com/MahyarNemati/ratelimit/clock"
)

// Start is aligned to an hour boundary so fixed windows start on it.
var Start = time.Unix(1_699_999_200, 0)

// Factory builds a limiter bound to clk.
type Factory func(t *testing.T, alg ratelimit.Algorithm, limit ratelimit.Limit, clk *clock.Fake) ratelimit.Limiter

// Run executes every scenario.
func Run(t *testing.T, f Factory) {
	t.Run("token_bucket", func(t *testing.T) { bucketScenario(t, f, ratelimit.TokenBucket) })
	t.Run("gcra", func(t *testing.T) { bucketScenario(t, f, ratelimit.GCRA) })
	t.Run("token_bucket/reserve", func(t *testing.T) { reserveScenario(t, f, ratelimit.TokenBucket) })
	t.Run("gcra/reserve", func(t *testing.T) { reserveScenario(t, f, ratelimit.GCRA) })
	t.Run("sliding_window_log", func(t *testing.T) { logScenario(t, f) })
	t.Run("sliding_window_counter", func(t *testing.T) { counterScenario(t, f) })
	for _, alg := range ratelimit.Algorithms {
		t.Run(string(alg)+"/keys_are_independent", func(t *testing.T) { independentKeys(t, f, alg) })
		t.Run(string(alg)+"/invalid_n", func(t *testing.T) { invalidN(t, f, alg) })
	}
}

type step struct {
	name      string
	advance   time.Duration
	n         int
	allowed   bool
	remaining int
	retry     time.Duration
	reset     time.Duration
}

func runSteps(t *testing.T, l ratelimit.Limiter, clk *clock.Fake, key string, steps []step) {
	t.Helper()
	ctx := context.Background()
	for _, s := range steps {
		clk.Advance(s.advance)
		n := s.n
		if n == 0 {
			n = 1
		}
		got, err := l.AllowN(ctx, key, n)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", s.name, err)
		}
		if got.Allowed != s.allowed || got.Remaining != s.remaining || got.RetryAfter != s.retry {
			t.Fatalf("%s: got allowed=%v remaining=%d retry=%s, want allowed=%v remaining=%d retry=%s",
				s.name, got.Allowed, got.Remaining, got.RetryAfter, s.allowed, s.remaining, s.retry)
		}
		if s.reset != 0 && got.ResetAfter != s.reset {
			t.Fatalf("%s: reset=%s, want %s", s.name, got.ResetAfter, s.reset)
		}
	}
}

func bucketScenario(t *testing.T, f Factory, alg ratelimit.Algorithm) {
	clk := clock.NewFake(Start)
	l := f(t, alg, ratelimit.PerSecond(10).WithBurst(5), clk)
	ms := time.Millisecond
	runSteps(t, l, clk, "k", []step{
		{name: "first", allowed: true, remaining: 4, reset: 100 * ms},
		{name: "burst 2", allowed: true, remaining: 3},
		{name: "burst 3", allowed: true, remaining: 2},
		{name: "burst 4", allowed: true, remaining: 1},
		{name: "burst 5", allowed: true, remaining: 0, reset: 500 * ms},
		{name: "exhausted", allowed: false, remaining: 0, retry: 100 * ms, reset: 500 * ms},
		{name: "partial refill", advance: 50 * ms, allowed: false, remaining: 0, retry: 50 * ms},
		{name: "one token refilled", advance: 50 * ms, allowed: true, remaining: 0},
		{name: "empty again", allowed: false, remaining: 0, retry: 100 * ms},
		{name: "full refill", advance: time.Second, n: 5, allowed: true, remaining: 0},
		{name: "batch denied", advance: 200 * ms, n: 3, allowed: false, remaining: 2, retry: 100 * ms},
		{name: "batch fits", n: 2, allowed: true, remaining: 0},
	})
}

func reserveScenario(t *testing.T, f Factory, alg ratelimit.Algorithm) {
	clk := clock.NewFake(Start)
	l := f(t, alg, ratelimit.PerSecond(10).WithBurst(5), clk)
	r, ok := l.(ratelimit.Reserver)
	if !ok {
		t.Fatalf("%s does not implement Reserver", alg)
	}
	ctx := context.Background()
	ms := time.Millisecond

	if res, err := r.AllowN(ctx, "k", 5); err != nil || !res.Allowed {
		t.Fatalf("drain: %+v %v", res, err)
	}
	cases := []struct {
		maxWait time.Duration
		ok      bool
		delay   time.Duration
	}{
		{maxWait: 50 * ms, ok: false},
		{maxWait: 200 * ms, ok: true, delay: 100 * ms},
		{maxWait: time.Second, ok: true, delay: 200 * ms},
		{maxWait: 250 * ms, ok: false},
	}
	for i, c := range cases {
		got, err := r.Reserve(ctx, "k", 1, c.maxWait)
		if err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		if got.OK != c.ok || got.Delay != c.delay {
			t.Fatalf("reserve %d: got ok=%v delay=%s, want ok=%v delay=%s", i, got.OK, got.Delay, c.ok, c.delay)
		}
	}
	res, err := r.Allow(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed || res.RetryAfter != 300*ms {
		t.Fatalf("after reservations: %+v, want denied with retry 300ms", res)
	}
	clk.Advance(300 * ms)
	if res, _ := r.Allow(ctx, "k"); !res.Allowed {
		t.Fatalf("after waiting out reservations: %+v", res)
	}
}

func logScenario(t *testing.T, f Factory) {
	clk := clock.NewFake(Start)
	l := f(t, ratelimit.SlidingWindowLog, ratelimit.PerSecond(3), clk)
	ms := time.Millisecond
	runSteps(t, l, clk, "k", []step{
		{name: "t=0", allowed: true, remaining: 2, reset: time.Second},
		{name: "t=200", advance: 200 * ms, allowed: true, remaining: 1},
		{name: "t=400", advance: 200 * ms, allowed: true, remaining: 0},
		{name: "t=500 full", advance: 100 * ms, allowed: false, remaining: 0, retry: 500 * ms, reset: 900 * ms},
		{name: "t=999 still full", advance: 499 * ms, allowed: false, remaining: 0, retry: time.Millisecond},
		{name: "t=1000 oldest expired", advance: time.Millisecond, allowed: true, remaining: 0},
		{name: "t=1200 batch needs two slots", advance: 200 * ms, n: 2, allowed: false, remaining: 1, retry: 200 * ms},
		{name: "t=1400 batch fits", advance: 200 * ms, n: 2, allowed: true, remaining: 0},
	})
}

func counterScenario(t *testing.T, f Factory) {
	clk := clock.NewFake(Start)
	l := f(t, ratelimit.SlidingWindowCounter, ratelimit.PerSecond(10), clk)
	ms := time.Millisecond
	runSteps(t, l, clk, "k", []step{
		{name: "fill window", n: 10, allowed: true, remaining: 0, reset: 2 * time.Second},
		{name: "full", allowed: false, remaining: 0, retry: 1100 * ms},
		// At 1.1s the previous window still weighs 90%: 10*0.9 = 9.
		{name: "next window, 90% weight", advance: 1100 * ms, allowed: true, remaining: 0},
		{name: "estimate at limit", allowed: false, remaining: 0, retry: 100 * ms},
		// At 1.5s: 10*0.5 + 1 = 6.
		{name: "half weight", advance: 400 * ms, n: 4, allowed: true, remaining: 0},
		{name: "two windows idle", advance: 3 * time.Second, n: 10, allowed: true, remaining: 0},
	})
}

func independentKeys(t *testing.T, f Factory, alg ratelimit.Algorithm) {
	clk := clock.NewFake(Start)
	l := f(t, alg, ratelimit.PerSecond(2), clk)
	ctx := context.Background()
	for range 2 {
		if r, err := l.Allow(ctx, "a"); err != nil || !r.Allowed {
			t.Fatalf("a: %+v %v", r, err)
		}
	}
	if r, _ := l.Allow(ctx, "a"); r.Allowed {
		t.Fatal("a should be limited")
	}
	if r, err := l.Allow(ctx, "b"); err != nil || !r.Allowed {
		t.Fatalf("b should be unaffected: %+v %v", r, err)
	}
}

func invalidN(t *testing.T, f Factory, alg ratelimit.Algorithm) {
	clk := clock.NewFake(Start)
	l := f(t, alg, ratelimit.PerSecond(2), clk)
	ctx := context.Background()
	if _, err := l.AllowN(ctx, "k", 3); !errors.Is(err, ratelimit.ErrExceedsCapacity) {
		t.Fatalf("n>capacity: got %v", err)
	}
	if _, err := l.AllowN(ctx, "k", 0); !errors.Is(err, ratelimit.ErrInvalidN) {
		t.Fatalf("n=0: got %v", err)
	}
	if r, err := l.AllowN(ctx, "k", 2); err != nil || !r.Allowed {
		t.Fatalf("rejected calls must not consume: %+v %v", r, err)
	}
}
