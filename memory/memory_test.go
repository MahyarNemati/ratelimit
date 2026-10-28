package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MahyarNemati/ratelimit"
	"github.com/MahyarNemati/ratelimit/clock"
	"github.com/MahyarNemati/ratelimit/internal/conformance"
)

func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T, alg ratelimit.Algorithm, limit ratelimit.Limit, clk *clock.Fake) ratelimit.Limiter {
		l, err := New(alg, limit, WithClock(clk))
		if err != nil {
			t.Fatal(err)
		}
		return l
	})
}

func TestConcurrentCallersNeverExceedCapacity(t *testing.T) {
	for _, alg := range ratelimit.Algorithms {
		t.Run(string(alg), func(t *testing.T) {
			clk := clock.NewFake(conformance.Start)
			l, err := New(alg, ratelimit.PerMinute(100), WithClock(clk))
			if err != nil {
				t.Fatal(err)
			}
			var admitted atomic.Int64
			var wg sync.WaitGroup
			for range 16 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 50 {
						r, err := l.Allow(context.Background(), "shared")
						if err != nil {
							t.Error(err)
							return
						}
						if r.Allowed {
							admitted.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			if got := admitted.Load(); got != 100 {
				t.Fatalf("admitted %d, want exactly 100", got)
			}
		})
	}
}

func TestIdleKeysAreEvicted(t *testing.T) {
	clk := clock.NewFake(conformance.Start)
	l, err := NewTokenBucket(ratelimit.PerSecond(10), WithClock(clk), WithSweepInterval(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if _, err := l.Allow(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	if n := l.store.len(); n != 8 {
		t.Fatalf("store has %d keys, want 8", n)
	}
	// Each shard sweeps on its next access after the interval; touch enough
	// keys to visit every shard.
	clk.Advance(2 * time.Second)
	for i := range 4096 {
		_, _ = l.Allow(ctx, string(rune('A'+i%26))+string(rune(i)))
	}
	clk.Advance(2 * time.Second)
	for i := range 4096 {
		_, _ = l.Allow(ctx, "z"+string(rune(i)))
	}
	if n := l.store.len(); n != 4096 {
		t.Fatalf("store has %d keys after sweep, want only the 4096 live ones", n)
	}
}

func TestInvalidLimit(t *testing.T) {
	bad := []ratelimit.Limit{
		{Rate: 0, Period: time.Second},
		{Rate: 1, Period: 0},
		{Rate: 1, Period: time.Second, Burst: -1},
		{Rate: 2_000_000, Period: time.Second},
	}
	for _, lim := range bad {
		for _, alg := range ratelimit.Algorithms {
			if _, err := New(alg, lim); err == nil {
				t.Errorf("%s %+v: expected error", alg, lim)
			}
		}
	}
	if _, err := New("leaky", ratelimit.PerSecond(1)); err == nil {
		t.Error("unknown algorithm accepted")
	}
}

func TestWait(t *testing.T) {
	l, err := NewGCRA(ratelimit.PerSecond(50).WithBurst(1))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	start := time.Now()
	for range 3 {
		if err := ratelimit.Wait(ctx, l, "k", 1); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("three waits at 50/s finished in %s, expected at least ~40ms", elapsed)
	}

	short, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	if err := ratelimit.Wait(short, l, "k", 1); err != ratelimit.ErrWaitExceedsDeadline {
		t.Fatalf("got %v, want ErrWaitExceedsDeadline", err)
	}
}
