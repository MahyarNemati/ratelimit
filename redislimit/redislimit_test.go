package redislimit

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/MahyarNemati/ratelimit"
	"github.com/MahyarNemati/ratelimit/clock"
	"github.com/MahyarNemati/ratelimit/internal/conformance"
)

func newMiniredis(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

// realRedis returns a client for REDIS_ADDR, or skips.
func realRedis(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("REDIS_ADDR=%s unreachable: %v", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func uniquePrefix(t testing.TB) string {
	return "test:" + t.Name() + ":" + time.Now().Format("150405.000000000") + ":"
}

func conformanceFactory(rdb Client) conformance.Factory {
	return func(t *testing.T, alg ratelimit.Algorithm, limit ratelimit.Limit, clk *clock.Fake) ratelimit.Limiter {
		l, err := New(rdb, alg, limit, WithClientClock(clk), WithPrefix(uniquePrefix(t)))
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
}

func TestConformanceMiniredis(t *testing.T) {
	_, rdb := newMiniredis(t)
	conformance.Run(t, conformanceFactory(rdb))
}

func TestConformanceRealRedis(t *testing.T) {
	conformance.Run(t, conformanceFactory(realRedis(t)))
}

func TestServerClock(t *testing.T) {
	mr, rdb := newMiniredis(t)
	mr.SetTime(conformance.Start)
	ctx := context.Background()
	for _, alg := range ratelimit.Algorithms {
		t.Run(string(alg), func(t *testing.T) {
			l, err := New(rdb, alg, ratelimit.PerSecond(2))
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if r, err := l.Allow(ctx, "k"); err != nil || !r.Allowed {
					t.Fatalf("%+v %v", r, err)
				}
			}
			r, err := l.Allow(ctx, "k")
			if err != nil || r.Allowed {
				t.Fatalf("expected denial: %+v %v", r, err)
			}
			if r.RetryAfter <= 0 || r.RetryAfter > 2*time.Second {
				t.Fatalf("retry after %s out of range", r.RetryAfter)
			}
			mr.SetTime(conformance.Start.Add(3 * time.Second))
			if r, err := l.Allow(ctx, "k"); err != nil || !r.Allowed {
				t.Fatalf("expected admission after server time advanced: %+v %v", r, err)
			}
			mr.SetTime(conformance.Start)
		})
	}
}

func TestKeysExpire(t *testing.T) {
	mr, rdb := newMiniredis(t)
	clk := clock.NewFake(conformance.Start)
	ctx := context.Background()
	for _, alg := range ratelimit.Algorithms {
		l, err := New(rdb, alg, ratelimit.PerSecond(10), WithClientClock(clk), WithPrefix("exp:"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Allow(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		key := "exp:" + string(alg) + ":k"
		ttl := mr.TTL(key)
		if ttl <= 0 || ttl > 2*time.Second {
			t.Errorf("%s: ttl %s, want a positive TTL bounded by the window", alg, ttl)
		}
	}
}

func TestRedisErrorsPropagate(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	l, err := NewGCRA(rdb, ratelimit.PerSecond(1))
	if err != nil {
		t.Fatal(err)
	}
	mr.Close()
	if _, err := l.Allow(context.Background(), "k"); err == nil {
		t.Fatal("expected an error with Redis down")
	}
}

func TestNilClient(t *testing.T) {
	if _, err := NewTokenBucket(nil, ratelimit.PerSecond(1)); err == nil {
		t.Fatal("expected error")
	}
	if _, err := New(nil, "nope", ratelimit.PerSecond(1)); err == nil {
		t.Fatal("expected error")
	}
	_, rdb := newMiniredis(t)
	if _, err := NewSlidingWindowLog(rdb, ratelimit.Limit{}); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("got %v", err)
	}
}

// TestDistributedCallers simulates several app instances, each with its own
// connection pool, sharing one limit through real Redis.
func TestDistributedCallers(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	prefix := uniquePrefix(t)
	for _, alg := range ratelimit.Algorithms {
		t.Run(string(alg), func(t *testing.T) {
			var admitted atomic.Int64
			var wg sync.WaitGroup
			for range 8 {
				rdb := redis.NewClient(&redis.Options{Addr: addr})
				defer rdb.Close()
				l, err := New(rdb, alg, ratelimit.PerMinute(200), WithPrefix(prefix))
				if err != nil {
					t.Fatal(err)
				}
				for range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range 25 {
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
			}
			wg.Wait()
			// 800 attempts within well under a second; refill adds at most a
			// handful of tokens for the bucket algorithms.
			if got := admitted.Load(); got < 200 || got > 205 {
				t.Fatalf("admitted %d, want 200 (+ a few refilled)", got)
			}
		})
	}
}

func lookupRealRedis() (string, bool) { return os.LookupEnv("REDIS_ADDR") }
