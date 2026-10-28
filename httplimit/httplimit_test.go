package httplimit_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MahyarNemati/ratelimit"
	"github.com/MahyarNemati/ratelimit/clock"
	"github.com/MahyarNemati/ratelimit/httplimit"
	"github.com/MahyarNemati/ratelimit/memory"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

func newLimiter(t *testing.T, limit ratelimit.Limit, c clock.Clock) ratelimit.Limiter {
	t.Helper()
	l, err := memory.NewTokenBucket(limit, memory.WithClock(c))
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	return l
}

func do(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func req(remote string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = remote
	return r
}

func TestAdmitsUpToBurstThenRejects(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(newLimiter(t, ratelimit.PerMinute(3), fake))(okHandler())

	for i := 1; i <= 3; i++ {
		rec := do(h, req("1.2.3.4:5555"))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: got %d, want 200", i, rec.Code)
		}
		if got, want := rec.Header().Get("RateLimit-Remaining"), map[int]string{1: "2", 2: "1", 3: "0"}[i]; got != want {
			t.Errorf("request %d: RateLimit-Remaining = %q, want %q", i, got, want)
		}
	}

	rec := do(h, req("1.2.3.4:5555"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("4th request: got %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("RateLimit-Limit"); got != "3" {
		t.Errorf("RateLimit-Limit = %q, want \"3\"", got)
	}
	if got := rec.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q, want a positive whole number of seconds", got)
	}
}

func TestRetryAfterIsRoundedUpNeverZero(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	// 100/min refills one token every 600ms, so RetryAfter is sub-second.
	h := httplimit.Middleware(newLimiter(t, ratelimit.PerMinute(100).WithBurst(1), fake))(okHandler())

	if rec := do(h, req("9.9.9.9:1")); rec.Code != http.StatusOK {
		t.Fatalf("first request: got %d, want 200", rec.Code)
	}
	rec := do(h, req("9.9.9.9:1"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: got %d, want 429", rec.Code)
	}
	// Sub-second retry must round up to 1, not truncate to 0.
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want \"1\" (sub-second delay rounded up)", got)
	}
}

func TestKeysAreIndependentPerIP(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(newLimiter(t, ratelimit.PerMinute(1), fake))(okHandler())

	if rec := do(h, req("10.0.0.1:1")); rec.Code != http.StatusOK {
		t.Fatalf("first IP: got %d, want 200", rec.Code)
	}
	if rec := do(h, req("10.0.0.1:1")); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("first IP repeat: got %d, want 429", rec.Code)
	}
	// A different IP must have its own bucket.
	if rec := do(h, req("10.0.0.2:1")); rec.Code != http.StatusOK {
		t.Fatalf("second IP: got %d, want 200", rec.Code)
	}
}

func TestRefillAdmitsAgainAfterClockAdvance(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(newLimiter(t, ratelimit.PerMinute(1), fake))(okHandler())

	do(h, req("7.7.7.7:1"))
	if rec := do(h, req("7.7.7.7:1")); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("got %d, want 429", rec.Code)
	}
	fake.Advance(time.Minute)
	if rec := do(h, req("7.7.7.7:1")); rec.Code != http.StatusOK {
		t.Fatalf("after refill: got %d, want 200", rec.Code)
	}
}

func TestIPIgnoresForwardedHeaderByDefault(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(newLimiter(t, ratelimit.PerMinute(1), fake))(okHandler())

	r1 := req("1.1.1.1:1")
	r1.Header.Set("X-Forwarded-For", "spoof-a")
	if rec := do(h, r1); rec.Code != http.StatusOK {
		t.Fatalf("first: got %d, want 200", rec.Code)
	}
	// Varying a client-controlled header must not buy more capacity.
	r2 := req("1.1.1.1:1")
	r2.Header.Set("X-Forwarded-For", "spoof-b")
	if rec := do(h, r2); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("spoofed header bypassed the limiter: got %d, want 429", rec.Code)
	}
}

func TestForwardedForSkipsTrustedProxies(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(
		newLimiter(t, ratelimit.PerMinute(1), fake),
		httplimit.WithKeyFunc(httplimit.ForwardedFor(1)),
	)(okHandler())

	r1 := req("10.0.0.9:1")
	r1.Header.Set("X-Forwarded-For", "203.0.113.5, 10.0.0.9")
	if rec := do(h, r1); rec.Code != http.StatusOK {
		t.Fatalf("first: got %d, want 200", rec.Code)
	}
	// Same client, same proxy: must hit the same bucket.
	r2 := req("10.0.0.9:1")
	r2.Header.Set("X-Forwarded-For", "203.0.113.5, 10.0.0.9")
	if rec := do(h, r2); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("same client: got %d, want 429", rec.Code)
	}
	// Different client through the same proxy: own bucket.
	r3 := req("10.0.0.9:1")
	r3.Header.Set("X-Forwarded-For", "203.0.113.6, 10.0.0.9")
	if rec := do(h, r3); rec.Code != http.StatusOK {
		t.Fatalf("different client: got %d, want 200", rec.Code)
	}
}

func TestForwardedForFallsBackWhenHeaderMissing(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(
		newLimiter(t, ratelimit.PerMinute(1), fake),
		httplimit.WithKeyFunc(httplimit.ForwardedFor(1)),
	)(okHandler())

	// No header: must fall back to the peer address, not a shared empty key.
	if rec := do(h, req("192.0.2.1:1")); rec.Code != http.StatusOK {
		t.Fatalf("first peer: got %d, want 200", rec.Code)
	}
	if rec := do(h, req("192.0.2.2:1")); rec.Code != http.StatusOK {
		t.Fatalf("second peer shares a bucket with the first: got %d, want 200", rec.Code)
	}
}

func TestHeaderKeyFuncRejectsMissingHeader(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(
		newLimiter(t, ratelimit.PerMinute(10), fake),
		httplimit.WithKeyFunc(httplimit.Header("X-API-Key")),
	)(okHandler())

	if rec := do(h, req("1.1.1.1:1")); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing key header: got %d, want 400", rec.Code)
	}
	r := req("1.1.1.1:1")
	r.Header.Set("X-API-Key", "abc")
	if rec := do(h, r); rec.Code != http.StatusOK {
		t.Fatalf("with key header: got %d, want 200", rec.Code)
	}
}

func TestCostChargesMultipleUnits(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(
		newLimiter(t, ratelimit.PerMinute(10), fake),
		httplimit.WithCost(func(*http.Request) int { return 4 }),
	)(okHandler())

	for i := 1; i <= 2; i++ {
		if rec := do(h, req("5.5.5.5:1")); rec.Code != http.StatusOK {
			t.Fatalf("request %d: got %d, want 200", i, rec.Code)
		}
	}
	// 8 of 10 units spent; a third 4-unit request does not fit.
	if rec := do(h, req("5.5.5.5:1")); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third request: got %d, want 429", rec.Code)
	}
}

func TestSkipBypassesLimiter(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(
		newLimiter(t, ratelimit.PerMinute(1), fake),
		httplimit.WithSkip(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
	)(okHandler())

	for i := 0; i < 5; i++ {
		r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		r.RemoteAddr = "1.1.1.1:1"
		if rec := do(h, r); rec.Code != http.StatusOK {
			t.Fatalf("health check %d: got %d, want 200", i, rec.Code)
		}
	}
}

// failing is a Limiter that always errors, standing in for an unreachable Redis.
type failing struct{ err error }

func (f failing) Allow(ctx context.Context, key string) (ratelimit.Result, error) {
	return f.AllowN(ctx, key, 1)
}
func (f failing) AllowN(context.Context, string, int) (ratelimit.Result, error) {
	return ratelimit.Result{}, f.err
}

func TestLimiterErrorFailsClosedByDefault(t *testing.T) {
	h := httplimit.Middleware(failing{err: errors.New("redis down")})(okHandler())
	if rec := do(h, req("1.1.1.1:1")); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", rec.Code)
	}
}

func TestFailOpenAdmitsOnLimiterError(t *testing.T) {
	h := httplimit.Middleware(
		failing{err: errors.New("redis down")},
		httplimit.WithFailOpen(true),
	)(okHandler())
	if rec := do(h, req("1.1.1.1:1")); rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rec.Code)
	}
}

func TestExceedsCapacityIsClientError(t *testing.T) {
	h := httplimit.Middleware(
		failing{err: ratelimit.ErrExceedsCapacity},
		httplimit.WithFailOpen(true), // must not mask a permanent client error
	)(okHandler())
	if rec := do(h, req("1.1.1.1:1")); rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", rec.Code)
	}
}

func TestCustomRejectedHandlerSeesHeaders(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	custom := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if w.Header().Get("RateLimit-Limit") == "" {
			t.Error("RateLimit-Limit not set before rejected handler ran")
		}
		w.WriteHeader(http.StatusTeapot)
	})
	h := httplimit.Middleware(
		newLimiter(t, ratelimit.PerMinute(1), fake),
		httplimit.WithRejectedHandler(custom),
	)(okHandler())

	do(h, req("1.1.1.1:1"))
	if rec := do(h, req("1.1.1.1:1")); rec.Code != http.StatusTeapot {
		t.Fatalf("got %d, want 418", rec.Code)
	}
}

func TestWithHeadersFalseOmitsHeaders(t *testing.T) {
	fake := clock.NewFake(time.Unix(1000, 0))
	h := httplimit.Middleware(
		newLimiter(t, ratelimit.PerMinute(5), fake),
		httplimit.WithHeaders(false),
	)(okHandler())

	rec := do(h, req("1.1.1.1:1"))
	if got := rec.Header().Get("RateLimit-Limit"); got != "" {
		t.Errorf("RateLimit-Limit = %q, want empty", got)
	}
}
