// Package httplimit adapts a ratelimit.Limiter into net/http middleware.
//
// The middleware derives a key from each request, asks the limiter whether to
// admit it, and sets the RateLimit-* response headers described in the IETF
// draft "RateLimit header fields for HTTP" on every response — allowed or not.
//
// Rejected requests get 429 with Retry-After in seconds, rounded up so a
// client that obeys it is never early.
package httplimit

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MahyarNemati/ratelimit"
)

// ErrNoKey is returned by a KeyFunc that cannot derive a key. The middleware
// treats it as a client error and responds 400 rather than failing open.
var ErrNoKey = errors.New("httplimit: no key for request")

// KeyFunc derives the limiter key for a request. Returning an error rejects
// the request with 400; see ErrNoKey.
type KeyFunc func(*http.Request) (string, error)

// IP keys on the request's direct peer address, ignoring any forwarding
// headers. This is the safe default: forwarding headers are client-controlled
// and trusting them blindly lets a caller bypass the limiter by varying a
// header. Use ForwardedFor only behind a proxy you control.
func IP(r *http.Request) (string, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// RemoteAddr is not always host:port (httptest, unix sockets).
		if r.RemoteAddr == "" {
			return "", fmt.Errorf("%w: empty RemoteAddr", ErrNoKey)
		}
		return r.RemoteAddr, nil
	}
	return host, nil
}

// ForwardedFor keys on the client address from the X-Forwarded-For header,
// skipping the last trustedProxies entries because those were appended by
// infrastructure you control.
//
// With trustedProxies=1 and "client, proxy1" the key is "client". If the
// header is absent or has fewer entries than expected, it falls back to IP:
// a missing header must not become a shared key that every caller can claim.
func ForwardedFor(trustedProxies int) KeyFunc {
	if trustedProxies < 0 {
		panic("httplimit: trustedProxies must not be negative")
	}
	return func(r *http.Request) (string, error) {
		raw := r.Header.Values("X-Forwarded-For")
		var hops []string
		for _, v := range raw {
			for _, p := range strings.Split(v, ",") {
				if p = strings.TrimSpace(p); p != "" {
					hops = append(hops, p)
				}
			}
		}
		// Need at least one hop beyond the proxies we appended ourselves.
		if len(hops) <= trustedProxies {
			return IP(r)
		}
		return hops[len(hops)-1-trustedProxies], nil
	}
}

// Header keys on a request header, for limiting per API key or tenant.
func Header(name string) KeyFunc {
	return func(r *http.Request) (string, error) {
		v := r.Header.Get(name)
		if v == "" {
			return "", fmt.Errorf("%w: missing header %s", ErrNoKey, name)
		}
		return v, nil
	}
}

// Const keys every request the same way, for a global limit.
func Const(key string) KeyFunc {
	return func(*http.Request) (string, error) { return key, nil }
}

type options struct {
	key        KeyFunc
	cost       func(*http.Request) int
	onRejected http.Handler
	onError    func(http.ResponseWriter, *http.Request, error)
	failOpen   bool
	headers    bool
	skip       func(*http.Request) bool
}

// Option configures the middleware.
type Option func(*options)

// WithKeyFunc sets how the key is derived. Default: IP.
func WithKeyFunc(f KeyFunc) Option { return func(o *options) { o.key = f } }

// WithCost charges each request a variable number of units, for weighting
// expensive endpoints. A cost below 1 is treated as 1. Default: 1.
func WithCost(f func(*http.Request) int) Option { return func(o *options) { o.cost = f } }

// WithRejectedHandler replaces the default 429 response. RateLimit-* headers
// are already set when it runs.
func WithRejectedHandler(h http.Handler) Option { return func(o *options) { o.onRejected = h } }

// WithErrorHandler replaces the default handling of limiter failures (for
// example Redis being unreachable). Ignored when WithFailOpen is set.
func WithErrorHandler(f func(http.ResponseWriter, *http.Request, error)) Option {
	return func(o *options) { o.onError = f }
}

// WithFailOpen admits requests when the limiter itself errors, trading
// enforcement for availability. Off by default: a limiter that silently stops
// limiting is usually worse than a request that fails loudly.
func WithFailOpen(b bool) Option { return func(o *options) { o.failOpen = b } }

// WithHeaders controls whether RateLimit-* headers are set. Default: true.
func WithHeaders(b bool) Option { return func(o *options) { o.headers = b } }

// WithSkip exempts requests for which f returns true, for health checks.
func WithSkip(f func(*http.Request) bool) Option { return func(o *options) { o.skip = f } }

// Middleware returns net/http middleware enforcing l.
func Middleware(l ratelimit.Limiter, opts ...Option) func(http.Handler) http.Handler {
	if l == nil {
		panic("httplimit: nil limiter")
	}
	o := options{key: IP, headers: true}
	for _, opt := range opts {
		opt(&o)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if o.skip != nil && o.skip(r) {
				next.ServeHTTP(w, r)
				return
			}

			key, err := o.key(r)
			if err != nil {
				http.Error(w, "cannot determine rate limit key", http.StatusBadRequest)
				return
			}

			n := 1
			if o.cost != nil {
				if c := o.cost(r); c > 1 {
					n = c
				}
			}

			res, err := l.AllowN(r.Context(), key, n)
			if err != nil {
				// A request the limiter can never admit is a client error, not
				// an outage: retrying will not help, so say so plainly.
				if errors.Is(err, ratelimit.ErrExceedsCapacity) {
					http.Error(w, "request cost exceeds rate limit capacity", http.StatusBadRequest)
					return
				}
				if o.failOpen {
					next.ServeHTTP(w, r)
					return
				}
				if o.onError != nil {
					o.onError(w, r, err)
					return
				}
				http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				return
			}

			if o.headers {
				setHeaders(w.Header(), res)
			}

			if !res.Allowed {
				if o.onRejected != nil {
					o.onRejected.ServeHTTP(w, r)
					return
				}
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func setHeaders(h http.Header, res ratelimit.Result) {
	h.Set("RateLimit-Limit", strconv.Itoa(res.Limit))
	h.Set("RateLimit-Remaining", strconv.Itoa(res.Remaining))
	h.Set("RateLimit-Reset", strconv.FormatInt(seconds(res.ResetAfter), 10))
	if !res.Allowed {
		// Never advertise 0: a client reading Retry-After: 0 retries
		// immediately and is rejected again.
		retry := seconds(res.RetryAfter)
		if retry < 1 {
			retry = 1
		}
		h.Set("Retry-After", strconv.FormatInt(retry, 10))
	}
}

// seconds rounds d up to whole seconds, so a client obeying the value is
// never early. Negative and zero durations map to 0.
func seconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}
