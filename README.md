# ratelimit

[![CI](https://github.com/MahyarNemati/ratelimit/actions/workflows/ci.yml/badge.svg)](https://github.com/MahyarNemati/ratelimit/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/MahyarNemati/ratelimit.svg)](https://pkg.go.dev/github.com/MahyarNemati/ratelimit)
[![Go Report Card](https://goreportcard.com/badge/github.com/MahyarNemati/ratelimit)](https://goreportcard.com/report/github.com/MahyarNemati/ratelimit)

Four rate limiting algorithms behind one interface, with an in-memory backend for
single-process limits and a Redis backend for distributed limits. The two backends
are held to **identical decisions** by a shared conformance suite.

```go
limiter, _ := memory.NewTokenBucket(ratelimit.PerMinute(100).WithBurst(10))

res, err := limiter.Allow(ctx, userID)
if err != nil {
    return err
}
if !res.Allowed {
    w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())))
    http.Error(w, "slow down", http.StatusTooManyRequests)
    return
}
```

## Install

```bash
go get github.com/MahyarNemati/ratelimit
```

Requires Go 1.24+. The Redis backend needs Redis 3.2+ (it uses `redis.call` in Lua
with `replicate_commands`-safe patterns) and go-redis v9.

## Why this exists

Most rate limiter libraries give you one algorithm and one backend. In practice you
need to switch: a token bucket is right for a public API, GCRA is right when you
care about smoothness, a sliding window log is right when a compliance rule says
"no more than N in any 60 seconds" and you must mean it literally. Swapping
algorithms should be a config change, not a rewrite — and moving from one process to
many should not silently change the limit you enforce.

That second point is the one that bites. Hand-rolled Redis limiters usually do
read-modify-write across two round trips, which means two pods can both read
"9 of 10 used" and both admit. Every algorithm here runs as a **single Lua script**,
so the decision is atomic server-side.

## Layout

```
ratelimit.go              Limit, Result, Limiter, Reserver, Wait
clock/                    injectable time source (Real, Fake)
internal/algo/            pure state transitions, shared by both backends
internal/conformance/     one test suite both backends must pass
memory/                   sharded in-memory backend (64 shards)
redislimit/               Redis backend, one atomic Lua script per algorithm
redislimit/scripts/       the Lua sources, embedded at build time
httplimit/                net/http middleware with RateLimit-* headers
examples/httpserver/      runnable demo
```

### How the two backends stay honest

`internal/algo` holds the algorithms as pure functions over integer ticks. The Lua
scripts mirror those functions operation for operation, and
`internal/conformance` runs the same scenario table against both backends with a
fake clock — same inputs, same `Allowed`/`Remaining`/`RetryAfter` out. If a Lua
script drifts from its Go counterpart, conformance fails.

This is the part worth copying if you build something similar. "Both backends
implement the same interface" is weak; a type checker cannot tell you that your Lua
rounds differently from your Go. A shared scenario table can.

## Choosing an algorithm

The short version:

| | Token bucket | GCRA | Sliding window log | Sliding window counter |
|---|---|---|---|---|
| Allows bursts | yes, up to `Burst` | yes, up to `Burst` | yes, within window | yes, approximately |
| Shapes traffic | no | **yes, smooth** | no | no |
| Window boundary spikes | no | no | no | **dampened, not eliminated** |
| Memory per key | O(1) — 2 fields | **O(1) — 1 field** | **O(N) — N timestamps** | O(1) — 2 counters |
| Exact | yes | yes | **yes, literally** | **no, approximate** |
| Supports `Reserve`/`Wait` | yes | yes | no | no |
| Good default | **✅** | smoothing | strict compliance | high cardinality |

### Token bucket — the default

Tokens refill continuously at `Rate/Period` up to `Burst`. A request takes `n`
tokens or is rejected.

**Pick it when** you want a sane default. Bursts are allowed, which matches how real
clients behave: a page load fires six requests at once and then goes quiet.

**Trade-off:** it permits a full burst immediately after a quiet period. If your
upstream cannot absorb `Burst` concurrent requests, token bucket will hand it
`Burst` concurrent requests. Cap `Burst` to what the dependency can actually take,
not to what feels generous.

State: `{tokens float64, updated int64}`.

### GCRA — when you want smoothness

The generic cell rate algorithm tracks a single "theoretical arrival time" (TAT) —
the earliest moment the next request conforms. Instead of a pool that drains, it
enforces spacing of `Period/Rate` between requests, with `Burst * interval` of
tolerance.

**Pick it when** the thing you are protecting cares about *pacing*, not just totals:
a downstream API with its own limiter, an SMS gateway, anything where 100 requests
in one second and 100 requests spread over a minute are very different events.

**Trade-off:** harder to explain. "You have 7 requests left" is a natural sentence;
"your next conforming request is at T+340ms" is not. Expect support questions. It
also rejects a perfectly reasonable-looking second request if it arrives too soon
after the first, which surprises people reading `RateLimit-Remaining`.

Cheapest state of all four: one integer.

### Sliding window log — when "any 60 seconds" is literal

Stores a timestamp per request and counts how many fall inside the trailing window.
This is the only algorithm here with **no** boundary artifacts: the answer to "how
many in the last 60 seconds" is exactly right because it is counted, not inferred.

**Pick it when** a contract or regulation says *no more than N in any rolling
window* and you need to be able to defend the number.

**Trade-off:** memory is O(N) per key, where N is the limit. At 10 requests/minute
that is nothing. At 10,000 requests/minute across 100,000 keys, you are storing a
billion timestamps in Redis and the sorted-set trim becomes the hot path. There is
no `Reserve` either — you cannot meaningfully reserve a slot in a log of the past.

Measure before choosing this at scale. Then measure again.

### Sliding window counter — the cheap approximation

Two fixed buckets — current and previous — with the previous one weighted by how
far into the current window you are. O(1) state, no boundary cliff.

**Pick it when** you have very high key cardinality and can accept approximation.

**Trade-off:** it is **wrong on purpose**, and it is important to know in which
direction. The weighting assumes requests in the previous window were spread evenly.
When they were actually clustered at its end, the estimate is too low and you admit
more than the nominal limit — up to roughly 2x in a crafted worst case, right at a
window boundary. That is usually fine for abuse mitigation and not fine for
billing or quota enforcement.

The fixed-window limiter it replaces is worse: that one admits `2 * Rate` across a
boundary with no cleverness required at all, which is why it is not implemented here.

## Backends

### In-memory

```go
l, err := memory.NewTokenBucket(ratelimit.PerSecond(1000))
```

64 shards keyed by xxhash to keep lock contention off the hot path, with a
background sweep reclaiming idle keys. Use `memory.WithSweepInterval` to tune
reclamation and `memory.WithClock` to inject a fake clock in tests.

**Limit:** per process. Three replicas behind a load balancer enforce 3x your limit.
That is sometimes acceptable — say so deliberately rather than discovering it in
production.

### Redis

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
l, err := redislimit.NewTokenBucket(rdb, ratelimit.PerMinute(100))
```

One atomic Lua script per algorithm, embedded with `go:embed` and run via `EVALSHA`.
One round trip per decision, no read-modify-write race.

Notes worth knowing:

- **Time comes from Redis** (`redis.call("TIME")`) by default, so replicas with
  skewed clocks still agree. `WithClientClock` overrides this for tests.
- **Keys carry a TTL** derived from the limit, so idle keys expire instead of leaking.
- **Cluster:** all keys for one decision are in one slot, so it is cluster-safe.
  Prefix keys with `WithPrefix` to share a Redis instance across services.
- **Availability:** a Redis outage makes `Allow` return an error. Decide explicitly
  whether to fail open or closed — `httplimit` fails **closed** by default, and
  `httplimit.WithFailOpen(true)` flips it.

## HTTP middleware

```go
mw := httplimit.Middleware(limiter,
    httplimit.WithKeyFunc(httplimit.Header("X-API-Key")),
    httplimit.WithCost(func(r *http.Request) int {
        if r.URL.Path == "/search" { return 10 }
        return 1
    }),
    httplimit.WithSkip(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
)
http.ListenAndServe(":8080", mw(mux))
```

Sets `RateLimit-Limit`, `RateLimit-Remaining` and `RateLimit-Reset` on every
response, plus `Retry-After` on a 429. `Retry-After` is **rounded up** to whole
seconds and never emitted as `0`, so a client that obeys it is never early and never
hot-loops.

### On keying by IP

`httplimit.IP` deliberately ignores `X-Forwarded-For`. That header is
client-controlled; trusting it blindly means an attacker varies one header and gets
unlimited capacity. There is a test asserting exactly that.

Behind a proxy you control, opt in and say how many hops are yours:

```go
httplimit.WithKeyFunc(httplimit.ForwardedFor(1)) // one trusted proxy
```

If the header is missing or shorter than expected it falls back to the peer address
rather than collapsing every caller onto one shared empty key.

## Run the demo

```bash
go run ./examples/httpserver -rate 3 -period 10s

curl -i localhost:8080/          # 200, RateLimit-Remaining: 2
curl -i localhost:8080/expensive # costs 5 units
curl -i localhost:8080/healthz   # never limited
```

With Redis, to see several instances share one budget:

```bash
docker run -p 6379:6379 -d redis:7-alpine
go run ./examples/httpserver -redis localhost:6379 -addr :8080 &
go run ./examples/httpserver -redis localhost:6379 -addr :8081 &
```

## Testing and benchmarks

```bash
make test        # unit + conformance, race detector on
make bench       # benchmarks for both backends
make lint        # go vet + gofmt check
```

Redis tests run against [miniredis](https://github.com/alicebob/miniredis) by
default, so `go test ./...` needs no running server. Point `REDIS_ADDR` at a real
instance to run them there instead — worth doing before trusting the Lua, since
miniredis implements its own Lua.

```bash
REDIS_ADDR=localhost:6379 go test ./redislimit/...
```

Benchmarks are `BenchmarkAllow` in `memory` and `redislimit`. Run them on your own
hardware — published numbers from someone else's laptop are decoration. What to look
for: memory should be tens of nanoseconds per `Allow` and flat as you add keys;
Redis should be dominated entirely by one network round trip, which tells you the
script is not the bottleneck.

## License

MIT — see [LICENSE](LICENSE).
