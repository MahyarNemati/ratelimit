// Command httpserver is a runnable demonstration of the ratelimit library.
//
// It serves two endpoints behind different limiters so you can watch the
// RateLimit-* headers change:
//
//	/          cheap, limited per IP
//	/expensive costs 5 units against the same budget
//	/healthz   exempt from limiting
//
// With -redis it uses the Redis backend, so several instances share one
// budget; without it, the in-memory backend limits this process only.
//
// Usage:
//
//	go run ./examples/httpserver -rate 5 -period 10s
//	go run ./examples/httpserver -redis localhost:6379
//
//	curl -i localhost:8080/
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MahyarNemati/ratelimit"
	"github.com/MahyarNemati/ratelimit/httplimit"
	"github.com/MahyarNemati/ratelimit/memory"
	"github.com/MahyarNemati/ratelimit/redislimit"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr      = flag.String("addr", ":8080", "listen address")
		redisAddr = flag.String("redis", "", "redis address; empty uses the in-memory backend")
		algName   = flag.String("algo", "token_bucket", "token_bucket, gcra, sliding_window_log or sliding_window_counter")
		rate      = flag.Int("rate", 5, "events allowed per period")
		period    = flag.Duration("period", 10*time.Second, "limit period")
		burst     = flag.Int("burst", 0, "burst size; 0 means same as rate")
	)
	flag.Parse()

	alg, err := ratelimit.ParseAlgorithm(*algName)
	if err != nil {
		return err
	}
	limit := ratelimit.Limit{Rate: *rate, Period: *period, Burst: *burst}
	if err := limit.Validate(); err != nil {
		return err
	}

	limiter, backend, err := newLimiter(*redisAddr, alg, limit)
	if err != nil {
		return err
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	limit429 := httplimit.Middleware(limiter,
		httplimit.WithKeyFunc(httplimit.IP),
		httplimit.WithSkip(func(r *http.Request) bool { return r.URL.Path == "/healthz" }),
		httplimit.WithCost(func(r *http.Request) int {
			if r.URL.Path == "/expensive" {
				return 5
			}
			return 1
		}),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "served %s (cost 1)\n", r.URL.Path)
	})
	mux.HandleFunc("/expensive", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "served %s (cost 5)\n", r.URL.Path)
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           limit429(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", *addr, "backend", backend, "algo", alg, "limit", limit.String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

func newLimiter(redisAddr string, alg ratelimit.Algorithm, limit ratelimit.Limit) (ratelimit.Limiter, string, error) {
	if redisAddr == "" {
		l, err := memory.New(alg, limit)
		return l, "memory", err
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, "", fmt.Errorf("ping redis at %s: %w", redisAddr, err)
	}
	l, err := redislimit.New(rdb, alg, limit)
	return l, "redis", err
}
