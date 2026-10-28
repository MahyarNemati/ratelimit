package redislimit

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/MahyarNemati/ratelimit"
)

func benchBackends(b *testing.B) map[string]*redis.Client {
	_, mini := newMiniredis(b)
	out := map[string]*redis.Client{"miniredis": mini}
	if _, ok := lookupRealRedis(); ok {
		out["redis"] = realRedis(b)
	}
	return out
}

func BenchmarkAllow(b *testing.B) {
	ctx := context.Background()
	for name, rdb := range benchBackends(b) {
		for _, alg := range ratelimit.Algorithms {
			b.Run(name+"/"+string(alg)+"/serial", func(b *testing.B) {
				l, err := New(rdb, alg, ratelimit.PerSecond(1_000_000), WithPrefix(uniquePrefix(b)))
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := l.Allow(ctx, "k"); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(name+"/"+string(alg)+"/parallel_1k_keys", func(b *testing.B) {
				l, err := New(rdb, alg, ratelimit.PerSecond(100), WithPrefix(uniquePrefix(b)))
				if err != nil {
					b.Fatal(err)
				}
				var ctr atomic.Uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := ctr.Add(1) * 7919
					for pb.Next() {
						if _, err := l.Allow(ctx, "user:"+strconv.FormatUint(i%1000, 10)); err != nil {
							b.Error(err)
							return
						}
						i++
					}
				})
			})
		}
	}
}
