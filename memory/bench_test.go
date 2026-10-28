package memory

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/MahyarNemati/ratelimit"
)

func BenchmarkAllow(b *testing.B) {
	ctx := context.Background()
	for _, alg := range ratelimit.Algorithms {
		b.Run(string(alg)+"/single_key", func(b *testing.B) {
			l, err := New(alg, ratelimit.PerSecond(1_000_000).WithBurst(1_000_000))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = l.Allow(ctx, "k")
			}
		})
		b.Run(string(alg)+"/parallel_10k_keys", func(b *testing.B) {
			l, err := New(alg, ratelimit.PerSecond(100))
			if err != nil {
				b.Fatal(err)
			}
			keys := make([]string, 10_000)
			for i := range keys {
				keys[i] = "user:" + strconv.Itoa(i)
			}
			var ctr atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := ctr.Add(1) * 7919
				for pb.Next() {
					_, _ = l.Allow(ctx, keys[i%uint64(len(keys))])
					i++
				}
			})
		})
	}
}
