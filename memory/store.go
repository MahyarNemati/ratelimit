package memory

import (
	"hash/maphash"
	"sync"
	"time"
)

const shardCount = 64

// store is a sharded map of per-key state. Expired entries are swept
// lazily: each shard scans itself at most once per sweepEvery, on the
// request path, so there is no background goroutine to manage.
type store[S any] struct {
	seed       maphash.Seed
	sweepEvery int64
	shards     [shardCount]shard[S]
}

type shard[S any] struct {
	mu        sync.Mutex
	items     map[string]*item[S]
	nextSweep int64
}

type item[S any] struct {
	state   S
	expires int64
}

func newStore[S any](sweepEvery time.Duration) *store[S] {
	s := &store[S]{seed: maphash.MakeSeed(), sweepEvery: int64(sweepEvery)}
	for i := range s.shards {
		s.shards[i].items = make(map[string]*item[S])
	}
	return s
}

// update runs fn on the state for key under the shard lock. fn returns how
// long, in nanoseconds from now, the state remains meaningful; after that it
// is indistinguishable from a fresh key and may be evicted.
func (s *store[S]) update(key string, now int64, fn func(*S) int64) {
	sh := &s.shards[maphash.String(s.seed, key)%shardCount]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if now >= sh.nextSweep {
		for k, it := range sh.items {
			if it.expires <= now {
				delete(sh.items, k)
			}
		}
		sh.nextSweep = now + s.sweepEvery
	}

	it, ok := sh.items[key]
	if !ok || it.expires <= now {
		it = &item[S]{}
		sh.items[key] = it
	}
	ttl := fn(&it.state)
	it.expires = now + max(ttl, 1)
}

func (s *store[S]) len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		n += len(sh.items)
		sh.mu.Unlock()
	}
	return n
}
