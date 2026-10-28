// Package clock provides an injectable time source so limiter behavior can be
// tested deterministically.
package clock

import (
	"sync"
	"time"
)

// Clock reports the current time.
type Clock interface {
	Now() time.Time
}

// Real is a Clock backed by time.Now.
type Real struct{}

// Now returns time.Now().
func (Real) Now() time.Time { return time.Now() }

// Fake is a manually advanced Clock. The zero value starts at the Unix epoch.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake clock set to t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.now.IsZero() {
		return time.Unix(0, 0)
	}
	return f.now
}

// Advance moves the clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.now.IsZero() {
		f.now = time.Unix(0, 0)
	}
	f.now = f.now.Add(d)
}

// Set moves the clock to t.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}
