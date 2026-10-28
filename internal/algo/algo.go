// Package algo holds the pure state transitions for every algorithm. Time is
// an abstract integer tick (nanoseconds in the memory backend). The Lua
// scripts in redislimit mirror these functions operation for operation so
// both backends make identical decisions; the conformance suite enforces it.
package algo

import "math"

// Decision is the outcome of a single state transition, in ticks.
type Decision struct {
	Allowed    bool
	Remaining  int64
	RetryAfter int64
	ResetAfter int64
	Delay      int64
}

// Bucket is token bucket state.
type Bucket struct {
	Tokens  float64
	Updated int64
	Init    bool
}

// Take refills the bucket to now and tries to remove n tokens. In reserve
// mode the bucket may go negative, which represents capacity promised to a
// caller that is waiting.
func (b *Bucket) Take(now, rate, period, burst, n int64, reserve bool, maxWait int64) Decision {
	fburst := float64(burst)
	if !b.Init {
		b.Tokens, b.Updated, b.Init = fburst, now, true
	}
	if now > b.Updated {
		b.Tokens = math.Min(fburst, b.Tokens+float64(now-b.Updated)*float64(rate)/float64(period))
		b.Updated = now
	}

	var d Decision
	fn := float64(n)
	if b.Tokens >= fn {
		b.Tokens -= fn
		d.Allowed = true
	} else if reserve {
		delay := ceil((fn - b.Tokens) * float64(period) / float64(rate))
		if delay <= maxWait {
			b.Tokens -= fn
			d.Allowed = true
			d.Delay = delay
		}
	}
	if !d.Allowed {
		d.RetryAfter = ceil((fn - b.Tokens) * float64(period) / float64(rate))
	}
	d.Remaining = int64(math.Floor(math.Max(b.Tokens, 0)))
	d.ResetAfter = ceil((fburst - b.Tokens) * float64(period) / float64(rate))
	return d
}

// GCRA is the generic cell rate algorithm state: the theoretical arrival
// time of the next conforming request.
type GCRA struct {
	TAT int64
}

// Take applies GCRA with the given emission interval and burst.
func (g *GCRA) Take(now, interval, burst, n int64, reserve bool, maxWait int64) Decision {
	tau := interval * burst
	tat := g.TAT
	if tat < now {
		tat = now
	}
	newTAT := tat + interval*n
	diff := newTAT - tau - now

	var d Decision
	switch {
	case diff <= 0:
		d.Allowed = true
	case reserve && diff <= maxWait:
		d.Allowed = true
		d.Delay = diff
	default:
		d.RetryAfter = diff
	}
	if d.Allowed {
		tat = newTAT
		g.TAT = newTAT
	}
	d.Remaining = max((tau-(tat-now))/interval, 0)
	d.ResetAfter = tat - now
	return d
}

// Log is sliding window log state: admitted timestamps in ascending order.
type Log struct {
	Stamps []int64
}

// Take evicts entries at or before now-window and admits n more if the
// window has room.
func (l *Log) Take(now, window, limit, n int64) Decision {
	cutoff := now - window
	i := 0
	for i < len(l.Stamps) && l.Stamps[i] <= cutoff {
		i++
	}
	// Reslicing keeps eviction O(1); append reallocates with only live
	// entries once capacity runs out, so the backing array stays bounded.
	l.Stamps = l.Stamps[i:]

	count := int64(len(l.Stamps))
	var d Decision
	if count+n <= limit {
		for range n {
			l.Stamps = append(l.Stamps, now)
		}
		d.Allowed = true
		d.Remaining = limit - count - n
		d.ResetAfter = window
		return d
	}
	d.Remaining = max(limit-count, 0)
	d.RetryAfter = l.Stamps[count+n-limit-1] + window - now
	d.ResetAfter = l.Stamps[count-1] + window - now
	return d
}

// Counter is sliding window counter state: counts for the current and
// previous fixed windows.
type Counter struct {
	Window int64
	Curr   int64
	Prev   int64
	Init   bool
}

// Take estimates the rolling count as prev*(overlap of the previous window)
// + curr and admits n more if the estimate leaves room.
func (c *Counter) Take(now, window, limit, n int64) Decision {
	idx := now / window
	if !c.Init {
		c.Window, c.Init = idx, true
	}
	switch {
	case idx == c.Window+1:
		c.Prev, c.Curr = c.Curr, 0
	case idx > c.Window+1:
		c.Prev, c.Curr = 0, 0
	}
	if idx > c.Window {
		c.Window = idx
	}
	start := c.Window * window
	elapsed := max(now-start, 0)

	// Compare in the scaled domain (multiplied by window) to keep the
	// arithmetic exact for realistic magnitudes.
	fw := float64(window)
	weighted := float64(c.Prev)*float64(window-elapsed) + float64(c.Curr)*fw
	need := limit - n

	var d Decision
	if weighted <= float64(need)*fw {
		c.Curr += n
		weighted += float64(n) * fw
		d.Allowed = true
	} else if c.Curr <= need {
		e := window - int64(math.Floor(float64(need-c.Curr)*fw/float64(c.Prev)))
		d.RetryAfter = start + e - now
	} else {
		e := window - int64(math.Floor(float64(need)*fw/float64(c.Curr)))
		d.RetryAfter = start + window + e - now
	}
	d.Remaining = max(int64(math.Floor((float64(limit)*fw-weighted)/fw)), 0)
	switch {
	case c.Curr > 0:
		d.ResetAfter = start + 2*window - now
	case c.Prev > 0:
		d.ResetAfter = start + window - now
	}
	return d
}

func ceil(x float64) int64 { return int64(math.Ceil(x)) }
