package discovery

import (
	"sync"
	"time"
)

// Clock abstracts time so expiry, lease and reconnect behavior can be
// exercised deterministically in tests.
type Clock interface {
	Now() time.Time
	// After waits for d relative to the clock's own progress. With a
	// FakeClock the channel fires only when the clock is advanced.
	After(d time.Duration) <-chan time.Time
}

// WallClock is backed by the real system clock.
type WallClock struct{}

func (WallClock) Now() time.Time { return time.Now() }
func (WallClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// timer is a handle to a pending clock callback.
type timer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// FakeClock is a manually controlled clock. Timers only fire when Advance
// moves the clock to (or past) their deadline. It is safe for concurrent
// use: advancing the clock and registering timers may happen from multiple
// goroutines, which lets tests drive concurrent renewals and reconnects.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*timer
}

// NewFakeClock returns a frozen clock starting at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that receives the clock time once the clock has
// been advanced to now+d.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &timer{deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t.ch
}

// Advance moves the clock forward by d and fires every timer whose deadline
// has been reached. Timers fire in deadline order; zero or negative duration
// is ignored.
func (c *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*timer, 0)
	remaining := c.timers[:0]
	for _, t := range c.timers {
		if !t.fired && !c.now.Before(t.deadline) {
			due = append(due, t)
		} else if !t.fired {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	now := c.now
	c.mu.Unlock()

	for _, t := range due {
		t.fired = true
		select {
		case t.ch <- now:
		default:
		}
	}
}
