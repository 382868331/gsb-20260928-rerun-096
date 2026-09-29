package discovery

import (
	"sort"
	"sync"
	"time"
)

// Clock is the time source used by the registry. Production code uses
// RealClock; tests use FakeClock and advance time deterministically.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// RealClock delegates to the wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// FakeTimer is a one-shot timer driven by FakeClock.Advance.
type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// FakeClock is a manually controlled clock. Timers registered with After
// fire when Advance moves the clock to or past their deadline.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock returns a fake clock initialized at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	// A single goroutine (the janitor) owns its timer chain; a new
	// registration supersedes any unfired prior timer.
	c.timers = c.timers[:0]
	c.timers = append(c.timers, t)
	return t.ch
}

// Advance moves the clock forward by d and fires every timer that is due.
// Timer callbacks are delivered on buffered channels, so callers may
// Advance without anyone receiving yet.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0)
	remaining := c.timers[:0]
	for _, t := range c.timers {
		if !t.deadline.After(c.now) {
			due = append(due, t)
		} else {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	c.mu.Unlock()

	sort.Slice(due, func(i, j int) bool { return due[i].deadline.Before(due[j].deadline) })
	for _, t := range due {
		select {
		case t.ch <- t.deadline:
		default:
		}
	}
}
