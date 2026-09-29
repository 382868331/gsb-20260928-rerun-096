package discovery

import (
	"context"
	"time"
)

// RunSweeper periodically expires dead leases until ctx is canceled. With a
// FakeClock it advances only when the clock does, making timing tests
// deterministic. interval defaults to 50ms when non-positive.
func (r *Registry) RunSweeper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.clock.After(interval):
			r.Sweep()
		}
	}
}
