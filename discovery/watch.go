package discovery

import (
	"context"
)

// WatchEvent is streamed to subscribers. Exactly one of Snapshot and Change
// is set. A Snapshot event means the subscriber's requested version was 0 or
// too old to continue incrementally; it must replace all local state with
// Snapshot rather than applying it as a delta.
type WatchEvent struct {
	Version  int64
	Snapshot *Snapshot
	Change   *Change
}

// Subscribe starts a change stream at fromVersion and returns the watcher.
// A fromVersion of 0 begins with a full snapshot; after a reconnect the
// caller passes the last version it fully observed. If that version has
// already been compacted, the first event is a fresh full snapshot, so no
// event range is ever silently skipped.
//
// The returned channel is closed when ctx is canceled. Subscribers should
// resubscribe with the last fully applied event version after any disconnect.
//
// partitions restricts the stream to instances in the named partitions; no
// partitions means every partition. A partition-scoped stream still uses the
// single global monotonic version: skipped versions belong to other
// partitions. If the client's version falls out of the retention window it
// receives one snapshot covering exactly its partitions at one consistent
// version, rather than a mix of stale per-partition tails.
func (r *Registry) Subscribe(ctx context.Context, fromVersion int64, partitions ...string) <-chan WatchEvent {
	ch := make(chan WatchEvent, 16)
	go r.watchLoop(ctx, fromVersion, partitionSet(partitions), ch)
	return ch
}

func (r *Registry) watchLoop(ctx context.Context, fromVersion int64, want map[string]bool, ch chan<- WatchEvent) {
	defer close(ch)
	last := fromVersion
	bootstrap := fromVersion == 0
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.cond.Broadcast()
			r.mu.Unlock()
		case <-stop:
		}
	}()

	// A fresh subscriber always receives a complete baseline first, even if
	// the directory (and version) is currently empty.
	if bootstrap {
		r.mu.Lock()
		r.expireStaleLocked(r.clock.Now())
		snap := r.snapshotLockedScoped(want)
		r.mu.Unlock()
		if !r.deliver(ctx, ch, WatchEvent{
			Version:  snap.Version,
			Snapshot: snap,
		}) {
			return
		}
		last = snap.Version
	}

	for {
		r.mu.Lock()
		for r.version == last && ctx.Err() == nil && !bootstrap {
			r.cond.Wait()
		}
		// bootstrap: fall through once to drain events since the empty baseline.
		bootstrap = false

		var events []WatchEvent
		switch {
		case ctx.Err() != nil:
			r.mu.Unlock()
			return
		case last > 0 && last < r.oldestVersionLocked():
			// History was compacted: resync from a full snapshot. This is the
			// only way to catch up, so no event range can be skipped.
			r.expireStaleLocked(r.clock.Now())
			snap := r.snapshotLockedScoped(want)
			r.mu.Unlock()
			if !r.deliver(ctx, ch, WatchEvent{Version: snap.Version, Snapshot: snap}) {
				return
			}
			last = snap.Version
			continue
		default:
			cur := r.version
			for _, c := range r.log {
				if c.Version > last && c.Version <= cur &&
					(want == nil || want[c.Instance.Partition]) {
					cc := c
					ev := WatchEvent{Version: cc.Version, Change: &cc}
					events = append(events, ev)
				}
			}
			r.mu.Unlock()
		}
		for i := range events {
			if !r.deliver(ctx, ch, events[i]) {
				return
			}
			last = events[i].Version
		}
	}
}

func (r *Registry) deliver(ctx context.Context, ch chan<- WatchEvent, ev WatchEvent) bool {
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}
