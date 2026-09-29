package discovery

import (
	"context"
	"fmt"
	"sort"
)

// WatchUpdate is either a full snapshot (the subscriber is too far
// behind or starting fresh) or one event. After a Snapshot update the
// subscriber continues from Snapshot.Version; events always follow in
// strictly increasing version order.
type WatchUpdate struct {
	Snapshot *Snapshot
	Event    *Event
}

// Version returns the current directory version.
func (r *Registry) Version() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.version
}

// Stats returns cumulative lifecycle counters.
func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// Lookup returns the live instances of a service ordered by endpoint.
func (r *Registry) Lookup(service string) []InstanceInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(r.clock.Now())
	out := make([]InstanceInfo, 0)
	for _, id := range r.slots[service] {
		if rec, ok := r.records[id]; ok {
			out = append(out, rec.info.clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Endpoint < out[j].Endpoint })
	return out
}

// Snapshot returns the complete directory at its current version.
func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(r.clock.Now())
	return r.snapshotLocked()
}

func (r *Registry) snapshotLocked() Snapshot {
	out := make([]InstanceInfo, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec.info.clone())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		return out[i].Endpoint < out[j].Endpoint
	})
	return Snapshot{Version: r.version, Instances: out}
}

func needsSnapshot(fromVersion, curVersion, oldest uint64, hasHistory bool) bool {
	if fromVersion == 0 {
		return true
	}
	if fromVersion > curVersion {
		return false // future versions are rejected by the caller
	}
	if !hasHistory {
		return fromVersion < curVersion
	}
	// Events older than oldest have been compacted away. Only when at
	// least one retained-window predecessor is missing do we need a
	// full snapshot; cursor == oldest-1 simply resumes at oldest.
	return fromVersion+1 < oldest
}

// Poll performs a non-blocking incremental fetch from fromVersion
// (exclusive). If fromVersion is 0 the caller starts fresh, or if it is
// older than the retained event window the caller is too far behind: in
// both cases a full snapshot is returned instead of events, so changes
// can never be silently skipped. When up to date, events is empty and
// currentVersion equals fromVersion.
//
// This is the call subscribers use after a reconnect: pass the last
// version they handled.
func (r *Registry) Poll(fromVersion uint64) (snap *Snapshot, events []Event, currentVersion uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, 0, ErrClosed
	}
	r.sweepLocked(r.clock.Now())
	if fromVersion > r.version {
		return nil, nil, r.version, fmt.Errorf("%w: version %d is ahead of current %d", ErrInvalidArgument, fromVersion, r.version)
	}
	if needsSnapshot(fromVersion, r.version, r.logFirst, len(r.log) > 0) {
		s := r.snapshotLocked()
		return &s, nil, s.Version, nil
	}
	events = r.eventsAfterLocked(fromVersion)
	return nil, events, r.version, nil
}

func (r *Registry) eventsAfterLocked(fromVersion uint64) []Event {
	var out []Event
	for i := range r.log {
		if r.log[i].Version > fromVersion {
			i0 := i
			out = make([]Event, len(r.log)-i0)
			copy(out, r.log[i0:])
			break
		}
	}
	return out
}

type liveWatcher struct {
	cursor uint64
	wake   chan struct{}
}

const watchWakeBuffer = 1

// Watch streams directory changes starting after fromVersion. The channel
// first delivers a Snapshot when starting fresh or when fromVersion is
// older than the retained window; otherwise it delivers every retained
// event after fromVersion followed by live events, in order, with no
// gaps. Slow consumers apply back-pressure instead of losing updates.
//
// On disconnect the subscriber simply calls Watch again (or Poll) with
// the last fully handled version. The channel is closed when ctx ends.
func (r *Registry) Watch(ctx context.Context, fromVersion uint64) (<-chan WatchUpdate, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	if fromVersion > r.version {
		r.mu.Unlock()
		return nil, fmt.Errorf("%w: version %d is ahead of current %d", ErrInvalidArgument, fromVersion, r.version)
	}

	updates := make(chan WatchUpdate, 256)

	var initialSnap *Snapshot
	var snapVersion uint64
	if needsSnapshot(fromVersion, r.version, r.logFirst, len(r.log) > 0) {
		snap := r.snapshotLocked()
		initialSnap = &snap
		snapVersion = snap.Version
	}

	// Do not register for live fan-out until the initial snapshot has
	// been delivered; otherwise events appended in the meantime would be
	// duplicated (once in the backlog, once as a wake-driven delta).
	r.mu.Unlock()

	if initialSnap != nil {
		select {
		case updates <- WatchUpdate{Snapshot: initialSnap}:
		case <-ctx.Done():
			close(updates)
			return updates, nil
		}
		fromVersion = snapVersion
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		close(updates)
		return updates, ErrClosed
	}
	w := &liveWatcher{cursor: fromVersion, wake: make(chan struct{}, watchWakeBuffer)}
	backlog := r.eventsAfterLocked(fromVersion)
	w.cursor = r.version
	r.watchers[w] = struct{}{}
	r.mu.Unlock()

	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.watchers, w)
			r.mu.Unlock()
			close(updates)
		}()

		send := func(u WatchUpdate) bool {
			select {
			case updates <- u:
				return true
			case <-ctx.Done():
				return false
			}
		}

		for i := range backlog {
			ev := backlog[i]
			if !send(WatchUpdate{Event: &ev}) {
				return
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
			}
			r.mu.Lock()
			// Even while streaming, a slow consumer can fall behind the
			// compacted window. Resync with a fresh snapshot instead of
			// silently skipping the versions that were discarded.
			var delta []Event
			var resync *Snapshot
			// A live watcher at the tip must not be resynced merely
			// because the log was compacted; resync only when at least
			// one event between cursor and the retained window is lost.
			trailing := len(r.log) > 0 && w.cursor+1 < r.logFirst
			if trailing {
				s := r.snapshotLocked()
				resync = &s
				w.cursor = s.Version
			} else {
				delta = r.eventsAfterLocked(w.cursor)
				w.cursor = r.version
			}
			r.mu.Unlock()
			if resync != nil {
				if !send(WatchUpdate{Snapshot: resync}) {
					return
				}
				continue
			}
			if len(delta) == 0 {
				// Spurious/stale wake: keep waiting for the next one.
				continue
			}
			for i := range delta {
				ev := delta[i]
				if !send(WatchUpdate{Event: &ev}) {
					return
				}
			}
		}
	}()

	return updates, nil
}
