package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T, opts ...Option) (*Registry, *FakeClock) {
	t.Helper()
	clk := NewFakeClock(time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC))
	defaults := []Option{WithClock(clk), WithoutJanitor()}
	r := New(append(defaults, opts...)...)
	t.Cleanup(func() { _ = r.Close() })
	return r, clk
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestRegisterAndLookup(t *testing.T) {
	r, _ := newTestRegistry(t)
	reg, err := r.Register(RegisterInput{Service: "user", Endpoint: "10.0.0.1:8080", TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if reg.Instance.Identity == "" {
		t.Fatal("missing identity")
	}
	got := r.Lookup("user")
	if len(got) != 1 || got[0].Identity != reg.Instance.Identity {
		t.Fatalf("lookup = %+v", got)
	}
}

func TestHeartbeatRenewalAndExpiry(t *testing.T) {
	r, clk := newTestRegistry(t)
	reg, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: 5 * time.Second})

	clk.Advance(4 * time.Second)
	if _, err := r.Heartbeat(reg.Instance.Identity); err != nil {
		t.Fatalf("renew: %v", err)
	}
	clk.Advance(4 * time.Second)
	if n := r.Sweep(); n != 0 {
		t.Fatalf("renewed lease expired: %d", n)
	}
	if len(r.Lookup("s")) != 1 {
		t.Fatal("instance missing after renewal")
	}

	clk.Advance(6 * time.Second)
	if n := r.Sweep(); n != 1 {
		t.Fatalf("expected 1 expiry, got %d", n)
	}
	// Old heartbeat must not resurrect or renew the expired lease.
	if _, err := r.Heartbeat(reg.Instance.Identity); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound, got %v", err)
	}
	if len(r.Lookup("s")) != 0 {
		t.Fatal("expired instance still listed")
	}
}

func TestRestartGetsNewIdentityOldLeaseFenced(t *testing.T) {
	r, _ := newTestRegistry(t)
	first, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Minute})

	// Process "restarts" and registers again at the same endpoint.
	second, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Minute})
	if first.Instance.Identity == second.Instance.Identity {
		t.Fatal("restart reused identity")
	}

	// Old incarnation's heartbeat can never touch the new lease.
	if _, err := r.Heartbeat(first.Instance.Identity); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("old heartbeat: %v", err)
	}
	if _, err := r.Heartbeat(second.Instance.Identity); err != nil {
		t.Fatalf("new heartbeat: %v", err)
	}

	got := r.Lookup("s")
	if len(got) != 1 || got[0].Identity != second.Instance.Identity {
		t.Fatalf("directory after restart = %+v", got)
	}
}

func TestExpiryAndDeregisterRecordedSeparately(t *testing.T) {
	r, clk := newTestRegistry(t)
	// Establish a non-zero cursor so Poll returns the event log rather
	// than the fresh-subscriber snapshot.
	seed, _ := r.Register(RegisterInput{Service: "seed", Endpoint: "x", TTL: time.Hour})
	if err := r.Deregister(seed.Instance.Identity); err != nil {
		t.Fatal(err)
	}
	base := r.Version()
	a, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Second})
	b, _ := r.Register(RegisterInput{Service: "s", Endpoint: "b", TTL: time.Minute})
	if err := r.Deregister(b.Instance.Identity); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	r.Sweep()

	s := r.Stats()
	if s.Registered != 3 || s.Deregistered != 2 || s.Expired != 1 {
		t.Fatalf("stats = %+v", s)
	}

	_, evs, _, err := r.Poll(base)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[EventKind]int{}
	for _, ev := range evs {
		kinds[ev.Kind]++
	}
	if kinds[KindRegistered] != 2 || kinds[KindDeregistered] != 1 || kinds[KindExpired] != 1 {
		t.Fatalf("event kinds = %+v (events=%d)", kinds, len(evs))
	}
	// Events are contiguous and ordered.
	for i, ev := range evs {
		if ev.Version != base+uint64(i)+1 {
			t.Fatalf("non-contiguous versions at %d: %d (want %d)", i, ev.Version, base+uint64(i)+1)
		}
	}
	if evs[len(evs)-1].Instance.Identity != a.Instance.Identity || evs[len(evs)-1].Kind != KindExpired {
		t.Fatalf("last event = %+v", evs[len(evs)-1])
	}
}

func TestPollIncrementAndStaleSnapshot(t *testing.T) {
	r, _ := newTestRegistry(t, WithHistoryCapacity(4))
	r1, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Hour})
	_ = r1
	v2, _ := r.Register(RegisterInput{Service: "s", Endpoint: "b", TTL: time.Hour})

	// Continue from v1: get only the second registration.
	snap, evs, cur, err := r.Poll(1)
	if err != nil || snap != nil || cur != 2 || len(evs) != 1 {
		t.Fatalf("poll(1): snap=%v evs=%d cur=%d err=%v", snap, len(evs), cur, err)
	}
	if evs[0].Kind != KindRegistered || evs[0].Version != 2 {
		t.Fatalf("unexpected event %+v", evs[0])
	}

	// Up to date: empty delta.
	if _, evs, _, _ := r.Poll(2); len(evs) != 0 {
		t.Fatal("expected empty delta")
	}

	// Push versions past the retained window (4): versions 1..7.
	for i := 0; i < 5; i++ {
		_, _ = r.Register(RegisterInput{Service: "s", Endpoint: fmt.Sprintf("e%d", i), TTL: time.Hour})
	}
	snap2, evs2, _, err := r.Poll(v2.Version)
	if err != nil {
		t.Fatal(err)
	}
	if snap2 == nil {
		t.Fatal("too-far-behind poll must return snapshot, got events")
	}
	if len(evs2) != 0 {
		t.Fatal("snapshot response must not also carry events")
	}
	if len(snap2.Instances) != 7 {
		t.Fatalf("snapshot instances = %d", len(snap2.Instances))
	}

	// Resuming from snapshot version works and gives no duplicates.
	if _, evs3, _, _ := r.Poll(snap2.Version); len(evs3) != 0 {
		t.Fatal("events after snapshot version")
	}
}

func TestPollRejectsFutureVersion(t *testing.T) {
	r, _ := newTestRegistry(t)
	_, _, _, err := r.Poll(99)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v", err)
	}
}

func TestWatchResyncsWithSnapshotAfterCompaction(t *testing.T) {
	r, _ := newTestRegistry(t, WithHistoryCapacity(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := r.Watch(ctx, 0)
	<-ch // initial snapshot

	// Create events, then compact the log well past the watcher cursor.
	first, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Hour})
	<-ch // consume registered event, cursor advances
	for i := 0; i < 5; i++ {
		_, _ = r.Register(RegisterInput{Service: "s", Endpoint: fmt.Sprintf("x%d", i), TTL: time.Hour})
	}
	// Pause consumption to simulate a slow subscriber: the retained window
	// now starts beyond the cursor; the next update must be a snapshot.
	var got WatchUpdate
	waitFor(t, func() bool {
		select {
		case got = <-ch:
			return true
		default:
			return false
		}
	})
	if got.Snapshot == nil {
		t.Fatalf("expected resync snapshot, got event %+v", got)
	}
	if len(got.Snapshot.Instances) != 6 {
		t.Fatalf("resync snapshot size = %d", len(got.Snapshot.Instances))
	}
	// After resync, live events stream normally from the new cursor.
	if err := r.Deregister(first.Instance.Identity); err != nil {
		t.Fatalf("deregister after resync: %v", err)
	}
	u := <-ch
	if u.Event == nil || u.Event.Version <= got.Snapshot.Version {
		t.Fatalf("post-resync event = %+v", u)
	}
}

func TestWatchStreamsAndReconnectsWithoutGaps(t *testing.T) {
	r, _ := newTestRegistry(t, WithHistoryCapacity(8))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := r.Watch(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	upd := <-ch
	if upd.Snapshot == nil || upd.Snapshot.Version != 0 {
		t.Fatal("first update must be a snapshot")
	}

	reg, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Hour})
	upd = <-ch
	if upd.Event == nil || upd.Event.Kind != KindRegistered || upd.Event.Version != 1 {
		t.Fatalf("event = %+v", upd)
	}
	if err := r.Deregister(reg.Instance.Identity); err != nil {
		t.Fatal(err)
	}
	upd = <-ch
	if upd.Event == nil || upd.Event.Kind != KindDeregistered {
		t.Fatalf("event = %+v", upd)
	}
	last := upd.Event.Version

	// Simulate disconnect + reconnect from the last handled version.
	cancel()
	for range ch {
	}
	for i := 0; i < 3; i++ {
		_, _ = r.Register(RegisterInput{Service: "s", Endpoint: fmt.Sprintf("r%d", i), TTL: time.Hour})
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ch2, err := r.Watch(ctx2, last)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for upd := range ch2 {
		if upd.Snapshot != nil {
			t.Fatal("resume within window must not resnapshot")
		}
		seen++
		if seen == 3 {
			break
		}
	}
}

func TestWatchReceivesExpiryAndSupersededKinds(t *testing.T) {
	r, clk := newTestRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := r.Watch(ctx, 0)
	<-ch // snapshot v0

	_, _ = r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Second})
	b, _ := r.Register(RegisterInput{Service: "s", Endpoint: "b", TTL: 5 * time.Second})
	kinds := map[EventKind]bool{}
	for i := 0; i < 2; i++ {
		u := <-ch
		kinds[u.Event.Kind] = true
	}

	_, _ = r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: time.Hour}) // supersedes a
	r.Deregister(b.Instance.Identity)
	clk.Advance(6 * time.Second)
	r.Sweep() // nothing of a (new ttl); b already gone — covers expiry elsewhere

	r2, _ := r.Register(RegisterInput{Service: "s", Endpoint: "c", TTL: time.Second})
	_ = r2
	for i := 0; i < 4; i++ {
		u := <-ch
		kinds[u.Event.Kind] = true
	}
	clk.Advance(2 * time.Second)
	r.Sweep()
	u := <-ch
	if u.Event.Kind != KindExpired {
		t.Fatalf("expected expiry event, got %+v", u)
	}
	if !kinds[KindSuperseded] || !kinds[KindDeregistered] {
		t.Fatalf("missing kinds: %+v", kinds)
	}
}

func TestConcurrentHeartbeatsKeepLease(t *testing.T) {
	r := New() // real clock + janitor
	t.Cleanup(func() { _ = r.Close() })
	reg, _ := r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: 300 * time.Millisecond})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(40 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					if _, err := r.Heartbeat(reg.Instance.Identity); err != nil {
						t.Errorf("hb: %v", err)
						return
					}
				}
			}
		}()
	}
	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()
	if got := r.Lookup("s"); len(got) != 1 || got[0].Identity != reg.Instance.Identity {
		t.Fatalf("lease lost under concurrent renewal: %+v", got)
	}
}

func TestJanitorExpiresWithRealClock(t *testing.T) {
	r := New()
	t.Cleanup(func() { _ = r.Close() })
	_, _ = r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: 50 * time.Millisecond})
	waitFor(t, func() bool { return r.Stats().Expired == 1 })
	waitFor(t, func() bool { return len(r.Lookup("s")) == 0 })
}

func TestDeregisterUnknownIdentity(t *testing.T) {
	r, _ := newTestRegistry(t)
	// A stale identity (expired or superseded) can never close out a lease.
	if err := r.Deregister("deadbeef"); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("deregister unknown: %v", err)
	}
}
