package discovery

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A lease that has lapsed but not yet been swept cannot be made ready or
// draining: the attempt expires it inline, emits the expiry event, and the
// instance never re-enters the routable set.
func TestLapsedLeaseCannotChangeState(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	h, _ := reg.Register(Instance{Service: "s", ID: "a", Partition: "p"}, time.Second)
	clk.Advance(2 * time.Second) // lapsed, sweeper has not run

	if _, err := reg.SetReady("s", "a", h.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ready on lapsed lease err = %v", err)
	}
	if got := reg.ReadyInstances("p"); len(got) != 0 {
		t.Fatalf("lapsed instance routable: %+v", got)
	}
	if rec, ok := reg.Removal("s", "a"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("removal = %+v ok=%v", rec, ok)
	}
	// The failed transition produced exactly the expiry event, no ready event.
	res, _ := reg.ChangesSince(h.Version)
	if len(res.Changes) != 1 || res.Changes[0].Kind != ChangeExpired ||
		res.Changes[0].Reason != ReasonExpired {
		t.Fatalf("changes = %+v", res.Changes)
	}

	// Same for Drain and Deregister on a second lapsed instance.
	h2, _ := reg.Register(Instance{Service: "s", ID: "b"}, time.Second)
	clk.Advance(2 * time.Second)
	if _, err := reg.Drain("s", "b", h2.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("drain on lapsed lease err = %v", err)
	}
	if err := reg.Deregister("s", "b", h2.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deregister on lapsed lease err = %v", err)
	}
	if rec, ok := reg.Removal("s", "b"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("lapsed deregister recorded as %+v ok=%v", rec, ok)
	}
}

// Read paths observe lease validity at query time: lapsed instances are
// invisible to ready sets, snapshots and partition-scoped reads even when
// no sweeper ever runs.
func TestQueriesObserveLeaseValidity(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	live, _ := reg.Register(Instance{Service: "s", ID: "live", Partition: "p1"}, time.Minute)
	stale, _ := reg.Register(Instance{Service: "s", ID: "stale", Partition: "p2"}, time.Second)
	reg.SetReady("s", "live", live.Token)
	readyV, _ := reg.SetReady("s", "stale", stale.Token)
	clk.Advance(2 * time.Second)

	if got := reg.ReadyInstances(); len(got) != 1 || got[0].ID != "live" {
		t.Fatalf("ready set = %+v", got)
	}
	if got := reg.ReadyInstances("p2"); len(got) != 0 {
		t.Fatalf("partition p2 ready set = %+v", got)
	}
	snap := reg.Snapshot()
	if len(snap.Instances) != 1 || snap.Instances[0].ID != "live" {
		t.Fatalf("snapshot = %+v", snap.Instances)
	}
	// A lagging reader forced to resync gets the same effective set.
	res, err := reg.ChangesSince(0, "p2")
	if err != nil || res.Snapshot == nil || len(res.Snapshot.Instances) != 0 {
		t.Fatalf("partition resync = %+v err=%v", res, err)
	}
	// An incremental reader sees the query-time expiry as a normal event.
	res, err = reg.ChangesSince(readyV)
	if err != nil || len(res.Changes) != 1 || res.Changes[0].Kind != ChangeExpired ||
		res.Changes[0].Instance.ID != "stale" {
		t.Fatalf("incremental = %+v err=%v", res, err)
	}
}

// Re-registering after expiry creates a new identity; late heartbeats and
// state changes carrying the previous token cannot affect the new lease.
func TestReregisterAfterExpiryNewIdentity(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	old, _ := reg.Register(Instance{Service: "s", ID: "n", Addr: "old"}, time.Second)
	clk.Advance(2 * time.Second)
	fresh, err := reg.Register(Instance{Service: "s", ID: "n", Addr: "new"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if old.Token == fresh.Token {
		t.Fatal("re-registration reused the lease token")
	}

	if _, err := reg.Heartbeat("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("late heartbeat err = %v", err)
	}
	if _, err := reg.SetReady("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("late ready err = %v", err)
	}
	if _, err := reg.Drain("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("late drain err = %v", err)
	}
	if err := reg.Deregister("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("late deregister err = %v", err)
	}

	// The new incarnation is untouched and can become ready.
	if _, err := reg.SetReady("s", "n", fresh.Token); err != nil {
		t.Fatal(err)
	}
	if got := reg.ReadyInstances(); len(got) != 1 || got[0].Addr != "new" {
		t.Fatalf("ready set = %+v", got)
	}
}

// A subscriber learns about query-time expiries as ordinary change events,
// so a slow subscriber that reconnects afterwards rebuilds the same
// effective directory as a fresh reader.
func TestSubscribeSeesQueryTimeExpiry(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := reg.Subscribe(ctx, 0)
	base := <-ch
	if base.Snapshot == nil {
		t.Fatalf("baseline = %+v", base)
	}
	h, _ := reg.Register(Instance{Service: "s", ID: "a", Partition: "p"}, time.Second)
	reg.SetReady("s", "a", h.Token)
	got := drainN(t, ch, 2) // registered, ready
	last := got[1].Version

	clk.Advance(2 * time.Second)
	// No sweeper runs; a query observes the expiry and publishes the event.
	if got := reg.ReadyInstances("p"); len(got) != 0 {
		t.Fatalf("lapsed instance routable: %+v", got)
	}
	ev := <-ch
	if ev.Change == nil || ev.Change.Kind != ChangeExpired || ev.Change.Instance.ID != "a" {
		t.Fatalf("expiry event = %+v", ev)
	}

	// A slow subscriber reconnecting from before the expiry catches up with
	// the same event stream and ends at the same effective set.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ch2 := reg.Subscribe(ctx2, last, "p")
	ev = <-ch2
	if ev.Change == nil || ev.Change.Kind != ChangeExpired {
		t.Fatalf("reconnect event = %+v", ev)
	}
}

// Save persists only leases valid at save time, so reopening never sees a
// registration that had already lapsed but was not yet swept.
func TestSaveDropsLapsedUnswept(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(testTime())
	reg := NewRegistry(WithClock(clk))
	h, _ := reg.Register(Instance{Service: "s", ID: "x", Partition: "p"}, time.Second)
	reg.SetReady("s", "x", h.Token)
	clk.Advance(2 * time.Second) // lapsed, unswept

	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}
	re, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	if got := re.ReadyInstances("p"); len(got) != 0 {
		t.Fatalf("reopened ready set = %+v", got)
	}
	if _, err := re.Heartbeat("s", "x", h.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reopened heartbeat err = %v", err)
	}
	if rec, ok := re.Removal("s", "x"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("reopened audit = %+v ok=%v", rec, ok)
	}
}
