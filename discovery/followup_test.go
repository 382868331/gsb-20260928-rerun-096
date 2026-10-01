package discovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Registration is not readiness: a new instance starts Preparing and is not
// routable until SetReady; Drain pulls traffic away again. Every transition
// gets a global monotonic version and a matching change event.
func TestReadinessLifecycle(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	h, err := reg.Register(Instance{Service: "s", ID: "a", Addr: "x", Partition: "p1"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := reg.ReadyInstances("p1"); len(got) != 0 {
		t.Fatalf("fresh instance must not be routable: %+v", got)
	}
	snap := reg.Snapshot()
	if snap.Instances[0].State != StatePreparing {
		t.Fatalf("initial state = %q", snap.Instances[0].State)
	}

	readyV, err := reg.SetReady("s", "a", h.Token)
	if err != nil {
		t.Fatal(err)
	}
	if readyV != h.Version+1 {
		t.Fatalf("ready version = %d, want %d", readyV, h.Version+1)
	}
	got := reg.ReadyInstances("p1")
	if len(got) != 1 || got[0].State != StateReady {
		t.Fatalf("ready set = %+v", got)
	}
	if len(reg.ReadyInstances("other")) != 0 {
		t.Fatal("partition filter leaked instances")
	}

	drainV, err := reg.Drain("s", "a", h.Token)
	if err != nil || drainV != readyV+1 {
		t.Fatalf("drain v=%d err=%v", drainV, err)
	}
	if len(reg.ReadyInstances("p1")) != 0 {
		t.Fatal("draining instance still routable")
	}

	// Ready after drain is illegal; repeat drain is an idempotent no-op.
	if _, err := reg.SetReady("s", "a", h.Token); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ready after drain err = %v", err)
	}
	if v, err := reg.Drain("s", "a", h.Token); err != nil || v != drainV {
		t.Fatalf("repeat drain v=%d err=%v", v, err)
	}

	res, _ := reg.ChangesSince(h.Version)
	kinds := []ChangeKind{ChangeReady, ChangeDraining}
	if len(res.Changes) != 2 {
		t.Fatalf("changes = %+v", res.Changes)
	}
	for i, c := range res.Changes {
		if c.Kind != kinds[i] || c.Instance.State != kinds[i].State() {
			t.Fatalf("change %d = %+v", i, c)
		}
	}
}

// A token is checked for every readiness transition, including in the same
// instant as heartbeat, deregister and a restart.
func TestReadinessRejectsStaleToken(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	old, _ := reg.Register(Instance{Service: "s", ID: "n", Addr: "old"}, time.Minute)
	newH, _ := reg.Register(Instance{Service: "s", ID: "n", Addr: "new"}, time.Minute)

	if _, err := reg.SetReady("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale ready err = %v", err)
	}
	if _, err := reg.Drain("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale drain err = %v", err)
	}
	if err := reg.Deregister("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale deregister err = %v", err)
	}
	if _, err := reg.Heartbeat("s", "n", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale heartbeat err = %v", err)
	}
	if _, err := reg.SetReady("s", "n", newH.Token); err != nil {
		t.Fatal(err)
	}
	if ready := reg.ReadyInstances(); len(ready) != 1 || ready[0].Addr != "new" {
		t.Fatalf("ready = %+v", ready)
	}
}

func (k ChangeKind) State() State {
	switch k {
	case ChangeReady:
		return StateReady
	case ChangeDraining:
		return StateDraining
	}
	return ""
}

// A partition subscriber sees only its partition, using one global version.
// When its last version ages out it receives a snapshot scoped to its
// partition at one consistent version, never a mix of old per-partition
// tails.
func TestPartitionSubscribe(t *testing.T) {
	reg, _ := newFakeRegistry(4)
	hA, _ := reg.Register(Instance{Service: "s", ID: "a", Partition: "pa"}, time.Minute)
	reg.Register(Instance{Service: "s", ID: "b", Partition: "pb"}, time.Minute)
	reg.SetReady("s", "a", hA.Token)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := reg.Subscribe(ctx, 0, "pa")
	base := <-ch
	if base.Snapshot == nil || len(base.Snapshot.Instances) != 1 ||
		base.Snapshot.Instances[0].Partition != "pa" {
		t.Fatalf("partition baseline = %+v", base)
	}

	// Flood history so the subscriber ages out across both partitions.
	for i := 0; i < 8; i++ {
		reg.Register(Instance{Service: "s", ID: idFor(i), Partition: "pb"}, time.Minute)
	}
	reg.Register(Instance{Service: "s", ID: "a2", Partition: "pa"}, time.Minute)
	ev, ok := <-ch
	if !ok || ev.Snapshot == nil {
		t.Fatalf("expected consistent snapshot, got %+v ok=%v", ev, ok)
	}
	for _, in := range ev.Snapshot.Instances {
		if in.Partition != "pa" {
			t.Fatalf("snapshot mixed partitions: %+v", ev.Snapshot.Instances)
		}
	}
}

func idFor(i int) string {
	return "f" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
}

// A lease that has lapsed but not yet been swept must not be reactivated by
// readiness transitions, and reads must not report it as available. Expiry
// is enforced inline at the moment of the operation, not whenever the
// background sweeper happens to run.
func TestLapsedLeaseCannotBecomeReady(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	h, _ := reg.Register(Instance{Service: "s", ID: "a", Addr: "x", Partition: "p"}, time.Second)

	clk.Advance(2 * time.Second) // lease lapsed; no Sweep ran

	if _, err := reg.SetReady("s", "a", h.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ready on lapsed lease err = %v", err)
	}
	if got := reg.ReadyInstances("p"); len(got) != 0 {
		t.Fatalf("lapsed instance routable: %+v", got)
	}
	if rec, ok := reg.Removal("s", "a"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("lapsed instance not expired inline: %+v ok=%v", rec, ok)
	}

	// Re-register after expiry: the new incarnation is a fresh identity and
	// late requests carrying the old token must not affect it.
	h2, err := reg.Register(Instance{Service: "s", ID: "a", Addr: "y", Partition: "p"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Token == h.Token {
		t.Fatal("re-registration reused token")
	}
	if _, err := reg.SetReady("s", "a", h.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("late ready with old token err = %v", err)
	}
	if _, err := reg.Heartbeat("s", "a", h.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("late heartbeat with old token err = %v", err)
	}
	if _, err := reg.SetReady("s", "a", h2.Token); err != nil {
		t.Fatalf("new incarnation ready: %v", err)
	}
	if got := reg.ReadyInstances("p"); len(got) != 1 || got[0].Addr != "y" {
		t.Fatalf("ready set = %+v", got)
	}
}

// Queries (ready set, snapshot, incremental reads) must all observe the same
// effective instance set even while lapsed leases are still unswept.
func TestQueriesAgreeOnEffectiveSet(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	live, _ := reg.Register(Instance{Service: "s", ID: "live", Partition: "p"}, time.Minute)
	reg.Register(Instance{Service: "s", ID: "stale", Partition: "p"}, time.Second)
	reg.SetReady("s", "live", live.Token)

	clk.Advance(2 * time.Second) // stale lapsed, sweeper idle

	if got := reg.ReadyInstances("p"); len(got) != 1 || got[0].ID != "live" {
		t.Fatalf("ready = %+v", got)
	}
	if snap := reg.Snapshot(); len(snap.Instances) != 1 || snap.Instances[0].ID != "live" {
		t.Fatalf("snapshot = %+v", snap)
	}
	res, err := reg.ChangesSince(0, "p")
	if err != nil || res.Snapshot == nil || len(res.Snapshot.Instances) != 1 ||
		res.Snapshot.Instances[0].ID != "live" {
		t.Fatalf("partition resync = %+v err=%v", res, err)
	}
	if rec, ok := reg.Removal("s", "stale"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("stale audit = %+v ok=%v", rec, ok)
	}
}

// Renew, restart, explicit leave and drain racing at one instant must leave
// exactly one live incarnation and invalidate every old token.
func TestConcurrentRestartDrainLeave(t *testing.T) {
	reg, _ := newFakeRegistry(128)
	base, _ := reg.Register(Instance{Service: "s", ID: "hot", Partition: "p"}, time.Minute)
	stop := make(chan struct{})
	var mus sync.Mutex
	tokens := []string{base.Token}

	var wgWorkers sync.WaitGroup
	for g := 0; g < 3; g++ {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					mus.Lock()
					tok := tokens[len(tokens)-1]
					mus.Unlock()
					reg.Heartbeat("s", "hot", tok)
					reg.SetReady("s", "hot", tok)
					reg.Drain("s", "hot", tok)
				}
			}
		}()
	}
	var wgRestart sync.WaitGroup
	wgRestart.Add(1)
	go func() {
		defer wgRestart.Done()
		for i := 0; i < 40; i++ {
			h, err := reg.Register(Instance{Service: "s", ID: "hot", Addr: "a", Partition: "p"}, time.Minute)
			if err != nil {
				return
			}
			mus.Lock()
			tokens = append(tokens, h.Token)
			mus.Unlock()
		}
	}()
	close(stop)
	wgRestart.Wait()
	wgWorkers.Wait()

	snap := reg.Snapshot()
	if len(snap.Instances) != 1 {
		t.Fatalf("directory = %+v", snap.Instances)
	}
	mus.Lock()
	newest := tokens[len(tokens)-1]
	mus.Unlock()
	for _, old := range tokens[:len(tokens)-1] {
		if _, err := reg.Heartbeat("s", "hot", old); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("old token still valid: %v", err)
		}
	}
	if _, err := reg.Heartbeat("s", "hot", newest); err != nil {
		t.Fatalf("current token rejected: %v", err)
	}
}

// Saving the directory and reopening preserves live leases, readiness and
// the audit trail, continues monotonic versions, and never revives an
// instance whose lease lapsed before the save.
func TestSaveOpenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(testTime())
	reg := NewRegistry(WithClock(clk), WithEventRetention(8))
	alive, _ := reg.Register(Instance{Service: "s", ID: "alive", Addr: "a", Partition: "p"}, time.Minute)
	dying, _ := reg.Register(Instance{Service: "s", ID: "dying", Addr: "d"}, 10*time.Second)
	gone, _ := reg.Register(Instance{Service: "s", ID: "gone", Addr: "g"}, time.Minute)
	reg.SetReady("s", "alive", alive.Token)
	reg.Deregister("s", "gone", gone.Token)
	beforeSave := reg.Version()
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}

	// Advance past the dying lease after saving.
	clk.Advance(11 * time.Second)
	re, err := OpenRegistry(dir, WithClock(clk), WithEventRetention(8))
	if err != nil {
		t.Fatal(err)
	}
	if re.Version() < beforeSave {
		t.Fatalf("version went backwards: %d vs %d", re.Version(), beforeSave)
	}
	if ready := re.ReadyInstances("p"); len(ready) != 1 || ready[0].ID != "alive" || ready[0].State != StateReady {
		t.Fatalf("restored ready set = %+v", ready)
	}
	if _, err := re.Heartbeat("s", "alive", alive.Token); err != nil {
		t.Fatalf("live lease token lost on reopen: %v", err)
	}
	if _, err := re.Heartbeat("s", "dying", dying.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired instance revived: %v", err)
	}
	if rec, ok := re.Removal("s", "dying"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("dying audit = %+v ok=%v", rec, ok)
	}
	if rec, ok := re.Removal("s", "gone"); !ok || rec.Reason != ReasonDeregistered {
		t.Fatalf("explicit-leave audit lost: %+v ok=%v", rec, ok)
	}

	// Versions keep advancing monotonically after reopen.
	h, err := re.Register(Instance{Service: "s", ID: "later"}, time.Minute)
	if err != nil || h.Version <= re.Version()-1 {
		t.Fatalf("post-open version = %d err=%v", h.Version, err)
	}
}

// An instance already expired at save time cannot be republished by copying
// the directory elsewhere and opening it with a different clock state.
func TestOpenDoesNotReviveExpiredFile(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(testTime())
	reg := NewRegistry(WithClock(clk))
	h, _ := reg.Register(Instance{Service: "s", ID: "x"}, time.Second)
	clk.Advance(2 * time.Second)
	reg.Sweep()
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}
	re, err := OpenRegistry(dir, WithClock(NewFakeClock(testTime())))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := re.Heartbeat("s", "x", h.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revived expired: %v", err)
	}
	if rec, ok := re.Removal("s", "x"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("audit = %+v ok=%v", rec, ok)
	}
}
