package discovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testTime() time.Time { return time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC) }

func newFakeRegistry(retention int) (*Registry, *FakeClock) {
	clk := NewFakeClock(testTime())
	return NewRegistry(WithClock(clk), WithEventRetention(retention)), clk
}

// A heartbeat before the deadline extends the lease; missing it lets the
// instance expire, and expiry is recorded distinctly from deregistration.
func TestLeaseHeartbeatExpiry(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	reg1, err := reg.Register(Instance{Service: "svc", ID: "a", Addr: "10.0.0.1:80"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Renew well within the lease.
	clk.Advance(700 * time.Millisecond)
	dl, err := reg.Heartbeat("svc", "a", reg1.Token)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if want := clk.Now().Add(time.Second); !dl.Equal(want) {
		t.Fatalf("deadline = %v, want %v", dl, want)
	}

	// Another instance explicitly leaves; advance past a's renewed deadline.
	reg2, _ := reg.Register(Instance{Service: "svc", ID: "b", Addr: "10.0.0.2:80"}, 2*time.Second)
	if err := reg.Deregister("svc", "b", reg2.Token); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	swept := reg.Sweep()
	if len(swept) != 1 || swept[0].Instance.ID != "a" || swept[0].Reason != ReasonExpired {
		t.Fatalf("swept = %+v", swept)
	}

	if rec, ok := reg.Removal("svc", "a"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("a removal = %+v ok=%v", rec, ok)
	}
	if rec, ok := reg.Removal("svc", "b"); !ok || rec.Reason != ReasonDeregistered {
		t.Fatalf("b removal = %+v ok=%v", rec, ok)
	}
	if snap := reg.Snapshot(); len(snap.Instances) != 0 {
		t.Fatalf("directory not empty: %+v", snap.Instances)
	}

	// A heartbeat after expiry cannot resurrect the instance.
	if _, err := reg.Heartbeat("svc", "a", reg1.Token); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-expiry heartbeat err = %v", err)
	}
}

// A restarted incarnation gets a new token. The old heartbeat must not extend
// the new lease and the old token must not evict the new instance.
func TestRestartNewIdentityRejectsOldLease(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	old, err := reg.Register(Instance{Service: "svc", ID: "node", Addr: "old:1"}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	newReg, err := reg.Register(Instance{Service: "svc", ID: "node", Addr: "new:1"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if old.Token == newReg.Token {
		t.Fatal("restart reused lease token")
	}

	// Stale heartbeat from the previous process is rejected.
	if _, err := reg.Heartbeat("svc", "node", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("old heartbeat err = %v", err)
	}
	// Stale explicit leave cannot remove the new incarnation.
	if err := reg.Deregister("svc", "node", old.Token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("old deregister err = %v", err)
	}

	// New incarnation still alive; old one recorded as forced departure.
	if rec, ok := reg.Removal("svc", "node"); !ok || rec.Reason != ReasonExpired || rec.Instance.Addr != "old:1" {
		t.Fatalf("old incarnation removal = %+v", rec)
	}
	if snap := reg.Snapshot(); len(snap.Instances) != 1 || snap.Instances[0].Addr != "new:1" {
		t.Fatalf("directory = %+v", snap.Instances)
	}

	// The old heartbeat must not have moved the new deadline: the new lease
	// expires exactly one second after the restart registration.
	clk.Advance(time.Second)
	swept := reg.Sweep()
	if len(swept) != 1 || swept[0].Instance.Addr != "new:1" {
		t.Fatalf("swept = %+v", swept)
	}
}

// Incremental reads continue from any version; compacted history forces a
// snapshot rather than skipping events.
func TestChangesSinceRetentionSnapshot(t *testing.T) {
	reg, _ := newFakeRegistry(3)
	var firstV int64
	for i := 0; i < 4; i++ {
		reg, err := reg.Register(Instance{Service: "svc", ID: string(rune('a' + i))}, time.Minute)
		if err != nil {
			panic(err)
		}
		if i == 0 {
			firstV = reg.Version
		}
	}

	// Version still within the window: incremental tail only.
	cur := reg.Version()
	res, err := reg.ChangesSince(cur - 1)
	if err != nil || res.Snapshot != nil || len(res.Changes) != 1 {
		t.Fatalf("tail read = %+v err=%v", res, err)
	}

	// Version 0 always means full snapshot.
	res, _ = reg.ChangesSince(0)
	if res.Snapshot == nil || len(res.Snapshot.Instances) != 4 {
		t.Fatalf("initial snapshot = %+v", res)
	}

	// Too old => full snapshot, never a silently shortened change list.
	res, err = reg.ChangesSince(firstV)
	if err != nil || res.Snapshot == nil {
		t.Fatalf("lagging read should snapshot: %+v err=%v", res, err)
	}
	if len(res.Snapshot.Instances) != 4 || res.Snapshot.Version != cur {
		t.Fatalf("snapshot mismatch: %+v", res.Snapshot)
	}
}

func TestSubscribeAndReconnect(t *testing.T) {
	reg, _ := newFakeRegistry(32)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := reg.Subscribe(ctx, 0)
	ev := <-ch
	if ev.Snapshot == nil || ev.Version != 0 || len(ev.Snapshot.Instances) != 0 {
		t.Fatalf("first event = %+v", ev)
	}

	r1, _ := reg.Register(Instance{Service: "s", ID: "1", Addr: "a"}, time.Minute)
	ev = <-ch
	if ev.Change == nil || ev.Change.Kind != ChangeRegistered || ev.Version != r1.Version {
		t.Fatalf("register event = %+v", ev)
	}
	lastApplied := ev.Version

	// Subscriber disconnects; changes keep happening server-side.
	cancel()
	for range ch {
	}
	r2, _ := reg.Register(Instance{Service: "s", ID: "2", Addr: "b"}, time.Minute)
	if err := reg.Deregister("s", "1", mustToken(t, reg, "s", "1")); err != nil {
		t.Fatal(err)
	}

	// Reconnect from the last fully applied version and catch up.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ch2 := reg.Subscribe(ctx2, lastApplied)
	got := drainN(t, ch2, 2)
	if got[0].Change.Instance.ID != "2" || got[0].Change.Kind != ChangeRegistered ||
		got[0].Version != r2.Version {
		t.Fatalf("event0 = %+v", got[0])
	}
	if got[1].Change.Instance.ID != "1" || got[1].Change.Kind != ChangeDeregistered {
		t.Fatalf("event1 = %+v", got[1])
	}

	// A later change is pushed live without polling.
	reg.Register(Instance{Service: "s", ID: "3", Addr: "c"}, time.Minute)
	if ev := <-ch2; ev.Change == nil || ev.Change.Instance.ID != "3" {
		t.Fatalf("live event = %+v", ev)
	}
}

// Reconnecting from a version older than retention yields a snapshot as the
// first event, proving no event range is skipped.
func TestSubscribeLaggingGetsSnapshot(t *testing.T) {
	reg, _ := newFakeRegistry(2)
	reg.Register(Instance{Service: "s", ID: "1"}, time.Minute)
	reg.Register(Instance{Service: "s", ID: "2"}, time.Minute)
	reg.Register(Instance{Service: "s", ID: "3"}, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := reg.Subscribe(ctx, 1)
	ev := <-ch
	if ev.Snapshot == nil || len(ev.Snapshot.Instances) != 3 {
		t.Fatalf("expected full snapshot, got %+v", ev)
	}
}

// Concurrent renewal and registration must keep lease state and versions
// consistent under the race detector.
func TestConcurrentRenewalsAndRestarts(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	base, err := reg.Register(Instance{Service: "s", ID: "hot", Addr: "0"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Multiple renewers; only the current token succeeds.
	tokens := []string{base.Token}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = reg.Heartbeat("s", "hot", token)
				}
			}
		}(tokens[len(tokens)-1])
	}

	// Restart loop rotates identities/tokens.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			nr, err := reg.Register(Instance{Service: "s", ID: "hot", Addr: string(rune('0' + i%10))}, time.Minute)
			if err != nil {
				return
			}
			tokens = append(tokens, nr.Token)
		}
	}()

	close(stop)
	wg.Wait()

	snap := reg.Snapshot()
	if len(snap.Instances) != 1 || snap.Instances[0].ID != "hot" {
		t.Fatalf("directory inconsistent: %+v", snap.Instances)
	}
	// Exactly one live incarnation answers to the newest token.
	newest := tokens[len(tokens)-1]
	if _, err := reg.Heartbeat("s", "hot", newest); err != nil {
		t.Fatalf("newest token rejected: %v", err)
	}
}

func TestRunSweeperFakeClock(t *testing.T) {
	reg, clk := newFakeRegistry(16)
	ctx, cancel := context.WithCancel(context.Background())
	reg.Register(Instance{Service: "s", ID: "dying"}, time.Second)

	go reg.RunSweeper(ctx, 100*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := reg.Removal("s", "dying"); ok {
			cancel()
			return
		}
		clk.Advance(100 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	t.Fatal("sweeper never expired the instance")
}

func mustToken(t *testing.T, reg *Registry, service, id string) string {
	t.Helper()
	reg.mu.Lock()
	e, ok := reg.instances[key(service, id)]
	reg.mu.Unlock()
	if !ok {
		t.Fatal("instance missing")
	}
	return e.token
}

func drainN(t *testing.T, ch <-chan WatchEvent, n int) []WatchEvent {
	t.Helper()
	out := make([]WatchEvent, 0, n)
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("channel closed early")
			}
			out = append(out, ev)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for events")
		}
	}
	return out
}
