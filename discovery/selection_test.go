package discovery

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func readyInstance(t *testing.T, reg *Registry, id string, meta map[string]string) Registration {
	t.Helper()
	h, err := reg.Register(Instance{Service: "svc", ID: id, Addr: id + ":1", Metadata: meta}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetReady("svc", id, h.Token); err != nil {
		t.Fatal(err)
	}
	return h
}

// A selection explains per instance why it was adopted or excluded, and
// pins the directory version it was based on.
func TestSelectionExplainsCandidates(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyInstance(t, reg, "a", map[string]string{MetaCapability: "compute", MetaRegion: "east"})
	readyInstance(t, reg, "b", map[string]string{MetaCapability: "compute", MetaRegion: "west"})
	readyInstance(t, reg, "c", map[string]string{MetaCapability: "store", MetaRegion: "east"})
	hd, _ := reg.Register(Instance{Service: "svc", ID: "d", Metadata: map[string]string{MetaCapability: "compute"}}, time.Minute)
	_ = hd // d stays preparing

	sess, sel, err := reg.BeginSelection(SelectionQuery{
		Service: "svc", Capability: "compute", PreferredRegion: "east", MinAvailable: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sel.Feasible || len(sel.Adopted) != 2 {
		t.Fatalf("selection = %+v", sel)
	}
	if sel.Version != reg.Version() {
		t.Fatalf("selection version %d, directory %d", sel.Version, reg.Version())
	}
	if len(sel.Candidates) != 4 {
		t.Fatalf("candidates = %+v", sel.Candidates)
	}
	reasons := map[string][]string{}
	for _, c := range sel.Candidates {
		reasons[c.Instance.ID] = c.Reasons
	}
	if got := strings.Join(reasons["a"], ";"); !strings.Contains(got, "preferred region") {
		t.Fatalf("a reasons = %v", reasons["a"])
	}
	if got := strings.Join(reasons["b"], ";"); !strings.Contains(got, "not preferred") {
		t.Fatalf("b reasons = %v", reasons["b"])
	}
	if got := reasons["c"][0]; !strings.Contains(got, "capability") {
		t.Fatalf("c reasons = %v", reasons["c"])
	}
	if got := reasons["d"][0]; !strings.Contains(got, "preparing") {
		t.Fatalf("d reasons = %v", reasons["d"])
	}
	if sess.ID() != sel.SessionID {
		t.Fatalf("session %d vs selection session %d", sess.ID(), sel.SessionID)
	}
}

// When the minimum cannot be met the selection says so and explains why.
func TestSelectionUnmetMinimum(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyInstance(t, reg, "a", map[string]string{MetaCapability: "compute"})
	readyInstance(t, reg, "b", map[string]string{MetaCapability: "store"})

	_, sel, err := reg.BeginSelection(SelectionQuery{Service: "svc", Capability: "compute", MinAvailable: 2})
	if err != nil {
		t.Fatal(err)
	}
	if sel.Feasible {
		t.Fatal("selection must be infeasible")
	}
	if !strings.Contains(sel.UnmetReason, "only 1 adopted") ||
		!strings.Contains(sel.UnmetReason, "b (capability") {
		t.Fatalf("unmet reason = %q", sel.UnmetReason)
	}
}

// After a lease expires mid-session the recorded selection is unchanged,
// the instance is no longer shown as usable, and the user can confirm a
// switch to a fresh evaluation.
func TestSelectionSessionExpiryAndConfirm(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyInstance(t, reg, "a", map[string]string{MetaCapability: "compute"})
	readyInstance(t, reg, "b", map[string]string{MetaCapability: "compute"})

	sess, first, err := reg.BeginSelection(SelectionQuery{Service: "svc", Capability: "compute", MinAvailable: 1})
	if err != nil {
		t.Fatal(err)
	}

	clk.Advance(2 * time.Minute) // both leases lapse
	view, err := sess.View()
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Usable) != 0 || len(view.Invalidated) != 2 {
		t.Fatalf("view = %+v", view)
	}
	for _, inv := range view.Invalidated {
		if !strings.Contains(inv.Reason, "expired") {
			t.Fatalf("invalidation reason = %q", inv.Reason)
		}
	}
	// The active record still reflects the original grounds.
	if view.Active.ID != first.ID || len(view.Active.Adopted) != 2 {
		t.Fatalf("active record rewritten: %+v", view.Active)
	}

	// Re-evaluating does not resurrect expired instances.
	pending, err := sess.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Adopted) != 0 || pending.Feasible {
		t.Fatalf("pending = %+v", pending)
	}
	if !strings.Contains(pending.UnmetReason, "only 0 adopted") {
		t.Fatalf("pending unmet = %q", pending.UnmetReason)
	}

	// A new instance arrives; confirm switches to the new evaluation.
	readyInstance(t, reg, "c", map[string]string{MetaCapability: "compute"})
	pending, err = sess.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Adopted) != 1 || pending.Adopted[0].ID != "c" {
		t.Fatalf("pending = %+v", pending)
	}
	confirmed, err := sess.Confirm()
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.ID != pending.ID {
		t.Fatalf("confirmed %d, want %d", confirmed.ID, pending.ID)
	}
	view, _ = sess.View()
	if len(view.Usable) != 1 || view.Usable[0].ID != "c" || len(view.Invalidated) != 0 {
		t.Fatalf("view after confirm = %+v", view)
	}
	if len(view.History) != 2 || view.History[0].ID != first.ID {
		t.Fatalf("history = %+v", view.History)
	}
	if _, err := sess.Confirm(); !errors.Is(err, ErrNoPendingSelection) {
		t.Fatalf("double confirm err = %v", err)
	}
}

// Deregistration and capability changes invalidate adopted instances with
// distinct reasons; deferring keeps the old selection active.
func TestSelectionInvalidationReasonsAndDefer(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	hb := readyInstance(t, reg, "b", map[string]string{MetaCapability: "compute"})
	hc := readyInstance(t, reg, "c", map[string]string{MetaCapability: "compute"})

	sess, _, err := reg.BeginSelection(SelectionQuery{Service: "svc", Capability: "compute", MinAvailable: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Deregister("svc", "b", hb.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.UpdateMetadata("svc", "c", hc.Token, map[string]string{MetaCapability: "store"}); err != nil {
		t.Fatal(err)
	}

	view, _ := sess.View()
	if len(view.Usable) != 0 || len(view.Invalidated) != 2 {
		t.Fatalf("view = %+v", view)
	}
	got := map[string]string{}
	for _, inv := range view.Invalidated {
		got[inv.Instance.ID] = inv.Reason
	}
	if !strings.Contains(got["b"], "deregistered") {
		t.Fatalf("b reason = %q", got["b"])
	}
	if !strings.Contains(got["c"], "capability") {
		t.Fatalf("c reason = %q", got["c"])
	}

	// Defer: stay on the old selection but see why conditions are unmet.
	pending, err := sess.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Feasible || !strings.Contains(pending.UnmetReason, "only 0 adopted") {
		t.Fatalf("pending = %+v", pending)
	}
	deferred, ok := sess.Defer()
	if !ok || deferred.ID != pending.ID {
		t.Fatalf("defer = %+v, %v", deferred, ok)
	}
	view, _ = sess.View()
	if view.Pending != nil {
		t.Fatal("pending must be cleared after defer")
	}
	if len(view.History) != 1 {
		t.Fatalf("defer must not commit history: %+v", view.History)
	}
}

// Selection grounds and invalidation reasons survive Save/OpenRegistry,
// including leases that lapse while the directory is closed.
func TestSelectionPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(testTime())
	reg := NewRegistry(WithClock(clk), WithEventRetention(64))
	readyInstance(t, reg, "a", map[string]string{MetaCapability: "compute"})
	keep, _ := reg.Register(Instance{Service: "svc", ID: "b", Addr: "b:1",
		Metadata: map[string]string{MetaCapability: "compute"}}, 10*time.Minute)
	if _, err := reg.SetReady("svc", "b", keep.Token); err != nil {
		t.Fatal(err)
	}

	sess, first, err := reg.BeginSelection(SelectionQuery{Service: "svc", Capability: "compute", MinAvailable: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Evaluate(); err != nil {
		t.Fatal(err)
	}
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}

	// Reopen after a's 1-minute lease lapsed; b's 10-minute lease holds.
	clk.Advance(2 * time.Minute)
	reg2, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	sess2, ok := reg2.Session(sess.ID())
	if !ok {
		t.Fatal("session not restored")
	}
	view, err := sess2.View()
	if err != nil {
		t.Fatal(err)
	}
	if view.Active.ID != first.ID || len(view.History) != 1 {
		t.Fatalf("view = %+v", view)
	}
	if len(view.Usable) != 1 || view.Usable[0].ID != "b" {
		t.Fatalf("usable = %+v", view.Usable)
	}
	if len(view.Invalidated) != 1 || !strings.Contains(view.Invalidated[0].Reason, "expired") {
		t.Fatalf("invalidated = %+v", view.Invalidated)
	}
	// Pending evaluation survived the reopen as well.
	if view.Pending == nil {
		t.Fatal("pending selection lost across reopen")
	}
	// New sessions continue with unique IDs.
	_, sel2, err := reg2.BeginSelection(SelectionQuery{Service: "svc", MinAvailable: 0})
	if err != nil {
		t.Fatal(err)
	}
	if sel2.ID <= first.ID+1 {
		t.Fatalf("selection IDs regressed: %d after %d", sel2.ID, first.ID)
	}
}

// Comparing two selections separates instance-state causes from condition
// changes, and never rewrites the older record.
func TestCompareSelections(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyInstance(t, reg, "a", map[string]string{MetaCapability: "compute"})
	hb := readyInstance(t, reg, "b", map[string]string{MetaCapability: "compute"})

	sess, first, err := reg.BeginSelection(SelectionQuery{Service: "svc", Capability: "compute", MinAvailable: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Deregister("svc", "b", hb.Token); err != nil {
		t.Fatal(err)
	}
	second, err := sess.Evaluate()
	if err != nil {
		t.Fatal(err)
	}

	cmp := CompareSelections(first, second)
	if cmp.QueryChanged {
		t.Fatalf("query unchanged but reported changed: %+v", cmp)
	}
	if len(cmp.Removed) != 1 || cmp.Removed[0].Instance.ID != "b" ||
		!strings.Contains(cmp.Removed[0].Reason, "no longer in directory") {
		t.Fatalf("removed = %+v", cmp.Removed)
	}

	// A comparison across different conditions attributes the difference
	// to the query, leaving the old record untouched.
	_, third, err := reg.BeginSelection(SelectionQuery{Service: "svc", Capability: "store", MinAvailable: 1})
	if err != nil {
		t.Fatal(err)
	}
	cmp = CompareSelections(second, third)
	if !cmp.QueryChanged || len(cmp.QueryChanges) != 1 ||
		!strings.Contains(cmp.QueryChanges[0], "capability") {
		t.Fatalf("query changes = %+v", cmp)
	}
	if len(cmp.Removed) != 1 || !strings.Contains(cmp.Removed[0].Reason, "capability") {
		t.Fatalf("removed = %+v", cmp.Removed)
	}
	view, _ := sess.View()
	if view.History[0].Query.Capability != "compute" || len(view.History[0].Adopted) != 2 {
		t.Fatalf("old record rewritten: %+v", view.History[0])
	}
}

// Concurrent evaluations, views and directory mutations must not race and
// must never report expired instances as usable.
func TestSelectionConcurrentAccess(t *testing.T) {
	reg, clk := newFakeRegistry(256)
	for _, id := range []string{"a", "b", "c"} {
		readyInstance(t, reg, id, map[string]string{MetaCapability: "compute"})
	}
	sess, _, err := reg.BeginSelection(SelectionQuery{Service: "svc", Capability: "compute", MinAvailable: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, err := sess.Evaluate(); err != nil {
					t.Error(err)
				}
				if _, err := sess.View(); err != nil {
					t.Error(err)
				}
				if j%3 == 0 {
					_, _ = sess.Confirm()
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		clk.Advance(2 * time.Minute) // expire everything mid-flight
		reg.Sweep()
	}()
	wg.Wait()

	view, err := sess.View()
	if err != nil {
		t.Fatal(err)
	}
	for _, inst := range view.Usable {
		found := false
		for _, live := range reg.ReadyInstances() {
			if live.ID == inst.ID {
				found = true
			}
		}
		if !found {
			t.Fatalf("usable instance %s not actually live", inst.ID)
		}
	}
}
