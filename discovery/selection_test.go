package discovery

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func readyInst(t *testing.T, reg *Registry, id, region, caps string) Registration {
	t.Helper()
	h, err := reg.Register(Instance{
		Service:  "checkout",
		ID:       id,
		Addr:     id + ":9000",
		Metadata: map[string]string{MetaRegion: region, MetaCapabilities: caps},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetReady("checkout", id, h.Token); err != nil {
		t.Fatal(err)
	}
	return h
}

func findVerdict(sel Selection, id string) Verdict {
	for _, v := range sel.Verdicts {
		if v.Instance.ID == id {
			return v
		}
	}
	return Verdict{}
}

// A selection explains per instance why it was adopted or excluded, ranks
// the preferred region first, and records the directory state it was based
// on.
func TestSelectionExplainsVerdicts(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyInst(t, reg, "a", "east", "grpc,http2")
	readyInst(t, reg, "b", "west", "grpc")
	readyInst(t, reg, "c", "east", "http2")
	reg.Register(Instance{Service: "checkout", ID: "d",
		Metadata: map[string]string{MetaRegion: "east", MetaCapabilities: "grpc"}}, time.Minute)

	s, err := reg.NewSelectionSession(Criteria{
		Service:         "checkout",
		Capabilities:    []string{"grpc"},
		PreferredRegion: "east",
		MinAvailable:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	cur := s.Current()
	if !cur.Satisfied || len(cur.Picked) != 2 {
		t.Fatalf("picked = %+v satisfied=%v", cur.Picked, cur.Satisfied)
	}
	if cur.Picked[0].ID != "a" {
		t.Fatalf("preferred region not ranked first: %+v", cur.Picked)
	}
	if v := findVerdict(cur, "c"); v.Adopted ||
		!strings.Contains(strings.Join(v.Reasons, ","), "missing capability") {
		t.Fatalf("c verdict = %+v", v)
	}
	if v := findVerdict(cur, "d"); v.Adopted ||
		!strings.Contains(v.Reasons[0], "not ready") {
		t.Fatalf("d verdict = %+v", v)
	}
	if cur.Basis.Version != cur.Version || len(cur.Basis.Instances) != 4 {
		t.Fatalf("basis = %+v version=%d", cur.Basis, cur.Version)
	}
	if len(s.History()) != 1 {
		t.Fatalf("history = %+v", s.History())
	}
}

// While a session lives, departures and capability changes must surface as
// stale entries with reasons; the confirmed record stays put until Confirm,
// and a deferred switch shows why conditions cannot be met.
func TestSessionRecheckAndConfirm(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	// a gets a short lease so it alone lapses when the clock advances.
	hA, err := reg.Register(Instance{Service: "checkout", ID: "a",
		Metadata: map[string]string{MetaRegion: "east", MetaCapabilities: "grpc"}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetReady("checkout", "a", hA.Token); err != nil {
		t.Fatal(err)
	}
	hB := readyInst(t, reg, "b", "west", "grpc")
	hC := readyInst(t, reg, "c", "west", "grpc")

	s, _ := reg.NewSelectionSession(Criteria{
		Service: "checkout", Capabilities: []string{"grpc"}, MinAvailable: 2,
	})
	if got := len(s.Current().Picked); got != 3 {
		t.Fatalf("picked = %d", got)
	}

	// a's lease lapses, b leaves explicitly, c drops the capability.
	clk.Advance(2 * time.Second)
	if err := reg.Deregister("checkout", "b", hB.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.UpdateMetadata("checkout", "c", hC.Token,
		map[string]string{MetaRegion: "west", MetaCapabilities: "http2"}); err != nil {
		t.Fatal(err)
	}
	rc := s.Recheck()
	if len(rc.Stale) != 3 || len(rc.StillUsable) != 0 {
		t.Fatalf("stale = %+v usable = %+v", rc.Stale, rc.StillUsable)
	}
	reasons := map[string]string{}
	for _, st := range rc.Stale {
		reasons[st.Instance.ID] = st.Reason
	}
	if reasons["a"] != "lease expired" {
		t.Fatalf("a reason = %q", reasons["a"])
	}
	if reasons["b"] != "deregistered" {
		t.Fatalf("b reason = %q", reasons["b"])
	}
	if !strings.Contains(reasons["c"], "missing capability") {
		t.Fatalf("c reason = %q", reasons["c"])
	}
	if got := len(s.Current().Picked); got != 3 {
		t.Fatalf("current changed before confirm: %d", got)
	}
	if rc.Candidate.Satisfied || rc.Candidate.Shortfall == "" {
		t.Fatalf("candidate = %+v", rc.Candidate)
	}
	if cur := s.Current(); !cur.Satisfied {
		t.Fatal("deferred switch must keep the confirmed record")
	}

	// Replacements arrive; re-check and confirm the switch.
	readyInst(t, reg, "d", "east", "grpc")
	readyInst(t, reg, "e", "west", "grpc")
	rc = s.Recheck()
	if !rc.Candidate.Satisfied || len(rc.Candidate.Picked) != 2 {
		t.Fatalf("candidate = %+v", rc.Candidate)
	}
	confirmed, err := s.Confirm()
	if err != nil {
		t.Fatal(err)
	}
	if len(confirmed.Picked) != 2 || confirmed.Picked[0].ID != "d" {
		t.Fatalf("confirmed = %+v", confirmed.Picked)
	}
	if len(s.History()) != 2 || len(s.History()[0].Picked) != 3 {
		t.Fatalf("history = %+v", s.History())
	}
}

// New conditions are evaluated without mutating history; comparing two
// records attributes differences to instance state vs. user conditions.
func TestCompareSelections(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyInst(t, reg, "a", "east", "grpc")
	hB := readyInst(t, reg, "b", "west", "grpc")

	s, _ := reg.NewSelectionSession(Criteria{
		Service: "checkout", Capabilities: []string{"grpc"}, MinAvailable: 1,
	})
	// Instance-state change only: b leaves.
	if err := reg.Deregister("checkout", "b", hB.Token); err != nil {
		t.Fatal(err)
	}
	s.Recheck()
	if _, err := s.Confirm(); err != nil {
		t.Fatal(err)
	}
	hist := s.History()
	d := CompareSelections(hist[0], hist[1])
	if d.CriteriaChanged {
		t.Fatalf("criteria flagged changed: %+v", d)
	}
	if len(d.Lost) != 1 || d.Lost[0] != "b" || d.LostReasons["b"] == "" {
		t.Fatalf("diff = %+v", d)
	}

	// Criteria change only: require a capability nobody has.
	rc, err := s.RecheckWith(Criteria{
		Service: "checkout", Capabilities: []string{"grpc", "http2"}, MinAvailable: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rc.CriteriaChanged {
		t.Fatal("criteria change not detected")
	}
	if _, err := s.Confirm(); err != nil {
		t.Fatal(err)
	}
	hist = s.History()
	d = CompareSelections(hist[1], hist[2])
	if !d.CriteriaChanged || len(d.CriteriaChanges) == 0 {
		t.Fatalf("diff = %+v", d)
	}
	if len(d.Lost) != 1 || d.Lost[0] != "a" ||
		!strings.Contains(d.LostReasons["a"], "missing capability") {
		t.Fatalf("diff = %+v", d)
	}
	// History is immutable: the middle record keeps its old criteria.
	if len(hist[1].Criteria.Capabilities) != 1 {
		t.Fatalf("history rewritten: %+v", hist[1].Criteria)
	}
	if _, err := s.Confirm(); !errors.Is(err, ErrNoPendingSelection) {
		t.Fatalf("confirm without recheck err = %v", err)
	}
}

// After a directory reopen, rechecking yields the same invalidation reasons
// as before the save, because the departure audit is persisted.
func TestSelectionReasonsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(testTime())
	reg := NewRegistry(WithClock(clk))
	readyInst(t, reg, "a", "east", "grpc")
	hB := readyInst(t, reg, "b", "west", "grpc")
	s, _ := reg.NewSelectionSession(Criteria{
		Service: "checkout", Capabilities: []string{"grpc"}, MinAvailable: 1,
	})

	if err := reg.Deregister("checkout", "b", hB.Token); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute) // a's lease lapses
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}

	re, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := re.NewSelectionSession(Criteria{
		Service: "checkout", Capabilities: []string{"grpc"}, MinAvailable: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	cur := s2.Current()
	if cur.Satisfied || len(cur.Picked) != 0 {
		t.Fatalf("reopened selection = %+v", cur)
	}
	if rec, ok := re.Removal("checkout", "a"); !ok || rec.Reason != ReasonExpired {
		t.Fatalf("a audit = %+v ok=%v", rec, ok)
	}
	if rec, ok := re.Removal("checkout", "b"); !ok || rec.Reason != ReasonDeregistered {
		t.Fatalf("b audit = %+v ok=%v", rec, ok)
	}
	// The pre-save session on the old registry object reports the same
	// reasons, so live and reopened views agree.
	rc := s.Recheck()
	reasons := map[string]string{}
	for _, st := range rc.Stale {
		reasons[st.Instance.ID] = st.Reason
	}
	if reasons["a"] != "lease expired" || reasons["b"] != "deregistered" {
		t.Fatalf("stale = %+v", rc.Stale)
	}
}

// Concurrent mutations and rechecks must never report a departed instance
// as usable, and the confirmed record stays consistent under interleaving.
func TestSelectionConcurrentInterleave(t *testing.T) {
	reg, _ := newFakeRegistry(256)
	handles := map[string]Registration{}
	for _, id := range []string{"a", "b", "c", "d"} {
		handles[id] = readyInst(t, reg, id, "east", "grpc")
	}
	s, _ := reg.NewSelectionSession(Criteria{
		Service: "checkout", Capabilities: []string{"grpc"}, MinAvailable: 1,
	})
	done := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-done:
				return
			default:
				rc := s.Recheck()
				for _, st := range rc.Stale {
					for _, p := range rc.Candidate.Picked {
						if p.ID == st.Instance.ID {
							t.Errorf("stale instance %s reported usable", p.ID)
						}
					}
				}
			}
		}
	}()
	for _, id := range []string{"a", "b", "c"} {
		reg.Deregister("checkout", id, handles[id].Token)
	}
	reg.UpdateMetadata("checkout", "d", handles["d"].Token,
		map[string]string{MetaRegion: "east", MetaCapabilities: "none"})
	readyInst(t, reg, "e", "east", "grpc")
	close(done)
	<-readerDone

	rc := s.Recheck()
	if len(rc.Candidate.Picked) != 1 || rc.Candidate.Picked[0].ID != "e" {
		t.Fatalf("picked = %+v", rc.Candidate.Picked)
	}
	if len(rc.Stale) != 4 {
		t.Fatalf("stale = %+v", rc.Stale)
	}
}
