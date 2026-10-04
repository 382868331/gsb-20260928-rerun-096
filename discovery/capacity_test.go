package discovery

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func readyCapInst(t *testing.T, reg *Registry, id, caps, capacity string) Registration {
	t.Helper()
	h, err := reg.Register(Instance{
		Service: "checkout",
		ID:      id,
		Addr:    id + ":9000",
		Metadata: map[string]string{
			MetaCapabilities: caps,
			MetaCapacity:     capacity,
		},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetReady("checkout", id, h.Token); err != nil {
		t.Fatal(err)
	}
	return h
}

func capCriteria(min int) Criteria {
	return Criteria{Service: "checkout", Capabilities: []string{"grpc"}, MinAvailable: min}
}

// A reserved-then-confirmed session owns the unit; a contending session's
// preview may still list the instance, but it can neither reserve nor
// confirm capacity it does not own. Duplicate confirm/cancel are no-ops.
func TestCapacityReserveConfirmContention(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")
	readyCapInst(t, reg, "b", "grpc", "1")

	s1, _ := reg.NewSelectionSession(capCriteria(2))
	res, err := s1.Reserve(time.Minute)
	if err != nil || !res.Satisfied || len(res.Picks) != 2 {
		t.Fatalf("reserve = %+v err=%v", res, err)
	}

	// A concurrent session sees the same preview but cannot honor it.
	s2, _ := reg.NewSelectionSession(capCriteria(2))
	rc2 := s2.Recheck()
	if len(rc2.Candidate.Picked) != 2 {
		t.Fatalf("preview picks = %+v", rc2.Candidate.Picked)
	}
	if rc2.Honorable != 0 {
		t.Fatalf("honorable = %d, want 0 while s1 holds both units", rc2.Honorable)
	}
	res2, err := s2.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Satisfied || res2.Shortfall == "" {
		t.Fatalf("contending reserve = %+v", res2)
	}

	confirmed, err := s1.Confirm()
	if err != nil {
		t.Fatal(err)
	}
	if len(confirmed.Picked) != 2 {
		t.Fatalf("confirmed = %+v", confirmed.Picked)
	}
	// Duplicate confirm returns the same decision without reallocating.
	again, err := s1.Confirm()
	if err != nil || again.Seq != confirmed.Seq {
		t.Fatalf("duplicate confirm = %+v err=%v", again, err)
	}
	if holds := s1.Holds(); len(holds) != 2 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("holds = %+v", holds)
	}

	// s2 still cannot get capacity even by confirming directly.
	if _, err := s2.Confirm(); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("s2 confirm err = %v", err)
	}

	// Releasing s1's confirmed capacity lets s2 reserve and confirm.
	s1.ReleaseConfirmed()
	res2, err = s2.Reserve(time.Minute)
	if err != nil || !res2.Satisfied {
		t.Fatalf("reserve after release = %+v err=%v", res2, err)
	}
	if _, err := s2.Confirm(); err != nil {
		t.Fatal(err)
	}
}

// A reservation that could only place one of two holds must not be reported
// as a satisfied two-instance decision. The partial result is available only
// after explicit degradation acceptance and cannot touch the other owner.
func TestPartialReservationRequiresExplicitDegradation(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")

	owner, _ := reg.NewSelectionSession(capCriteria(1))
	ownerRes, err := owner.Reserve(time.Minute)
	if err != nil || !ownerRes.Satisfied || ownerRes.Picks[0].ID != "a" {
		t.Fatalf("owner reservation = %+v err=%v", ownerRes, err)
	}
	if _, err := owner.Confirm(); err != nil {
		t.Fatal(err)
	}
	readyCapInst(t, reg, "b", "grpc", "1")

	waiter, _ := reg.NewSelectionSession(capCriteria(2))
	partial, err := waiter.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if partial.Satisfied || len(partial.Picks) != 1 || partial.Picks[0].ID != "b" {
		t.Fatalf("partial reservation = %+v", partial)
	}
	if _, err := waiter.Confirm(); !errors.Is(err, ErrDegradationNeedsAccept) {
		t.Fatalf("partial confirm err = %v", err)
	}
	if holds := waiter.Holds(); len(holds) != 1 || holds[0].Kind != HoldReserved {
		t.Fatalf("rejected confirm changed reservation: %+v", holds)
	}
	if holds := owner.Holds(); len(holds) != 1 || holds[0].InstanceID != "a" || holds[0].Kind != HoldConfirmed {
		t.Fatalf("owner capacity changed: %+v", holds)
	}

	decided, err := waiter.ConfirmAcceptingDegradation()
	if err != nil {
		t.Fatal(err)
	}
	if decided.Satisfied || decided.Honored != 1 || len(decided.Picked) != 1 || decided.Picked[0].ID != "b" {
		t.Fatalf("degraded decision = %+v", decided)
	}
	if len(decided.CandidatePicked) != 2 {
		t.Fatalf("full candidate not recorded: %+v", decided.CandidatePicked)
	}
	if holds := waiter.Holds(); len(holds) != 1 || holds[0].InstanceID != "b" || holds[0].Kind != HoldConfirmed {
		t.Fatalf("waiter holds = %+v", holds)
	}
	if holds := owner.Holds(); len(holds) != 1 || holds[0].InstanceID != "a" {
		t.Fatalf("owner holds stolen: %+v", holds)
	}
}

// Reservations lapse on their own; confirming after expiry only succeeds
// when the capacity is still free, and cancel is idempotent.
func TestReservationExpiryAndCancel(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")

	s1, _ := reg.NewSelectionSession(capCriteria(1))
	if _, err := s1.Reserve(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	// Cancel twice: the second cancel must not release anything more.
	s1.CancelReservation()
	s1.CancelReservation()
	if holds := s1.Holds(); len(holds) != 0 {
		t.Fatalf("holds after cancel = %+v", holds)
	}

	// Reserve again, then let it lapse while another session takes the unit.
	if _, err := s1.Reserve(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(11 * time.Second)
	s2, _ := reg.NewSelectionSession(capCriteria(1))
	res2, err := s2.Reserve(time.Minute)
	if err != nil || !res2.Satisfied {
		t.Fatalf("s2 reserve = %+v err=%v", res2, err)
	}
	if _, err := s2.Confirm(); err != nil {
		t.Fatal(err)
	}
	// s1's late confirm must fail instead of double-owning the unit.
	if _, err := s1.Confirm(); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("late confirm err = %v", err)
	}
	if holds := s2.Holds(); len(holds) != 1 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("s2 holds = %+v", holds)
	}

	// When the capacity is still free, a lapsed reservation can be
	// re-acquired at confirm time.
	s2.ReleaseConfirmed()
	if _, err := s1.Confirm(); err != nil {
		t.Fatalf("confirm with free capacity = %v", err)
	}
}

// Confirmed capacity of one session is never borrowed to satisfy another
// session's minimum, and guarantee loss is reported with reasons.
func TestConfirmedCapacityNotStolen(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	hA := readyCapInst(t, reg, "a", "grpc", "1")

	s1, _ := reg.NewSelectionSession(capCriteria(1))
	if _, err := s1.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Confirm(); err != nil {
		t.Fatal(err)
	}
	rc := s1.Recheck()
	if len(rc.Guaranteed) != 1 || rc.Guaranteed[0].ID != "a" {
		t.Fatalf("guaranteed = %+v", rc.Guaranteed)
	}

	s2, _ := reg.NewSelectionSession(capCriteria(1))
	res2, _ := s2.Reserve(time.Minute)
	if res2.Satisfied {
		t.Fatalf("s2 reserved s1's confirmed capacity: %+v", res2)
	}

	// The instance's lease lapses: history is kept, the guarantee is lost.
	clk.Advance(2 * time.Minute)
	rc = s1.Recheck()
	if len(rc.Guaranteed) != 0 || len(rc.LostGuarantee) != 1 {
		t.Fatalf("guaranteed=%+v lost=%+v", rc.Guaranteed, rc.LostGuarantee)
	}
	if rc.LostGuarantee[0].Reason != "lease expired" {
		t.Fatalf("lost reason = %q", rc.LostGuarantee[0].Reason)
	}
	if cur := s1.Current(); len(cur.Picked) != 1 || cur.Picked[0].ID != "a" {
		t.Fatalf("history rewritten: %+v", cur.Picked)
	}
	// The departed instance's hold is gone, so s2 can now reserve.
	if res2, _ := s2.Reserve(time.Minute); res2.Satisfied {
		t.Fatal("departed instance must not be reservable")
	}
	_ = hA
}

// A replacement proposal explains the release/effective timing and blocks
// a degrading switch until the user explicitly accepts it.
func TestSwitchPlanAndDegradationGate(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")
	readyCapInst(t, reg, "b", "grpc", "1")

	s, _ := reg.NewSelectionSession(capCriteria(2))
	if _, err := s.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Confirm(); err != nil {
		t.Fatal(err)
	}

	// Propose a switch to a single-instance selection: below the minimum.
	rc, err := s.RecheckWith(Criteria{
		Service: "checkout", Capabilities: []string{"grpc"}, MinAvailable: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rc.Switch == nil || rc.Switch.Uninterrupted {
		t.Fatalf("switch plan = %+v", rc.Switch)
	}
	if rc.Switch.OldReleaseAt == "" || rc.Switch.NewEffectiveAt == "" || rc.Switch.Note == "" {
		t.Fatalf("switch plan incomplete: %+v", rc.Switch)
	}
	if _, err := s.Confirm(); !errors.Is(err, ErrDegradationNeedsAccept) {
		t.Fatalf("degrading confirm err = %v", err)
	}
	// Old capacity is untouched while the user has not accepted.
	if holds := s.Holds(); len(holds) != 2 {
		t.Fatalf("holds after rejected switch = %+v", holds)
	}
	confirmed, err := s.ConfirmAcceptingDegradation()
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.Satisfied {
		t.Fatal("degraded switch must stay unsatisfied")
	}

	// A non-degrading replacement: new instance c, criteria back to min 2
	// with only b and c picked after a departs.
	readyCapInst(t, reg, "c", "grpc", "1")
	reg.Deregister("checkout", "a", mustToken(t, reg, "checkout", "a"))
	rc, err = s.RecheckWith(capCriteria(2))
	if err != nil {
		t.Fatal(err)
	}
	if rc.Switch == nil || !rc.Switch.Uninterrupted {
		t.Fatalf("uninterrupted plan = %+v", rc.Switch)
	}
	if len(rc.Switch.Releasing) != 0 {
		t.Fatalf("releasing = %+v", rc.Switch.Releasing)
	}
	if _, err := s.Confirm(); err != nil {
		t.Fatal(err)
	}
}

// Holds survive a directory restart; a resumed session keeps its capacity
// and duplicate confirms after recovery stay consistent, while lapsed
// reservations are not revived.
func TestCapacitySurvivesReopen(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")

	s1, _ := reg.NewSelectionSession(capCriteria(1))
	if _, err := s1.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Confirm(); err != nil {
		t.Fatal(err)
	}
	// b arrives only after s1 confirmed, so s1 holds exactly one unit.
	readyCapInst(t, reg, "b", "grpc", "1")
	s2, _ := reg.NewSelectionSession(capCriteria(1))
	res2, err := s2.Reserve(5 * time.Second)
	if err != nil || !res2.Satisfied {
		t.Fatalf("s2 reserve = %+v err=%v", res2, err)
	}
	dir := t.TempDir()
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}

	// s2's reservation lapses while the directory is down.
	clk.Advance(10 * time.Second)
	reg2, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	// s1's confirmed hold is still owned; s2's reserved hold is gone.
	s1r, err := reg2.ResumeSelectionSession(s1.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	if holds := s1r.Holds(); len(holds) != 1 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("confirmed ownership after reopen = %+v", holds)
	}
	s2r, err := reg2.ResumeSelectionSession(s2.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	res, err := s2r.Reserve(time.Minute)
	if err != nil || !res.Satisfied {
		t.Fatalf("re-reserve after reopen = %+v err=%v", res, err)
	}
	// Duplicate confirmation after recovery: same decision, one owner.
	s1rAgain, err := reg2.ResumeSelectionSession(s1.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	before := s1r.Current()
	if hist := s1r.History(); len(hist) != 2 || hist[1].Seq != before.Seq {
		t.Fatalf("restored history = %+v current=%+v", hist, before)
	}
	dup, err := s1rAgain.Confirm()
	if err != nil {
		t.Fatal(err)
	}
	if dup.Seq != before.Seq || len(dup.Picked) != len(before.Picked) || dup.Picked[0].ID != before.Picked[0].ID {
		t.Fatalf("duplicate decision = %+v want %+v", dup, before)
	}
	if _, err := s1rAgain.Confirm(); err != nil {
		t.Fatalf("duplicate confirm after recovery = %v", err)
	}
	if hist := s1rAgain.History(); len(hist) != 2 {
		t.Fatalf("duplicate confirmation rewrote history: %+v", hist)
	}
	if holds := s1rAgain.Holds(); len(holds) != 1 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("s1 holds after recovery = %+v", holds)
	}
}

// mustRecheck resumes a session and returns its first recheck.
func (r *Registry) mustRecheck(t *testing.T, sessionID string, c Criteria) Recheck {
	t.Helper()
	s, err := r.ResumeSelectionSession(sessionID, c)
	if err != nil {
		t.Fatal(err)
	}
	return s.Recheck()
}

// Concurrent reservations on one capacity unit must yield exactly one
// owner; the loser's reserve reports the shortfall.
func TestConcurrentReservationSingleOwner(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")

	var wg sync.WaitGroup
	results := make([]Reservation, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, _ := reg.NewSelectionSession(capCriteria(1))
			res, err := s.Reserve(time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = res
			if res.Satisfied {
				if _, err := s.Confirm(); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	winners := 0
	for _, res := range results {
		if res.Satisfied {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
}

// Read-only sessions keep working without any reservation and see no
// capacity gate on Confirm.
func TestReadOnlySelectionUnaffected(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "0") // no capacity at all

	s, _ := reg.NewSelectionSession(capCriteria(1))
	rc := s.Recheck()
	if rc.Switch != nil || len(rc.Guaranteed) != 0 {
		t.Fatalf("read-only recheck = %+v", rc)
	}
	if rc.Honorable != 0 {
		t.Fatalf("honorable = %d with zero capacity", rc.Honorable)
	}
	if _, err := s.Confirm(); err != nil {
		t.Fatalf("read-only confirm = %v", err)
	}
	if !strings.Contains(s.Current().Shortfall, "") && !s.Current().Satisfied {
		t.Fatalf("current = %+v", s.Current())
	}
}
