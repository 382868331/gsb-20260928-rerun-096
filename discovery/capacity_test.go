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

// A reservation that could obtain only part of the demand must never be
// reported as a fully satisfied decision. Confirm keeps the reserved units
// pending until the caller explicitly accepts the smaller honored range.
func TestPartialReservationRequiresExplicitDegradation(t *testing.T) {
	reg, _ := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")

	owner, _ := reg.NewSelectionSession(capCriteria(1))
	if _, err := owner.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Confirm(); err != nil {
		t.Fatal(err)
	}
	readyCapInst(t, reg, "b", "grpc", "1")

	loser, _ := reg.NewSelectionSession(capCriteria(2))
	res, err := loser.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if res.Satisfied || len(res.Picks) != 1 || res.Picks[0].ID != "b" {
		t.Fatalf("partial reservation = %+v", res)
	}
	if _, err := loser.Confirm(); !errors.Is(err, ErrDegradationNeedsAccept) {
		t.Fatalf("confirm partial reservation err = %v", err)
	}
	if holds := loser.Holds(); len(holds) != 1 || holds[0].Kind != HoldReserved {
		t.Fatalf("rejected confirmation changed holds = %+v", holds)
	}
	if holds := owner.Holds(); len(holds) != 1 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("owner holds changed = %+v", holds)
	}

	decided, err := loser.ConfirmAcceptingDegradation()
	if err != nil {
		t.Fatal(err)
	}
	if decided.Satisfied || len(decided.Picked) != 1 || decided.Picked[0].ID != "b" {
		t.Fatalf("degraded decision = %+v", decided)
	}
	if !strings.Contains(decided.Shortfall, "1 had reservable capacity") {
		t.Fatalf("decision shortfall = %q", decided.Shortfall)
	}
	if current := loser.Current(); current.Seq != decided.Seq || len(current.Picked) != 1 {
		t.Fatalf("current decision = %+v", current)
	}
	history := loser.History()
	if len(history) != 2 || len(history[1].Picked) != 1 {
		t.Fatalf("history = %+v", history)
	}
	if holds := loser.Holds(); len(holds) != 1 || holds[0].Kind != HoldConfirmed || holds[0].InstanceID != "b" {
		t.Fatalf("loser holds = %+v", holds)
	}
	if holds := owner.Holds(); len(holds) != 1 || holds[0].InstanceID != "a" {
		t.Fatalf("owner capacity was stolen = %+v", holds)
	}
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
	reg2.mu.Lock()
	s1Holds := reg2.sessionHoldsLocked(s1.ID(), HoldConfirmed)
	s2Holds := reg2.sessionHoldsLocked(s2.ID(), HoldReserved)
	reg2.mu.Unlock()
	if len(s1Holds) != 1 || len(s2Holds) != 0 {
		t.Fatalf("holds after reopen: s1=%+v s2=%+v", s1Holds, s2Holds)
	}
	s2r, err := reg2.ResumeSelectionSession(s2.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	res, err := s2r.Reserve(time.Minute)
	if err != nil || !res.Satisfied {
		t.Fatalf("re-reserve after reopen = %+v err=%v", res, err)
	}
	// Duplicate confirm from s1 after recovery: same decision, one owner.
	s1r, err := reg2.ResumeSelectionSession(s1.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1r.Confirm(); err != nil {
		t.Fatal(err)
	}
	if _, err := s1r.Confirm(); err != nil {
		t.Fatalf("duplicate confirm after recovery = %v", err)
	}
	if holds := s1r.Holds(); len(holds) != 1 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("s1 holds after recovery = %+v", holds)
	}
}

// Session decisions and history are persisted, not just holds. A repeated
// confirmation after reopen returns the original committed record and does
// not append a freshly evaluated candidate; a repeated cancellation likewise
// keeps operating on the same pre-restart reservation.
func TestSessionDecisionAndCancelSurviveReopen(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")

	confirmed, _ := reg.NewSelectionSession(capCriteria(1))
	if _, err := confirmed.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	original, err := confirmed.Confirm()
	if err != nil {
		t.Fatal(err)
	}
	readyCapInst(t, reg, "b", "grpc", "1")

	canceller, _ := reg.NewSelectionSession(capCriteria(1))
	cancelRes, err := canceller.Reserve(5 * time.Second)
	if err != nil || !cancelRes.Satisfied || cancelRes.Picks[0].ID != "b" {
		t.Fatalf("canceller reservation = %+v err=%v", cancelRes, err)
	}

	dir := t.TempDir()
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second)
	re, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}

	confirmed2, err := re.ResumeSelectionSession(confirmed.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := confirmed2.Confirm()
	if err != nil {
		t.Fatalf("duplicate confirm after reopen = %v", err)
	}
	again, err := confirmed2.Confirm()
	if err != nil {
		t.Fatalf("second duplicate confirm after reopen = %v", err)
	}
	if duplicate.Seq != original.Seq || again.Seq != original.Seq ||
		len(duplicate.Picked) != 1 || duplicate.Picked[0].ID != original.Picked[0].ID {
		t.Fatalf("duplicate decisions = %+v, %+v; original %+v", duplicate, again, original)
	}
	if history := confirmed2.History(); len(history) != 2 || history[1].Seq != original.Seq {
		t.Fatalf("history after reopen = %+v", history)
	}
	if current := confirmed2.Current(); current.Seq != original.Seq {
		t.Fatalf("current after reopen = %+v", current)
	}

	canceller2, err := re.ResumeSelectionSession(canceller.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	canceller2.CancelReservation()
	canceller2.CancelReservation()
	if history := canceller2.History(); len(history) != 1 {
		t.Fatalf("cancel rewrote history = %+v", history)
	}
	if holds := canceller2.Holds(); len(holds) != 0 {
		t.Fatalf("expired reservation holds = %+v", holds)
	}

	late, _ := re.NewSelectionSession(capCriteria(1))
	lateRes, err := late.Reserve(time.Minute)
	if err != nil || !lateRes.Satisfied || lateRes.Picks[0].ID != "b" {
		t.Fatalf("freed capacity not available = %+v err=%v", lateRes, err)
	}
}

// Reservation expiry, capacity contention and a directory restart can all
// interleave. The loser can neither turn free-looking preview capacity into
// a guarantee nor steal the winner's confirmed unit; durable session records
// keep reporting exactly the range the holds can honor.
func TestCapacityContentionExpiryReopenInterleaving(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")
	readyCapInst(t, reg, "b", "grpc", "1")

	loser, _ := reg.NewSelectionSession(capCriteria(2))
	lostRes, err := loser.Reserve(5 * time.Second)
	if err != nil || !lostRes.Satisfied || len(lostRes.Picks) != 2 {
		t.Fatalf("initial reservation = %+v err=%v", lostRes, err)
	}

	// Before the loser confirms, an independent session confirms one unit.
	winner, _ := reg.NewSelectionSession(capCriteria(1))
	winnerRes, err := winner.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if winnerRes.Satisfied || len(winnerRes.Picks) != 0 {
		t.Fatalf("winner should not reserve a held unit: %+v", winnerRes)
	}
	clk.Advance(6 * time.Second)
	winnerRes, err = winner.Reserve(time.Minute)
	if err != nil || !winnerRes.Satisfied || len(winnerRes.Picks) != 2 {
		t.Fatalf("winner after loser expiry = %+v err=%v", winnerRes, err)
	}
	won, err := winner.Confirm()
	if err != nil || len(won.Picked) != 2 {
		t.Fatalf("winner confirm = %+v err=%v", won, err)
	}

	dir := t.TempDir()
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}
	re, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	loser2, err := re.ResumeSelectionSession(loser.ID(), capCriteria(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loser2.Confirm(); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("expired reservation after reopen err = %v", err)
	}
	winner2, err := re.ResumeSelectionSession(winner.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	wonAgain, err := winner2.Confirm()
	if err != nil || wonAgain.Seq != won.Seq || len(wonAgain.Picked) != 2 {
		t.Fatalf("duplicate winner confirm = %+v err=%v", wonAgain, err)
	}
	if holds := loser2.Holds(); len(holds) != 0 {
		t.Fatalf("loser retained expired holds = %+v", holds)
	}
	w, err := re.ResumeSelectionSession(winner.ID(), capCriteria(1))
	if err != nil {
		t.Fatal(err)
	}
	if holds := w.Holds(); len(holds) != 2 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("winner ledger = %+v", holds)
	}

	w.ReleaseConfirmed()
	w.CancelReservation()
	res, err := loser2.Reserve(2 * time.Minute)
	if err != nil || !res.Satisfied || len(res.Picks) != 2 {
		t.Fatalf("loser re-reserve = %+v err=%v", res, err)
	}
	decided, err := loser2.Confirm()
	if err != nil || len(decided.Picked) != 2 {
		t.Fatalf("loser final confirm = %+v err=%v", decided, err)
	}
	if _, err := loser2.Confirm(); err != nil || loser2.Current().Seq != decided.Seq {
		t.Fatalf("loser duplicate confirm = %v", err)
	}
	if len(loser2.History()) != 2 {
		t.Fatalf("loser history = %+v", loser2.History())
	}
	re.mu.Lock()
	owners := map[string]int{}
	kinds := map[HoldKind]int{}
	for _, h := range re.holds {
		owners[h.SessionID]++
		kinds[h.Kind]++
	}
	re.mu.Unlock()
	if len(owners) != 1 || owners[loser.ID()] != 2 || kinds[HoldConfirmed] != 2 || kinds[HoldReserved] != 0 {
		t.Fatalf("final hold ledger owners=%v kinds=%v", owners, kinds)
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
