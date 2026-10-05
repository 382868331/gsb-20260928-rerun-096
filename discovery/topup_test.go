package discovery

import (
	"errors"
	"testing"
	"time"
)

func readyInstMeta(t *testing.T, reg *Registry, id, caps, capacity string, lease time.Duration) Registration {
	t.Helper()
	h, err := reg.Register(Instance{
		Service: "checkout",
		ID:      id,
		Addr:    id + ":9000",
		Metadata: map[string]string{
			MetaCapabilities: caps,
			MetaCapacity:     capacity,
		},
	}, lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetReady("checkout", id, h.Token); err != nil {
		t.Fatal(err)
	}
	return h
}

func idsOf(insts []Instance) []string {
	out := make([]string, 0, len(insts))
	for _, in := range insts {
		out = append(out, in.ID)
	}
	return out
}

// The full top-up journey: a session accepts a partial decision, new
// instances arrive, the original instance expires before the top-up, and a
// repeated top-up notification after a directory restart returns the same
// decision. Every result separates what was historically promised, what is
// still honored now, and what is still missing; expired capacity is never
// merged into the current guarantee.
func TestTopUpAfterDegradationExpiryAndReopen(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyInstMeta(t, reg, "a", "grpc", "1", 90*time.Second)
	readyInstMeta(t, reg, "b", "grpc,extra", "1", 10*time.Minute)

	// Another session owns b's only unit, confirmed.
	owner, _ := reg.NewSelectionSession(Criteria{
		Service: "checkout", Capabilities: []string{"grpc", "extra"}, MinAvailable: 1,
	})
	ownerRes, err := owner.Reserve(time.Minute)
	if err != nil || !ownerRes.Satisfied || len(ownerRes.Picks) != 1 || ownerRes.Picks[0].ID != "b" {
		t.Fatalf("owner reservation = %+v err=%v", ownerRes, err)
	}
	if _, err := owner.Confirm(); err != nil {
		t.Fatal(err)
	}

	// s needs two instances but can only reserve a; b is held by owner.
	s, _ := reg.NewSelectionSession(capCriteria(2))
	res, err := s.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if res.Satisfied || len(res.Picks) != 1 || res.Picks[0].ID != "a" {
		t.Fatalf("partial reservation = %+v", res)
	}
	if _, err := s.Confirm(); !errors.Is(err, ErrDegradationNeedsAccept) {
		t.Fatalf("partial confirm err = %v", err)
	}
	degraded, err := s.ConfirmAcceptingDegradation()
	if err != nil {
		t.Fatal(err)
	}
	if degraded.Satisfied || degraded.Honored != 1 || len(degraded.CandidatePicked) != 2 {
		t.Fatalf("degraded decision = %+v", degraded)
	}
	if degraded.Degradation == nil || degraded.Degradation.Promised != 2 ||
		degraded.Degradation.Honored != 1 || len(degraded.Degradation.Missing) != 1 ||
		degraded.Degradation.Missing[0] != "b" {
		t.Fatalf("degradation record = %+v", degraded.Degradation)
	}

	// The explanation view ties the promised set to the live guarantee.
	rc := s.Recheck()
	if rc.TopUp == nil {
		t.Fatal("missing top-up plan")
	}
	if got := idsOf(rc.TopUp.Promised); len(got) != 2 {
		t.Fatalf("promised = %v", got)
	}
	if got := idsOf(rc.TopUp.GuaranteedNow); len(got) != 1 || got[0] != "a" {
		t.Fatalf("guaranteed now = %v", got)
	}
	if rc.TopUp.Missing != 1 || len(rc.TopUp.Fillable) != 0 {
		t.Fatalf("missing=%d fillable=%v", rc.TopUp.Missing, idsOf(rc.TopUp.Fillable))
	}
	if rc.TopUp.Note == "" || rc.TopUp.AmendsSeq != degraded.Seq {
		t.Fatalf("top-up plan not linked to decision: %+v", rc.TopUp)
	}

	// New instances arrive, then the original instance's lease expires
	// before any top-up happens.
	readyInstMeta(t, reg, "c", "grpc", "1", 10*time.Minute)
	readyInstMeta(t, reg, "d", "grpc", "1", 10*time.Minute)
	readyInstMeta(t, reg, "e", "grpc", "1", 10*time.Minute)
	clk.Advance(91 * time.Second)

	rc = s.Recheck()
	if len(rc.TopUp.GuaranteedNow) != 0 {
		t.Fatalf("expired capacity counted as guaranteed: %v", idsOf(rc.TopUp.GuaranteedNow))
	}
	lostA := false
	for _, le := range rc.TopUp.LostGuarantee {
		if le.Instance.ID == "a" && le.Reason == "lease expired" {
			lostA = true
		}
	}
	if !lostA {
		t.Fatalf("lost guarantee missing expired a: %+v", rc.TopUp.LostGuarantee)
	}
	if rc.TopUp.Missing != 2 {
		t.Fatalf("missing = %d, want 2", rc.TopUp.Missing)
	}
	if got := idsOf(rc.TopUp.Fillable); len(got) != 3 {
		t.Fatalf("fillable = %v, want c,d,e", got)
	}

	// The top-up reserves only the missing count, never pre-filling the
	// candidate list; other sessions keep contending for the rest.
	topRes, err := s.TopUpReserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !topRes.Satisfied || len(topRes.Picks) != 2 {
		t.Fatalf("top-up reservation = %+v", topRes)
	}
	holds := s.Holds()
	if len(holds) != 2 || holds[0].Kind != HoldReserved {
		t.Fatalf("top-up holds = %+v", holds)
	}
	for _, h := range holds {
		if h.InstanceID == "b" || h.InstanceID == "e" {
			t.Fatalf("top-up over-reserved: %+v", h)
		}
	}
	// A contending session fairly takes the remaining free unit e.
	contender, _ := reg.NewSelectionSession(capCriteria(1))
	cres, err := contender.Reserve(time.Minute)
	if err != nil || !cres.Satisfied || len(cres.Picks) != 1 || cres.Picks[0].ID != "e" {
		t.Fatalf("contender reservation = %+v err=%v", cres, err)
	}
	if holds := owner.Holds(); len(holds) != 1 || holds[0].InstanceID != "b" {
		t.Fatalf("owner capacity disturbed: %+v", holds)
	}

	// Directory restart; the resumed session confirms the top-up.
	dir := t.TempDir()
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}
	reg2, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := reg2.ResumeSelectionSession(s.ID(), capCriteria(2))
	if err != nil {
		t.Fatal(err)
	}
	topped, err := s2.ConfirmTopUp()
	if err != nil {
		t.Fatal(err)
	}
	if !topped.Satisfied || topped.Honored != 2 {
		t.Fatalf("topped decision = %+v", topped)
	}
	for _, inst := range topped.Picked {
		if inst.ID == "a" {
			t.Fatalf("expired capacity merged into guarantee: %+v", topped.Picked)
		}
	}
	if got := idsOf(topped.CandidatePicked); len(got) != 2 {
		t.Fatalf("historical promise lost: %v", got)
	}
	if topped.TopUp == nil || topped.TopUp.Amends != degraded.Seq {
		t.Fatalf("top-up record not linked: %+v", topped.TopUp)
	}
	if got := idsOf(topped.TopUp.Added); len(got) != 2 {
		t.Fatalf("added = %v", got)
	}
	if got := idsOf(topped.TopUp.StillMissing); len(got) != 2 {
		t.Fatalf("still missing = %v, want a,b", got)
	}
	if len(topped.TopUp.ExpiredSincePromise) != 1 ||
		topped.TopUp.ExpiredSincePromise[0].Instance.ID != "a" ||
		topped.TopUp.ExpiredSincePromise[0].Reason != "lease expired" {
		t.Fatalf("expired since promise = %+v", topped.TopUp.ExpiredSincePromise)
	}
	if topped.Degradation == nil || topped.Degradation.Honored != 1 {
		t.Fatalf("degradation link lost: %+v", topped.Degradation)
	}
	if holds := s2.Holds(); len(holds) != 2 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("holds after top-up = %+v", holds)
	}

	// Another restart: a repeated top-up notification returns the same
	// decision without reallocating or appending history.
	dir2 := t.TempDir()
	if err := reg2.Save(dir2); err != nil {
		t.Fatal(err)
	}
	reg3, err := OpenRegistry(dir2, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	s3, err := reg3.ResumeSelectionSession(s.ID(), capCriteria(2))
	if err != nil {
		t.Fatal(err)
	}
	dup, err := s3.ConfirmTopUp()
	if err != nil {
		t.Fatal(err)
	}
	if dup.Seq != topped.Seq || dup.Honored != topped.Honored {
		t.Fatalf("duplicate top-up = %+v want seq %d", dup, topped.Seq)
	}
	if hist, want := s3.History(), len(s2.History()); len(hist) != want {
		t.Fatalf("duplicate top-up rewrote history: %d vs %d", len(hist), want)
	}
	if holds := s3.Holds(); len(holds) != 2 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("holds after duplicate = %+v", holds)
	}
	if _, err := s3.ConfirmTopUp(); err != nil {
		t.Fatalf("repeated duplicate top-up = %v", err)
	}
	if _, err := owner.ConfirmTopUp(); !errors.Is(err, ErrNoPendingTopUp) {
		t.Fatalf("top-up without reservation err = %v", err)
	}
}

// A lapsed top-up reservation is re-acquired only from still-free capacity
// at confirm time; units another session took in the meantime are lost, and
// the result reports exactly what could still be honored.
func TestTopUpReservationExpiryReacquiresOnlyFree(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyInstMeta(t, reg, "a", "grpc,extra", "1", time.Minute)
	readyCapInst(t, reg, "b", "grpc", "1")
	readyCapInst(t, reg, "c", "grpc", "1")

	owner, _ := reg.NewSelectionSession(Criteria{
		Service: "checkout", Capabilities: []string{"grpc", "extra"}, MinAvailable: 1,
	})
	if _, err := owner.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Confirm(); err != nil {
		t.Fatal(err)
	}
	// Owner confirmed a. s needs three but can only reserve b and c.
	s, _ := reg.NewSelectionSession(capCriteria(3))
	if _, err := s.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	degraded, err := s.ConfirmAcceptingDegradation()
	if err != nil {
		t.Fatal(err)
	}
	if degraded.Honored != 2 {
		t.Fatalf("degraded decision = %+v", degraded)
	}
	// d arrives after the degraded decision; the top-up targets it.
	readyCapInst(t, reg, "d", "grpc", "1")
	topRes, err := s.TopUpReserve(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !topRes.Satisfied || len(topRes.Picks) != 1 || topRes.Picks[0].ID != "d" {
		t.Fatalf("top-up reservation = %+v", topRes)
	}
	// The top-up reservation lapses and a contender grabs d.
	clk.Advance(11 * time.Second)
	thief, _ := reg.NewSelectionSession(capCriteria(1))
	if _, err := thief.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	stolen, err := thief.Confirm()
	if err != nil {
		t.Fatal(err)
	}
	if len(stolen.Picked) != 1 || stolen.Picked[0].ID != "d" {
		t.Fatalf("contender confirm = %+v", stolen)
	}
	// Confirming the lapsed top-up adds nothing, but never steals d back
	// and never disturbs the previously confirmed holds on b and c.
	topped, err := s.ConfirmTopUp()
	if err != nil {
		t.Fatal(err)
	}
	if topped.Satisfied || topped.Honored != 2 || len(topped.Picked) != 2 {
		t.Fatalf("lapsed top-up decision = %+v", topped)
	}
	if len(topped.TopUp.Added) != 0 {
		t.Fatalf("lapsed top-up added capacity: %+v", topped.TopUp.Added)
	}
	if holds := thief.Holds(); len(holds) != 1 || holds[0].InstanceID != "d" {
		t.Fatalf("contender capacity stolen: %+v", holds)
	}
	if holds := s.Holds(); len(holds) != 2 || holds[0].Kind != HoldConfirmed {
		t.Fatalf("existing confirmed hold replaced: %+v", holds)
	}
}
