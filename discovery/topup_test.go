package discovery

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func idsOf(insts []Instance) []string {
	out := make([]string, 0, len(insts))
	for _, inst := range insts {
		out = append(out, inst.ID)
	}
	return out
}

func readyCapInstLease(t *testing.T, reg *Registry, id, caps, capacity string, lease time.Duration) {
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
}

func decisionKinds(ds []CapacityDecision) []DecisionKind {
	out := make([]DecisionKind, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.Kind)
	}
	return out
}

// The full top-up journey: a partial commitment is explicitly accepted, a
// new instance arrives, the original instance expires before the top-up,
// other sessions keep contending fairly, and after a directory reopen a
// duplicate top-up confirmation returns the same decision. Every result
// separates historically promised, currently redeemable and still missing
// capacity; expired capacity is never merged into the current guarantee.
func TestTopUpAfterDegradationExpiryAndReopen(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")
	readyCapInstLease(t, reg, "d", "grpc", "1", 10*time.Minute)

	s, _ := reg.NewSelectionSession(capCriteria(3))
	res, err := s.Reserve(time.Minute)
	if err != nil || res.Satisfied || len(res.Picks) != 2 {
		t.Fatalf("partial reserve = %+v err=%v", res, err)
	}
	if _, err := s.Confirm(); !errors.Is(err, ErrDegradationNeedsAccept) {
		t.Fatalf("confirm err = %v", err)
	}
	decided, err := s.ConfirmAcceptingDegradation()
	if err != nil || decided.Honored != 2 || decided.Satisfied {
		t.Fatalf("degraded decision = %+v err=%v", decided, err)
	}

	// The decision log links candidate, rejection and accepted degradation
	// of the same candidate.
	ds := s.Decisions()
	want := []DecisionKind{DecisionCandidate, DecisionRejected, DecisionDegradationAccepted}
	if len(ds) != 3 {
		t.Fatalf("decisions = %+v", decisionKinds(ds))
	}
	for i, k := range want {
		if ds[i].Kind != k || ds[i].CandidateSeq != decided.Seq {
			t.Fatalf("decision %d = %+v, want kind %q linked to seq %d", i, ds[i], k, decided.Seq)
		}
	}
	if ds[1].Reason == "" || ds[2].Missing != 1 || ds[2].Promised != 2 || ds[2].Honored != 2 {
		t.Fatalf("rejection/acceptance scope = %+v %+v", ds[1], ds[2])
	}

	// A new instance arrives; a contending session fairly reserves it first.
	readyCapInstLease(t, reg, "b", "grpc", "1", 10*time.Minute)
	s2, _ := reg.NewSelectionSession(capCriteria(1))
	res2, err := s2.Reserve(10 * time.Minute)
	if err != nil || !res2.Satisfied || res2.Picks[0].ID != "b" {
		t.Fatalf("contending reserve = %+v err=%v", res2, err)
	}

	// The original instance's lease expires before the top-up.
	clk.Advance(90 * time.Second)
	rc := s.Recheck()
	if got := idsOf(rc.Guaranteed); len(got) != 1 || got[0] != "d" {
		t.Fatalf("guaranteed = %v", got)
	}
	if len(rc.LostGuarantee) != 1 || rc.LostGuarantee[0].Instance.ID != "a" ||
		rc.LostGuarantee[0].Reason != "lease expired" {
		t.Fatalf("lost guarantee = %+v", rc.LostGuarantee)
	}

	// Another new instance; the top-up may only take what is free.
	readyCapInst(t, reg, "c", "grpc", "1")
	top, err := s.TopUp(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(top.Promised); len(got) != 2 || got[0] != "a" || got[1] != "d" {
		t.Fatalf("promised = %v", got)
	}
	if got := idsOf(top.Redeemable); len(got) != 1 || got[0] != "d" {
		t.Fatalf("redeemable = %v", got)
	}
	if len(top.Lost) != 1 || top.Lost[0].Instance.ID != "a" || top.Lost[0].Reason != "lease expired" {
		t.Fatalf("lost = %+v", top.Lost)
	}
	// b is fairly held by s2, so only c is added; the gap stays visible.
	if got := idsOf(top.Added); len(got) != 1 || got[0] != "c" {
		t.Fatalf("added = %v", got)
	}
	if top.Satisfied || top.Missing != 1 || top.Shortfall == "" {
		t.Fatalf("top-up = %+v", top)
	}
	if holds := s2.Holds(); len(holds) != 1 || holds[0].InstanceID != "b" {
		t.Fatalf("contending holds stolen: %+v", holds)
	}

	// Once s2 lets go, a repeated top-up replaces its own group and closes
	// the gap without touching the confirmed hold on d.
	before := s.Holds()
	var dHoldID string
	for _, h := range before {
		if h.InstanceID == "d" && h.Kind == HoldConfirmed {
			dHoldID = h.ID
		}
	}
	if dHoldID == "" {
		t.Fatalf("no confirmed hold on d: %+v", before)
	}
	s2.CancelReservation()
	top, err = s.TopUp(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(top.Added); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("added = %v", got)
	}
	if !top.Satisfied || top.Missing != 0 {
		t.Fatalf("top-up = %+v", top)
	}
	settled, err := s.ConfirmTopUp()
	if err != nil {
		t.Fatal(err)
	}
	if !settled.Satisfied || len(settled.Added) != 2 || len(settled.Redeemable) != 1 {
		t.Fatalf("settled = %+v", settled)
	}
	// Expired capacity is not merged back: a stays lost, the guarantee is
	// exactly d (redeemable) plus b and c (added).
	if len(settled.Lost) != 1 || settled.Lost[0].Instance.ID != "a" {
		t.Fatalf("settled lost = %+v", settled.Lost)
	}
	holds := s.Holds()
	if len(holds) != 3 {
		t.Fatalf("holds = %+v", holds)
	}
	for _, h := range holds {
		if h.Kind != HoldConfirmed {
			t.Fatalf("unconfirmed hold left: %+v", h)
		}
		if h.InstanceID == "d" && h.ID != dHoldID {
			t.Fatalf("confirmed hold on d was replaced: %+v", h)
		}
		if h.InstanceID == "a" {
			t.Fatalf("expired capacity resurrected: %+v", h)
		}
	}

	// Persist, reopen, and resume: a duplicate top-up notification returns
	// the same decision without reallocating or rewriting history.
	dir := t.TempDir()
	if err := reg.Save(dir); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second)
	reg2, err := OpenRegistry(dir, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	sr, err := reg2.ResumeSelectionSession(s.ID(), capCriteria(3))
	if err != nil {
		t.Fatal(err)
	}
	beforeDecisions := len(sr.Decisions())
	beforeHistory := len(sr.History())
	dup, err := sr.ConfirmTopUp()
	if err != nil {
		t.Fatal(err)
	}
	if dup.ID != settled.ID || len(dup.Added) != 2 || !dup.Satisfied {
		t.Fatalf("duplicate top-up = %+v want %+v", dup, settled)
	}
	if holds := sr.Holds(); len(holds) != 3 {
		t.Fatalf("holds after reopen = %+v", holds)
	}
	if len(sr.Decisions()) != beforeDecisions || len(sr.History()) != beforeHistory {
		t.Fatalf("duplicate top-up rewrote log/history: %d->%d decisions, %d->%d history",
			beforeDecisions, len(sr.Decisions()), beforeHistory, len(sr.History()))
	}
	// The resumed view still separates redeemable from expired capacity.
	rc = sr.Recheck()
	if got := idsOf(rc.Guaranteed); len(got) != 1 || got[0] != "d" {
		t.Fatalf("guaranteed after reopen = %v", got)
	}
	if len(rc.LostGuarantee) != 1 || rc.LostGuarantee[0].Reason != "lease expired" {
		t.Fatalf("lost after reopen = %+v", rc.LostGuarantee)
	}
}

// Top-up groups expire and are revoked independently of the main
// reservation and of confirmed usage, and a lapsed top-up is re-acquired
// at confirm time only when the capacity is still free.
func TestTopUpIndependentExpiryAndCancel(t *testing.T) {
	reg, clk := newFakeRegistry(64)
	readyCapInst(t, reg, "a", "grpc", "1")

	s, _ := reg.NewSelectionSession(capCriteria(2))
	if _, err := s.Reserve(time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ConfirmAcceptingDegradation(); err != nil {
		t.Fatal(err)
	}
	readyCapInst(t, reg, "b", "grpc", "1")

	// Cancel revokes only the top-up group; the confirmed hold stays.
	top, err := s.TopUp(time.Minute)
	if err != nil || len(top.Added) != 1 {
		t.Fatalf("top-up = %+v err=%v", top, err)
	}
	s.CancelTopUp()
	s.CancelTopUp()
	holds := s.Holds()
	if len(holds) != 1 || holds[0].InstanceID != "a" || holds[0].Kind != HoldConfirmed {
		t.Fatalf("holds after cancel = %+v", holds)
	}
	if _, err := s.ConfirmTopUp(); !errors.Is(err, ErrNoPendingTopUp) {
		t.Fatalf("confirm after cancel err = %v", err)
	}

	// A lapsed top-up is re-acquired at confirm time only while free.
	if _, err := s.TopUp(10 * time.Second); err != nil {
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
	settled, err := s.ConfirmTopUp()
	if err != nil {
		t.Fatal(err)
	}
	if settled.Satisfied || len(settled.Added) != 0 || settled.Missing != 1 {
		t.Fatalf("settled over contended capacity = %+v", settled)
	}
	if holds := s2.Holds(); len(holds) != 1 || holds[0].InstanceID != "b" {
		t.Fatalf("s2 holds stolen: %+v", holds)
	}

	// When the capacity is free again, a fresh top-up confirms it.
	s2.ReleaseConfirmed()
	top, err = s.TopUp(time.Minute)
	if err != nil || !top.Satisfied {
		t.Fatalf("top-up = %+v err=%v", top, err)
	}
	settled, err = s.ConfirmTopUp()
	if err != nil || !settled.Satisfied || len(settled.Added) != 1 || settled.Added[0].ID != "b" {
		t.Fatalf("settled = %+v err=%v", settled, err)
	}
	// The decision log records the whole top-up lifecycle in order.
	var kinds []string
	for _, d := range s.Decisions() {
		kinds = append(kinds, string(d.Kind))
	}
	joined := strings.Join(kinds, ",")
	for _, want := range []string{"topup-reserved", "topup-cancelled", "topup-confirmed"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("decisions %v missing %q", kinds, want)
		}
	}
}
