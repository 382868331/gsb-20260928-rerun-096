package discovery

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Well-known metadata keys used by selection. Capabilities is a
// comma-separated list (e.g. "grpc,http2"); Region is a single label.
const (
	MetaCapabilities = "capabilities"
	MetaRegion       = "region"
)

// ErrNoPendingSelection is returned by Confirm when no Recheck has produced
// a candidate to switch to.
var ErrNoPendingSelection = errors.New("discovery: no pending selection to confirm")

// Criteria describes what a client needs from one selection: a service, the
// capabilities every usable instance must offer, a preferred region (a
// ranking preference, never an exclusion), and the minimum number of usable
// instances the selection must yield.
type Criteria struct {
	Service         string
	Capabilities    []string
	PreferredRegion string
	MinAvailable    int
}

func (c Criteria) validate() error {
	if c.Service == "" {
		return fmt.Errorf("%w: selection requires a service", ErrBadArgument)
	}
	if c.MinAvailable < 0 {
		return fmt.Errorf("%w: MinAvailable must be >= 0", ErrBadArgument)
	}
	return nil
}

// equal reports whether two criteria are identical, so a comparison can
// attribute differences to instance state vs. user conditions.
func (c Criteria) equal(o Criteria) bool {
	if c.Service != o.Service || c.PreferredRegion != o.PreferredRegion ||
		c.MinAvailable != o.MinAvailable {
		return false
	}
	if len(c.Capabilities) != len(o.Capabilities) {
		return false
	}
	a, b := sortedCaps(c.Capabilities), sortedCaps(o.Capabilities)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// describe returns human-readable differences between c and o, used to
// explain that two selections differ because the user changed conditions.
func (c Criteria) describe(o Criteria) []string {
	var out []string
	if c.Service != o.Service {
		out = append(out, fmt.Sprintf("service %q -> %q", c.Service, o.Service))
	}
	if c.PreferredRegion != o.PreferredRegion {
		out = append(out, fmt.Sprintf("preferred region %q -> %q", c.PreferredRegion, o.PreferredRegion))
	}
	if c.MinAvailable != o.MinAvailable {
		out = append(out, fmt.Sprintf("min available %d -> %d", c.MinAvailable, o.MinAvailable))
	}
	a, b := capSet(c.Capabilities), capSet(o.Capabilities)
	for cap := range b {
		if !a[cap] {
			out = append(out, fmt.Sprintf("capability %q added", cap))
		}
	}
	for cap := range a {
		if !b[cap] {
			out = append(out, fmt.Sprintf("capability %q removed", cap))
		}
	}
	sort.Strings(out)
	return out
}

// Verdict explains why one directory instance was adopted or excluded in a
// selection. Reasons are stable, human-readable strings.
type Verdict struct {
	Instance Instance
	Adopted  bool
	// Reasons lists why the instance was excluded, or, when adopted, the
	// positive notes (matched capabilities, preferred region).
	Reasons []string
}

// Selection is one immutable selection record. It captures the criteria,
// the directory version and the full directory snapshot the decision was
// based on, and a per-instance verdict. Records are never mutated after
// creation: re-checking with new conditions appends a new record and leaves
// history untouched.
type Selection struct {
	// Seq is the session-local sequence number, starting at 1.
	Seq int64
	// Criteria is the exact user condition set used for this selection.
	Criteria Criteria
	// Version is the directory version the selection was computed from.
	Version int64
	// At is the registry-clock time of the selection.
	At time.Time
	// Basis is the directory snapshot the selection was based on.
	Basis Snapshot
	// Verdicts covers every live instance of the requested service,
	// adopted or excluded, with reasons.
	Verdicts []Verdict
	// Picked are the adopted instances, preferred region first, then by ID.
	Picked []Instance
	// CandidatePicked is the complete candidate set for a capacity-aware
	// decision. Picked is narrowed to capacity actually owned after
	// Confirm when the candidate could only be partially honored.
	CandidatePicked []Instance `json:"candidate_picked,omitempty"`
	// Honored is the number of candidate instances backed by the session's
	// own live capacity at decision time.
	Honored int `json:"honored,omitempty"`
	// Satisfied reports whether the owned Picked capacity meets the minimum.
	// Capacity-aware confirmations can be unsatisfied only after the user
	// explicitly accepts a degradation; CandidatePicked preserves the full
	// request so Picked remains the actual deliverable set.
	Satisfied bool
	// Shortfall explains why the criteria cannot be met, empty when
	// Satisfied is true.
	Shortfall string
}

// StaleEntry explains why an instance from the confirmed selection is no
// longer usable right now.
type StaleEntry struct {
	Instance Instance
	Reason   string
}

// Recheck is the result of evaluating the current directory against a
// session. Candidate is a fresh, uncommitted selection; Stale lists
// instances from the confirmed selection that have become unusable (lease
// expired, deregistered, drained, capability changed) with the reason.
type Recheck struct {
	Candidate Selection
	// Stale are confirmed picks that are no longer usable, with why.
	Stale []StaleEntry
	// StillUsable are confirmed picks that remain usable under the
	// candidate criteria.
	StillUsable []Instance
	// CriteriaChanged reports whether the candidate used different
	// conditions than the confirmed selection.
	CriteriaChanged bool
	// Honorable is how many candidate picks the session could still
	// reserve right now, given capacity already held by other sessions.
	// A preview may look sufficient while Honorable is smaller; only
	// reserved or confirmed capacity is actually guaranteed.
	Honorable int
	// Guaranteed are confirmed picks whose capacity the session still
	// holds confirmed and which remain usable right now.
	Guaranteed []Instance
	// LostGuarantee explains confirmed picks that currently lack a
	// capacity guarantee (departed, capability lost, hold released or
	// reservation expired).
	LostGuarantee []StaleEntry
	// Switch describes the capacity transition a Confirm would perform;
	// nil for read-only sessions that never reserved capacity.
	Switch *SwitchPlan
}

// SelectionDiff compares two selection records and attributes the
// difference to instance-state changes, user-condition changes, or both.
type SelectionDiff struct {
	FromSeq, ToSeq  int64
	FromVersion     int64
	ToVersion       int64
	CriteriaChanged bool
	CriteriaChanges []string
	Gained          []string // instance IDs picked in To but not From
	Lost            []string // instance IDs picked in From but not To
	LostReasons     map[string]string
}

// CompareSelections explains how selection b differs from selection a. Lost
// reasons come from b's verdicts when the instance was still in the
// directory, or from the departure audit when it has left, so a difference
// is always traceable to either an instance-state change or a criteria
// change.
func CompareSelections(a, b Selection) SelectionDiff {
	d := SelectionDiff{
		FromSeq:     a.Seq,
		ToSeq:       b.Seq,
		FromVersion: a.Version,
		ToVersion:   b.Version,
		LostReasons: map[string]string{},
	}
	if !a.Criteria.equal(b.Criteria) {
		d.CriteriaChanged = true
		d.CriteriaChanges = a.Criteria.describe(b.Criteria)
	}
	inA := map[string]bool{}
	for _, inst := range a.Picked {
		inA[inst.ID] = true
	}
	inB := map[string]bool{}
	for _, inst := range b.Picked {
		inB[inst.ID] = true
		if !inA[inst.ID] {
			d.Gained = append(d.Gained, inst.ID)
		}
	}
	verdictByID := map[string]Verdict{}
	for _, v := range b.Verdicts {
		verdictByID[v.Instance.ID] = v
	}
	for _, inst := range a.Picked {
		if inB[inst.ID] {
			continue
		}
		d.Lost = append(d.Lost, inst.ID)
		if v, ok := verdictByID[inst.ID]; ok && len(v.Reasons) > 0 {
			d.LostReasons[inst.ID] = strings.Join(v.Reasons, "; ")
		} else {
			d.LostReasons[inst.ID] = "no longer in the directory"
		}
	}
	sort.Strings(d.Gained)
	sort.Strings(d.Lost)
	return d
}

// Session is an explainable client selection session over a Registry. It
// holds the confirmed selection, an immutable history of all committed
// selections, and a pending candidate produced by Recheck. A Session never
// sends traffic; it only reads the local directory.
type Session struct {
	reg        *Registry
	mu         sync.Mutex
	id         string
	seq        int64
	current    Selection
	history    []Selection
	pending    *Selection
	hasPending bool
	// capacityAware is true once the session has reserved capacity; such
	// sessions get guarantee reporting and degradation gates on Confirm.
	capacityAware bool
	// reservationID identifies the session's live reservation group.
	reservationID string
	// lastConfirm makes a duplicate Confirm return the same decision
	// instead of failing, so retries never double-allocate capacity.
	lastConfirm    Selection
	hasLastConfirm bool
	// pendingTopUp is the live top-up reservation group, if any.
	pendingTopUp *TopUp
	// lastTopUp makes a duplicate ConfirmTopUp return the same decision.
	lastTopUp    TopUp
	hasLastTopUp bool
	// decisions is the session's immutable capacity decision log, linking
	// candidates, rejections, accepted degradations and top-ups.
	decisions []CapacityDecision
}

type persistedSession struct {
	ID             string             `json:"id"`
	Seq            int64              `json:"seq"`
	Current        Selection          `json:"current"`
	History        []Selection        `json:"history"`
	Pending        *Selection         `json:"pending,omitempty"`
	HasPending     bool               `json:"has_pending"`
	CapacityAware  bool               `json:"capacity_aware"`
	ReservationID  string             `json:"reservation_id,omitempty"`
	LastConfirm    Selection          `json:"last_confirm"`
	HasLastConfirm bool               `json:"has_last_confirm"`
	PendingTopUp   *TopUp             `json:"pending_top_up,omitempty"`
	LastTopUp      TopUp              `json:"last_top_up"`
	HasLastTopUp   bool               `json:"has_last_top_up"`
	Decisions      []CapacityDecision `json:"decisions,omitempty"`
}

func (s *Session) snapshotStateLocked() persistedSession {
	return persistedSession{
		ID:             s.id,
		Seq:            s.seq,
		Current:        cloneSelection(s.current),
		History:        cloneSelections(s.history),
		Pending:        cloneSelectionPtr(s.pending),
		HasPending:     s.hasPending,
		CapacityAware:  s.capacityAware,
		ReservationID:  s.reservationID,
		LastConfirm:    cloneSelection(s.lastConfirm),
		HasLastConfirm: s.hasLastConfirm,
		PendingTopUp:   cloneTopUpPtr(s.pendingTopUp),
		LastTopUp:      cloneTopUp(s.lastTopUp),
		HasLastTopUp:   s.hasLastTopUp,
		Decisions:      append([]CapacityDecision(nil), s.decisions...),
	}
}

func (s *Session) persistState() {
	s.mu.Lock()
	state := s.snapshotStateLocked()
	s.reg.mu.Lock()
	s.reg.sessions[state.ID] = state
	s.reg.mu.Unlock()
	s.mu.Unlock()
}

func (s *Session) restoreStateLocked(state persistedSession) {
	s.id = state.ID
	s.seq = state.Seq
	s.current = cloneSelection(state.Current)
	s.history = cloneSelections(state.History)
	s.pending = cloneSelectionPtr(state.Pending)
	s.hasPending = state.HasPending
	s.capacityAware = state.CapacityAware
	s.reservationID = state.ReservationID
	s.lastConfirm = cloneSelection(state.LastConfirm)
	s.hasLastConfirm = state.HasLastConfirm
	s.pendingTopUp = cloneTopUpPtr(state.PendingTopUp)
	s.lastTopUp = cloneTopUp(state.LastTopUp)
	s.hasLastTopUp = state.HasLastTopUp
	s.decisions = append([]CapacityDecision(nil), state.Decisions...)
}

func cloneSelectionPtr(in *Selection) *Selection {
	if in == nil {
		return nil
	}
	out := cloneSelection(*in)
	return &out
}

// missingCount clamps min-available minus honored at zero.
func missingCount(c Criteria, honored int) int {
	if m := c.MinAvailable - honored; m > 0 {
		return m
	}
	return 0
}

// recordDecisionLocked appends one entry to the session's capacity
// decision log. Read-only sessions keep no log. Callers must hold s.mu.
func (s *Session) recordDecisionLocked(kind DecisionKind, candSeq int64, promised, honored, missing int, reason string) {
	if !s.capacityAware {
		return
	}
	s.decisions = append(s.decisions, CapacityDecision{
		Seq:          int64(len(s.decisions)) + 1,
		CandidateSeq: candSeq,
		Kind:         kind,
		At:           s.reg.clock.Now(),
		Promised:     promised,
		Honored:      honored,
		Missing:      missing,
		Reason:       reason,
	})
}

// Decisions returns the session's capacity decision log in order, oldest
// first. Each candidate, rejection, accepted degradation and top-up is one
// immutable entry; entries sharing a CandidateSeq describe the same
// selection candidate, so an operator can trace which candidate was
// rejected and which degradation scope was explicitly accepted.
func (s *Session) Decisions() []CapacityDecision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]CapacityDecision(nil), s.decisions...)
}

func cloneSelections(in []Selection) []Selection {
	if in == nil {
		return nil
	}
	out := make([]Selection, len(in))
	for i := range in {
		out[i] = cloneSelection(in[i])
	}
	return out
}

func cloneSelection(in Selection) Selection {
	out := in
	out.Criteria.Capabilities = append([]string(nil), in.Criteria.Capabilities...)
	out.Basis.Instances = cloneInstances(in.Basis.Instances)
	out.Verdicts = append([]Verdict(nil), in.Verdicts...)
	for i := range out.Verdicts {
		out.Verdicts[i].Instance = cloneInst(in.Verdicts[i].Instance)
		out.Verdicts[i].Reasons = append([]string(nil), in.Verdicts[i].Reasons...)
	}
	out.Picked = cloneInstances(in.Picked)
	out.CandidatePicked = cloneInstances(in.CandidatePicked)
	return out
}

func cloneInstances(in []Instance) []Instance {
	if in == nil {
		return nil
	}
	out := make([]Instance, len(in))
	for i := range in {
		out[i] = cloneInst(in[i])
	}
	return out
}

// NewSelectionSession starts a session and commits the first selection
// computed from the current directory state.
func (r *Registry) NewSelectionSession(c Criteria) (*Session, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	s := &Session{reg: r, id: newToken()}
	first := r.evaluate(c, 1)
	s.seq = 1
	s.current = first
	s.history = append(s.history, first)
	s.persistState()
	return s, nil
}

// Current returns the confirmed selection record. It stays valid as a
// historical record even after instances in it depart; use Recheck to see
// what is usable right now.
func (s *Session) Current() Selection {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// History returns every committed selection in order, oldest first. Records
// are immutable snapshots; later criteria changes never rewrite them.
func (s *Session) History() []Selection {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Selection(nil), s.history...)
}

// Recheck evaluates the session's current criteria against the live
// directory without switching. The result separates the immutable confirmed
// record from what is usable at this moment: expired, deregistered or
// capability-changed picks appear in Stale with their reason and are never
// reported as usable.
func (s *Session) Recheck() Recheck {
	s.mu.Lock()
	c := s.current.Criteria
	s.mu.Unlock()
	return s.recheck(c)
}

// RecheckWith evaluates new user conditions against the live directory
// without committing anything. The confirmed record and history keep their
// original criteria; only a later Confirm appends a new record.
func (s *Session) RecheckWith(c Criteria) (Recheck, error) {
	if err := c.validate(); err != nil {
		return Recheck{}, err
	}
	return s.recheck(c), nil
}

func (s *Session) recheck(c Criteria) Recheck {
	s.mu.Lock()
	base := s.current
	s.seq++
	seq := s.seq
	aware := s.capacityAware
	s.reg.mu.Lock()
	cand := s.reg.evaluateLocked(c, seq)
	s.pending = &cand
	s.hasPending = true

	rc := Recheck{
		Candidate:       cand,
		CriteriaChanged: !base.Criteria.equal(c),
	}
	usable := map[string]bool{}
	for _, inst := range cand.Picked {
		usable[inst.ID] = true
	}
	verdictByID := map[string]Verdict{}
	for _, v := range cand.Verdicts {
		verdictByID[v.Instance.ID] = v
	}
	for _, inst := range base.Picked {
		if usable[inst.ID] {
			rc.StillUsable = append(rc.StillUsable, inst)
			continue
		}
		rc.Stale = append(rc.Stale, StaleEntry{
			Instance: inst,
			Reason:   s.reg.staleReasonLocked(inst, c, verdictByID[inst.ID]),
		})
	}
	s.fillCapacityViewLocked(&rc, base, c, usable, aware)
	if aware {
		s.recordDecisionLocked(DecisionCandidate, cand.Seq,
			len(cand.Picked), rc.Honorable, missingCount(c, rc.Honorable), "")
	}
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return rc
}

// fillCapacityView annotates a recheck with the session's capacity
// position: how many candidate picks remain honorable, which confirmed
// picks are still guaranteed, and what a switch would do. Read-only
// sessions only get the Honorable count.
func (s *Session) fillCapacityViewLocked(rc *Recheck, base Selection, c Criteria, usable map[string]bool, aware bool) {
	now := s.reg.clock.Now()
	s.reg.expireStaleLocked(now)
	for _, inst := range rc.Candidate.Picked {
		if s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) ||
			s.reg.freeCapacityLocked(inst, s.id) > 0 {
			rc.Honorable++
		}
	}
	if !aware {
		return
	}
	confirmed := map[string]bool{}
	for _, h := range s.reg.sessionHoldsLocked(s.id, HoldConfirmed) {
		confirmed[h.InstanceID] = true
	}
	verdictByID := map[string]Verdict{}
	for _, v := range rc.Candidate.Verdicts {
		verdictByID[v.Instance.ID] = v
	}
	for _, inst := range base.Picked {
		if confirmed[inst.ID] && usable[inst.ID] {
			rc.Guaranteed = append(rc.Guaranteed, inst)
			continue
		}
		reason := ""
		switch {
		case confirmed[inst.ID]:
			reason = s.reg.staleReasonLocked(inst, c, verdictByID[inst.ID])
		default:
			if rec, gone := s.reg.removals[key(inst.Service, inst.ID)]; gone {
				reason = departureReason(rec)
			} else {
				reason = "no capacity hold (reservation expired, cancelled or released)"
			}
		}
		rc.LostGuarantee = append(rc.LostGuarantee, StaleEntry{Instance: inst, Reason: reason})
	}
	if len(confirmed) > 0 {
		plan := &SwitchPlan{
			OldReleaseAt:   "confirmed holds on instances outside the candidate are released when Confirm commits the switch",
			NewEffectiveAt: "reserved capacity becomes confirmed usage at Confirm; unreserved units are acquired then if still free",
		}
		picked := map[string]bool{}
		for _, inst := range rc.Candidate.Picked {
			picked[inst.ID] = true
		}
		for _, h := range s.reg.sessionHoldsLocked(s.id, HoldConfirmed) {
			if h.Service != c.Service || !picked[h.InstanceID] {
				plan.Releasing = append(plan.Releasing, h.InstanceID)
			}
		}
		sort.Strings(plan.Releasing)
		plan.Uninterrupted = rc.Honorable >= c.MinAvailable
		if !plan.Uninterrupted {
			plan.Note = fmt.Sprintf(
				"only %d of %d required instance(s) can be guaranteed after the switch; Confirm requires explicit degradation acceptance",
				rc.Honorable, c.MinAvailable)
		}
		rc.Switch = plan
	}
}

func departureReason(rec RemovalRecord) string {
	switch rec.Reason {
	case ReasonExpired:
		return "lease expired"
	case ReasonDeregistered:
		return "deregistered"
	}
	return "left the directory"
}

// Confirm switches the session to the pending candidate from the last
// Recheck/RecheckWith/Reserve and appends it to the immutable history. The
// previous record is kept unchanged.
//
// For capacity-aware sessions Confirm also settles capacity: live reserved
// holds become confirmed usage, a reservation that already lapsed is
// re-acquired only if the capacity is still free (ErrCapacityUnavailable
// otherwise, leaving the candidate pending), and confirmed holds on
// instances outside the new selection are released. A switch that drops a
// currently-satisfied session below its minimum is rejected with
// ErrDegradationNeedsAccept unless the user explicitly accepts via
// ConfirmAcceptingDegradation. Repeating Confirm after a successful one
// returns the same decision without allocating anything again.
func (s *Session) Confirm() (Selection, error) {
	return s.confirm(false)
}

// ConfirmAcceptingDegradation is Confirm with an explicit user acceptance
// of dropping below the minimum number of guaranteed instances.
func (s *Session) ConfirmAcceptingDegradation() (Selection, error) {
	return s.confirm(true)
}

func (s *Session) confirm(acceptDegradation bool) (Selection, error) {
	s.mu.Lock()
	if !s.hasPending {
		if s.capacityAware && s.hasLastConfirm {
			sel := s.lastConfirm
			s.mu.Unlock()
			return sel, nil
		}
		s.mu.Unlock()
		return Selection{}, ErrNoPendingSelection
	}
	cand := *s.pending
	aware := s.capacityAware
	resID := s.reservationID
	s.mu.Unlock()

	if aware {
		now := s.reg.clock.Now()
		s.reg.mu.Lock()
		s.reg.expireStaleLocked(now)
		var plan []Instance
		liveReservation := false
		for _, inst := range cand.Picked {
			if _, live := s.reg.instances[key(inst.Service, inst.ID)]; !live {
				continue
			}
			if s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
				plan = append(plan, inst)
				continue
			}
			if resID != "" {
				reservedID := resID + ":" + inst.ID
				if h, ok := s.reg.holds[reservedID]; ok && h.Kind == HoldReserved &&
					h.SessionID == s.id && h.Service == inst.Service {
					liveReservation = true
					plan = append(plan, inst)
				}
				continue
			}
		}
		canAcquire := resID == "" || !liveReservation
		if canAcquire {
			planned := map[string]bool{}
			for _, inst := range plan {
				planned[inst.ID] = true
			}
			for _, inst := range cand.Picked {
				if planned[inst.ID] {
					continue
				}
				if _, live := s.reg.instances[key(inst.Service, inst.ID)]; live &&
					s.reg.freeCapacityLocked(inst, s.id) > 0 {
					plan = append(plan, inst)
				}
			}
		}
		honored := len(plan)
		if honored < cand.Criteria.MinAvailable {
			var reject error
			switch {
			case resID != "" && !liveReservation && honored == 0:
				reject = fmt.Errorf(
					"%w: reservation is gone and only %d of %d required instance(s) still have free capacity",
					ErrCapacityUnavailable, honored, cand.Criteria.MinAvailable)
			case (resID == "" || liveReservation) && honored == 0:
				reject = fmt.Errorf(
					"%w: only %d of %d required instance(s) have free capacity",
					ErrCapacityUnavailable, honored, cand.Criteria.MinAvailable)
			case !acceptDegradation:
				reject = fmt.Errorf(
					"%w: only %d of %d required instance(s) can be guaranteed; explicit acceptance required",
					ErrDegradationNeedsAccept, honored, cand.Criteria.MinAvailable)
			}
			if reject != nil {
				s.reg.mu.Unlock()
				s.noteDecision(DecisionRejected, cand.Seq,
					len(cand.Picked), honored, missingCount(cand.Criteria, honored), reject.Error())
				return Selection{}, reject
			}
		}

		committedPicked := map[string]bool{}
		for _, inst := range plan {
			committedPicked[inst.ID] = true
			if s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
				continue
			}
			if canAcquire || liveReservation {
				reservedID := resID + ":" + inst.ID
				if h, ok := s.reg.holds[reservedID]; ok && h.Kind == HoldReserved &&
					h.SessionID == s.id && h.Service == inst.Service {
					h.Kind = HoldConfirmed
					h.ExpiresAt = time.Time{}
					s.reg.holds[reservedID] = h
					continue
				}
			}
			if canAcquire && s.reg.freeCapacityLocked(inst, s.id) > 0 {
				hid := newToken()
				s.reg.holds[hid] = Hold{
					ID:         hid,
					SessionID:  s.id,
					Service:    inst.Service,
					InstanceID: inst.ID,
					Kind:       HoldConfirmed,
					CreatedAt:  now,
				}
			}
		}

		for id, h := range s.reg.holds {
			if h.SessionID == s.id && h.Kind == HoldReserved &&
				(resID == "" || strings.HasPrefix(id, resID+":")) {
				delete(s.reg.holds, id)
			}
			if h.SessionID == s.id && h.Kind == HoldConfirmed &&
				(h.Service != cand.Criteria.Service || !committedPicked[h.InstanceID]) {
				delete(s.reg.holds, id)
			}
		}
		s.reg.mu.Unlock()

		committed := cand
		committed.CandidatePicked = cloneInstances(cand.Picked)
		committed.Picked = cloneInstances(plan)
		committed.Honored = honored
		committed.Satisfied = committed.Honored >= committed.Criteria.MinAvailable
		if !committed.Satisfied {
			committed.Shortfall = fmt.Sprintf(
				"only %d of %d required instance(s) are confirmed and usable",
				committed.Honored, committed.Criteria.MinAvailable)
		}
		cand = committed
	}

	s.mu.Lock()
	s.current = cand
	s.history = append(s.history, s.current)
	s.pending = nil
	s.hasPending = false
	s.reservationID = ""
	s.lastConfirm = cand
	s.hasLastConfirm = true
	if aware {
		kind := DecisionConfirmed
		if acceptDegradation && !cand.Satisfied {
			kind = DecisionDegradationAccepted
		}
		promised := len(cand.Picked)
		if len(cand.CandidatePicked) > 0 {
			promised = len(cand.CandidatePicked)
		}
		s.recordDecisionLocked(kind, cand.Seq,
			promised, cand.Honored, missingCount(cand.Criteria, cand.Honored), "")
	}
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return s.current, nil
}

// noteDecision records a capacity decision outside the session lock and
// persists the session state, used on confirm rejection paths.
func (s *Session) noteDecision(kind DecisionKind, candSeq int64, promised, honored, missing int, reason string) {
	s.mu.Lock()
	s.recordDecisionLocked(kind, candSeq, promised, honored, missing, reason)
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
}

// evaluate computes a selection from the live directory under the registry
// lock, so expiries are applied inline and the basis snapshot, version and
// verdicts are mutually consistent even while notifications and queries
// interleave.
func (r *Registry) evaluate(c Criteria, seq int64) Selection {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.evaluateLocked(c, seq)
}

func (r *Registry) evaluateLocked(c Criteria, seq int64) Selection {
	now := r.clock.Now()
	r.expireStaleLocked(now)
	basis := r.snapshotLocked()

	var candidates []Instance
	for _, inst := range basis.Instances {
		if inst.Service == c.Service {
			candidates = append(candidates, inst)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })

	sel := Selection{
		Seq: seq,
		Criteria: Criteria{
			Service:         c.Service,
			Capabilities:    append([]string(nil), c.Capabilities...),
			PreferredRegion: c.PreferredRegion,
			MinAvailable:    c.MinAvailable,
		},
		Version: r.version,
		At:      now,
		Basis:   *basis,
	}
	for _, inst := range candidates {
		v := Verdict{Instance: inst}
		if st := inst.State; st != StateReady {
			if st == "" {
				st = StatePreparing
			}
			v.Reasons = append(v.Reasons, "not ready (state="+string(st)+")")
		}
		have := capSet(strings.Split(inst.Metadata[MetaCapabilities], ","))
		for _, cap := range sortedCaps(c.Capabilities) {
			if !have[cap] {
				v.Reasons = append(v.Reasons, "missing capability "+quote(cap))
			}
		}
		if len(v.Reasons) == 0 {
			v.Adopted = true
			v.Reasons = append(v.Reasons, "ready with all required capabilities")
			if c.PreferredRegion != "" && inst.Metadata[MetaRegion] == c.PreferredRegion {
				v.Reasons = append(v.Reasons, "in preferred region "+quote(c.PreferredRegion))
			}
			sel.Picked = append(sel.Picked, inst)
		}
		sel.Verdicts = append(sel.Verdicts, v)
	}
	// Rank: preferred region first, then stable by ID.
	sort.SliceStable(sel.Picked, func(i, j int) bool {
		pi := sel.Picked[i].Metadata[MetaRegion] == c.PreferredRegion && c.PreferredRegion != ""
		pj := sel.Picked[j].Metadata[MetaRegion] == c.PreferredRegion && c.PreferredRegion != ""
		if pi != pj {
			return pi
		}
		return sel.Picked[i].ID < sel.Picked[j].ID
	})
	sel.Satisfied = len(sel.Picked) >= c.MinAvailable
	if !sel.Satisfied {
		sel.Shortfall = fmt.Sprintf(
			"need at least %d usable instance(s) for %q but only %d satisfy the conditions",
			c.MinAvailable, c.Service, len(sel.Picked))
	}
	return sel
}

// staleReason explains why a previously picked instance is unusable now,
// preferring the departure audit (expired vs deregistered) and falling back
// to the candidate verdict for live-but-ineligible instances.
func (r *Registry) staleReason(inst Instance, c Criteria, v Verdict) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.staleReasonLocked(inst, c, v)
}

// staleReasonLocked is staleReason for callers already holding r.mu.
func (r *Registry) staleReasonLocked(inst Instance, c Criteria, v Verdict) string {
	rec, gone := r.removals[key(inst.Service, inst.ID)]
	if gone {
		return departureReason(rec)
	}
	if len(v.Reasons) > 0 {
		return strings.Join(v.Reasons, "; ")
	}
	return "no longer selected"
}

func capSet(caps []string) map[string]bool {
	m := make(map[string]bool, len(caps))
	for _, c := range caps {
		c = strings.TrimSpace(c)
		if c != "" {
			m[c] = true
		}
	}
	return m
}

func sortedCaps(caps []string) []string {
	set := capSet(caps)
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func quote(s string) string { return fmt.Sprintf("%q", s) }
