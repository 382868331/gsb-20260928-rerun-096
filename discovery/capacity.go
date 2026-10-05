package discovery

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MetaCapacity is the metadata key carrying an instance's capacity: the
// number of capacity units it can serve concurrently. Instances without
// the key have capacity 1. A unit is held by exactly one session at a
// time, either as a temporary reservation or as confirmed usage.
const MetaCapacity = "capacity"

// Capacity-related errors.
var (
	// ErrCapacityUnavailable is returned when a reservation expired before
	// Confirm and the required free capacity no longer exists.
	ErrCapacityUnavailable = errors.New("discovery: capacity no longer available")
	// ErrDegradationNeedsAccept is returned when confirming the pending
	// candidate would drop the session below its minimum while it currently
	// holds enough confirmed capacity; the user must explicitly accept the
	// downgrade via ConfirmAcceptingDegradation or keep the old selection.
	ErrDegradationNeedsAccept = errors.New("discovery: switch degrades below minimum; explicit acceptance required")

	// ErrNoPendingTopUp is returned by ConfirmTopUp when no TopUp reservation
	// is pending and no top-up has been confirmed yet.
	ErrNoPendingTopUp = errors.New("discovery: no pending top-up to confirm")
)

// DecisionKind classifies the entries of a session's capacity decision log.
type DecisionKind string

const (
	// DecisionCandidate marks a freshly computed capacity-aware candidate.
	DecisionCandidate DecisionKind = "candidate"
	// DecisionRejected marks a Confirm rejected because capacity was gone
	// or the switch needed an explicit degradation acceptance.
	DecisionRejected DecisionKind = "rejected"
	// DecisionConfirmed marks a successful Confirm at full coverage.
	DecisionConfirmed DecisionKind = "confirmed"
	// DecisionDegradationAccepted marks a Confirm where the user explicitly
	// accepted a partial result below the minimum.
	DecisionDegradationAccepted DecisionKind = "degradation-accepted"
	// DecisionTopUpReserved marks a TopUp reservation over the missing gap.
	DecisionTopUpReserved DecisionKind = "topup-reserved"
	// DecisionTopUpConfirmed marks a ConfirmTopUp settling added capacity.
	DecisionTopUpConfirmed DecisionKind = "topup-confirmed"
	// DecisionTopUpCancelled marks a CancelTopUp revoking only the top-up
	// group, leaving confirmed usage and main reservations untouched.
	DecisionTopUpCancelled DecisionKind = "topup-cancelled"
)

// CapacityDecision is one immutable entry in the session's capacity
// decision log. Candidates, rejections and accepted degradations are
// linked through CandidateSeq, the sequence number of the selection
// candidate they refer to, so an operator can trace exactly which
// candidate was rejected and which degradation scope was accepted.
type CapacityDecision struct {
	// Seq is the session-local decision sequence, starting at 1.
	Seq int64 `json:"seq"`
	// CandidateSeq links the decision to the selection candidate it
	// concerns (the confirmed selection's Seq for top-ups).
	CandidateSeq int64        `json:"candidate_seq"`
	Kind         DecisionKind `json:"kind"`
	At           time.Time    `json:"at"`
	// Promised is the number of instances the candidate committed to.
	Promised int `json:"promised"`
	// Honored is how many were (or would be) actually backed by the
	// session's own capacity at decision time.
	Honored int `json:"honored"`
	// Missing is how many units remained short of the minimum.
	Missing int    `json:"missing"`
	Reason  string `json:"reason,omitempty"`
}

// HoldKind distinguishes a temporary reservation from confirmed usage.
type HoldKind string

const (
	// HoldReserved is a temporary hold that expires unless confirmed in time.
	HoldReserved HoldKind = "reserved"
	// HoldConfirmed is capacity in confirmed use; it has no expiry and is
	// released only by a switch, ReleaseConfirmed, or the instance leaving.
	HoldConfirmed HoldKind = "confirmed"
)

// Hold is one capacity unit held by a session on one instance. Holds are
// the single source of truth for capacity ownership, so contending
// sessions, expiring reservations and recovery-time duplicate
// notifications can never produce contradictory ownership.
type Hold struct {
	// ID uniquely identifies this hold.
	ID string `json:"id"`
	// SessionID identifies the owning selection session.
	SessionID string `json:"session_id"`
	Service   string `json:"service"`
	// InstanceID is the instance the unit is held on.
	InstanceID string    `json:"instance_id"`
	Kind       HoldKind  `json:"kind"`
	CreatedAt  time.Time `json:"created_at"`
	// ExpiresAt bounds reserved holds; zero for confirmed holds.
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// instanceCapacity reads MetaCapacity, defaulting to 1. Invalid or
// negative values mean the instance offers no capacity.
func instanceCapacity(inst Instance) int {
	raw, ok := inst.Metadata[MetaCapacity]
	if !ok || strings.TrimSpace(raw) == "" {
		return 1
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// expireHoldsLocked drops reserved holds whose deadline has passed. It is
// invoked from expireStaleLocked so every read and mutation path applies
// reservation expiry inline, exactly like lease expiry.
func (r *Registry) expireHoldsLocked(now time.Time) {
	for id, h := range r.holds {
		if h.Kind == HoldReserved && !now.Before(h.ExpiresAt) {
			delete(r.holds, id)
		}
	}
}

// dropHoldsForLocked releases every hold on an instance that left the
// directory; the departure itself stays auditable via the removals log and
// the session's immutable selection history.
func (r *Registry) dropHoldsForLocked(service, id string) {
	for hid, h := range r.holds {
		if h.Service == service && h.InstanceID == id {
			delete(r.holds, hid)
		}
	}
}

// freeCapacityLocked reports how many units of inst the given session
// could still take: instance capacity minus units held by OTHER sessions.
// The session's own holds are already its own and never count against it.
func (r *Registry) freeCapacityLocked(inst Instance, sessionID string) int {
	used := 0
	for _, h := range r.holds {
		if h.Service == inst.Service && h.InstanceID == inst.ID && h.SessionID != sessionID {
			used++
		}
	}
	free := instanceCapacity(inst) - used
	if free < 0 {
		return 0
	}
	return free
}

// hasConfirmedHoldLocked reports whether the session already holds
// confirmed capacity on the instance.
func (r *Registry) hasConfirmedHoldLocked(sessionID, service, instanceID string) bool {
	for _, h := range r.holds {
		if h.SessionID == sessionID && h.Service == service &&
			h.InstanceID == instanceID && h.Kind == HoldConfirmed {
			return true
		}
	}
	return false
}

// sessionHoldsLocked returns the session's holds of one kind, sorted by
// instance ID for deterministic output.
func (r *Registry) sessionHoldsLocked(sessionID string, kind HoldKind) []Hold {
	var out []Hold
	for _, h := range r.holds {
		if h.SessionID == sessionID && h.Kind == kind {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out
}

// Reservation is a temporary capacity hold on a set of instances, valid
// until ExpiresAt. Confirm turns it into confirmed usage;
// CancelReservation releases it early; otherwise it lapses on its own.
type Reservation struct {
	// ID identifies the reservation group; each held unit carries it.
	ID        string
	SessionID string
	// Criteria is the condition set the reserved picks were computed from.
	Criteria Criteria
	// Picks are the instances capacity was actually held on, including
	// units the session already held confirmed.
	Picks []Instance
	// ExpiresAt is the registry-clock deadline for Confirm.
	ExpiresAt time.Time
	// Satisfied reports whether len(Picks) >= Criteria.MinAvailable.
	Satisfied bool
	// Shortfall explains why the full demand could not be reserved.
	Shortfall string
}

// SwitchPlan explains the capacity transition a Confirm would perform when
// the session currently holds confirmed capacity: which old holds are
// released, when the new holds take effect, and whether the switch can
// happen without dropping below the required minimum.
type SwitchPlan struct {
	// Releasing lists instance IDs whose confirmed holds are dropped when
	// the pending candidate is confirmed.
	Releasing []string
	// OldReleaseAt describes when the old occupancy is released.
	OldReleaseAt string
	// NewEffectiveAt describes when the new holds take effect.
	NewEffectiveAt string
	// Uninterrupted reports whether the candidate can be confirmed while
	// keeping at least Criteria.MinAvailable guaranteed instances.
	Uninterrupted bool
	// Note explains the degradation when Uninterrupted is false.
	Note string
}

// ID returns the session's stable identity. It survives directory restarts
// when the session is resumed with ResumeSelectionSession.
func (s *Session) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

// Reserve temporarily holds capacity on the pending candidate's picks (or
// on a fresh evaluation of the current criteria when no candidate is
// pending). The candidate becomes the pending selection, so a later
// Confirm switches to exactly the reserved set. Re-reserving replaces the
// session's previous reserved holds; units the session already holds
// confirmed are reused, never double-counted. Capacity held by other
// sessions is never touched, so two sessions cannot reserve the same unit.
func (s *Session) Reserve(ttl time.Duration) (Reservation, error) {
	if ttl <= 0 {
		return Reservation{}, fmt.Errorf("%w: reservation ttl must be positive", ErrBadArgument)
	}
	s.mu.Lock()
	if !s.hasPending {
		s.seq++
		cand := s.reg.evaluate(s.current.Criteria, s.seq)
		s.pending = &cand
		s.hasPending = true
	}
	cand := *s.pending
	oldResID := s.reservationID
	s.mu.Unlock()

	now := s.reg.clock.Now()
	resID := newToken()
	res := Reservation{
		ID:        resID,
		SessionID: s.id,
		Criteria:  cand.Criteria,
		ExpiresAt: now.Add(ttl),
	}
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(now)
	// Re-reserving replaces the session's previous main reservation group;
	// top-up groups have their own lifecycle and stay untouched.
	s.reg.dropHoldGroupLocked(s.id, oldResID)
	for _, inst := range cand.Picked {
		if _, live := s.reg.instances[key(inst.Service, inst.ID)]; !live {
			continue
		}
		if s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
			res.Picks = append(res.Picks, inst)
			continue
		}
		if s.reg.freeCapacityLocked(inst, s.id) <= 0 {
			continue
		}
		s.reg.holds[resID+":"+inst.ID] = Hold{
			ID:         resID + ":" + inst.ID,
			SessionID:  s.id,
			Service:    inst.Service,
			InstanceID: inst.ID,
			Kind:       HoldReserved,
			CreatedAt:  now,
			ExpiresAt:  res.ExpiresAt,
		}
		res.Picks = append(res.Picks, inst)
	}
	s.reg.mu.Unlock()

	res.Satisfied = len(res.Picks) >= cand.Criteria.MinAvailable
	if !res.Satisfied {
		res.Shortfall = fmt.Sprintf(
			"need at least %d usable instance(s) for %q but only %d had reservable capacity",
			cand.Criteria.MinAvailable, cand.Criteria.Service, len(res.Picks))
	}

	s.mu.Lock()
	s.reservationID = resID
	s.capacityAware = true
	s.recordDecisionLocked(DecisionCandidate, cand.Seq,
		len(cand.Picked), len(res.Picks), missingCount(cand.Criteria, len(res.Picks)), "")
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return res, nil
}

// CancelReservation releases the session's reserved (not yet confirmed)
// holds. It is idempotent: repeating it is a no-op and never releases
// capacity twice or touches confirmed usage.
func (s *Session) CancelReservation() {
	s.mu.Lock()
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(s.reg.clock.Now())
	// Only the main reservation group is released; a pending top-up group
	// keeps its own independent expiry and revocation.
	s.reg.dropHoldGroupLocked(s.id, s.reservationID)
	s.reservationID = ""
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
}

// ReleaseConfirmed returns the session's confirmed capacity (e.g. when the
// client is done with the selection). Historical selection records are
// kept; only the live holds are dropped. Idempotent.
func (s *Session) ReleaseConfirmed() {
	s.reg.mu.Lock()
	for id, h := range s.reg.holds {
		if h.SessionID == s.id && h.Kind == HoldConfirmed {
			delete(s.reg.holds, id)
		}
	}
	s.reg.mu.Unlock()
}

// Holds returns the session's currently live holds, reserved first, for
// inspection and tests.
func (s *Session) Holds() []Hold {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	s.reg.expireStaleLocked(s.reg.clock.Now())
	out := s.reg.sessionHoldsLocked(s.id, HoldReserved)
	return append(out, s.reg.sessionHoldsLocked(s.id, HoldConfirmed)...)
}

// ResumeSelectionSession recreates a session with a known ID after a
// directory restart, adopting any holds persisted for that ID. A client
// retrying a confirm or cancel after recovery therefore operates on the
// same capacity it held before, and duplicate notifications cannot create
// contradictory ownership.
func (r *Registry) ResumeSelectionSession(sessionID string, c Criteria) (*Session, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("%w: session id required", ErrBadArgument)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	s := &Session{reg: r, id: sessionID}
	r.mu.Lock()
	state, ok := r.sessions[sessionID]
	r.mu.Unlock()
	if ok {
		s.mu.Lock()
		s.restoreStateLocked(state)
		s.mu.Unlock()
		return s, nil
	}
	first := r.evaluate(c, 1)
	s.seq = 1
	s.current = first
	s.history = append(s.history, first)
	r.mu.Lock()
	for _, h := range r.holds {
		if h.SessionID == sessionID {
			s.capacityAware = true
			break
		}
	}
	r.mu.Unlock()
	s.persistState()
	return s, nil
}

// TopUp is a capacity top-up reservation over the gap between what the
// session's confirmed decision promised and what is still redeemable now.
// It never replaces already-confirmed valid occupancy: existing confirmed
// holds stay untouched and the top-up only adds newly reserved units. The
// top-up group carries its own ID and ExpiresAt, independent of the main
// reservation, and is revoked separately via CancelTopUp. While a top-up
// is pending other sessions contend for the same free capacity on equal
// terms; the top-up reserves only the missing units, never the whole
// candidate list.
type TopUp struct {
	// ID identifies the top-up reservation group; each added unit's hold
	// carries it as a prefix.
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	// Criteria is the condition set of the confirmed decision being topped up.
	Criteria Criteria `json:"criteria"`
	// CandidateSeq links the top-up to the confirmed selection it completes.
	CandidateSeq int64 `json:"candidate_seq"`
	// Promised is the full candidate set the confirmed decision committed to.
	Promised []Instance `json:"promised"`
	// Redeemable are promised instances still backed by the session's live
	// confirmed holds and still usable right now.
	Redeemable []Instance `json:"redeemable"`
	// Lost explains promised instances no longer redeemable (lease expired,
	// deregistered, capability lost, hold gone). Expired capacity is listed
	// here and never merged into the current guarantee.
	Lost []StaleEntry `json:"lost,omitempty"`
	// Added are the newly reserved instances filling (part of) the gap.
	Added []Instance `json:"added,omitempty"`
	// ExpiresAt bounds the added reserved holds; confirmed holds have none.
	ExpiresAt time.Time `json:"expires_at"`
	// Missing is how many units remain short of the minimum after
	// Redeemable+Added.
	Missing int `json:"missing"`
	// Satisfied reports whether Redeemable+Added meets the minimum.
	Satisfied bool `json:"satisfied"`
	// Shortfall explains what is still missing, empty when Satisfied.
	Shortfall string `json:"shortfall,omitempty"`
}

func cloneTopUp(in TopUp) TopUp {
	out := in
	out.Criteria.Capabilities = append([]string(nil), in.Criteria.Capabilities...)
	out.Promised = cloneInstances(in.Promised)
	out.Redeemable = cloneInstances(in.Redeemable)
	out.Lost = append([]StaleEntry(nil), in.Lost...)
	for i := range out.Lost {
		out.Lost[i].Instance = cloneInst(in.Lost[i].Instance)
	}
	out.Added = cloneInstances(in.Added)
	return out
}

func cloneTopUpPtr(in *TopUp) *TopUp {
	if in == nil {
		return nil
	}
	out := cloneTopUp(*in)
	return &out
}

// splitPromisedLocked separates the promised set into instances still
// redeemable now (live, usable and backed by the session's confirmed hold)
// and lost ones with the reason. Expired or departed capacity always lands
// in Lost, never in the redeemable guarantee.
func (s *Session) splitPromisedLocked(promised []Instance, c Criteria, cand Selection) (redeemable []Instance, lost []StaleEntry) {
	usable := map[string]bool{}
	for _, inst := range cand.Picked {
		usable[inst.ID] = true
	}
	verdictByID := map[string]Verdict{}
	for _, v := range cand.Verdicts {
		verdictByID[v.Instance.ID] = v
	}
	for _, inst := range promised {
		if usable[inst.ID] && s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
			redeemable = append(redeemable, inst)
			continue
		}
		reason := ""
		switch {
		case !usable[inst.ID]:
			reason = s.reg.staleReasonLocked(inst, c, verdictByID[inst.ID])
		default:
			reason = "no capacity hold (reservation expired, cancelled or released)"
		}
		lost = append(lost, StaleEntry{Instance: inst, Reason: reason})
	}
	return redeemable, lost
}

func (t *TopUp) settle(c Criteria) {
	t.Missing = c.MinAvailable - len(t.Redeemable) - len(t.Added)
	if t.Missing < 0 {
		t.Missing = 0
	}
	t.Satisfied = t.Missing == 0
	if !t.Satisfied {
		t.Shortfall = fmt.Sprintf(
			"still missing %d of %d required instance(s): %d redeemable, %d newly reserved",
			t.Missing, c.MinAvailable, len(t.Redeemable), len(t.Added))
	} else {
		t.Shortfall = ""
	}
}

// TopUp reserves additional capacity to close the gap between the session's
// confirmed commitment and what is still redeemable, without replacing any
// already-confirmed valid occupancy. Only the missing units are reserved,
// chosen from currently free capacity, so other sessions keep contending
// fairly and the candidate list is never pre-filled. Re-topping-up replaces
// the session's previous (unconfirmed) top-up group; confirmed holds are
// never touched. The result separates historically promised, currently
// redeemable and still missing capacity.
func (s *Session) TopUp(ttl time.Duration) (TopUp, error) {
	if ttl <= 0 {
		return TopUp{}, fmt.Errorf("%w: top-up ttl must be positive", ErrBadArgument)
	}
	s.mu.Lock()
	if !s.capacityAware {
		s.mu.Unlock()
		return TopUp{}, fmt.Errorf("%w: session holds no capacity decision to top up", ErrBadArgument)
	}
	cur := cloneSelection(s.current)
	s.mu.Unlock()

	now := s.reg.clock.Now()
	c := cur.Criteria
	promised := cur.CandidatePicked
	if len(promised) == 0 {
		promised = cur.Picked
	}

	s.reg.mu.Lock()
	s.reg.expireStaleLocked(now)
	cand := s.reg.evaluateLocked(c, 0)
	top := TopUp{
		ID:           newToken(),
		SessionID:    s.id,
		Criteria:     c,
		CandidateSeq: cur.Seq,
		Promised:     cloneInstances(promised),
		ExpiresAt:    now.Add(ttl),
	}
	top.Redeemable, top.Lost = s.splitPromisedLocked(promised, c, cand)
	redeemable := map[string]bool{}
	for _, inst := range top.Redeemable {
		redeemable[inst.ID] = true
	}
	// Replace any previous unconfirmed top-up group; confirmed holds and
	// the main reservation group are independent and stay untouched.
	s.reg.dropHoldGroupLocked(s.id, s.pendingTopUpIDLocked())
	missing := c.MinAvailable - len(top.Redeemable)
	for _, inst := range cand.Picked {
		if missing <= 0 || len(top.Added) >= missing {
			break
		}
		if redeemable[inst.ID] {
			continue
		}
		if _, live := s.reg.instances[key(inst.Service, inst.ID)]; !live {
			continue
		}
		if s.reg.freeCapacityLocked(inst, s.id) <= 0 {
			continue
		}
		s.reg.holds[top.ID+":"+inst.ID] = Hold{
			ID:         top.ID + ":" + inst.ID,
			SessionID:  s.id,
			Service:    inst.Service,
			InstanceID: inst.ID,
			Kind:       HoldReserved,
			CreatedAt:  now,
			ExpiresAt:  top.ExpiresAt,
		}
		top.Added = append(top.Added, inst)
	}
	top.settle(c)
	s.reg.mu.Unlock()

	s.mu.Lock()
	s.pendingTopUp = cloneTopUpPtr(&top)
	s.recordDecisionLocked(DecisionTopUpReserved, cur.Seq,
		len(promised), len(top.Redeemable)+len(top.Added), top.Missing, "")
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return top, nil
}

// ConfirmTopUp settles the pending top-up: live top-up reservations become
// confirmed usage, lapsed ones are re-acquired only if the capacity is
// still free. Already-confirmed occupancy is never replaced or moved. The
// confirmed result is remembered, so a duplicate ConfirmTopUp (e.g. a
// retried notification after a directory restart) returns the same
// decision without allocating anything again.
func (s *Session) ConfirmTopUp() (TopUp, error) {
	s.mu.Lock()
	pend := cloneTopUpPtr(s.pendingTopUp)
	if pend == nil {
		if s.hasLastTopUp {
			t := cloneTopUp(s.lastTopUp)
			s.mu.Unlock()
			return t, nil
		}
		s.mu.Unlock()
		return TopUp{}, ErrNoPendingTopUp
	}
	s.mu.Unlock()

	now := s.reg.clock.Now()
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(now)
	var added []Instance
	for _, inst := range pend.Added {
		if _, live := s.reg.instances[key(inst.Service, inst.ID)]; !live {
			continue
		}
		hid := pend.ID + ":" + inst.ID
		if h, ok := s.reg.holds[hid]; ok && h.SessionID == s.id && h.Kind == HoldReserved {
			h.Kind = HoldConfirmed
			h.ExpiresAt = time.Time{}
			s.reg.holds[hid] = h
			added = append(added, inst)
			continue
		}
		// The reservation lapsed: re-acquire only if still free.
		if s.reg.freeCapacityLocked(inst, s.id) > 0 {
			nid := newToken()
			s.reg.holds[nid] = Hold{
				ID:         nid,
				SessionID:  s.id,
				Service:    inst.Service,
				InstanceID: inst.ID,
				Kind:       HoldConfirmed,
				CreatedAt:  now,
			}
			added = append(added, inst)
		}
	}
	// Drop leftover reserved holds of this group (units not confirmable).
	s.reg.dropHoldGroupLocked(s.id, pend.ID)
	cand := s.reg.evaluateLocked(pend.Criteria, 0)
	result := cloneTopUp(*pend)
	result.Added = added
	result.Redeemable, result.Lost = s.splitPromisedLocked(pend.Promised, pend.Criteria, cand)
	result.settle(pend.Criteria)
	s.reg.mu.Unlock()

	s.mu.Lock()
	s.pendingTopUp = nil
	s.lastTopUp = cloneTopUp(result)
	s.hasLastTopUp = true
	s.recordDecisionLocked(DecisionTopUpConfirmed, result.CandidateSeq,
		len(result.Promised), len(result.Redeemable)+len(result.Added), result.Missing, "")
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return result, nil
}

// CancelTopUp revokes only the pending top-up group. Confirmed usage and
// the main reservation keep their own lifecycle and are never affected.
// Idempotent: repeating it is a no-op.
func (s *Session) CancelTopUp() {
	s.mu.Lock()
	pend := s.pendingTopUp
	s.pendingTopUp = nil
	if pend != nil {
		s.reg.mu.Lock()
		s.reg.dropHoldGroupLocked(s.id, pend.ID)
		s.recordDecisionLocked(DecisionTopUpCancelled, pend.CandidateSeq,
			len(pend.Promised), 0, 0, "top-up reservation cancelled")
		s.reg.sessions[s.id] = s.snapshotStateLocked()
		s.reg.mu.Unlock()
	}
	s.mu.Unlock()
}

// pendingTopUpIDLocked returns the live top-up group ID, or "".
func (s *Session) pendingTopUpIDLocked() string {
	if s.pendingTopUp == nil {
		return ""
	}
	return s.pendingTopUp.ID
}

// dropHoldGroupLocked deletes the session's reserved holds of one group.
// An empty group ID matches nothing, so confirmed usage and other groups
// are never touched.
func (r *Registry) dropHoldGroupLocked(sessionID, groupID string) {
	if groupID == "" {
		return
	}
	for id, h := range r.holds {
		if h.SessionID == sessionID && h.Kind == HoldReserved &&
			strings.HasPrefix(id, groupID+":") {
			delete(r.holds, id)
		}
	}
}
