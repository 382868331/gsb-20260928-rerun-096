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
	// ErrNoPendingTopUp is returned by ConfirmTopUp when no top-up
	// reservation is pending and no top-up decision exists to repeat.
	ErrNoPendingTopUp = errors.New("discovery: no pending top-up to confirm")
)

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
	prevResID := s.reservationID
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
	// Re-reserving replaces the session's previous switch reservation
	// group; an independent top-up group is left untouched.
	if prevResID != "" {
		for id, h := range s.reg.holds {
			if h.SessionID == s.id && h.Kind == HoldReserved &&
				strings.HasPrefix(id, prevResID+":") {
				delete(s.reg.holds, id)
			}
		}
	}
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
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return res, nil
}

// CancelReservation releases the session's reserved (not yet confirmed)
// switch holds. It is idempotent: repeating it is a no-op and never
// releases capacity twice or touches confirmed usage. An independent
// top-up reservation group is not affected; use CancelTopUp for it.
func (s *Session) CancelReservation() {
	s.mu.Lock()
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(s.reg.clock.Now())
	if s.reservationID != "" {
		for id, h := range s.reg.holds {
			if h.SessionID == s.id && h.Kind == HoldReserved &&
				strings.HasPrefix(id, s.reservationID+":") {
				delete(s.reg.holds, id)
			}
		}
	}
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

// TopUpReserve reserves additional capacity to close the gap left by a
// partially honored (explicitly degraded) decision, without touching the
// session's confirmed holds. Only currently free units are held, and never
// more than the missing count, so the candidate list is not pre-filled and
// other sessions keep contending fairly for the remaining free capacity.
// The new holds form their own reservation group with their own expiry,
// independent of any switch reservation.
func (s *Session) TopUpReserve(ttl time.Duration) (Reservation, error) {
	if ttl <= 0 {
		return Reservation{}, fmt.Errorf("%w: reservation ttl must be positive", ErrBadArgument)
	}
	s.mu.Lock()
	if !s.capacityAware {
		s.mu.Unlock()
		return Reservation{}, fmt.Errorf("%w: session holds no capacity decision to top up", ErrBadArgument)
	}
	cur := s.current
	prevTopUpID := s.topUpReservationID
	s.mu.Unlock()

	promised := cur.CandidatePicked
	if len(promised) == 0 {
		promised = cur.Picked
	}
	now := s.reg.clock.Now()
	res := Reservation{
		ID:        newToken(),
		SessionID: s.id,
		Criteria:  cur.Criteria,
		ExpiresAt: now.Add(ttl),
	}
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(now)
	cand := s.reg.evaluateLocked(cur.Criteria, 0)
	usable := map[string]Instance{}
	for _, inst := range cand.Picked {
		usable[inst.ID] = inst
	}
	guaranteed := 0
	for _, inst := range promised {
		if _, ok := usable[inst.ID]; ok &&
			s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
			guaranteed++
		}
	}
	missing := cur.Criteria.MinAvailable - guaranteed
	if missing <= 0 {
		s.reg.mu.Unlock()
		res.Satisfied = true
		return res, nil
	}
	// Replace any previous top-up group; only the top-up group is touched.
	if prevTopUpID != "" {
		for id, h := range s.reg.holds {
			if h.SessionID == s.id && h.Kind == HoldReserved &&
				strings.HasPrefix(id, prevTopUpID+":") {
				delete(s.reg.holds, id)
			}
		}
	}
	for _, inst := range cand.Picked {
		if len(res.Picks) >= missing {
			break
		}
		if s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
			continue
		}
		if s.reg.freeCapacityLocked(inst, s.id) <= 0 {
			continue
		}
		s.reg.holds[res.ID+":"+inst.ID] = Hold{
			ID:         res.ID + ":" + inst.ID,
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

	res.Satisfied = len(res.Picks) >= missing
	if !res.Satisfied {
		res.Shortfall = fmt.Sprintf(
			"need %d more usable instance(s) for %q but only %d had free capacity",
			missing, cur.Criteria.Service, len(res.Picks))
	}

	s.mu.Lock()
	s.topUpReservationID = res.ID
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return res, nil
}

// CancelTopUp releases the session's top-up reservation group only. It is
// idempotent and never touches confirmed usage or a switch reservation.
func (s *Session) CancelTopUp() {
	s.mu.Lock()
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(s.reg.clock.Now())
	if s.topUpReservationID != "" {
		for id, h := range s.reg.holds {
			if h.SessionID == s.id && h.Kind == HoldReserved &&
				strings.HasPrefix(id, s.topUpReservationID+":") {
				delete(s.reg.holds, id)
			}
		}
	}
	s.topUpReservationID = ""
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
}

// ConfirmTopUp turns the pending top-up reservation into confirmed usage,
// keeping every previously confirmed hold. If the top-up reservation
// lapsed, only still-free units are re-acquired, capped at the remaining
// deficit; capacity owned by other sessions is never taken. The committed
// record keeps the historically promised set (CandidatePicked), reports
// what is honored now (Picked/Honored) and what is still missing, and
// lists promised capacity that expired in the meantime separately instead
// of merging it into the current guarantee. Repeating ConfirmTopUp after a
// successful one (e.g. a duplicate notification after a directory restart)
// returns the same decision without allocating anything again.
func (s *Session) ConfirmTopUp() (Selection, error) {
	s.mu.Lock()
	resID := s.topUpReservationID
	if resID == "" {
		if s.hasLastTopUp {
			sel := s.lastTopUp
			s.mu.Unlock()
			return sel, nil
		}
		s.mu.Unlock()
		return Selection{}, ErrNoPendingTopUp
	}
	cur := s.current
	s.mu.Unlock()

	promised := cur.CandidatePicked
	if len(promised) == 0 {
		promised = cur.Picked
	}
	now := s.reg.clock.Now()
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(now)
	cand := s.reg.evaluateLocked(cur.Criteria, 0)
	usable := map[string]Instance{}
	for _, inst := range cand.Picked {
		usable[inst.ID] = inst
	}

	// Convert live top-up reserved holds to confirmed usage. Holds whose
	// instance left or became unusable are dropped, never confirmed.
	added := map[string]bool{}
	prefix := resID + ":"
	for id, h := range s.reg.holds {
		if h.SessionID != s.id || h.Kind != HoldReserved ||
			!strings.HasPrefix(id, prefix) {
			continue
		}
		if _, ok := usable[h.InstanceID]; ok {
			h.Kind = HoldConfirmed
			h.ExpiresAt = time.Time{}
			s.reg.holds[id] = h
			added[h.InstanceID] = true
		} else {
			delete(s.reg.holds, id)
		}
	}
	// A lapsed top-up reservation is re-acquired only from still-free
	// capacity, capped at the remaining deficit.
	if len(added) == 0 {
		guaranteed := 0
		for _, inst := range promised {
			if _, ok := usable[inst.ID]; ok &&
				s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
				guaranteed++
			}
		}
		missing := cur.Criteria.MinAvailable - guaranteed
		for _, inst := range cand.Picked {
			if missing <= 0 {
				break
			}
			if s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) ||
				s.reg.freeCapacityLocked(inst, s.id) <= 0 {
				continue
			}
			hid := newToken()
			s.reg.holds[hid] = Hold{
				ID:         hid,
				SessionID:  s.id,
				Service:    inst.Service,
				InstanceID: inst.ID,
				Kind:       HoldConfirmed,
				CreatedAt:  now,
			}
			added[inst.ID] = true
			missing--
		}
	}

	// The deliverable set is every usable instance the session now holds
	// confirmed, in candidate ranking order. Expired promised capacity is
	// excluded here and reported separately on the top-up record.
	var picked []Instance
	for _, inst := range cand.Picked {
		if s.reg.hasConfirmedHoldLocked(s.id, inst.Service, inst.ID) {
			picked = append(picked, inst)
		}
	}
	pickedSet := map[string]bool{}
	for _, inst := range picked {
		pickedSet[inst.ID] = true
	}
	topUp := &TopUpRecord{Amends: cur.Seq}
	for id := range added {
		if inst, ok := usable[id]; ok {
			topUp.Added = append(topUp.Added, inst)
		}
	}
	sort.Slice(topUp.Added, func(i, j int) bool { return topUp.Added[i].ID < topUp.Added[j].ID })
	for _, inst := range promised {
		if pickedSet[inst.ID] {
			continue
		}
		topUp.StillMissing = append(topUp.StillMissing, inst)
		if rec, gone := s.reg.removals[key(inst.Service, inst.ID)]; gone {
			topUp.ExpiredSincePromise = append(topUp.ExpiredSincePromise,
				StaleEntry{Instance: inst, Reason: departureReason(rec)})
		}
	}
	s.reg.mu.Unlock()

	committed := cand
	committed.Criteria = Criteria{
		Service:         cur.Criteria.Service,
		Capabilities:    append([]string(nil), cur.Criteria.Capabilities...),
		PreferredRegion: cur.Criteria.PreferredRegion,
		MinAvailable:    cur.Criteria.MinAvailable,
	}
	committed.CandidatePicked = cloneInstances(promised)
	committed.Picked = picked
	committed.Honored = len(picked)
	committed.Satisfied = committed.Honored >= cur.Criteria.MinAvailable
	if !committed.Satisfied {
		committed.Shortfall = fmt.Sprintf(
			"only %d of %d required instance(s) are confirmed and usable",
			committed.Honored, cur.Criteria.MinAvailable)
	}
	if cur.Degradation != nil {
		deg := *cur.Degradation
		deg.Missing = append([]string(nil), cur.Degradation.Missing...)
		committed.Degradation = &deg
	}
	committed.TopUp = topUp

	s.mu.Lock()
	s.seq++
	committed.Seq = s.seq
	s.current = committed
	s.history = append(s.history, committed)
	s.topUpReservationID = ""
	s.lastTopUp = committed
	s.hasLastTopUp = true
	s.reg.mu.Lock()
	s.reg.sessions[s.id] = s.snapshotStateLocked()
	s.reg.mu.Unlock()
	s.mu.Unlock()
	return committed, nil
}
