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
	// Re-reserving replaces the session's previous reserved holds.
	for id, h := range s.reg.holds {
		if h.SessionID == s.id && h.Kind == HoldReserved {
			delete(s.reg.holds, id)
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
// holds. It is idempotent: repeating it is a no-op and never releases
// capacity twice or touches confirmed usage.
func (s *Session) CancelReservation() {
	s.mu.Lock()
	s.reg.mu.Lock()
	s.reg.expireStaleLocked(s.reg.clock.Now())
	for id, h := range s.reg.holds {
		if h.SessionID == s.id && h.Kind == HoldReserved {
			delete(s.reg.holds, id)
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
