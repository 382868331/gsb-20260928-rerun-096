package discovery

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SelectionQuery describes what a client needs from one selection round.
// It is fixed when a session begins; changing conditions means evaluating a
// new selection, never editing an existing record.
type SelectionQuery struct {
	// Service selects the virtual service whose instances are candidates.
	Service string
	// Capability is the required value of the instance MetaCapability
	// metadata. Empty means no capability constraint.
	Capability string
	// PreferredRegion is a soft preference on the instance MetaRegion
	// metadata. Instances outside the region are still adopted but the
	// mismatch is recorded in their explanation.
	PreferredRegion string
	// MinAvailable is the minimum number of adopted instances for the
	// selection to be feasible.
	MinAvailable int
}

// Candidate is one evaluated instance with the reasons it was adopted or
// excluded. Every live instance of the queried service appears exactly once.
type Candidate struct {
	Instance Instance
	Adopted  bool
	// Reasons explain the outcome, e.g. adopted with preferred region, or
	// excluded because the capability does not match.
	Reasons []string
}

// Selection is one immutable, explainable selection record. Version pins the
// directory state the evaluation was based on, so later changes never alter
// what this record meant when it was committed.
type Selection struct {
	ID        int64
	SessionID int64
	Query     SelectionQuery
	// Version is the directory version at evaluation time.
	Version int64
	At      time.Time
	// Candidates covers every live instance of the service, adopted or not.
	Candidates []Candidate
	// Adopted is the chosen subset, sorted by instance ID.
	Adopted []Instance
	// Feasible reports whether len(Adopted) >= Query.MinAvailable.
	Feasible bool
	// UnmetReason explains why the conditions cannot currently be met; it
	// is empty when Feasible is true.
	UnmetReason string
}

// InvalidatedInstance is an instance from the active selection that is no
// longer usable, with the reason it dropped out.
type InvalidatedInstance struct {
	Instance Instance
	Reason   string
}

// SessionView separates the recorded selection from what is usable right
// now: Active and History are immutable records, while Usable and
// Invalidated reflect the current directory.
type SessionView struct {
	ID      int64
	Query   SelectionQuery
	Active  Selection
	Pending *Selection
	// History lists every committed selection of this session, oldest
	// first. Records are never mutated after the fact.
	History []Selection
	// Usable is the subset of Active.Adopted still usable at view time.
	Usable []Instance
	// Invalidated is the rest of Active.Adopted, with per-instance reasons.
	Invalidated []InvalidatedInstance
}

// sessionState is the registry-owned state behind a SelectionSession handle.
type sessionState struct {
	ID        int64          `json:"id"`
	Query     SelectionQuery `json:"query"`
	ActiveID  int64          `json:"active_selection_id"`
	PendingID int64          `json:"pending_selection_id,omitempty"`
	History   []int64        `json:"history"`
}

// ErrNoPendingSelection is returned by Confirm when there is nothing to
// switch to.
var ErrNoPendingSelection = errors.New("discovery: no pending selection to confirm")

// ErrSessionNotFound is returned when a session ID is unknown.
var ErrSessionNotFound = errors.New("discovery: selection session not found")

// SelectionSession is a handle to one ongoing explainable selection. It is
// safe for concurrent use; all methods serialize on the registry lock.
type SelectionSession struct {
	reg *Registry
	id  int64
}

// BeginSelection starts an explainable selection session for q and commits
// the first selection record against the current directory. Only locally
// registered instances are considered; no requests are sent anywhere.
func (r *Registry) BeginSelection(q SelectionQuery) (*SelectionSession, Selection, error) {
	if q.Service == "" || q.MinAvailable < 0 {
		return nil, Selection{}, fmt.Errorf("%w: service required and MinAvailable must be >= 0", ErrBadArgument)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireStaleLocked(r.clock.Now())
	r.nextSessionID++
	sess := &sessionState{ID: r.nextSessionID, Query: q}
	sel := r.evaluateLocked(sess.ID, q)
	sess.ActiveID = sel.ID
	sess.History = append(sess.History, sel.ID)
	r.sessions[sess.ID] = sess
	return &SelectionSession{reg: r, id: sess.ID}, sel, nil
}

// Session returns the handle for an existing session, e.g. after reopening
// a persisted directory.
func (r *Registry) Session(id int64) (*SelectionSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[id]; !ok {
		return nil, false
	}
	return &SelectionSession{reg: r, id: id}, true
}

// ID returns the session identifier, stable across Save/OpenRegistry.
func (s *SelectionSession) ID() int64 { return s.id }

// Evaluate re-runs the session query against the current directory and
// records the result as the pending selection. The active selection is left
// untouched until Confirm; the pending record is immutable like any other.
func (s *SelectionSession) Evaluate() (Selection, error) {
	r := s.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	sess, ok := r.sessions[s.id]
	if !ok {
		return Selection{}, ErrSessionNotFound
	}
	r.expireStaleLocked(r.clock.Now())
	sel := r.evaluateLocked(sess.ID, sess.Query)
	sess.PendingID = sel.ID
	return sel, nil
}

// Confirm switches the session to the pending selection. The previous
// active record stays in History unchanged.
func (s *SelectionSession) Confirm() (Selection, error) {
	r := s.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	sess, ok := r.sessions[s.id]
	if !ok {
		return Selection{}, ErrSessionNotFound
	}
	if sess.PendingID == 0 {
		return Selection{}, ErrNoPendingSelection
	}
	sess.ActiveID = sess.PendingID
	sess.PendingID = 0
	sess.History = append(sess.History, sess.ActiveID)
	return *r.selections[sess.ActiveID], nil
}

// Defer keeps the active selection and drops the pending one, returning the
// discarded evaluation so the caller can show why conditions are currently
// unmet. The second result is false when nothing was pending.
func (s *SelectionSession) Defer() (Selection, bool) {
	r := s.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	sess, ok := r.sessions[s.id]
	if !ok || sess.PendingID == 0 {
		return Selection{}, false
	}
	sel := *r.selections[sess.PendingID]
	sess.PendingID = 0
	return sel, true
}

// View reports the session state, distinguishing the recorded selection from
// what is usable right now. Expired or departed instances are never
// reported as usable; they appear in Invalidated with their reason.
func (s *SelectionSession) View() (SessionView, error) {
	r := s.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	sess, ok := r.sessions[s.id]
	if !ok {
		return SessionView{}, ErrSessionNotFound
	}
	r.expireStaleLocked(r.clock.Now())
	view := SessionView{ID: sess.ID, Query: sess.Query}
	active := r.selections[sess.ActiveID]
	view.Active = *active
	if sess.PendingID != 0 {
		p := *r.selections[sess.PendingID]
		view.Pending = &p
	}
	for _, id := range sess.History {
		view.History = append(view.History, *r.selections[id])
	}
	for _, inst := range active.Adopted {
		if reason, bad := r.invalidationLocked(sess.Query, inst, active.Version); bad {
			view.Invalidated = append(view.Invalidated, InvalidatedInstance{Instance: inst, Reason: reason})
		} else {
			view.Usable = append(view.Usable, cloneInst(inst))
		}
	}
	return view, nil
}

// evaluateLocked commits one immutable selection record against the current
// (already expiry-checked) directory.
func (r *Registry) evaluateLocked(sessionID int64, q SelectionQuery) Selection {
	r.nextSelectID++
	sel := Selection{
		ID:        r.nextSelectID,
		SessionID: sessionID,
		Query:     q,
		Version:   r.version,
		At:        r.clock.Now(),
	}
	var excluded []string
	for _, e := range r.sortedEntriesLocked(q.Service) {
		cand := Candidate{Instance: cloneInst(e.inst)}
		if reason, bad := r.candidateExcludedLocked(q, e); bad {
			cand.Reasons = []string{"excluded: " + reason}
			excluded = append(excluded, fmt.Sprintf("%s (%s)", e.inst.ID, reason))
		} else {
			cand.Adopted = true
			cand.Reasons = []string{"adopted: ready, capability match"}
			if q.PreferredRegion != "" {
				if e.inst.Metadata[MetaRegion] == q.PreferredRegion {
					cand.Reasons = append(cand.Reasons, "preferred region")
				} else {
					cand.Reasons = append(cand.Reasons,
						fmt.Sprintf("region %q not preferred %q", e.inst.Metadata[MetaRegion], q.PreferredRegion))
				}
			}
			sel.Adopted = append(sel.Adopted, cand.Instance)
		}
		sel.Candidates = append(sel.Candidates, cand)
	}
	sel.Feasible = len(sel.Adopted) >= q.MinAvailable
	if !sel.Feasible {
		reason := fmt.Sprintf("need at least %d available instance(s), only %d adopted",
			q.MinAvailable, len(sel.Adopted))
		if len(excluded) > 0 {
			reason += "; excluded: " + strings.Join(excluded, ", ")
		}
		sel.UnmetReason = reason
	}
	stored := sel
	r.selections[sel.ID] = &stored
	return sel
}

// sortedEntriesLocked returns live entries of one service ordered by ID so
// evaluations are deterministic.
func (r *Registry) sortedEntriesLocked(service string) []*entry {
	var out []*entry
	for _, e := range r.instances {
		if e.inst.Service == service {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].inst.ID < out[j].inst.ID })
	return out
}

// candidateExcludedLocked explains why a live entry cannot be adopted under
// q. It only covers present-state causes; departures are answered from the
// removal audit by invalidationLocked.
func (r *Registry) candidateExcludedLocked(q SelectionQuery, e *entry) (string, bool) {
	if st := e.stateLocked(); st != StateReady {
		return fmt.Sprintf("state is %s, not ready", st), true
	}
	if q.Capability != "" && e.inst.Metadata[MetaCapability] != q.Capability {
		got := e.inst.Metadata[MetaCapability]
		if got == "" {
			return fmt.Sprintf("capability %q required, instance has none", q.Capability), true
		}
		return fmt.Sprintf("capability %q required, instance has %q", q.Capability, got), true
	}
	return "", false
}

// invalidationLocked reports why an instance adopted at sinceVersion is no
// longer usable under q. Departed incarnations are explained from the
// removal audit (expired vs deregistered); live ones from current state.
func (r *Registry) invalidationLocked(q SelectionQuery, inst Instance, sinceVersion int64) (string, bool) {
	k := key(inst.Service, inst.ID)
	if rec, ok := r.removals[k]; ok && rec.Version > sinceVersion {
		return fmt.Sprintf("instance %s at version %d", rec.Reason, rec.Version), true
	}
	e, ok := r.instances[k]
	if !ok {
		return "instance no longer in directory", true
	}
	return r.candidateExcludedLocked(q, e)
}

// SelectionComparison explains how selection b differs from selection a,
// separating changes in the user conditions from changes in instance state.
// Both records are left untouched.
type SelectionComparison struct {
	// QueryChanged is true when the two selections ran under different
	// conditions.
	QueryChanged bool
	// QueryChanges lists each differing condition, e.g. capability a to b.
	QueryChanges []string
	// Added are instances adopted in b but not in a.
	Added []Instance
	// Removed are instances adopted in a but not in b, each with the reason
	// recorded in b evaluation (or its absence from the directory).
	Removed []InvalidatedInstance
}

// CompareSelections explains the difference between two selection records.
// Instance-level causes are read from b candidate explanations, so the
// comparison reflects the directory state b was evaluated against.
func CompareSelections(a, b Selection) SelectionComparison {
	cmp := SelectionComparison{}
	if a.Query != b.Query {
		cmp.QueryChanged = true
		cmp.QueryChanges = diffQuery(a.Query, b.Query)
	}
	inA := make(map[string]Instance)
	for _, inst := range a.Adopted {
		inA[key(inst.Service, inst.ID)] = inst
	}
	inB := make(map[string]bool)
	for _, inst := range b.Adopted {
		inB[key(inst.Service, inst.ID)] = true
		if _, ok := inA[key(inst.Service, inst.ID)]; !ok {
			cmp.Added = append(cmp.Added, inst)
		}
	}
	reasons := make(map[string]string)
	for _, cand := range b.Candidates {
		if !cand.Adopted && len(cand.Reasons) > 0 {
			reasons[key(cand.Instance.Service, cand.Instance.ID)] = strings.Join(cand.Reasons, "; ")
		}
	}
	for k, inst := range inA {
		if inB[k] {
			continue
		}
		reason, ok := reasons[k]
		if !ok {
			reason = "instance no longer in directory"
		}
		cmp.Removed = append(cmp.Removed, InvalidatedInstance{Instance: inst, Reason: reason})
	}
	sort.Slice(cmp.Removed, func(i, j int) bool { return cmp.Removed[i].Instance.ID < cmp.Removed[j].Instance.ID })
	return cmp
}

func diffQuery(a, b SelectionQuery) []string {
	var out []string
	if a.Service != b.Service {
		out = append(out, fmt.Sprintf("service: %q -> %q", a.Service, b.Service))
	}
	if a.Capability != b.Capability {
		out = append(out, fmt.Sprintf("capability: %q -> %q", a.Capability, b.Capability))
	}
	if a.PreferredRegion != b.PreferredRegion {
		out = append(out, fmt.Sprintf("preferred region: %q -> %q", a.PreferredRegion, b.PreferredRegion))
	}
	if a.MinAvailable != b.MinAvailable {
		out = append(out, fmt.Sprintf("min available: %d -> %d", a.MinAvailable, b.MinAvailable))
	}
	return out
}
