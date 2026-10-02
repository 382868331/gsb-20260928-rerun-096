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
	// Satisfied reports whether len(Picked) >= Criteria.MinAvailable.
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
	seq        int64
	current    Selection
	history    []Selection
	pending    *Selection
	hasPending bool
}

// NewSelectionSession starts a session and commits the first selection
// computed from the current directory state.
func (r *Registry) NewSelectionSession(c Criteria) (*Session, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	s := &Session{reg: r}
	first := r.evaluate(c, 1)
	s.seq = 1
	s.current = first
	s.history = append(s.history, first)
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
	cand := s.reg.evaluate(c, s.seq)
	s.pending = &cand
	s.hasPending = true
	s.mu.Unlock()

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
			Reason:   s.reg.staleReason(inst, c, verdictByID[inst.ID]),
		})
	}
	return rc
}

// Confirm switches the session to the pending candidate from the last
// Recheck/RecheckWith and appends it to the immutable history. The previous
// record is kept unchanged.
func (s *Session) Confirm() (Selection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasPending {
		return Selection{}, ErrNoPendingSelection
	}
	s.current = *s.pending
	s.history = append(s.history, s.current)
	s.pending = nil
	s.hasPending = false
	return s.current, nil
}

// evaluate computes a selection from the live directory under the registry
// lock, so expiries are applied inline and the basis snapshot, version and
// verdicts are mutually consistent even while notifications and queries
// interleave.
func (r *Registry) evaluate(c Criteria, seq int64) Selection {
	r.mu.Lock()
	defer r.mu.Unlock()
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
	rec, gone := r.removals[key(inst.Service, inst.ID)]
	r.mu.Unlock()
	if gone {
		switch rec.Reason {
		case ReasonExpired:
			return "lease expired"
		case ReasonDeregistered:
			return "deregistered"
		}
		return "left the directory"
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
