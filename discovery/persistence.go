package discovery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// duration is time.Duration with JSON (un)marshalling in nanoseconds.
type duration time.Duration

func (d duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(int64(d))
}

func (d *duration) UnmarshalJSON(data []byte) error {
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*d = duration(n)
	return nil
}

// onDiskEntry is one persisted lease.
type onDiskEntry struct {
	Instance Instance  `json:"instance"`
	Token    string    `json:"token"`
	Deadline time.Time `json:"deadline"`
	Lease    duration  `json:"lease"`
}

// stateFile is the persisted directory image. JSON keeps it inspectable.
type stateFile struct {
	Version         int64           `json:"version"`
	CompactedBefore int64           `json:"compacted_before"`
	Entries         []onDiskEntry   `json:"entries"`
	Log             []Change        `json:"log"`
	Removals        []RemovalRecord `json:"removals"`
	// Holds persists live capacity holds so a reopened directory cannot
	// hand already-owned capacity to another session.
	Holds    []Hold          `json:"holds,omitempty"`
	Sessions []onDiskSession `json:"sessions,omitempty"`
}

// onDiskSession is the durable decision state for one selection session.
// Holds alone identify ownership but cannot reconstruct the immutable
// decision/history needed to make post-restart retries idempotent.
type onDiskSession struct {
	ID            string      `json:"id"`
	Seq           int64       `json:"seq"`
	Current       Selection   `json:"current"`
	History       []Selection `json:"history"`
	Pending       *Selection  `json:"pending,omitempty"`
	HasPending    bool        `json:"has_pending"`
	CapacityAware bool        `json:"capacity_aware"`
	ReservationID string      `json:"reservation_id,omitempty"`
	LastConfirm   Selection   `json:"last_confirm"`
	HasLast       bool        `json:"has_last_confirm"`
}

// Save atomically persists the live directory, including readiness state,
// the retained change log and departure audit records. Expired instances are
// already gone from the directory, so a saved image can never bring a lapsed
// instance back on reopen.
func (r *Registry) Save(dir string) error {
	var img stateFile
	for {
		r.mu.Lock()
		sessionRefs := make([]*Session, 0, len(r.sessions))
		for _, sess := range r.sessions {
			sessionRefs = append(sessionRefs, sess)
		}
		r.mu.Unlock()
		sort.Slice(sessionRefs, func(i, j int) bool { return sessionRefs[i].id < sessionRefs[j].id })
		for _, sess := range sessionRefs {
			sess.mu.Lock()
		}

		r.mu.Lock()
		stable := len(r.sessions) == len(sessionRefs)
		if stable {
			for _, sess := range sessionRefs {
				if r.sessions[sess.id] != sess {
					stable = false
					break
				}
			}
		}
		if !stable {
			r.mu.Unlock()
			for i := len(sessionRefs) - 1; i >= 0; i-- {
				sessionRefs[i].mu.Unlock()
			}
			continue
		}

		// Persist only leases that are still effective at save time; lapsed
		// leases are expired inline (with events and audit) instead of being
		// written as live entries.
		r.expireStaleLocked(r.clock.Now())
		img = stateFile{
			Version:         r.version,
			CompactedBefore: r.compactedBefore,
			Log:             append([]Change(nil), r.log...),
			Removals:        make([]RemovalRecord, 0, len(r.removals)),
		}
		for _, e := range r.instances {
			img.Entries = append(img.Entries, onDiskEntry{
				Instance: cloneInst(e.inst),
				Token:    e.token,
				Deadline: e.deadline,
				Lease:    duration(e.lease),
			})
		}
		for _, rec := range r.removals {
			img.Removals = append(img.Removals, rec)
		}
		for _, h := range r.holds {
			img.Holds = append(img.Holds, h)
		}
		for _, sess := range sessionRefs {
			onDisk := onDiskSession{
				ID:            sess.id,
				Seq:           sess.seq,
				Current:       cloneSelection(sess.current),
				CapacityAware: sess.capacityAware,
				ReservationID: sess.reservationID,
				LastConfirm:   cloneSelection(sess.lastConfirm),
				HasLast:       sess.hasLastConfirm,
				HasPending:    sess.hasPending,
			}
			for _, rec := range sess.history {
				onDisk.History = append(onDisk.History, cloneSelection(rec))
			}
			if sess.pending != nil {
				pending := cloneSelection(*sess.pending)
				onDisk.Pending = &pending
			}
			img.Sessions = append(img.Sessions, onDisk)
		}
		r.mu.Unlock()
		for i := len(sessionRefs) - 1; i >= 0; i-- {
			sessionRefs[i].mu.Unlock()
		}
		break
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("discovery: create state dir: %w", err)
	}
	path := filepath.Join(dir, "registry.json")
	tmp, err := os.CreateTemp(dir, "registry-*.tmp")
	if err != nil {
		return fmt.Errorf("discovery: create state file: %w", err)
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&img); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("discovery: encode state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("discovery: close state file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("discovery: commit state file: %w", err)
	}
	return nil
}

// OpenRegistry loads a directory previously written by Save. Leases whose
// deadline has already passed under the configured clock are not revived:
// they are appended to the departure audit as expiries and receive fresh
// expired change events. The monotonic version continues past the saved one.
func OpenRegistry(dir string, opts ...Option) (*Registry, error) {
	r := NewRegistry(opts...)
	path := filepath.Join(dir, "registry.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return r, nil
		}
		return nil, fmt.Errorf("discovery: read state file: %w", err)
	}
	var img stateFile
	if err := json.Unmarshal(data, &img); err != nil {
		return nil, fmt.Errorf("discovery: decode state file: %w", err)
	}

	now := r.clock.Now()
	r.version = img.Version
	r.compactedBefore = img.CompactedBefore
	r.log = append([]Change(nil), img.Log...)
	for _, rec := range img.Removals {
		r.removals[key(rec.Instance.Service, rec.Instance.ID)] = rec
	}
	for _, h := range img.Holds {
		// Reserved holds that lapsed while the directory was down are
		// dropped, never revived; confirmed holds carry no expiry.
		if h.Kind == HoldReserved && !now.Before(h.ExpiresAt) {
			continue
		}
		r.holds[h.ID] = h
	}
	for _, onDisk := range img.Sessions {
		sess := &Session{
			reg:            r,
			id:             onDisk.ID,
			seq:            onDisk.Seq,
			current:        cloneSelection(onDisk.Current),
			capacityAware:  onDisk.CapacityAware,
			reservationID:  onDisk.ReservationID,
			lastConfirm:    cloneSelection(onDisk.LastConfirm),
			hasLastConfirm: onDisk.HasLast,
			hasPending:     onDisk.HasPending && onDisk.Pending != nil,
		}
		for _, rec := range onDisk.History {
			sess.history = append(sess.history, cloneSelection(rec))
		}
		if onDisk.Pending != nil {
			pending := cloneSelection(*onDisk.Pending)
			sess.pending = &pending
		}
		r.sessions[sess.id] = sess
	}
	var lapsed []onDiskEntry
	for _, de := range img.Entries {
		de.Instance.Metadata = cloneMeta(de.Instance.Metadata)
		if de.Instance.State == "" {
			de.Instance.State = StatePreparing
		}
		if !now.Before(de.Deadline) {
			lapsed = append(lapsed, de)
			continue
		}
		r.instances[key(de.Instance.Service, de.Instance.ID)] = &entry{
			inst:     de.Instance,
			token:    de.Token,
			deadline: de.Deadline,
			lease:    time.Duration(de.Lease),
		}
	}
	// Expired-on-load entries are audited and announced exactly as if the
	// sweeper had run, so restoring stale files cannot republish them.
	for _, de := range lapsed {
		r.appendLocked(Change{
			Kind:     ChangeExpired,
			Instance: cloneInst(de.Instance),
			Reason:   ReasonExpired,
			At:       now,
		})
		r.removals[key(de.Instance.Service, de.Instance.ID)] = RemovalRecord{
			Instance: de.Instance,
			Reason:   ReasonExpired,
			At:       now,
			Version:  r.version,
		}
	}
	return r, nil
}
