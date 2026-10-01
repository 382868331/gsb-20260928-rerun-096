package discovery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
}

// Save atomically persists the live directory, including readiness state,
// the retained change log and departure audit records. Expired instances are
// already gone from the directory, so a saved image can never bring a lapsed
// instance back on reopen.
func (r *Registry) Save(dir string) error {
	r.mu.Lock()
	// Persist only leases that are still effective at save time; lapsed
	// leases are expired inline (with events and audit) instead of being
	// written as live entries.
	r.expireStaleLocked(r.clock.Now())
	img := stateFile{
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
	r.mu.Unlock()

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
