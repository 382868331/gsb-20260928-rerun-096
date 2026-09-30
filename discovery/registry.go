package discovery

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Reason describes why an instance left the directory.
type Reason string

const (
	// ReasonExpired means the lease lapsed because no valid heartbeat arrived
	// before its deadline.
	ReasonExpired Reason = "expired"
	// ReasonDeregistered means the holder left explicitly with Deregister.
	ReasonDeregistered Reason = "deregistered"
)

// ChangeKind enumerates directory mutations exposed to subscribers.
type ChangeKind string

const (
	ChangeRegistered   ChangeKind = "registered"
	ChangeStateChanged ChangeKind = "state_changed"
	ChangeDeregistered ChangeKind = "deregistered"
	ChangeExpired      ChangeKind = "expired"
)

// InstanceState describes whether a registered instance can receive traffic.
type InstanceState string

const (
	// StatePreparing is the initial state. The instance has a lease but is not
	// addressable by callers yet.
	StatePreparing InstanceState = "preparing"
	// StateReady means the instance can receive requests.
	StateReady InstanceState = "ready"
	// StateDraining means the instance remains registered but must not receive
	// new requests while in-flight work finishes.
	StateDraining InstanceState = "draining"
)

// Instance is the addressable unit published by a virtual service.
type Instance struct {
	// Service groups instances that serve the same virtual service name.
	Service string
	// Partition scopes subscriptions to an independent directory view.
	Partition string
	// ID identifies a concrete incarnation. A restarted process must use a
	// fresh ID; the ID plus the lease token together prove the holder.
	ID string
	// Addr is the contact address handed to later call frameworks.
	Addr string
	// Metadata carries optional, opaque routing attributes.
	Metadata map[string]string
	// State is the traffic readiness state. Registration does not imply ready.
	State InstanceState
}

// Registration is the lease handle returned by Register.
type Registration struct {
	Instance Instance
	// Token is the secret returned only at registration. Heartbeats and
	// deregistration require it, so a stale client cannot extend a new
	// incarnation's lease.
	Token string
	// Version is the directory version produced by this registration.
	Version int64
	// Lease is the granted lease duration; the deadline is renewed on each
	// accepted heartbeat.
	Lease time.Duration
	// Deadline is the current lease expiry time.
	Deadline time.Time
}

// Change is one versioned directory event. Versors are dense and
// monotonically increasing across the whole directory.
type Change struct {
	Version  int64
	Kind     ChangeKind
	Instance Instance
	// Reason is populated for removals (expired vs explicit deregister).
	Reason Reason
	// At is the clock time at which the change was committed.
	At time.Time
}

// Snapshot is a complete directory at Version.
type Snapshot struct {
	Version   int64
	Instances []Instance
}

// AllPartitions selects every partition when reading the global directory.
const AllPartitions = "*"

// Ready returns instances that are allowed to receive new requests.
func (s Snapshot) Ready() []Instance {
	out := make([]Instance, 0, len(s.Instances))
	for _, inst := range s.Instances {
		if inst.State == StateReady {
			out = append(out, cloneInst(inst))
		}
	}
	return out
}

// ReadResult is returned by ChangesSince: either a compact list of changes
// or, when the requested version is no longer retained, a full snapshot.
type ReadResult struct {
	// FromVersion is the caller-known version when changes follow.
	FromVersion int64
	// Version is the current directory version.
	Version int64
	// Snapshot is set when events since the requested version are not
	// available. The caller must replace its whole directory; it must not
	// try to patch itself with a partial tail.
	Snapshot *Snapshot
	// Changes are the retained events after FromVersion, in version order.
	Changes []Change
}

// Errors returned by registry operations.
var (
	ErrNotFound     = errors.New("discovery: instance not found")
	ErrInvalidToken = errors.New("discovery: lease token does not match active instance")
	ErrBadArgument  = errors.New("discovery: invalid argument")
	ErrNoChange     = errors.New("discovery: version is ahead of directory")
	ErrInvalidState = errors.New("discovery: invalid instance state transition")
)

// entry is the live lease record.
type entry struct {
	inst     Instance
	token    string
	deadline time.Time
	lease    time.Duration
	expired  bool
}

type persistedRegistry struct {
	Version         int64              `json:"version"`
	CompactedBefore int64              `json:"compacted_before"`
	Retention       int                `json:"retention"`
	Instances       []persistedEntry   `json:"instances"`
	Changes         []Change           `json:"changes"`
	Removals        []persistedRemoval `json:"removals"`
}

type persistedEntry struct {
	Instance Instance      `json:"instance"`
	Token    string        `json:"token"`
	Deadline time.Time     `json:"deadline"`
	Lease    time.Duration `json:"lease"`
}

type persistedRemoval struct {
	Key string `json:"key"`
	RemovalRecord
}

// Option configures a Registry.
type Option func(*Registry)

// WithClock overrides the clock, mainly for tests.
func WithClock(c Clock) Option {
	return func(r *Registry) { r.clock = c }
}

// WithEventRetention bounds how many historical changes are retained for
// incremental subscribers. Subscribers older than this receive a snapshot
// instead of a silently truncated change stream. Defaults to 256.
func WithEventRetention(n int) Option {
	return func(r *Registry) {
		if n > 0 {
			r.retention = n
		}
	}
}

// Registry is the in-process service discovery core. It is intentionally
// independent of any external registry: all state lives in memory and is
// guarded by a single lock so lease and event-log decisions are atomic.
type Registry struct {
	mu        sync.Mutex
	cond      *sync.Cond
	clock     Clock
	retention int
	version   int64
	instances map[string]*entry
	// log holds recent committed changes; log[0].Version is the oldest
	// retained version and equals compactedBefore+1.
	log []Change
	// compactedBefore is the version before log[0] (0 until trimming starts).
	compactedBefore int64
	// removals keeps an audit trail of how instances left (expiry vs explicit
	// deregister), keyed by service + "\x00" + id.
	removals map[string]RemovalRecord
}

// RemovalRecord captures a departure, distinguishing lease failure from an
// active leave.
type RemovalRecord struct {
	Instance Instance
	Reason   Reason
	At       time.Time
	Version  int64
}

// NewRegistry builds an empty registry.
func NewRegistry(opts ...Option) *Registry {
	r := &Registry{
		clock:     WallClock{},
		retention: 256,
		instances: make(map[string]*entry),
		removals:  make(map[string]RemovalRecord),
	}
	for _, opt := range opts {
		opt(r)
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

const stateFileName = "registry.json"

// Save atomically persists the directory. A path without .json is treated as
// a state directory and receives registry.json inside it.
func (r *Registry) Save(path string) error {
	r.mu.Lock()
	data := persistedRegistry{
		Version:         r.version,
		CompactedBefore: r.compactedBefore,
		Retention:       r.retention,
		Instances:       make([]persistedEntry, 0, len(r.instances)),
		Changes:         append([]Change(nil), r.log...),
		Removals:        make([]persistedRemoval, 0, len(r.removals)),
	}
	for _, e := range r.instances {
		data.Instances = append(data.Instances, persistedEntry{
			Instance: cloneInst(e.inst),
			Token:    e.token,
			Deadline: e.deadline,
			Lease:    e.lease,
		})
	}
	for k, rec := range r.removals {
		data.Removals = append(data.Removals, persistedRemoval{Key: k, RemovalRecord: rec})
	}
	sortPersistedEntries(data.Instances)
	r.mu.Unlock()

	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	file, isDir := statePath(path)
	if isDir {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
	} else if dir := filepath.Dir(file); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(file), ".registry-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, file)
}

// OpenRegistry restores a registry previously written by Save. Expired leases
// are filtered against the configured clock and cannot be resurrected.
func OpenRegistry(path string, opts ...Option) (*Registry, error) {
	file, _ := statePath(path)
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var data persistedRegistry
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	r := NewRegistry(append([]Option{WithEventRetention(data.Retention)}, opts...)...)
	r.version = data.Version
	r.compactedBefore = data.CompactedBefore
	r.log = append([]Change(nil), data.Changes...)
	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, pe := range data.Instances {
		k := instanceKey(pe.Instance)
		if !now.Before(pe.Deadline) {
			inst := cloneInst(pe.Instance)
			r.appendLocked(Change{
				Kind:     ChangeExpired,
				Instance: inst,
				Reason:   ReasonExpired,
				At:       now,
			})
			r.removals[k] = RemovalRecord{
				Instance: inst,
				Reason:   ReasonExpired,
				At:       now,
				Version:  r.version,
			}
			continue
		}
		r.instances[k] = &entry{
			inst:     cloneInst(pe.Instance),
			token:    pe.Token,
			deadline: pe.Deadline,
			lease:    pe.Lease,
		}
	}
	for _, pr := range data.Removals {
		if pr.Key != "" || pr.Instance.Service != "" {
			r.removals[pr.Key] = pr.RemovalRecord
		}
	}
	return r, nil
}

func statePath(path string) (string, bool) {
	if filepath.Ext(path) == ".json" {
		return path, false
	}
	return filepath.Join(path, stateFileName), true
}

func sortPersistedEntries(entries []persistedEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return instanceSortKey(entries[i].Instance) < instanceSortKey(entries[j].Instance)
	})
}

func instanceSortKey(inst Instance) string {
	return strings.Join([]string{inst.Partition, inst.Service, inst.ID, inst.Addr}, "\x00")
}

// SaveDirectory persists state as registry.json in dir.
func (r *Registry) SaveDirectory(dir string) error { return r.Save(dir) }

// OpenDirectory restores registry.json from dir.
func OpenDirectory(dir string, opts ...Option) (*Registry, error) {
	return OpenRegistry(dir, opts...)
}

// LoadRegistry is an explicit alias for reopening persisted state.
func LoadRegistry(path string, opts ...Option) (*Registry, error) {
	return OpenRegistry(path, opts...)
}

func key(service, partition, id string) string {
	return service + "\x00" + partition + "\x00" + id
}

func instanceKey(inst Instance) string {
	return key(inst.Service, inst.Partition, inst.ID)
}

func validState(state InstanceState) bool {
	switch state {
	case StatePreparing, StateReady, StateDraining:
		return true
	default:
		return false
	}
}

func validStateTransition(from, to InstanceState) bool {
	switch {
	case from == to:
		return true
	case from == StatePreparing && to == StateReady:
		return true
	case from == StateReady && to == StateDraining:
		return true
	case from == StateDraining && to == StateReady:
		return true
	default:
		return false
	}
}

func newToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Register publishes a new incarnation of a virtual service instance and
// grants a lease. Re-registering the same service/id replaces the old
// incarnation: it gets a new token, so heartbeats carrying the old token are
// rejected and cannot extend the new lease. The replacement is recorded as
// an expiry-style removal of the prior incarnation followed by registration.
func (r *Registry) Register(inst Instance, lease time.Duration) (Registration, error) {
	if inst.Service == "" || inst.Partition == AllPartitions || lease <= 0 {
		return Registration{}, fmt.Errorf("%w: service, partition and lease must be valid", ErrBadArgument)
	}
	if inst.ID == "" {
		inst.ID = newToken()
	}
	inst.State = StatePreparing
	inst.Metadata = cloneMeta(inst.Metadata)

	k := instanceKey(inst)
	now := r.clock.Now()

	r.mu.Lock()
	if old, ok := r.instances[k]; ok {
		// A new incarnation supersedes the old one. The old holder can no
		// longer renew anything; record the forced departure separately.
		r.appendLocked(Change{
			Kind:     ChangeExpired,
			Instance: old.inst,
			Reason:   ReasonExpired,
			At:       now,
		})
		r.removals[k] = RemovalRecord{
			Instance: old.inst,
			Reason:   ReasonExpired,
			At:       now,
			Version:  r.version,
		}
	}
	token := newToken()
	e := &entry{
		inst:     inst,
		token:    token,
		deadline: now.Add(lease),
		lease:    lease,
	}
	r.instances[k] = e
	r.appendLocked(Change{Kind: ChangeRegistered, Instance: inst, At: now})
	v := r.version
	deadline := e.deadline
	r.mu.Unlock()

	return Registration{
		Instance: inst,
		Token:    token,
		Version:  v,
		Lease:    lease,
		Deadline: deadline,
	}, nil
}

// RegisterInPartition registers an instance that starts in preparing.
func (r *Registry) RegisterInPartition(partition, service, id, addr string, lease time.Duration) (Registration, error) {
	return r.Register(Instance{Partition: partition, Service: service, ID: id, Addr: addr, State: StatePreparing}, lease)
}

// Heartbeat extends the lease iff service, id and token identify the same
// active incarnation. After a restart the new identity/token means a stale
// heartbeat is rejected rather than silently keeping the entry alive.
func (r *Registry) Heartbeat(service, id, token string) (deadline time.Time, err error) {
	return r.HeartbeatInPartition("", service, id, token)
}

// HeartbeatPartition is a short partition-specific lease alias.
func (r *Registry) HeartbeatPartition(partition, service, id, token string) (time.Time, error) {
	return r.HeartbeatInPartition(partition, service, id, token)
}

// HeartbeatInPartition renews an incarnation in a partition.
func (r *Registry) HeartbeatInPartition(partition, service, id, token string) (deadline time.Time, err error) {
	k := key(service, partition, id)
	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.instances[k]
	if !ok {
		return time.Time{}, ErrNotFound
	}
	if e.token != token {
		return time.Time{}, ErrInvalidToken
	}
	if !now.Before(e.deadline) {
		// The lease has lapsed but Sweep has not run yet. Do not grant an
		// extension; expire it inline so the directory stays consistent.
		r.expireLocked(k, e, now)
		return time.Time{}, ErrNotFound
	}
	e.deadline = now.Add(e.lease)
	return e.deadline, nil
}

// Deregister removes an incarnation on explicit shutdown. A token mismatch
// is rejected so that a stale process cannot evict a restarted incarnation.
func (r *Registry) Deregister(service, id, token string) error {
	return r.DeregisterInPartition("", service, id, token)
}

// DeregisterPartition is a short partition-specific leave alias.
func (r *Registry) DeregisterPartition(partition, service, id, token string) error {
	return r.DeregisterInPartition(partition, service, id, token)
}

// DeregisterInPartition removes an incarnation in a partition.
func (r *Registry) DeregisterInPartition(partition, service, id, token string) error {
	k := key(service, partition, id)
	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.instances[k]
	if !ok {
		return ErrNotFound
	}
	if e.token != token {
		return ErrInvalidToken
	}
	if !now.Before(e.deadline) {
		r.expireLocked(k, e, now)
		return ErrNotFound
	}
	r.removeLocked(k, e, ReasonDeregistered, now)
	return nil
}

// SetState changes readiness state using the current incarnation token. It
// commits a globally versioned event only when the state actually changes.
func (r *Registry) SetState(service, id, token string, state InstanceState) (Change, error) {
	return r.SetStateInPartition("", service, id, token, state)
}

// MarkReady moves a current incarnation to ready.
func (r *Registry) MarkReady(service, id, token string) (Change, error) {
	return r.SetState(service, id, token, StateReady)
}

// BeginDrain moves a current incarnation to draining.
func (r *Registry) BeginDrain(service, id, token string) (Change, error) {
	return r.SetState(service, id, token, StateDraining)
}

// SetStateInPartition changes readiness state within a partition.
func (r *Registry) SetStateInPartition(partition, service, id, token string, state InstanceState) (Change, error) {
	if !validState(state) {
		return Change{}, fmt.Errorf("%w: unknown state %q", ErrBadArgument, state)
	}
	k := key(service, partition, id)
	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.instances[k]
	if !ok {
		return Change{}, ErrNotFound
	}
	if e.token != token {
		return Change{}, ErrInvalidToken
	}
	if !now.Before(e.deadline) {
		r.expireLocked(k, e, now)
		return Change{}, ErrNotFound
	}
	if !validStateTransition(e.inst.State, state) {
		return Change{}, fmt.Errorf("%w: %s -> %s", ErrInvalidState, e.inst.State, state)
	}
	if e.inst.State == state {
		return Change{}, nil
	}
	e.inst.State = state
	c := Change{Kind: ChangeStateChanged, Instance: cloneInst(e.inst), At: now}
	r.appendLocked(c)
	return c, nil
}

// Sweep expires every lease whose deadline has passed. Returns the changes
// produced so callers/tests can observe what lapsed.
func (r *Registry) Sweep() []Change {
	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	startVersion := r.version
	var swept []Change
	for k, e := range r.instances {
		if !now.Before(e.deadline) {
			r.expireLocked(k, e, now)
		}
	}
	for _, c := range r.log {
		if c.Version > startVersion && c.Kind == ChangeExpired && c.At.Equal(now) {
			swept = append(swept, c)
		}
	}
	return swept
}

func (r *Registry) expireLocked(k string, e *entry, now time.Time) {
	r.removeLocked(k, e, ReasonExpired, now)
}

func (r *Registry) removeLocked(k string, e *entry, reason Reason, now time.Time) {
	delete(r.instances, k)
	kind := ChangeDeregistered
	if reason == ReasonExpired {
		kind = ChangeExpired
	}
	r.appendLocked(Change{
		Kind:     kind,
		Instance: e.inst,
		Reason:   reason,
		At:       now,
	})
	r.removals[k] = RemovalRecord{
		Instance: e.inst,
		Reason:   reason,
		At:       now,
		Version:  r.version,
	}
}

// appendLocked commits a change, advancing the global version and trimming
// history to the retention window.
func (r *Registry) appendLocked(c Change) {
	r.version++
	c.Version = r.version
	r.log = append(r.log, c)
	r.cond.Broadcast()
	if len(r.log) > r.retention {
		drop := len(r.log) - r.retention
		r.compactedBefore = r.log[drop-1].Version
		r.log = append([]Change(nil), r.log[drop:]...)
	}
}

// Snapshot returns the complete live directory at the current version.
func (r *Registry) Snapshot() Snapshot {
	return r.SnapshotPartition(AllPartitions)
}

// SnapshotPartition returns live instances in one partition. AllPartitions
// returns every partition. Preparing and draining instances remain visible
// but can be excluded with ReadyInstances.
func (r *Registry) SnapshotPartition(partition string) Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	snap := r.snapshotLocked(partition)
	return *snap
}

// ReadyInstances returns every instance that can receive new requests.
func (r *Registry) ReadyInstances() []Instance {
	return r.ReadyInstancesInPartition(AllPartitions)
}

// ReadyInstancesInPartition returns ready instances in one partition.
func (r *Registry) ReadyInstancesInPartition(partition string) []Instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked(partition).Ready()
}

// ChangesSince returns directory changes strictly after fromVersion.
//
// If fromVersion is 0 the caller gets a full snapshot. If the requested
// version has fallen out of the retention window the caller also gets a full
// snapshot instead of a partial tail, so a lagging subscriber never silently
// skips events. If fromVersion equals the current version the change list is
// empty and Snapshot is nil.
func (r *Registry) ChangesSince(fromVersion int64) (ReadResult, error) {
	return r.ChangesSinceInPartition(AllPartitions, fromVersion)
}

// ChangesSinceInPartition returns changes after fromVersion for a partition.
// Global versions are retained even after partition filtering, so a slow
// subscriber receives one snapshot rather than stitching stale partitions.
func (r *Registry) ChangesSinceInPartition(partition string, fromVersion int64) (ReadResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fromVersion < 0 {
		return ReadResult{}, fmt.Errorf("%w: negative version", ErrBadArgument)
	}
	if fromVersion > r.version {
		return ReadResult{}, ErrNoChange
	}
	oldest := r.oldestVersionLocked()
	if fromVersion != 0 && fromVersion < oldest {
		// Events were compacted; force a full resync.
		return ReadResult{
			FromVersion: fromVersion,
			Version:     r.version,
			Snapshot:    r.snapshotLocked(partition),
		}, nil
	}
	res := ReadResult{FromVersion: fromVersion, Version: r.version}
	if fromVersion == 0 {
		res.Snapshot = r.snapshotLocked(partition)
		return res, nil
	}
	for _, c := range r.log {
		if c.Version > fromVersion && partitionMatches(c.Instance.Partition, partition) {
			res.Changes = append(res.Changes, c)
		}
	}
	return res, nil
}

func (r *Registry) oldestVersionLocked() int64 {
	if len(r.log) == 0 {
		return r.compactedBefore + 1
	}
	return r.log[0].Version
}

func (r *Registry) snapshotLocked(partitions ...string) *Snapshot {
	s := &Snapshot{Version: r.version}
	partition := AllPartitions
	if len(partitions) > 0 {
		partition = partitions[0]
	}
	for _, e := range r.instances {
		if !partitionMatches(e.inst.Partition, partition) {
			continue
		}
		s.Instances = append(s.Instances, cloneInst(e.inst))
	}
	sort.Slice(s.Instances, func(i, j int) bool {
		left := s.Instances[i]
		right := s.Instances[j]
		if left.Partition != right.Partition {
			return left.Partition < right.Partition
		}
		if left.Service != right.Service {
			return left.Service < right.Service
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		return left.Addr < right.Addr
	})
	return s
}

func partitionMatches(instancePartition, requested string) bool {
	return requested == AllPartitions || instancePartition == requested
}

// Version returns the current global directory version.
func (r *Registry) Version() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.version
}

// Removal looks up how an incarnation departed. The second result is false if
// the instance is still live or was never registered.
func (r *Registry) Removal(service, id string) (RemovalRecord, bool) {
	return r.RemovalInPartition("", service, id)
}

// RemovalInPartition looks up an audit record within a partition.
func (r *Registry) RemovalInPartition(partition, service, id string) (RemovalRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.removals[key(service, partition, id)]
	return rec, ok
}

func cloneMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func cloneInst(in Instance) Instance {
	in.Metadata = cloneMeta(in.Metadata)
	return in
}
