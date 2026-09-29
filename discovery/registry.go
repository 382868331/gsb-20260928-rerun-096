package discovery

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	ChangeDeregistered ChangeKind = "deregistered"
	ChangeExpired      ChangeKind = "expired"
)

// Instance is the addressable unit published by a virtual service.
type Instance struct {
	// Service groups instances that serve the same virtual service name.
	Service string
	// ID identifies a concrete incarnation. A restarted process must use a
	// fresh ID; the ID plus the lease token together prove the holder.
	ID string
	// Addr is the contact address handed to later call frameworks.
	Addr string
	// Metadata carries optional, opaque routing attributes.
	Metadata map[string]string
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
)

// entry is the live lease record.
type entry struct {
	inst     Instance
	token    string
	deadline time.Time
	lease    time.Duration
	expired  bool
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
	instances map[string]*entry // key: service + "\x00" + id
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

func key(service, id string) string { return service + "\x00" + id }

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
	if inst.Service == "" || lease <= 0 {
		return Registration{}, fmt.Errorf("%w: service required and lease must be positive", ErrBadArgument)
	}
	if inst.ID == "" {
		inst.ID = newToken()
	}
	inst.Metadata = cloneMeta(inst.Metadata)

	k := key(inst.Service, inst.ID)
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

// Heartbeat extends the lease iff service, id and token identify the same
// active incarnation. After a restart the new identity/token means a stale
// heartbeat is rejected rather than silently keeping the entry alive.
func (r *Registry) Heartbeat(service, id, token string) (deadline time.Time, err error) {
	k := key(service, id)
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
	k := key(service, id)
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
	r.removeLocked(k, e, ReasonDeregistered, now)
	return nil
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
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Instance, 0, len(r.instances))
	for _, e := range r.instances {
		out = append(out, cloneInst(e.inst))
	}
	return Snapshot{Version: r.version, Instances: out}
}

// ChangesSince returns directory changes strictly after fromVersion.
//
// If fromVersion is 0 the caller gets a full snapshot. If the requested
// version has fallen out of the retention window the caller also gets a full
// snapshot instead of a partial tail, so a lagging subscriber never silently
// skips events. If fromVersion equals the current version the change list is
// empty and Snapshot is nil.
func (r *Registry) ChangesSince(fromVersion int64) (ReadResult, error) {
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
			Snapshot:    r.snapshotLocked(),
		}, nil
	}
	res := ReadResult{FromVersion: fromVersion, Version: r.version}
	if fromVersion == 0 {
		res.Snapshot = r.snapshotLocked()
		return res, nil
	}
	for _, c := range r.log {
		if c.Version > fromVersion {
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

func (r *Registry) snapshotLocked() *Snapshot {
	s := &Snapshot{Version: r.version}
	for _, e := range r.instances {
		s.Instances = append(s.Instances, cloneInst(e.inst))
	}
	return s
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
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.removals[key(service, id)]
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
