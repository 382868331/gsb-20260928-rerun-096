// Package discovery implements the in-memory service-discovery runtime
// core. Virtual service instances register ephemeral leases, keep them
// alive with heartbeats, and subscribers observe a versioned change log.
//
// There is deliberately no external registry dependency: this package is
// intended to become the local address source for a call framework.
package discovery

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	defaultTTL        = 10 * time.Second
	defaultHistoryCap = 4096
	janitorInterval   = 100 * time.Millisecond
)

type record struct {
	info       InstanceInfo
	ttl        time.Duration
	deadline   time.Time
	generation uint64
}

// Registry is the directory. The zero value is not usable; call New.
type Registry struct {
	clock   Clock
	histCap int
	janitor bool

	mu       sync.Mutex
	closed   bool
	version  uint64
	records  map[string]*record           // identity -> record
	slots    map[string]map[string]string // service -> endpoint -> identity
	log      []Event                      // versions oldest..latest within retained window
	logFirst uint64                       // version of log[0]; 0 when empty
	watchers map[*liveWatcher]struct{}
	stats    Stats
	stopCh   chan struct{}
	stopOnce sync.Once
	doneCh   chan struct{}
}

// Option configures a Registry.
type Option func(*Registry)

// WithClock sets the time source (defaults to RealClock).
func WithClock(c Clock) Option { return func(r *Registry) { r.clock = c } }

// WithHistoryCapacity bounds how many events the compacted log retains.
// Subscribers requesting changes from before the retained window receive
// a full snapshot instead of silently missing events.
func WithHistoryCapacity(n int) Option {
	return func(r *Registry) {
		if n > 0 {
			r.histCap = n
		}
	}
}

// WithoutJanitor disables the background expiry sweeper. It is mainly
// useful with a FakeClock when tests drive expiry via Sweep directly.
func WithoutJanitor() Option { return func(r *Registry) { r.janitor = false } }

// New creates a Registry.
func New(opts ...Option) *Registry {
	r := &Registry{
		clock:    RealClock{},
		histCap:  defaultHistoryCap,
		janitor:  true,
		records:  make(map[string]*record),
		slots:    make(map[string]map[string]string),
		watchers: make(map[*liveWatcher]struct{}),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	for _, o := range opts {
		o(r)
	}
	if r.janitor {
		go r.sweepLoop()
	} else {
		close(r.doneCh)
	}
	return r
}

func (r *Registry) sweepLoop() {
	defer close(r.doneCh)
	for {
		r.Sweep()
		timer := r.clock.After(janitorInterval)
		select {
		case <-r.stopCh:
			return
		case <-timer:
		}
	}
}

// Close stops the background sweeper.
func (r *Registry) Close() error {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		close(r.stopCh)
	})
	<-r.doneCh
	return nil
}

func newIdentity() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Register publishes a new incarnation and returns its lease credential.
// A fresh random identity is allocated on every call; credentials from a
// prior restart can never renew the new lease.
//
// If the same (service, endpoint) slot is already held by a live
// incarnation, the old one is evicted with a KindSuperseded event and the
// directory ends with exactly one holder: the new identity.
func (r *Registry) Register(in RegisterInput) (Registration, error) {
	if in.Service == "" || in.Endpoint == "" {
		return Registration{}, fmt.Errorf("%w: service and endpoint are required", ErrInvalidArgument)
	}
	ttl := in.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Registration{}, ErrClosed
	}

	now := r.clock.Now()
	r.sweepLocked(now)

	identity := newIdentity()
	info := InstanceInfo{
		Service:    in.Service,
		Endpoint:   in.Endpoint,
		Identity:   identity,
		Generation: r.version + 1,
		Metadata:   cloneMeta(in.Metadata),
	}
	rec := &record{
		info:       info,
		ttl:        ttl,
		deadline:   now.Add(ttl),
		generation: info.Generation,
	}

	var pending []Event
	if endpoints := r.slots[in.Service]; endpoints != nil {
		if oldID, ok := endpoints[in.Endpoint]; ok {
			if old := r.records[oldID]; ok {
				pending = append(pending, Event{
					Kind:       KindSuperseded,
					At:         now,
					Instance:   old.info.clone(),
					Generation: old.generation,
				})
				r.stats.Superseded++
				delete(r.records, oldID)
			}
			delete(endpoints, in.Endpoint)
		}
	}

	r.records[identity] = rec
	if r.slots[in.Service] == nil {
		r.slots[in.Service] = make(map[string]string)
	}
	r.slots[in.Service][in.Endpoint] = identity
	r.stats.Registered++
	pending = append(pending, Event{
		Kind:       KindRegistered,
		At:         now,
		Instance:   info.clone(),
		Generation: rec.generation,
	})
	regVer := r.appendLocked(pending)

	return Registration{
		Instance: info.clone(),
		Version:  regVer,
		Deadline: rec.deadline,
		TTL:      ttl,
	}, nil
}

// Heartbeat extends the lease. It succeeds only for the lease's current
// identity; an identity from a restarted (new) incarnation fails with
// ErrLeaseNotFound and therefore cannot steal or extend the new lease.
func (r *Registry) Heartbeat(identity string) (deadline time.Time, err error) {
	if identity == "" {
		return time.Time{}, fmt.Errorf("%w: identity is required", ErrInvalidArgument)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return time.Time{}, ErrClosed
	}
	r.sweepLocked(r.clock.Now())
	rec, ok := r.records[identity]
	if !ok {
		return time.Time{}, ErrLeaseNotFound
	}
	rec.deadline = r.clock.Now().Add(rec.ttl)
	return rec.deadline, nil
}

// Deregister voluntarily gives up the lease and records a
// KindDeregistered event, distinct from a timeout's KindExpired.
func (r *Registry) Deregister(identity string) error {
	if identity == "" {
		return fmt.Errorf("%w: identity is required", ErrInvalidArgument)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	now := r.clock.Now()
	r.sweepLocked(now)
	if _, ok := r.records[identity]; !ok {
		return ErrLeaseNotFound
	}
	r.removeLocked(identity, KindDeregistered, now)
	r.stats.Deregistered++
	return nil
}

func (r *Registry) removeLocked(identity string, kind EventKind, at time.Time) {
	rec, ok := r.records[identity]
	if !ok {
		return
	}
	delete(r.records, identity)
	if endpoints := r.slots[rec.info.Service]; endpoints != nil {
		if cur := endpoints[rec.info.Endpoint]; cur == identity {
			delete(endpoints, rec.info.Endpoint)
			if len(endpoints) == 0 {
				delete(r.slots, rec.info.Service)
			}
		}
	}
	r.appendLocked([]Event{{
		Kind:       kind,
		At:         at,
		Instance:   rec.info.clone(),
		Generation: rec.generation,
	}})
}

// Sweep expires every lease whose deadline has passed. It is safe to call
// concurrently and is invoked automatically by the background janitor.
func (r *Registry) Sweep() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sweepLocked(r.clock.Now())
}

func (r *Registry) sweepLocked(now time.Time) int {
	var expired []string
	for id, rec := range r.records {
		if !rec.deadline.After(now) {
			expired = append(expired, id)
		}
	}
	if len(expired) == 0 {
		return 0
	}
	sort.Strings(expired)
	events := make([]Event, 0, len(expired))
	for _, id := range expired {
		rec := r.records[id]
		delete(r.records, id)
		if endpoints := r.slots[rec.info.Service]; endpoints != nil {
			if cur := endpoints[rec.info.Endpoint]; cur == id {
				delete(endpoints, rec.info.Endpoint)
				if len(endpoints) == 0 {
					delete(r.slots, rec.info.Service)
				}
			}
		}
		events = append(events, Event{
			Kind:       KindExpired,
			At:         now,
			Instance:   rec.info.clone(),
			Generation: rec.generation,
		})
	}
	r.stats.Expired += uint64(len(expired))
	r.appendLocked(events)
	return len(expired)
}

// appendLocked assigns contiguous versions, appends to the retained log,
// compacts beyond history capacity and fans events out. Versions are
// finalized and visible before the lock is released.
func (r *Registry) appendLocked(evs []Event) uint64 {
	last := r.version
	for i := range evs {
		r.version++
		evs[i].Version = r.version
	}
	r.log = append(r.log, evs...)
	if r.logFirst == 0 && len(r.log) > 0 {
		r.logFirst = r.log[0].Version
	}
	if excess := len(r.log) - r.histCap; excess > 0 {
		r.log = r.log[excess:]
	}
	if len(r.log) > 0 {
		r.logFirst = r.log[0].Version
	} else {
		r.logFirst = 0
	}
	last = r.version

	for w := range r.watchers {
		select {
		case w.wake <- struct{}{}:
		default:
			// A wake is already pending; the goroutine drains the
			// versioned log from its cursor, so no event is lost.
		}
	}
	return last
}

func cloneMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
