package discovery

import "time"

// EventKind classifies a directory change. Expiry and voluntary departure
// are deliberately distinct kinds so subscribers never confuse a crash
// with an intentional shutdown.
type EventKind int

const (
	// KindRegistered is emitted when a new incarnation is published.
	KindRegistered EventKind = iota + 1
	// KindDeregistered is emitted when the holder actively gives up the lease.
	KindDeregistered
	// KindExpired is emitted when a lease reaches its deadline without renewal.
	KindExpired
	// KindSuperseded is emitted when a restart registers the same
	// (service, endpoint) slot under a new identity and the old
	// incarnation is evicted.
	KindSuperseded
)

func (k EventKind) String() string {
	switch k {
	case KindRegistered:
		return "registered"
	case KindDeregistered:
		return "deregistered"
	case KindExpired:
		return "expired"
	case KindSuperseded:
		return "superseded"
	default:
		return "unknown"
	}
}

// InstanceInfo is the publicly visible description of a live instance.
type InstanceInfo struct {
	Service    string
	Endpoint   string
	Identity   string
	Generation uint64
	Metadata   map[string]string
}

func (i InstanceInfo) clone() InstanceInfo {
	if i.Metadata == nil {
		return i
	}
	m := make(map[string]string, len(i.Metadata))
	for k, v := range i.Metadata {
		m[k] = v
	}
	i.Metadata = m
	return i
}

// Event is one versioned directory change. Versions are contiguous,
// strictly increasing and never reused.
type Event struct {
	Version    uint64
	Kind       EventKind
	At         time.Time
	Instance   InstanceInfo
	Generation uint64
}

// Snapshot is a complete, self-contained view of the directory at Version.
// A snapshot followed by events with version greater than Version
// reconstructs every later state without gaps.
type Snapshot struct {
	Version   uint64
	Instances []InstanceInfo
}

// RegisterInput describes a virtual service instance incarnation.
type RegisterInput struct {
	Service  string
	Endpoint string
	Metadata map[string]string
	TTL      time.Duration
}

// Registration is the lease credential returned by Register. Identity is
// ephemeral: every restart produces a new one and the old one stops being
// accepted for heartbeats.
type Registration struct {
	Instance InstanceInfo
	Version  uint64
	Deadline time.Time
	TTL      time.Duration
}

// Stats records lifecycle outcomes separately.
type Stats struct {
	Registered   uint64
	Deregistered uint64
	Expired      uint64
	Superseded   uint64
}
