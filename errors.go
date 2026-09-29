package discovery

import "errors"

var (
	// ErrLeaseNotFound is returned when a heartbeat or deregistration
	// references an identity whose lease no longer exists. A restarted
	// instance receives a brand-new identity, so credentials from the
	// previous incarnation always fail with this error and can never
	// renew the new lease.
	ErrLeaseNotFound = errors.New("discovery: lease not found")

	// ErrInvalidArgument is returned for malformed registration requests.
	ErrInvalidArgument = errors.New("discovery: invalid argument")

	// ErrClosed is returned after the registry has been shut down.
	ErrClosed = errors.New("discovery: registry closed")
)
