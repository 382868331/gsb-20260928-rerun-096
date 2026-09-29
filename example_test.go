package discovery_test

import (
	"context"
	"fmt"
	"time"

	discovery "servicediscovery"
)

func Example() {
	clk := discovery.NewFakeClock(time.Now())
	reg := discovery.New(
		discovery.WithClock(clk),
		discovery.WithoutJanitor(),
		discovery.WithHistoryCapacity(128),
	)
	defer reg.Close()

	// Instance registers an ephemeral lease.
	lease, _ := reg.Register(discovery.RegisterInput{
		Service:  "checkout",
		Endpoint: "10.0.0.7:9000",
		TTL:      5 * time.Second,
		Metadata: map[string]string{"zone": "cn-shanghai-a"},
	})

	// Heartbeats renew only the current identity.
	_, _ = reg.Heartbeat(lease.Instance.Identity)

	// Subscriber starts at version 0: first update is a full snapshot.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, _ := reg.Watch(ctx, 0)
	first := <-updates
	cursor := first.Snapshot.Version

	// Reconnect at any time with the last handled version: Poll returns
	// either the missing event window or, if too far behind, a snapshot.
	_, evs, cursor, _ := reg.Poll(cursor)
	_ = evs
	fmt.Printf("service %s at version %d with %d instance(s)",
		"checkout", cursor, len(reg.Lookup("checkout")))

	// Output: service checkout at version 1 with 1 instance(s)
}
