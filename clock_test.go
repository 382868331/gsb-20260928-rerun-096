package discovery

import (
	"testing"
	"time"
)

func TestFakeClockTimerFiresOnAdvance(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0).UTC())
	ch := clk.After(3 * time.Second)
	clk.Advance(2 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired early")
	default:
	}
	clk.Advance(1 * time.Second)
	select {
	case fired := <-ch:
		if !fired.Equal(time.Unix(3, 0)) {
			t.Fatalf("fired at %v", fired)
		}
	default:
		t.Fatal("timer did not fire")
	}
}

func TestJanitorUsesFakeClock(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0).UTC())
	r := New(WithClock(clk)) // janitor enabled, driven by fake clock
	defer func() { _ = r.Close() }()
	_, _ = r.Register(RegisterInput{Service: "s", Endpoint: "a", TTL: 250 * time.Millisecond})
	clk.Advance(300 * time.Millisecond)
	waitFor(t, func() bool { return r.Stats().Expired == 1 })
}
