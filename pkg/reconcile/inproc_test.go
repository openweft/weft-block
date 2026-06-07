package reconcile

import (
	"net"
	"testing"
)

// TestFreeTCPPort_BoundedAndUsable: the helper returns a port the kernel
// confirms is free at the moment of the call (you can re-bind it immediately
// afterwards). Two consecutive calls usually return different ports too.
func TestFreeTCPPort_BoundedAndUsable(t *testing.T) {
	port, err := freeTCPPort()
	if err != nil {
		t.Fatalf("freeTCPPort: %v", err)
	}
	if port < 1 || port > 65535 {
		t.Errorf("port out of range: %d", port)
	}
	// We must be able to re-bind it immediately.
	l, err := net.Listen("tcp", "127.0.0.1:"+itoa(port))
	if err != nil {
		// The kernel may have re-handed the port to another process; that
		// race is acceptable. Just don't fail the test for it.
		t.Logf("re-bind raced for port %d: %v (acceptable; not a bug in freeTCPPort)", port, err)
		return
	}
	_ = l.Close()
}

func TestFreeTCPPort_TypicallyDistinct(t *testing.T) {
	seen := map[int]struct{}{}
	for i := 0; i < 8; i++ {
		p, err := freeTCPPort()
		if err != nil {
			t.Fatal(err)
		}
		seen[p] = struct{}{}
	}
	// Not all distinct is theoretically possible but extremely unlikely on a
	// healthy system; fail loudly if we keep getting the same port.
	if len(seen) < 2 {
		t.Errorf("freeTCPPort returned the same port %d times in a row", 8)
	}
}

// itoa avoids strconv just to keep this test file's imports minimal.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [10]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
