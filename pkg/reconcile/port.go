package reconcile

import (
	"fmt"
	"net"
)

// freeTCPPort asks the kernel for an unused TCP port by binding to :0, reading
// the assigned port, and releasing the listener. Subject to a tiny race
// between release and re-bind by the caller — acceptable for spawn time;
// production hardening replaces this with a reserved-port pool. Pure Go,
// cross-platform.
func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

// freeTriplet finds the base of three consecutive free TCP ports — the layout
// upstream Longhorn expects per replica: base = gRPC control, base+1 = data
// (dataconn), base+2 = sync. Probes the kernel-assigned :0 ports until a hit
// where +1 and +2 are also bindable. Tries 64 times before giving up.
func freeTriplet() (int, error) {
	for attempts := 0; attempts < 64; attempts++ {
		base, err := freeTCPPort()
		if err != nil {
			return 0, err
		}
		// Reserve +1 and +2 to verify they're free RIGHT NOW. They get
		// released back to the kernel immediately; a tiny race remains
		// where another process steals them — production replaces this
		// with a reserved pool.
		l1, err := net.Listen("tcp", fmt.Sprintf(":%d", base+1))
		if err != nil {
			continue
		}
		l2, err := net.Listen("tcp", fmt.Sprintf(":%d", base+2))
		if err != nil {
			l1.Close()
			continue
		}
		l1.Close()
		l2.Close()
		return base, nil
	}
	return 0, fmt.Errorf("could not find 3 consecutive free TCP ports after 64 attempts")
}
