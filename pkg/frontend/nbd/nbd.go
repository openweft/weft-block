// Package nbd implements types.Frontend by exposing the controller's
// in-memory block device as a Linux NBD device (/dev/nbdN). Userspace speaks
// the NBD protocol on one end of a socketpair; the kernel NBD client speaks
// it on the other end (after NBD_SET_SOCK + NBD_DO_IT). The path returned by
// Endpoint() is the path the hypervisor mounts into the VM as virtio-blk.
//
// Why NBD and not iSCSI/tgt: NBD's client lives in the upstream Linux kernel,
// so there is no userspace target daemon to ship or configure — just a server
// process talking to /dev/nbdN through ioctls. This package is Linux-only;
// the non-linux build is a stub that errors on Startup so the rest of
// weft-block still compiles on dev hosts.
package nbd

import (
	"net"
	"os"
	"sync"

	"github.com/openweft/weft-block/pkg/types"
)

// blockSize is the logical block size advertised to the kernel via
// NBD_SET_BLKSIZE. 4 KiB matches virtio-blk and the replica's sparse-file
// chunking; volume sizes are required to be multiples of it.
const blockSize = 4096

// Nbd is the NBD frontend. The struct fields are cross-platform; the
// startup / shutdown / expand methods that actually drive the kernel are in
// nbd_linux.go (and stubbed on other platforms in nbd_other.go).
type Nbd struct {
	mu sync.Mutex

	// init-time
	name       string
	size       int64
	sectorSize int64

	// running
	state    types.State
	endpoint string
	rwu      types.ReaderWriterUnmapperAt
	devFile  *os.File // open /dev/nbdN
	cliConn  net.Conn // server side of the socketpair
	kernelFd int      // kernel-side fd of the socketpair; kept open during NBD_DO_IT
	doneCh   chan struct{}
}

// New returns an NBD frontend in the Down state.
func New() *Nbd { return &Nbd{state: types.StateDown} }

func (n *Nbd) FrontendName() string { return "nbd" }

func (n *Nbd) State() types.State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Nbd) Endpoint() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != types.StateUp {
		return ""
	}
	return n.endpoint
}

func (n *Nbd) Init(name string, size, sectorSize int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.name = name
	n.size = size
	n.sectorSize = sectorSize
	return nil
}

// Startup grabs a free /dev/nbdN, plumbs it to a userspace NBD server backed
// by rwu, and returns once the device is ready. See nbd_linux.go for the real
// body; the non-linux stub returns an error.
func (n *Nbd) Startup(rwu types.ReaderWriterUnmapperAt) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == types.StateUp {
		return nil
	}
	if err := n.startup(rwu); err != nil {
		return err
	}
	n.state = types.StateUp
	return nil
}

func (n *Nbd) Shutdown() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != types.StateUp {
		return nil
	}
	err := n.shutdown()
	n.state = types.StateDown
	n.endpoint = ""
	return err
}

// Upgrade swaps the backing rwu (e.g. after a controller reload) by doing a
// shutdown / re-init / startup, mirroring the upstream Frontend contract.
func (n *Nbd) Upgrade(name string, size, sectorSize int64, rwu types.ReaderWriterUnmapperAt) error {
	if err := n.Shutdown(); err != nil {
		return err
	}
	if err := n.Init(name, size, sectorSize); err != nil {
		return err
	}
	return n.Startup(rwu)
}

func (n *Nbd) Expand(size int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != types.StateUp {
		n.size = size
		return nil
	}
	if err := n.expand(size); err != nil {
		return err
	}
	n.size = size
	return nil
}
