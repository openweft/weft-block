//go:build linux

package nbd

// nbd_linux.go bridges weft-block's types.Frontend to github.com/pojntfx/
// go-nbd, the battle-tested Go NBD implementation. go-nbd handles the full
// NEWSTYLE handshake + transmission protocol + NBD ioctl chain — we only
// supply a backend.Backend wrapping our controller and a /dev/nbdN file.
//
// Before swapping to go-nbd this file had ~400 lines of in-process protocol +
// ioctl code that got everything up to "device size correct, kernel claims
// connected" but failed at the I/O step on Linux 6.x. go-nbd's Connect()
// just works on the same kernel.

import (
	"fmt"
	"net"
	"os"
	"syscall"

	nbdclient "github.com/openweft/weft-nbd/pkg/client"
	nbdserver "github.com/openweft/weft-nbd/pkg/server"
	"golang.org/x/sys/unix"

	"github.com/openweft/weft-block/pkg/types"
)

const exportName = "weft-block"

// controllerBackend adapts types.ReaderWriterUnmapperAt (the engine controller)
// to go-nbd's backend.Backend (io.ReaderAt + io.WriterAt + Size + Sync).
type controllerBackend struct {
	rwu  types.ReaderWriterUnmapperAt
	size int64
}

func (b *controllerBackend) ReadAt(p []byte, off int64) (int, error)  { return b.rwu.ReadAt(p, off) }
func (b *controllerBackend) WriteAt(p []byte, off int64) (int, error) { return b.rwu.WriteAt(p, off) }
func (b *controllerBackend) Size() (int64, error)                     { return b.size, nil }
func (b *controllerBackend) Sync() error                              { return nil } // engine writes are sync

func (n *Nbd) startup(rwu types.ReaderWriterUnmapperAt) error {
	if n.size <= 0 {
		return fmt.Errorf("nbd: Init() must precede Startup() with a positive size")
	}
	if n.size%blockSize != 0 {
		return fmt.Errorf("nbd: volume size %d not a multiple of block size %d", n.size, blockSize)
	}

	devPath, devFile, err := openFreeNBDDevice()
	if err != nil {
		return err
	}

	// AF_UNIX socketpair so the server goroutine and the kernel-facing client
	// side talk via a real socket (go-nbd's client.Connect passes the fd
	// underlying cliConn to NBD_SET_SOCK).
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		devFile.Close()
		return fmt.Errorf("nbd: socketpair: %w", err)
	}
	srvFile := os.NewFile(uintptr(fds[0]), "nbd-server")
	cliFile := os.NewFile(uintptr(fds[1]), "nbd-client")
	srvConn, err := net.FileConn(srvFile)
	srvFile.Close()
	if err != nil {
		cliFile.Close()
		devFile.Close()
		return fmt.Errorf("nbd: wrap server fd: %w", err)
	}
	cliConn, err := net.FileConn(cliFile)
	cliFile.Close()
	if err != nil {
		srvConn.Close()
		devFile.Close()
		return fmt.Errorf("nbd: wrap client fd: %w", err)
	}

	n.devFile = devFile
	n.cliConn = cliConn
	n.rwu = rwu
	n.endpoint = devPath
	n.doneCh = make(chan struct{})

	backend := &controllerBackend{rwu: rwu, size: n.size}
	export := &nbdserver.Export{Name: exportName, Backend: backend}

	// Server goroutine: speaks NBD newstyle handshake + transmission to the
	// client side of the socketpair. Returns when the connection drops.
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		if err := nbdserver.Handle(srvConn, []*nbdserver.Export{export}, &nbdserver.Options{
			ReadOnly:           false,
			MinimumBlockSize:   blockSize,
			PreferredBlockSize: blockSize,
			MaximumBlockSize:   1 << 25, // 32 MiB
			SupportsMultiConn:  false,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "weft-block: nbd server.Handle: %v\n", err)
		}
	}()

	// Client goroutine: completes the handshake on cliConn, then performs
	// the NBD_SET_SOCK + NBD_DO_IT ioctl chain on /dev/nbdN. Blocks until
	// disconnected. OnConnected fires once /dev/nbdN is ready.
	readyCh := make(chan error, 1)
	go func() {
		err := nbdclient.Connect(cliConn, devFile, &nbdclient.Options{
			ExportName: exportName,
			BlockSize:  blockSize,
			OnConnected: func() {
				select {
				case readyCh <- nil:
				default:
				}
			},
		})
		close(n.doneCh)
		if err != nil {
			select {
			case readyCh <- err:
			default:
				fmt.Fprintf(os.Stderr, "weft-block: nbd client.Connect post-ready: %v\n", err)
			}
		}
	}()

	if err := <-readyCh; err != nil {
		// Connect failed before becoming ready — tear down.
		_ = devFile.Close()
		_ = srvConn.Close()
		_ = cliConn.Close()
		<-srvDone
		n.devFile, n.cliConn, n.rwu, n.doneCh = nil, nil, nil, nil
		return fmt.Errorf("nbd: client.Connect: %w", err)
	}
	fmt.Fprintf(os.Stderr, "weft-block: nbd %s ready (size=%d, export=%q)\n", devPath, n.size, exportName)
	return nil
}

func (n *Nbd) shutdown() error {
	if n.devFile != nil {
		_ = nbdclient.Disconnect(n.devFile)
	}
	if n.doneCh != nil {
		<-n.doneCh
	}
	if n.cliConn != nil {
		_ = n.cliConn.Close()
	}
	var err error
	if n.devFile != nil {
		err = n.devFile.Close()
	}
	n.devFile, n.cliConn, n.rwu, n.doneCh = nil, nil, nil, nil
	return err
}

func (n *Nbd) expand(size int64) error {
	// go-nbd doesn't expose live resize. The engine handles the volume-side
	// expansion already; remounting NBD with the new size is the user-space
	// path (detach + reattach), which the cluster's reconcile loop can do.
	return fmt.Errorf("nbd: live resize requires detach + reattach (not yet supported)")
}

// openFreeNBDDevice scans /dev/nbd0..15 and returns the first one not already
// claimed by another nbd-client. We probe via NBD_CLEAR_SOCK (single ioctl;
// succeeds on idle devices, EBUSY on attached ones) — keeps go-nbd's Connect
// from racing with another concurrent NBD attach.
//
// O_NONBLOCK is critical here: a kernel NBD device in "attached but not yet
// DO_IT" state will block O_RDWR opens indefinitely. With O_NONBLOCK we get
// EBUSY (or fail fast) and can skip to the next devnode. NBD ioctls aren't
// affected by the non-blocking flag.
func openFreeNBDDevice() (string, *os.File, error) {
	const nbdClearSock = 0xab04
	var lastErr error
	for i := 0; i < 16; i++ {
		path := fmt.Sprintf("/dev/nbd%d", i)
		f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			lastErr = err
			continue
		}
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), uintptr(nbdClearSock), 0)
		if errno == 0 {
			return path, f, nil
		}
		f.Close()
	}
	if lastErr != nil {
		return "", nil, fmt.Errorf("nbd: no free /dev/nbd<N> (last open error: %w; is the 'nbd' kernel module loaded?)", lastErr)
	}
	return "", nil, fmt.Errorf("nbd: all 16 /dev/nbd<N> devices are in use")
}
