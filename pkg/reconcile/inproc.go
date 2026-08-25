//go:build linux

package reconcile

// inproc.go is the real Spawner: it launches pkg/replica.Server + the two
// gRPC services (control plane + data plane) for each ReplicaRequest, and a
// pkg/controller.Controller wrapping the NBD frontend for each
// EngineRequest. Everything runs as goroutines inside this process; no fork.
//
// Linux-only build: pkg/replica + pkg/controller pull in sparse-tools and
// backupstore which depend on Linux-only syscall fields (Stat_t.Ctim,
// unix.NFS_SUPER_MAGIC, syscall.Fallocate). The data plane is Linux-only
// by design; inproc_other.go provides a stub Spawner so the package builds
// on darwin (handy for the dev host).

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/openweft/weft-block/pkg/backend/dynamic"
	"github.com/openweft/weft-block/pkg/backend/remote"
	"github.com/openweft/weft-block/pkg/controller"
	"github.com/openweft/weft-block/pkg/frontend/nbd"
	"github.com/openweft/weft-block/pkg/replica"
	replicarpc "github.com/openweft/weft-block/pkg/replica/rpc"
	"github.com/openweft/weft-block/pkg/types"
)

// Sensible defaults that match upstream Longhorn's `longhorn-engine` flags.
// Surfaced as struct fields would be the next refinement; for now they're
// the same hard-coded values app/cmd/replica.go + controller.go use.
const (
	defaultSnapshotMaxCount   = 250
	defaultEngineReplicaShort = 8 * time.Second
	defaultEngineReplicaLong  = 16 * time.Second
	defaultFileSyncTimeoutSec = 5
	defaultRebuildConcurrency = 4
)

// InProcessSpawner runs replicas + engines as goroutines in the same process
// as the reconcile loop. One per host (one per weft-block plugin instance).
type InProcessSpawner struct {
	// HostAddr is the overlay-reachable address engines use to dial replicas
	// hosted here (e.g. "10.9.0.1"). Combined with allocated TCP ports it
	// becomes the Address stored in each Replica record.
	HostAddr string
	// StateDir is the host-local root under which each replica gets its own
	// directory (<StateDir>/replicas/<replicaUUID>/) holding its sparse
	// chunks + snapshot metadata.
	StateDir string

	mu       sync.Mutex
	replicas map[string]*runningReplica
	engines  map[string]*runningEngine
}

type runningReplica struct {
	server   *replica.Server
	grpc     *grpc.Server
	grpcLis  net.Listener
	data     *replicarpc.DataServer
	dataAddr string // overlay-reachable host:port the engine dials
}

type runningEngine struct {
	controller *controller.Controller
	frontend   *nbd.Nbd
}

// NewInProcessSpawner builds a Spawner ready to receive Spawn* calls.
func NewInProcessSpawner(hostAddr, stateDir string) *InProcessSpawner {
	return &InProcessSpawner{
		HostAddr: hostAddr,
		StateDir: stateDir,
		replicas: map[string]*runningReplica{},
		engines:  map[string]*runningEngine{},
	}
}

// SpawnReplica starts (or no-ops on) a replica.Server backing volumeUUID at
// sizeBytes, exposing a gRPC control plane on an ephemeral port and a TCP
// data server on another. Returns the data-plane address engines dial.
func (s *InProcessSpawner) SpawnReplica(ctx context.Context, req ReplicaRequest) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.replicas[req.ReplicaUUID]; ok {
		return r.dataAddr, nil
	}

	dir := filepath.Join(s.StateDir, "replicas", req.ReplicaUUID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("replica state dir: %w", err)
	}

	rs := replica.NewServer(ctx, dir, nil, req.SectorSize,
		false, false, defaultSnapshotMaxCount, 0, false)

	// Materialise the volume-head + meta on disk so the engine's Create flow
	// finds the replica in state "closed". Without this the engine rejects
	// us with "replica must be closed, cannot add in state: initial".
	// Create is idempotent (no-op when the dir already has the artefacts).
	if err := rs.Create(req.SizeBytes); err != nil {
		return "", fmt.Errorf("replica.Create(size=%d): %w", req.SizeBytes, err)
	}

	// CoW-clone bootstrap : populate the chain from the parent's named
	// snapshot before the engine attaches. Empty SourceSnapshot is the
	// non-clone path (fresh empty volume) and we skip straight to gRPC
	// startup. bootstrapFromSnapshot leaves rs in the same Closed state
	// the empty path does, so the rest of SpawnReplica is path-agnostic.
	if req.SourceSnapshot != "" {
		if err := bootstrapFromSnapshot(ctx, rs, req); err != nil {
			return "", err
		}
	}

	// Upstream Longhorn replicas advertise a base port and reserve TWO more:
	// base = gRPC control, base+1 = dataconn, base+2 = sync — pkg/util.
	// ParseAddresses() derives the trio from the base on the engine side.
	basePort, err := freeTriplet()
	if err != nil {
		return "", fmt.Errorf("alloc replica port triplet: %w", err)
	}
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", basePort))
	if err != nil {
		return "", fmt.Errorf("listen control plane: %w", err)
	}
	// Control-plane gRPC server (Longhorn API: snapshot create / list, …).
	grpcServer := replicarpc.NewReplicaServer(req.VolumeName, req.ReplicaUUID, rs)

	// DataServer binds host:base+1; the engine reaches it by deriving +1 from
	// the base port the Replica record advertises.
	bindAddr := fmt.Sprintf("%s:%d", s.HostAddr, basePort+1)
	// Address stored in the Replica record: tcp:// scheme so dynamic.Factory
	// routes via remote.New(), pointing at the BASE (gRPC) port.
	dataAddr := fmt.Sprintf("tcp://%s:%d", s.HostAddr, basePort)
	ds := replicarpc.NewDataServer(types.DataServerProtocolTCP, bindAddr, rs)

	go func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			// Serve returns nil on GracefulStop; anything else is unexpected.
			fmt.Fprintf(os.Stderr, "weft-block: replica %s grpc.Serve: %v\n", req.ReplicaUUID, err)
		}
	}()
	go func() {
		if err := ds.ListenAndServe(); err != nil {
			fmt.Fprintf(os.Stderr, "weft-block: replica %s data ListenAndServe: %v\n", req.ReplicaUUID, err)
		}
	}()

	s.replicas[req.ReplicaUUID] = &runningReplica{
		server:   rs,
		grpc:     grpcServer,
		grpcLis:  grpcLis,
		data:     ds,
		dataAddr: dataAddr,
	}
	return dataAddr, nil
}

// StopReplica shuts down the gRPC control + data servers for replicaUUID and
// closes the underlying replica.Server. Idempotent.
func (s *InProcessSpawner) StopReplica(_ context.Context, replicaUUID string) error {
	s.mu.Lock()
	r, ok := s.replicas[replicaUUID]
	delete(s.replicas, replicaUUID)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	r.grpc.GracefulStop()
	_ = r.data.Close()
	_ = r.server.Close()
	return nil
}

// SpawnEngine starts (or no-ops on) a controller.Controller for engineUUID
// dialing the given replica addresses, with the NBD frontend brought up.
// Returns the frontend path the hypervisor opens (e.g. "/dev/nbd0").
func (s *InProcessSpawner) SpawnEngine(_ context.Context, req EngineRequest) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.engines[req.EngineUUID]; ok {
		return e.frontend.Endpoint(), nil
	}

	factory := dynamic.New(map[string]types.BackendFactory{
		"tcp": remote.New(),
	})
	fe := nbd.New()

	cc := controller.NewController(req.VolumeName, factory, fe,
		false /* isUpgrade */, false /* disableRevCounter */, false, /* salvageRequested */
		false /* unmapMarkSnapChainRemoved */, 0, /* iscsiTargetRequestTimeout (NBD has none) */
		defaultEngineReplicaShort, defaultEngineReplicaLong,
		types.DataServerProtocolTCP, defaultFileSyncTimeoutSec,
		defaultSnapshotMaxCount, 0, defaultRebuildConcurrency)

	// cc.Start ends with c.startFrontend() (control.go:1039), which calls
	// fe.Init + fe.Startup — meaning Start() on a controller built with a
	// non-nil frontend already brings the NBD device up. Don't call
	// cc.StartFrontend() again: it would NewFrontend(name, ...) → new *Nbd
	// instance, replace c.frontend, and re-attach /dev/nbdN, which the kernel
	// rejects (EOF on the prior socketpair) and which leaves the device in
	// a half-attached state where dd returns EIO immediately.
	if err := cc.Start(req.SizeBytes, req.SizeBytes, req.ReplicaAddresses...); err != nil {
		return "", fmt.Errorf("controller.Start: %w", err)
	}

	s.engines[req.EngineUUID] = &runningEngine{controller: cc, frontend: fe}
	return fe.Endpoint(), nil
}

// LocalEngine returns a per-engine handle if engineUUID is running locally.
// The handle wraps the in-process controller + replica addresses so callers
// (volumeDriver snapshot/backup) can dispatch without re-discovering the
// engine's components. Returns (nil, false) when the engine isn't local.
func (s *InProcessSpawner) LocalEngine(engineUUID string) (EngineHandle, bool) {
	s.mu.Lock()
	e, ok := s.engines[engineUUID]
	s.mu.Unlock()
	if !ok {
		return nil, false
	}
	return &inProcEngineHandle{eng: e, volumeName: lookupVolumeName(e)}, true
}

// lookupVolumeName extracts the volume name from a runningEngine. The
// pkg/controller.Controller carries it as an unexported field so we use the
// public accessor — defensive against future refactors.
func lookupVolumeName(e *runningEngine) string {
	if e == nil || e.controller == nil {
		return ""
	}
	return e.controller.VolumeName
}

// StopEngine tears down the engine for engineUUID: NBD frontend first
// (detach /dev/nbdN), then the controller. Idempotent.
func (s *InProcessSpawner) StopEngine(_ context.Context, engineUUID string) error {
	s.mu.Lock()
	e, ok := s.engines[engineUUID]
	delete(s.engines, engineUUID)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	_ = e.controller.ShutdownFrontend()
	_ = e.controller.Shutdown()
	_ = e.controller.Close()
	return nil
}

// (freeTCPPort lives in port.go — cross-platform, used by both this Linux
// implementation and the test suite.)
