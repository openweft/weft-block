//go:build !linux

package reconcile

// inproc_other.go is the non-Linux stub of the in-process spawner. The real
// implementation in inproc.go (build-tagged linux) imports pkg/replica and
// pkg/controller which depend on Linux-only syscalls (sparse-tools' Stat_t.
// Ctim, backupstore's NFS_SUPER_MAGIC, syscall.Fallocate). On darwin / other
// dev hosts this stub keeps pkg/reconcile compiling and offers an
// InProcessSpawner that fails cleanly — callers should use LogSpawner there.

import (
	"context"
	"errors"
)

// errLinuxOnly is returned by every Spawner method when the data plane is
// invoked off-Linux.
var errLinuxOnly = errors.New("weft-block: in-process spawner is Linux-only (sparse-tools/backupstore deps); use LogSpawner on non-Linux dev hosts")

// InProcessSpawner is the no-op type satisfying Spawner on non-Linux. The
// Linux build (inproc.go) defines the real one with replicas/engines maps and
// process plumbing. Same exported name → cmd/weft-block compiles unchanged.
type InProcessSpawner struct {
	HostAddr string
	StateDir string
}

// NewInProcessSpawner builds a stub on non-Linux. Use NewLogSpawner instead
// for offline development.
func NewInProcessSpawner(hostAddr, stateDir string) *InProcessSpawner {
	return &InProcessSpawner{HostAddr: hostAddr, StateDir: stateDir}
}

func (*InProcessSpawner) SpawnReplica(context.Context, ReplicaRequest) (string, error) {
	return "", errLinuxOnly
}
func (*InProcessSpawner) StopReplica(context.Context, string) error { return errLinuxOnly }
func (*InProcessSpawner) SpawnEngine(context.Context, EngineRequest) (string, error) {
	return "", errLinuxOnly
}
func (*InProcessSpawner) StopEngine(context.Context, string) error { return errLinuxOnly }
func (*InProcessSpawner) LocalEngine(string) (EngineHandle, bool)  { return nil, false }

// noopEngineHandle satisfies EngineHandle off-Linux for tests that
// inadvertently grab it. Every method returns an error/empty.
type noopEngineHandle struct{}

func (noopEngineHandle) CreateSnapshot(string, map[string]string, bool) (string, error) {
	return "", errLinuxOnly
}
func (noopEngineHandle) ListSnapshots() ([]SnapshotInfo, error) { return nil, errLinuxOnly }
func (noopEngineHandle) DeleteSnapshot(string) error            { return errLinuxOnly }
func (noopEngineHandle) RevertSnapshot(string) error            { return errLinuxOnly }
func (noopEngineHandle) ReplicaAddresses() []string             { return nil }
func (noopEngineHandle) VolumeSize() int64                      { return 0 }
func (noopEngineHandle) StreamSnapshotBytes(string, int64, SnapshotByteWriter) (int64, error) {
	return 0, errLinuxOnly
}
func (noopEngineHandle) ImportBytes(int64, int64, SnapshotByteReader) (int64, error) {
	return 0, errLinuxOnly
}
func (noopEngineHandle) CompareSnapshots(string, string, int64) ([]Range, error) {
	return nil, errLinuxOnly
}
func (noopEngineHandle) StreamSnapshotRanges(string, int64, []Range, SnapshotByteWriter) (int64, error) {
	return 0, errLinuxOnly
}
func (noopEngineHandle) ImportBytesAtRanges(int64, []Range, SnapshotByteReader) (int64, error) {
	return 0, errLinuxOnly
}
