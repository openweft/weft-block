// Package reconcile is the per-host loop that turns desired-state records
// (Replica / Engine in the control store) into running processes on the host.
// It runs inside the weft-block plugin process: one loop per host, polling the
// store and dispatching to a Spawner. Splitting the dispatch behind an
// interface keeps the store-driven control flow tested in isolation from the
// (Linux-only, port-allocating, gRPC-spawning) backend.
package reconcile

import "context"

// Spawner brings up / tears down the per-volume processes a host owns:
// pkg/replica.Server instances for each Replica placed on this host, and a
// pkg/controller.Controller (with the NBD frontend) for each Engine targeting
// this host. Implementations must be idempotent — the loop will call them
// repeatedly as it re-observes the same desired state.
type Spawner interface {
	// SpawnReplica starts (or no-ops on) a replica server for replicaUUID
	// backing volumeUUID at sizeBytes. Returns the overlay-reachable address
	// (host:port) the engine will dial — written back to the Replica record
	// as Address + State=Running.
	SpawnReplica(ctx context.Context, req ReplicaRequest) (address string, err error)

	// StopReplica tears down the replica server for replicaUUID. No-op if it
	// was never running locally.
	StopReplica(ctx context.Context, replicaUUID string) error

	// SpawnEngine starts (or no-ops on) a controller for engineUUID backing
	// volumeUUID, dialing the given replica addresses, with the NBD frontend
	// brought up. Returns the host-local frontend path (e.g. "/dev/nbd0"),
	// written back to the Engine record as FrontendPath + State=Running.
	SpawnEngine(ctx context.Context, req EngineRequest) (frontendPath string, err error)

	// StopEngine detaches NBD and shuts down the controller for engineUUID.
	StopEngine(ctx context.Context, engineUUID string) error

	// LocalEngine returns a handle on the live engine for engineUUID if (and
	// only if) it's running locally on this host. The handle exposes the
	// snapshot / backup operations defined in the EngineHandle interface.
	// Returns (nil, false) when the engine isn't local — callers should then
	// dispatch the operation to the engine's host through the control plane.
	LocalEngine(engineUUID string) (EngineHandle, bool)
}

// EngineHandle is the per-engine surface exposed by the local Spawner for
// callers that need to invoke snapshot / backup operations on a running
// engine. Kept minimal so the Spawner abstraction stays cheap to fake.
type EngineHandle interface {
	// CreateSnapshot freezes the volume's current state under name (empty
	// asks the engine to generate one). Labels propagate to the on-disk
	// snapshot metadata. Returns the final snapshot name.
	CreateSnapshot(name string, labels map[string]string, freezeFS bool) (string, error)
	// ListSnapshots returns the snapshot chain for the engine's volume,
	// pulling the disk catalog from one of its RW replicas.
	ListSnapshots() ([]SnapshotInfo, error)
	// DeleteSnapshot marks a snapshot for removal across the engine's
	// replicas and triggers a background purge that physically merges the
	// snapshot into its parent / child.
	DeleteSnapshot(name string) error
	// RevertSnapshot rolls the volume back to a previous snapshot. The
	// volume MUST be detached first — the engine fails fast otherwise.
	RevertSnapshot(name string) error

	// ReplicaAddresses returns the engine's current replica addresses. Used
	// by backup orchestration that needs to pick one replica to ship data
	// from.
	ReplicaAddresses() []string

	// VolumeSize returns the volume's logical size in bytes. Used by backup
	// orchestration to bound the streaming loop.
	VolumeSize() int64

	// StreamSnapshotBytes reads the snapshot's contents block-by-block
	// from the controller's data plane (which sees the engine head + chain
	// as one logical volume). Important caveat : it reads the LIVE state
	// observable via the controller, not a frozen point-in-time view of
	// just the snapshot's delta. Practically that's the same when the
	// caller follows the snap-then-backup-immediately pattern with no
	// intervening writes ; for the strict "snapshot frozen at T" semantics
	// we'd need a replica-direct read path (TODO : wire pkg/replica's
	// BackupStatus.OpenSnapshot/ReadSnapshot via a new replica RPC).
	//
	// Writes happen as full passes of blockSize bytes ; the writer sees
	// only the portion ReadAt actually filled (short reads are propagated).
	// Returns the total bytes streamed.
	StreamSnapshotBytes(snapshotName string, blockSize int64, dst SnapshotByteWriter) (int64, error)

	// ImportBytes is the inverse of StreamSnapshotBytes : write `total`
	// bytes from `src` into the engine's volume head starting at offset 0.
	// Used by RestoreBackup to populate a freshly-created volume from a
	// backup's .bin object. The volume must be sized to match (the engine
	// rejects writes past its declared size). Returns the bytes written ;
	// short reads from src truncate the import and surface as an error.
	ImportBytes(blockSize int64, total int64, src SnapshotByteReader) (int64, error)

	// CompareSnapshots returns the (offset, size) byte windows where
	// snapshotName differs from parentSnapshotName. Used by the
	// incremental backup path : the driver ships only those windows
	// instead of the whole volume. Empty result = identical snapshots.
	// Returns ErrCompareUnimplemented when the underlying server hasn't
	// wired its compare path (the driver falls back to full backup).
	CompareSnapshots(snapshotName, parentSnapshotName string, blockSize int64) ([]Range, error)

	// StreamSnapshotRanges is the sparse twin of StreamSnapshotBytes :
	// only the listed ranges are read from the snapshot and written to
	// dst, in the order they appear in `ranges`. Used by incremental
	// backup after CompareSnapshots reports the diff. Empty ranges =
	// equivalent to StreamSnapshotBytes (full stream).
	StreamSnapshotRanges(snapshotName string, blockSize int64, ranges []Range, dst SnapshotByteWriter) (int64, error)

	// ImportBytesAtRanges is the inverse of StreamSnapshotRanges : read
	// the concatenated plaintext bytes from src and write each range's
	// worth at the matching volume offset. Used by RestoreBackup when
	// the backup is incremental — the parent backup has already been
	// applied at offset 0 ; this call only fills in the delta windows.
	// Returns the total plaintext bytes written.
	ImportBytesAtRanges(blockSize int64, ranges []Range, src SnapshotByteReader) (int64, error)
}

// Range is one contiguous (offset, size) byte window — used by the
// incremental backup path on both the streaming side (which bytes to
// emit) and the restore side (where to apply each chunk).
type Range struct {
	Offset int64
	Size   int64
}

// ErrCompareUnimplemented is the sentinel CompareSnapshots returns when
// the replica server hasn't wired its Compare path yet (the gRPC server
// surfaces grpc.codes.Unimplemented). The driver tests `errors.Is(err,
// ErrCompareUnimplemented)` and falls back to a full backup, logging
// the reason — preferable to silently shipping a corrupt incremental.
var ErrCompareUnimplemented = errCompareUnimplemented{}

type errCompareUnimplemented struct{}

func (errCompareUnimplemented) Error() string {
	return "weft-block: replica Compare RPC is Unimplemented ; driver falls back to full backup"
}

// SnapshotByteReader is what EngineHandle.ImportBytes reads from. Mirrors
// SnapshotByteWriter for symmetry.
type SnapshotByteReader interface {
	Read(p []byte) (int, error)
}

// SnapshotByteWriter is what EngineHandle.StreamSnapshotBytes writes into.
// Kept tiny (just Write) to avoid forcing callers through io.Writer's full
// interface when only one method is meaningful.
type SnapshotByteWriter interface {
	Write(p []byte) (int, error)
}

// SnapshotInfo is the protocol-shape descriptor for a single snapshot in
// the engine's chain. Mirrors what the on-disk replica catalogue stores so
// callers don't take a dep on pkg/types' DiskInfo.
type SnapshotInfo struct {
	Name        string
	Parent      string
	Removed     bool
	UserCreated bool
	Created     string // RFC3339 timestamp string as stored on disk
	SizeBytes   int64  // delta on disk
	Labels      map[string]string
}

// ReplicaRequest is the input to SpawnReplica.
//
// SourceVolumeUUID + SourceSnapshot + SourceVolumeName + SourceReplicaAddresses,
// when populated, instruct the spawner to bootstrap the freshly-created replica
// chain from a named snapshot of an existing parent volume — the data-plane
// half of the control-plane CreateFromSnapshot (clone) primitive in
// control/clone.go. The spawner dials one of the parent's healthy replicas via
// the existing weftsnap.SnapshotReader gRPC + writes the streamed bytes into
// the new replica's chain before returning. An empty SourceSnapshot is the
// V0.5-and-earlier "fresh empty volume" path : no copy, the replica comes up
// empty and the engine populates the head on its own.
//
// The fields are carried per-request (rather than passed via a side channel)
// so the Spawner abstraction stays self-contained — LogSpawner and the
// non-Linux stub can no-op on them without growing extra surface.
type ReplicaRequest struct {
	ReplicaUUID string
	VolumeUUID  string
	VolumeName  string
	SizeBytes   int64
	SectorSize  int64
	StateDir    string // <hostStateDir>/replicas/<replicaUUID>

	// Bootstrap-from-snapshot fields. Non-empty SourceSnapshot triggers the
	// CoW-clone bootstrap path on the SpawnReplica side.
	SourceVolumeUUID       string
	SourceSnapshot         string
	SourceVolumeName       string
	SourceReplicaAddresses []string
}

// EngineRequest is the input to SpawnEngine.
type EngineRequest struct {
	EngineUUID       string
	VolumeUUID       string
	VolumeName       string
	SizeBytes        int64
	SectorSize       int64
	ReplicaAddresses []string // engine dials these as the data backends
}
