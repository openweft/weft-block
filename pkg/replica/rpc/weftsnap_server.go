package rpc

// weftsnap_server.go implements weft-block's streaming "read this snapshot's
// frozen bytes" RPC. Co-registered with the upstream ReplicaService on the
// same gRPC port (see server.go::NewReplicaServer) so existing replica
// clients reach both surfaces without an extra listener.
//
// The server wraps pkg/replica.BackupStatus's OpenSnapshot/ReadSnapshot/
// CloseSnapshot lifecycle — same primitives the upstream sync-agent uses,
// just routed through our own bytes-only RPC rather than the Longhorn-
// blessed full-backup-to-backupstore flow.
//
// Concurrency model : one BackupStatus instance per RPC call, scoped to the
// stream's lifetime. The replica supports concurrent opens of distinct
// snapshots ; two concurrent Read calls on the SAME snapshot are also
// safe because each call gets its own NewReadOnly handle.

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openweft/weft-block/pkg/replica"
	"github.com/openweft/weft-block/pkg/replica/rpc/weftsnap"
)

// defaultChunkSize matches what the driver-side requests by default.
// Server-emitted chunks of this size balance grpc framing overhead against
// the buffer footprint per concurrent stream.
const defaultChunkSize = 4 * 1024 * 1024 // 4 MiB

// snapshotReaderServer is the gRPC handler. Backed by the same
// *replica.Server the ReplicaServer uses, but only ever calls its
// read-only entry points (NewReadOnly + ReadAt + Close).
type snapshotReaderServer struct {
	weftsnap.UnimplementedSnapshotReaderServer
	volumeName string
	s          *replica.Server
}

func newSnapshotReaderServer(volumeName string, s *replica.Server) *snapshotReaderServer {
	return &snapshotReaderServer{volumeName: volumeName, s: s}
}

// Read streams the snapshot's bytes from offset 0 in fixed-size chunks.
// Implementation contract :
//   * If req.SnapshotName isn't in the chain, return NotFound-shaped error.
//   * Stream chunks ; last chunk carries Last=true even if it's a full
//     chunk's worth, so clients can rely on the flag.
//   * Stream-end (natural OR ctx cancel) closes the underlying replica
//     handle to release its FDs.
func (sr *snapshotReaderServer) Read(req *weftsnap.ReadRequest, stream weftsnap.SnapshotReader_ReadServer) error {
	if req.SnapshotName == "" {
		return fmt.Errorf("snapshot_name is required")
	}
	if req.VolumeName == "" {
		// Default to the server's own volume name when the client
		// didn't bother to spell it out — saves a redundant arg in
		// the common case.
		req.VolumeName = sr.volumeName
	}
	if req.VolumeName != sr.volumeName {
		return fmt.Errorf("volume mismatch: this replica serves %q, got request for %q", sr.volumeName, req.VolumeName)
	}
	chunkSize := req.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	ctx := stream.Context()
	// Open a read-only handle on the snapshot. NewReadOnly takes the
	// replica's state dir + the disk name + an optional backing-file —
	// we don't carry backing files here (Longhorn-style backing image
	// chains aren't a weft-block concept yet), so nil.
	r := sr.s.Replica()
	if r == nil {
		return fmt.Errorf("replica server has no open replica")
	}
	dir := sr.s.Dir()
	if dir == "" {
		return fmt.Errorf("replica server has no state dir set")
	}
	snapDisk := snapshotDiskName(req.SnapshotName)
	ro, err := replica.NewReadOnly(ctx, dir, snapDisk, nil /* no backing file */)
	if err != nil {
		return fmt.Errorf("open snapshot %s in %s: %w", snapDisk, dir, err)
	}
	defer func() { _ = ro.Close() }()
	// Build the effective range list. Empty req.Ranges → one range
	// covering the full snapshot ; non-empty → use them verbatim (we
	// trust the client to have dedup'd / merged). Each range is streamed
	// in chunkSize hops ; the final chunk of the last range carries
	// Last=true.
	size := ro.Info().Size
	var ranges []rangePair
	if len(req.Ranges) == 0 {
		ranges = []rangePair{{0, size}}
	} else {
		for _, r := range req.Ranges {
			if r.Offset < 0 || r.Size <= 0 || r.Offset+r.Size > size {
				return fmt.Errorf("invalid range offset=%d size=%d (snapshot size=%d)", r.Offset, r.Size, size)
			}
			ranges = append(ranges, rangePair{r.Offset, r.Size})
		}
	}
	buf := make([]byte, chunkSize)
	for ri, rg := range ranges {
		end := rg.offset + rg.size
		for off := rg.offset; off < end; {
			if err := ctx.Err(); err != nil {
				return err
			}
			remaining := end - off
			b := buf
			if remaining < chunkSize {
				b = buf[:remaining]
			}
			n, err := ro.ReadAt(b, off)
			if err != nil && err != io.EOF {
				return fmt.Errorf("read snapshot at offset %d: %w", off, err)
			}
			if n == 0 {
				break
			}
			isLast := ri == len(ranges)-1 && off+int64(n) >= end
			if err := stream.Send(&weftsnap.ReadChunk{
				Offset: off,
				Data:   b[:n],
				Last:   isLast,
			}); err != nil {
				return fmt.Errorf("send chunk at offset %d: %w", off, err)
			}
			off += int64(n)
		}
	}
	return nil
}

// rangePair is the in-memory shape we use after validating req.Ranges
// (or the synthesised single full-range when req.Ranges is empty).
type rangePair struct {
	offset, size int64
}

// defaultCompareBlockSize is the block-size granularity the server
// falls back to when the client doesn't specify one. 4 MiB matches
// defaultChunkSize so a Compare-then-Read cycle ships one chunk per
// reported range, no fragmentation on either side.
const defaultCompareBlockSize = 4 * 1024 * 1024 // 4 MiB

// Compare returns the byte windows where snapshot_name differs from
// parent_snapshot_name. Backed by Replica.CompareSnapshot, which walks
// the sector→file location table — O(volume sectors), no per-block
// SHA-256 sweep, no BackupStatus lifecycle.
//
// Resolution :
//   - Open the newer snapshot via NewReadOnly (same path as Read).
//   - Call Replica.CompareSnapshot(snapshot, parent, blockSize).
//   - Translate []SnapshotRange to the wire Range slice.
//
// Empty parent_snapshot_name means "full footprint relative to the
// empty volume" — the caller gets every allocated block instead of
// the diff, which is the legitimate "first incremental in a chain"
// case. Both snapshots must exist in the replica's chain ; missing
// snapshots surface NotFound, not Unimplemented.
func (sr *snapshotReaderServer) Compare(ctx context.Context, req *weftsnap.CompareRequest) (*weftsnap.CompareResponse, error) {
	if req.SnapshotName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "snapshot_name is required")
	}
	if req.VolumeName == "" {
		req.VolumeName = sr.volumeName
	}
	if req.VolumeName != sr.volumeName {
		return nil, status.Errorf(codes.InvalidArgument, "volume mismatch: this replica serves %q, got request for %q", sr.volumeName, req.VolumeName)
	}
	blockSize := req.BlockSize
	if blockSize <= 0 {
		blockSize = defaultCompareBlockSize
	}
	r := sr.s.Replica()
	if r == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "replica server has no open replica")
	}
	dir := sr.s.Dir()
	if dir == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "replica server has no state dir set")
	}
	snapDisk := snapshotDiskName(req.SnapshotName)
	parentDisk := ""
	if req.ParentSnapshotName != "" {
		parentDisk = snapshotDiskName(req.ParentSnapshotName)
	}
	ro, err := replica.NewReadOnly(ctx, dir, snapDisk, nil /* no backing file */)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "open snapshot %s in %s: %v", snapDisk, dir, err)
	}
	defer func() { _ = ro.Close() }()
	ranges, err := ro.CompareSnapshot(snapDisk, parentDisk, blockSize)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "compare snapshots %s vs %s: %v", snapDisk, parentDisk, err)
	}
	out := make([]*weftsnap.Range, 0, len(ranges))
	for _, rg := range ranges {
		out = append(out, &weftsnap.Range{Offset: rg.Offset, Size: rg.Size})
	}
	return &weftsnap.CompareResponse{BlockSize: blockSize, Ranges: out}, nil
}

// snapshotDiskName is the on-disk filename pkg/replica gives a snapshot.
// Mirrors diskutil.GenerateSnapshotDiskName ("volume-snap-<name>.img").
// We reproduce the formatter here rather than importing diskutil because
// it sits next to a pile of sparse-tools Linux-only headers that we don't
// want to drag into this rpc package's import graph.
func snapshotDiskName(snapName string) string {
	const (
		snapshotPrefix = "volume-snap-"
		snapshotSuffix = ".img"
	)
	return snapshotPrefix + snapName + snapshotSuffix
}
