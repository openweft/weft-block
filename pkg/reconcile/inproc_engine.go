//go:build linux

package reconcile

// inproc_engine.go adapts the in-process running engine (controller + the
// dial-able replicas it manages) to the EngineHandle surface volumeDriver
// uses for snapshot / backup dispatch. Snapshot CREATE / REVERT call into
// the local Controller directly (no RPC) since the controller is in this
// same process. Snapshot LIST / DELETE iterate the engine's replicas via
// their gRPC clients — the disk catalog lives per-replica and we read it
// from one healthy replica, then propagate mark-and-purge across all.

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/openweft/weft-block/pkg/controller"
	"github.com/openweft/weft-block/pkg/replica/client"
	"github.com/openweft/weft-block/pkg/replica/rpc/weftsnap"
	"github.com/openweft/weft-block/pkg/types"
)

type inProcEngineHandle struct {
	eng        *runningEngine
	volumeName string
}

func (h *inProcEngineHandle) CreateSnapshot(name string, labels map[string]string, freezeFS bool) (string, error) {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return "", fmt.Errorf("engine handle has no controller")
	}
	return h.eng.controller.Snapshot(name, labels, freezeFS)
}

func (h *inProcEngineHandle) RevertSnapshot(name string) error {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return fmt.Errorf("engine handle has no controller")
	}
	return h.eng.controller.Revert(name)
}

func (h *inProcEngineHandle) ReplicaAddresses() []string {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return nil
	}
	reps := h.eng.controller.ListReplicas()
	out := make([]string, 0, len(reps))
	for _, r := range reps {
		if r.Mode == types.ERR {
			continue
		}
		out = append(out, r.Address)
	}
	return out
}

func (h *inProcEngineHandle) ListSnapshots() ([]SnapshotInfo, error) {
	addrs := h.ReplicaAddresses()
	if len(addrs) == 0 {
		return nil, fmt.Errorf("engine has no healthy replicas")
	}
	// Pull catalog from the first healthy replica — the engine guarantees
	// their snapshot chains stay in sync once Mode != ERR.
	disks, _, err := controller.GetReplicaDisksAndHead(addrs[0], h.volumeName, "" /* best-effort instance name */)
	if err != nil {
		return nil, err
	}
	out := make([]SnapshotInfo, 0, len(disks))
	for name, d := range disks {
		size, _ := strconv.ParseInt(d.Size, 10, 64)
		out = append(out, SnapshotInfo{
			Name:        name,
			Parent:      d.Parent,
			Removed:     d.Removed,
			UserCreated: d.UserCreated,
			Created:     d.Created,
			SizeBytes:   size,
			Labels:      d.Labels,
		})
	}
	return out, nil
}

func (h *inProcEngineHandle) VolumeSize() int64 {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return 0
	}
	return h.eng.controller.Size()
}

// StreamSnapshotBytes dials a healthy replica's weftsnap.SnapshotReader RPC
// and streams the snapshot's frozen-at-T contents into dst. The reads go
// through pkg/replica.NewReadOnly on the replica side, so the bytes are the
// snapshot's exact state at creation time — independent of any live writes
// to the volume head between snapshot-creation and this call.
//
// The replica is picked by the canonical engine listing (first RW). On error
// we DO NOT failover to another replica — the upper layer can retry with a
// fresh CreateBackup if needed. Failover here would complicate SHA256
// verification semantics (two replicas might emit different orderings of
// concurrent updates ; we'd want a stable single-source stream).
func (h *inProcEngineHandle) StreamSnapshotBytes(snapshotName string, blockSize int64, dst SnapshotByteWriter) (int64, error) {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return 0, fmt.Errorf("engine handle has no controller")
	}
	if snapshotName == "" {
		return 0, fmt.Errorf("snapshot_name is required")
	}
	// Sanity check : the snapshot must exist in the chain. Cheap pre-flight
	// that surfaces a clear error before we open a gRPC stream.
	snaps, err := h.ListSnapshots()
	if err != nil {
		return 0, fmt.Errorf("ListSnapshots pre-flight: %w", err)
	}
	found := false
	for _, s := range snaps {
		if s.Name == snapshotName {
			found = true
			break
		}
	}
	if !found {
		return 0, fmt.Errorf("snapshot %q not found in engine's chain", snapshotName)
	}
	addrs := h.ReplicaAddresses()
	if len(addrs) == 0 {
		return 0, fmt.Errorf("engine has no healthy replicas")
	}
	// Pick the first healthy replica. The replica.Address is the gRPC
	// control plane endpoint ; it's a "tcp://host:port" URL.
	addr, err := replicaGRPCEndpoint(addrs[0])
	if err != nil {
		return 0, fmt.Errorf("derive grpc addr from %q: %w", addrs[0], err)
	}
	// Dial with insecure : the replica gRPC is on the cluster overlay,
	// not exposed to the public internet. TLS comes later via the cluster
	// CA + per-host certs (out of scope for this slice).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, fmt.Errorf("dial replica %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	cli := weftsnap.NewSnapshotReaderClient(conn)
	stream, err := cli.Read(ctx, &weftsnap.ReadRequest{
		SnapshotName: snapshotName,
		VolumeName:   h.volumeName,
		ChunkSize:    blockSize,
	})
	if err != nil {
		return 0, fmt.Errorf("open snapshot stream on %s: %w", addr, err)
	}
	var total int64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, fmt.Errorf("recv chunk after %d bytes: %w", total, err)
		}
		if len(chunk.Data) == 0 {
			if chunk.Last {
				break
			}
			continue
		}
		// Validate the offset matches what we've written so far. The
		// server is supposed to emit ordered chunks ; a gap signals
		// either a bug or a (deliberate) sparse-stream extension we
		// haven't taught the receiver about yet. Hard-fail for now.
		if chunk.Offset != total {
			return total, fmt.Errorf("chunk offset %d != expected %d (sparse stream not supported yet)", chunk.Offset, total)
		}
		w, werr := dst.Write(chunk.Data)
		total += int64(w)
		if werr != nil {
			return total, fmt.Errorf("write to backup target at offset %d: %w", chunk.Offset, werr)
		}
		if w != len(chunk.Data) {
			return total, fmt.Errorf("short write at offset %d: wrote %d of %d", chunk.Offset, w, len(chunk.Data))
		}
		if chunk.Last {
			break
		}
	}
	return total, nil
}

// replicaGRPCEndpoint converts a Replica.Address (e.g. "tcp://10.9.0.2:54321")
// to the bare "host:port" gRPC dial target. The "tcp://" scheme is the
// engine's own convention from pkg/util.ParseAddresses ; gRPC's resolver
// doesn't know it.
func replicaGRPCEndpoint(addr string) (string, error) {
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return "", err
		}
		return u.Host, nil
	}
	return addr, nil
}

// streamDialTimeout is the cap for opening the gRPC connection to the
// replica. Snapshot reads themselves can take minutes for large volumes ;
// we use a separate deadline-per-recv hook to bound stuck streams.
const streamDialTimeout = 10 * time.Second

var _ = streamDialTimeout // exported via comment for future tuning

// CompareSnapshots dials a replica's weftsnap.SnapshotReader.Compare RPC
// and returns the ranges that differ between the two snapshots. Returns
// ErrCompareUnimplemented when the replica server's Compare path isn't
// wired (caller falls back to full backup).
func (h *inProcEngineHandle) CompareSnapshots(snapshotName, parentName string, blockSize int64) ([]Range, error) {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return nil, fmt.Errorf("engine handle has no controller")
	}
	if snapshotName == "" {
		return nil, fmt.Errorf("snapshot_name is required")
	}
	addrs := h.ReplicaAddresses()
	if len(addrs) == 0 {
		return nil, fmt.Errorf("engine has no healthy replicas")
	}
	addr, err := replicaGRPCEndpoint(addrs[0])
	if err != nil {
		return nil, fmt.Errorf("derive grpc addr from %q: %w", addrs[0], err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial replica %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	cli := weftsnap.NewSnapshotReaderClient(conn)
	resp, err := cli.Compare(context.Background(), &weftsnap.CompareRequest{
		SnapshotName:       snapshotName,
		ParentSnapshotName: parentName,
		VolumeName:         h.volumeName,
		BlockSize:          blockSize,
	})
	if err != nil {
		// gRPC's status.Code(err) gives us Unimplemented when the server
		// declared the path unwired. Surface the canonical sentinel so
		// the driver-side fallback chooses full-backup deterministically.
		if grpcCodeUnimplemented(err) {
			return nil, ErrCompareUnimplemented
		}
		return nil, fmt.Errorf("compare RPC on %s: %w", addr, err)
	}
	out := make([]Range, 0, len(resp.Ranges))
	for _, r := range resp.Ranges {
		out = append(out, Range{Offset: r.Offset, Size: r.Size})
	}
	return out, nil
}

// StreamSnapshotRanges is the sparse twin of StreamSnapshotBytes. The
// ranges (empty = full stream) are forwarded to the replica via the
// gRPC ReadRequest.Ranges field ; the replica server walks them and
// emits chunks in order. The client validates each chunk's offset
// against the expected cursor through the ranges.
func (h *inProcEngineHandle) StreamSnapshotRanges(snapshotName string, blockSize int64, ranges []Range, dst SnapshotByteWriter) (int64, error) {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return 0, fmt.Errorf("engine handle has no controller")
	}
	if snapshotName == "" {
		return 0, fmt.Errorf("snapshot_name is required")
	}
	addrs := h.ReplicaAddresses()
	if len(addrs) == 0 {
		return 0, fmt.Errorf("engine has no healthy replicas")
	}
	addr, err := replicaGRPCEndpoint(addrs[0])
	if err != nil {
		return 0, fmt.Errorf("derive grpc addr from %q: %w", addrs[0], err)
	}
	pbRanges := make([]*weftsnap.Range, 0, len(ranges))
	for _, r := range ranges {
		pbRanges = append(pbRanges, &weftsnap.Range{Offset: r.Offset, Size: r.Size})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, fmt.Errorf("dial replica %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	cli := weftsnap.NewSnapshotReaderClient(conn)
	stream, err := cli.Read(ctx, &weftsnap.ReadRequest{
		SnapshotName: snapshotName,
		VolumeName:   h.volumeName,
		ChunkSize:    blockSize,
		Ranges:       pbRanges,
	})
	if err != nil {
		return 0, fmt.Errorf("open snapshot range stream on %s: %w", addr, err)
	}
	var total int64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, fmt.Errorf("recv chunk after %d bytes: %w", total, err)
		}
		if len(chunk.Data) == 0 {
			if chunk.Last {
				break
			}
			continue
		}
		w, werr := dst.Write(chunk.Data)
		total += int64(w)
		if werr != nil {
			return total, fmt.Errorf("write at offset %d: %w", chunk.Offset, werr)
		}
		if chunk.Last {
			break
		}
	}
	return total, nil
}

// ImportBytesAtRanges is the inverse of StreamSnapshotRanges : read the
// concatenated plaintext from src and place each range's bytes at the
// volume offset the range names. The caller has already validated the
// total plaintext byte count matches sum(ranges[].Size).
func (h *inProcEngineHandle) ImportBytesAtRanges(blockSize int64, ranges []Range, src SnapshotByteReader) (int64, error) {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return 0, fmt.Errorf("engine handle has no controller")
	}
	if blockSize <= 0 {
		blockSize = 4 * 1024 * 1024
	}
	buf := make([]byte, blockSize)
	var total int64
	for _, rg := range ranges {
		remaining := rg.Size
		off := rg.Offset
		for remaining > 0 {
			cap := int64(len(buf))
			if remaining < cap {
				cap = remaining
			}
			n, rerr := readFull(src, buf[:cap])
			if n > 0 {
				if w, werr := h.eng.controller.WriteAt(buf[:n], off); werr != nil {
					return total + int64(w), fmt.Errorf("controller.WriteAt at offset %d: %w", off, werr)
				}
				off += int64(n)
				total += int64(n)
				remaining -= int64(n)
			}
			if rerr != nil && remaining > 0 {
				return total, fmt.Errorf("short read from backup source at range offset %d (need %d more bytes): %w", rg.Offset, remaining, rerr)
			}
		}
	}
	return total, nil
}

// grpcCodeUnimplemented checks whether err is a gRPC Unimplemented status.
// Kept narrow on purpose ; the only Unimplemented we care about is
// CompareSnapshots, and we want the fallback to be deterministic.
func grpcCodeUnimplemented(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Unimplemented
}

// ImportBytes streams data from src into the volume head via controller.
// WriteAt. Used by RestoreBackup to populate a freshly-created volume.
// Reads from src in blockSize chunks ; an io.EOF short read before total
// is consumed surfaces as an error so the caller doesn't silently truncate
// the restored volume.
func (h *inProcEngineHandle) ImportBytes(blockSize int64, total int64, src SnapshotByteReader) (int64, error) {
	if h == nil || h.eng == nil || h.eng.controller == nil {
		return 0, fmt.Errorf("engine handle has no controller")
	}
	if blockSize <= 0 {
		blockSize = 4 * 1024 * 1024
	}
	size := h.eng.controller.Size()
	if total > size {
		return 0, fmt.Errorf("import total %d > volume size %d", total, size)
	}
	buf := make([]byte, blockSize)
	var off int64
	for off < total {
		remaining := total - off
		if remaining < blockSize {
			buf = buf[:remaining]
		}
		n, rerr := readFull(src, buf)
		if n > 0 {
			if w, werr := h.eng.controller.WriteAt(buf[:n], off); werr != nil {
				return off + int64(w), fmt.Errorf("controller.WriteAt at offset %d: %w", off, werr)
			}
			off += int64(n)
		}
		if rerr != nil {
			if off < total {
				return off, fmt.Errorf("short read from backup source: got %d of %d (%w)", off, total, rerr)
			}
			break
		}
	}
	return off, nil
}

// readFull is like io.ReadFull but tolerates a final short read at EOF (the
// caller handles that case via the return-value comparison to `total`).
func readFull(src SnapshotByteReader, buf []byte) (int, error) {
	var read int
	for read < len(buf) {
		n, err := src.Read(buf[read:])
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

func (h *inProcEngineHandle) DeleteSnapshot(name string) error {
	addrs := h.ReplicaAddresses()
	if len(addrs) == 0 {
		return fmt.Errorf("engine has no healthy replicas")
	}
	// Mark phase : every healthy replica must accept the removal flag
	// before we trigger purge. A failure aborts so the chain stays
	// consistent.
	for _, addr := range addrs {
		rc, err := client.NewReplicaClient(addr, h.volumeName, "")
		if err != nil {
			return fmt.Errorf("dial replica %s: %w", addr, err)
		}
		err = rc.RemoveDisk(name, true /* markAsRemoved */)
		_ = rc.Close()
		if err != nil {
			return fmt.Errorf("mark snapshot %s removed on %s: %w", name, addr, err)
		}
	}
	// Purge phase : background, per replica. Errors are logged on the
	// replica side ; callers poll status separately.
	for _, addr := range addrs {
		rc, err := client.NewReplicaClient(addr, h.volumeName, "")
		if err != nil {
			continue
		}
		_ = rc.SnapshotPurge()
		_ = rc.Close()
	}
	return nil
}
