// Copyright 2026 openweft contributors. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license that can be
// found in the LICENSE file.

//go:build linux

package reconcile

// inproc_bootstrap.go is the data-plane half of the V0.5.1 CoW-clone primitive
// defined by control.CreateFromSnapshot. The control plane writes Replica
// records tagged with (SourceVolumeUUID, SourceSnapshot, SourceReplicaAddresses)
// when a clone is requested ; the reconcile loop forwards those fields to
// SpawnReplica via ReplicaRequest ; this file holds the helper SpawnReplica
// calls to populate the freshly-created replica chain from the parent's
// snapshot before handing it back to the engine.
//
// The bootstrap is conceptually a CoW-by-copy : the upstream Longhorn replica
// chain doesn't natively expose a "chain pointer at another replica's
// snapshot" reference, and a cross-host pointer would couple replica lifetimes
// in ways the reconcile loop doesn't yet model. Byte-copying the snapshot
// payload into the new replica's head + sealing it as a snapshot named after
// the source gives us the same observable semantics : the cloned volume comes
// up with the source data, and subsequent writes land on top of the sealed
// bootstrap snapshot. Storage cost is O(used bytes in the source snapshot) per
// replica — same as a deep copy, but the API stays a single high-level
// CreateFromSnapshot call.
//
// The wire protocol is the existing weftsnap.SnapshotReader gRPC (already used
// by the backup path for snapshot streaming). We reuse it verbatim : no proto
// changes, no new replica RPC surface. The source replica process is unaware
// it's being read for a clone vs a backup.

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/openweft/weft-block/pkg/replica"
	"github.com/openweft/weft-block/pkg/replica/rpc/weftsnap"
)

// bootstrapBlockSize is the chunk size we ask the source replica to emit. 4 MiB
// matches the backup-path default and amortises gRPC framing overhead without
// pushing message-size limits.
const bootstrapBlockSize int64 = 4 * 1024 * 1024

// bootstrapFromSnapshot populates a freshly-created replica.Server with the
// bytes of a named snapshot served by one of the parent volume's replicas.
// Called by SpawnReplica when ReplicaRequest.SourceSnapshot is non-empty.
//
// Sequence :
//
//  1. Open the local replica.Server so WriteAt is callable. (After Create the
//     server is Closed — Open transitions it to Open with a live *Replica
//     under s.r.)
//  2. Dial the first SourceReplicaAddress via gRPC + open a weftsnap.Read
//     stream for (SourceSnapshot, SourceVolumeName) on the parent replica.
//  3. Stream chunks ; for each, WriteAt(chunk.Data, chunk.Offset) into the
//     local replica's head.
//  4. Take a local snapshot named after the source so the bootstrap bytes
//     settle into a base disk and the engine's eventual writes land on top.
//  5. Close the local replica so the caller's SpawnReplica returns it in the
//     same Closed state the empty-replica path leaves it in (the engine's
//     Controller.Start re-opens via the AddReplica handshake).
//
// On any failure the local replica is closed best-effort and the error is
// surfaced — the caller marks the replica Faulted and the reconcile loop's
// teardown path collects the partial state. The fault is recoverable : the
// control plane can retry the clone by re-issuing CreateFromSnapshot with the
// same NewVolumeUUID (it's idempotent at the Volume Put level) once the
// parent replicas are again reachable.
func bootstrapFromSnapshot(ctx context.Context, rs *replica.Server, req ReplicaRequest) error {
	if req.SourceSnapshot == "" {
		return nil // nothing to do — caller should not even invoke us
	}
	if len(req.SourceReplicaAddresses) == 0 {
		return fmt.Errorf("weft-block: clone bootstrap : no source replica addresses provided for snapshot %s", req.SourceSnapshot)
	}
	if req.SourceVolumeName == "" {
		// weftsnap.Read's server-side assertOpen check matches on volume
		// name ; without it the parent replica refuses the stream.
		return fmt.Errorf("weft-block: clone bootstrap : source volume name is required for snapshot %s", req.SourceSnapshot)
	}

	// Open the local replica so WriteAt is callable. Open expects the on-disk
	// state to be at least Closed (which Create just left it in).
	if err := rs.Open(false, req.SizeBytes); err != nil {
		return fmt.Errorf("weft-block: clone bootstrap : open local replica : %w", err)
	}
	// Best-effort Close on the way out — the engine's Controller.Start
	// handshake re-opens the replica through the AddReplica path. If Close
	// itself fails we surface that error too (it's load-bearing : a dirty
	// shutdown leaves the volume-meta file in a state the engine then refuses
	// to add).
	defer func() {
		_ = rs.Close()
	}()

	// Dial the first source replica. The weftsnap server runs on the BASE
	// gRPC port the parent replica advertises ; replicaGRPCEndpoint
	// (inproc_engine.go) strips the "tcp://" scheme + returns host:port. We
	// keep the parent-address selection trivial — first healthy — and let
	// the upper layers retry on failure ; failover across multiple parent
	// replicas mid-stream would complicate offset bookkeeping.
	addr, err := replicaGRPCEndpoint(req.SourceReplicaAddresses[0])
	if err != nil {
		return fmt.Errorf("weft-block: clone bootstrap : derive grpc addr from %q : %w", req.SourceReplicaAddresses[0], err)
	}
	// Raise the receive cap : the server emits 4 MiB chunks (defaultChunkSize
	// in weftsnap_server.go) plus protobuf framing overhead, which the gRPC
	// default 4 MiB limit refuses. Allow up to 2× bootstrapBlockSize so the
	// framing headroom never bites ; the wire is loopback / cluster-overlay,
	// not the public internet, so an oversized message bound is benign.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(2*bootstrapBlockSize))),
	)
	if err != nil {
		return fmt.Errorf("weft-block: clone bootstrap : dial source replica %s : %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	cli := weftsnap.NewSnapshotReaderClient(conn)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := cli.Read(streamCtx, &weftsnap.ReadRequest{
		SnapshotName: req.SourceSnapshot,
		VolumeName:   req.SourceVolumeName,
		ChunkSize:    bootstrapBlockSize,
	})
	if err != nil {
		return fmt.Errorf("weft-block: clone bootstrap : open snapshot stream on %s : %w", addr, err)
	}

	var total int64
	for {
		chunk, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fmt.Errorf("weft-block: clone bootstrap : recv chunk after %d bytes : %w", total, rerr)
		}
		if len(chunk.Data) == 0 {
			if chunk.Last {
				break
			}
			continue
		}
		if _, werr := rs.WriteAt(chunk.Data, chunk.Offset); werr != nil {
			return fmt.Errorf("weft-block: clone bootstrap : WriteAt at offset %d (already wrote %d) : %w", chunk.Offset, total, werr)
		}
		total += int64(len(chunk.Data))
		if chunk.Last {
			break
		}
	}

	// Seal the bootstrap bytes under a deterministic snapshot name so the
	// engine's subsequent writes land on the head rather than overwriting
	// the imported bytes in place. The name mirrors the source so operators
	// can correlate the chain across the clone boundary. UserCreated=false
	// marks it as a system snapshot (it won't show up in user-facing chain
	// listings as a manual snapshot).
	snapName := "clone-bootstrap-" + req.SourceSnapshot
	if err := rs.Snapshot(snapName, false, "", map[string]string{
		"clone.source_volume":   req.SourceVolumeUUID,
		"clone.source_snapshot": req.SourceSnapshot,
	}); err != nil {
		return fmt.Errorf("weft-block: clone bootstrap : seal local snapshot %s : %w", snapName, err)
	}

	return nil
}
