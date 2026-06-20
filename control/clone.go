// Copyright 2026 openweft contributors. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license that can be
// found in the LICENSE file.

package control

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CloneRequest is the input to CreateFromSnapshot.
//
// ParentVolumeUUID + SnapshotName name the source : an existing Volume in the
// store and a snapshot in that Volume's chain. The control plane does NOT
// validate that the snapshot exists on the engine — that's the per-host
// reconcile loop's job when it brings up the cloned replicas (it dials the
// parent's replicas to read the snapshot bytes via the existing
// CompareSnapshot/ReadSnapshot machinery in pkg/replica). A non-existent
// snapshot surfaces later as a Faulted replica on each host, which the caller
// observes via the standard reconcile state machine. Fail-loud is the right
// default at the control plane : we don't want to refuse a clone just because
// the engine happens to be local-not-running on the receiving host.
//
// NewVolumeUUID + NewVolumeName name the destination. The caller is
// responsible for picking a unique UUID (typically a fresh ksuid). The new
// Volume inherits the parent's SizeBytes, ReplicaCount + ProjectUUID — the
// clone is a sibling under the same tenant, not a copy that crosses project
// boundaries.
type CloneRequest struct {
	ParentVolumeUUID string
	SnapshotName     string
	NewVolumeUUID    string
	NewVolumeName    string
}

// ErrCloneInvalid is the sentinel CreateFromSnapshot returns when the request
// is malformed (empty UUID / snapshot / name). Wraps with %w on the call side
// so callers can errors.Is it.
var ErrCloneInvalid = errors.New("weft-block: clone request is invalid")

// CreateFromSnapshot is the high-level CoW-clone primitive on the control
// plane. It allocates a new Volume + N Replica records (where N == parent
// ReplicaCount), marks each new Replica with the parent volume + snapshot it
// must bootstrap from, and PlaceReplicas-spreads them across the candidate
// host pool. Returns the new Volume so the caller can persist its UUID,
// surface it through the upper RPC, and (optionally) call AttachVolume.
//
// The function is store-atomic at the per-record level (the underlying store
// doesn't expose transactions), so a partial failure mid-way leaves orphan
// records the reconcile loop will clean up via the standard Deleting state
// path. Callers retrying with the same NewVolumeUUID get an idempotent re-run
// once the first attempt has landed past the Volume Put.
//
// CGO-clean : no fork/exec, no /dev access — pure store operations + a
// PlaceReplicas call. Safe to call from the linux/arm64 binary path.
func CreateFromSnapshot(
	ctx context.Context,
	store VolumeStore,
	hosts []HostCandidate,
	req CloneRequest,
	now func() time.Time,
) (Volume, error) {
	if req.ParentVolumeUUID == "" || req.SnapshotName == "" {
		return Volume{}, fmt.Errorf("%w: ParentVolumeUUID and SnapshotName required", ErrCloneInvalid)
	}
	if req.NewVolumeUUID == "" || req.NewVolumeName == "" {
		return Volume{}, fmt.Errorf("%w: NewVolumeUUID and NewVolumeName required", ErrCloneInvalid)
	}

	if now == nil {
		now = time.Now
	}

	parent, err := store.GetVolume(ctx, req.ParentVolumeUUID)
	if err != nil {
		return Volume{}, fmt.Errorf("weft-block: clone: load parent %s: %w", req.ParentVolumeUUID, err)
	}

	// Idempotency : a clone request for an existing destination UUID is a
	// no-op once the Volume is persisted. Returns whatever is in the store
	// so callers retrying see a consistent shape.
	if existing, err := store.GetVolume(ctx, req.NewVolumeUUID); err == nil {
		return existing, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Volume{}, fmt.Errorf("weft-block: clone: load destination %s: %w", req.NewVolumeUUID, err)
	}

	// Honour the parent's ReplicaCount — symmetric with EnsureVolume's
	// effectiveReplicaCount on the driver side. Caller is expected to pass a
	// host pool large enough ; PlaceReplicas surfaces the gap explicitly.
	replicaCount := parent.ReplicaCount
	if replicaCount <= 0 {
		replicaCount = 1
	}
	picks, perr := PlaceReplicas(hosts, replicaCount)
	if perr != nil {
		return Volume{}, fmt.Errorf("weft-block: clone: placement: %w", perr)
	}

	stamp := now()
	newVol := Volume{
		UUID:             req.NewVolumeUUID,
		ProjectUUID:      parent.ProjectUUID,
		Name:             req.NewVolumeName,
		SizeBytes:        parent.SizeBytes,
		ReplicaCount:     replicaCount,
		State:            VolumeStateProvisioning,
		SourceVolumeUUID: req.ParentVolumeUUID,
		SourceSnapshot:   req.SnapshotName,
		UpdatedAt:        stamp,
	}
	if err := store.PutVolume(ctx, newVol); err != nil {
		return Volume{}, fmt.Errorf("weft-block: clone: persist new volume: %w", err)
	}

	// Write Pending replicas tagged with the bootstrap source. The reconcile
	// loop on each placed-on host observes SourceSnapshot != "" and walks the
	// parent's replicas to read the snapshot bytes when it spawns the local
	// replica.Server — same code path SpawnReplica takes today, plus a
	// per-replica bootstrap-from-snapshot step (out of scope for V0.5.1 ; the
	// records are durable so the upgrade is in-place).
	for i, h := range picks {
		r := Replica{
			UUID:             fmt.Sprintf("%s-r%d", req.NewVolumeUUID, i),
			VolumeUUID:       req.NewVolumeUUID,
			HostUUID:         h.UUID,
			DC:               h.DC,
			State:            ReplicaStatePending,
			SourceVolumeUUID: req.ParentVolumeUUID,
			SourceSnapshot:   req.SnapshotName,
		}
		if err := store.PutReplica(ctx, r); err != nil {
			return Volume{}, fmt.Errorf("weft-block: clone: persist replica %s: %w", r.UUID, err)
		}
	}

	// Round-trip the Volume so callers see canonical CreatedAt / UpdatedAt as
	// the store stamped them (MemStore + etcd binding both stamp on Put).
	final, err := store.GetVolume(ctx, req.NewVolumeUUID)
	if err != nil {
		return Volume{}, fmt.Errorf("weft-block: clone: re-read new volume: %w", err)
	}
	return final, nil
}
