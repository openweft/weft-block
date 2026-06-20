// Copyright 2026 openweft contributors. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license that can be
// found in the LICENSE file.

package control

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCreateFromSnapshot_Happy(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	frozen := time.Unix(1700000000, 0)
	s.SetClock(func() time.Time { return frozen })

	// Seed a parent volume with 3 replicas across 3 DCs.
	parent := Volume{
		UUID:         "parent-vol",
		ProjectUUID:  "proj-A",
		Name:         "parent",
		SizeBytes:    1 << 30,
		ReplicaCount: 3,
		State:        VolumeStateHealthy,
	}
	if err := s.PutVolume(ctx, parent); err != nil {
		t.Fatal(err)
	}
	for i, dc := range []string{"dc1", "dc2", "dc3"} {
		_ = s.PutReplica(ctx, Replica{
			UUID:       parent.UUID + "-r" + string(rune('0'+i)),
			VolumeUUID: parent.UUID,
			HostUUID:   "h" + dc,
			DC:         dc,
			State:      ReplicaStateRunning,
			Address:    "tcp://10.9.0." + string(rune('0'+i)) + ":54321",
		})
	}

	hosts := []HostCandidate{
		{UUID: "hdc1", DC: "dc1"},
		{UUID: "hdc2", DC: "dc2"},
		{UUID: "hdc3", DC: "dc3"},
	}

	newVol, err := CreateFromSnapshot(ctx, s, hosts, CloneRequest{
		ParentVolumeUUID: parent.UUID,
		SnapshotName:     "snap-pre-upgrade",
		NewVolumeUUID:    "clone-vol",
		NewVolumeName:    "clone",
	}, func() time.Time { return frozen })
	if err != nil {
		t.Fatalf("CreateFromSnapshot: %v", err)
	}

	// 1. The new Volume inherits ProjectUUID / SizeBytes / ReplicaCount and
	//    carries the provenance fields.
	if newVol.ProjectUUID != parent.ProjectUUID {
		t.Errorf("ProjectUUID = %q, want %q", newVol.ProjectUUID, parent.ProjectUUID)
	}
	if newVol.SizeBytes != parent.SizeBytes {
		t.Errorf("SizeBytes = %d, want %d", newVol.SizeBytes, parent.SizeBytes)
	}
	if newVol.ReplicaCount != parent.ReplicaCount {
		t.Errorf("ReplicaCount = %d, want %d", newVol.ReplicaCount, parent.ReplicaCount)
	}
	if newVol.State != VolumeStateProvisioning {
		t.Errorf("State = %s, want provisioning", newVol.State)
	}
	if newVol.SourceVolumeUUID != parent.UUID || newVol.SourceSnapshot != "snap-pre-upgrade" {
		t.Errorf("provenance = (%s, %s), want (%s, snap-pre-upgrade)", newVol.SourceVolumeUUID, newVol.SourceSnapshot, parent.UUID)
	}

	// 2. The new Replicas are Pending, tagged with the parent's snapshot,
	//    spread across the three DCs.
	rs, err := s.ListReplicasFor(ctx, newVol.UUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 3 {
		t.Fatalf("replica count = %d, want 3", len(rs))
	}
	dcs := map[string]bool{}
	for _, r := range rs {
		if r.State != ReplicaStatePending {
			t.Errorf("replica %s state = %s, want pending", r.UUID, r.State)
		}
		if r.SourceVolumeUUID != parent.UUID || r.SourceSnapshot != "snap-pre-upgrade" {
			t.Errorf("replica %s provenance = (%s, %s), want (%s, snap-pre-upgrade)", r.UUID, r.SourceVolumeUUID, r.SourceSnapshot, parent.UUID)
		}
		dcs[r.DC] = true
	}
	if len(dcs) != 3 {
		t.Errorf("replicas spread over %d DCs, want 3", len(dcs))
	}
}

func TestCreateFromSnapshot_Idempotent(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	_ = s.PutVolume(ctx, Volume{UUID: "p", Name: "p", SizeBytes: 1 << 20, ReplicaCount: 1, State: VolumeStateHealthy})
	hosts := []HostCandidate{{UUID: "h1", DC: "dc1"}}
	req := CloneRequest{ParentVolumeUUID: "p", SnapshotName: "s", NewVolumeUUID: "c", NewVolumeName: "c"}
	first, err := CreateFromSnapshot(ctx, s, hosts, req, nil)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := CreateFromSnapshot(ctx, s, hosts, req, nil)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first.UUID != second.UUID || first.CreatedAt != second.CreatedAt {
		t.Errorf("idempotent retry returned a different Volume: %+v vs %+v", first, second)
	}
	// Replica count must NOT have doubled on retry.
	rs, _ := s.ListReplicasFor(ctx, "c")
	if len(rs) != 1 {
		t.Errorf("replicas after retry = %d, want 1 (no duplicate placement)", len(rs))
	}
}

func TestCreateFromSnapshot_RejectsMissingParent(t *testing.T) {
	s := NewMemStore()
	_, err := CreateFromSnapshot(context.Background(), s, []HostCandidate{{UUID: "h1", DC: "dc1"}}, CloneRequest{
		ParentVolumeUUID: "ghost",
		SnapshotName:     "s",
		NewVolumeUUID:    "c",
		NewVolumeName:    "c",
	}, nil)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("missing parent should surface ErrNotFound, got %v", err)
	}
}

func TestCreateFromSnapshot_RejectsEmptyArgs(t *testing.T) {
	s := NewMemStore()
	cases := []CloneRequest{
		{SnapshotName: "s", NewVolumeUUID: "c", NewVolumeName: "c"},
		{ParentVolumeUUID: "p", NewVolumeUUID: "c", NewVolumeName: "c"},
		{ParentVolumeUUID: "p", SnapshotName: "s", NewVolumeName: "c"},
		{ParentVolumeUUID: "p", SnapshotName: "s", NewVolumeUUID: "c"},
	}
	for i, req := range cases {
		_, err := CreateFromSnapshot(context.Background(), s, nil, req, nil)
		if !errors.Is(err, ErrCloneInvalid) {
			t.Errorf("case %d: want ErrCloneInvalid, got %v", i, err)
		}
	}
}

func TestCreateFromSnapshot_PropagatesPlacementError(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	_ = s.PutVolume(ctx, Volume{UUID: "p", Name: "p", SizeBytes: 1 << 20, ReplicaCount: 3, State: VolumeStateHealthy})
	_, err := CreateFromSnapshot(ctx, s, []HostCandidate{{UUID: "h1", DC: "dc1"}}, CloneRequest{
		ParentVolumeUUID: "p",
		SnapshotName:     "s",
		NewVolumeUUID:    "c",
		NewVolumeName:    "c",
	}, nil)
	if err == nil {
		t.Fatal("expected placement error when host count < replica count")
	}
}
