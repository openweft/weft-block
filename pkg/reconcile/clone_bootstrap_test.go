// Copyright 2026 openweft contributors. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license that can be
// found in the LICENSE file.

package reconcile

// clone_bootstrap_test.go covers the data-plane half of the V0.5.1 CoW-clone
// primitive : the reconcile loop's job is to detect Replica records with
// (SourceVolumeUUID, SourceSnapshot) populated, look up the parent volume's
// healthy replicas, and forward their addresses to SpawnReplica so the
// spawner can stream the snapshot bytes into the new replica's chain.
//
// The actual byte-streaming (bootstrapFromSnapshot in inproc_bootstrap.go) is
// linux-only and exercised end-to-end by the integration-test scaffold under
// cmd/weft-block ; these unit tests use a recordingSpawner to assert the
// fields the loop puts on the ReplicaRequest the spawner sees.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/openweft/weft-block/control"
)

// recordingSpawner is a Spawner that captures every ReplicaRequest the loop
// sends it. Like LogSpawner it doesn't actually run anything ; it just lets
// tests assert the loop populated the request the way they expect.
type recordingSpawner struct {
	mu    sync.Mutex
	calls []ReplicaRequest
	addr  string // deterministic Address to return so the replica reaches Running
}

func newRecordingSpawner(addr string) *recordingSpawner {
	return &recordingSpawner{addr: addr}
}

func (r *recordingSpawner) SpawnReplica(_ context.Context, req ReplicaRequest) (string, error) {
	r.mu.Lock()
	r.calls = append(r.calls, req)
	r.mu.Unlock()
	return r.addr, nil
}
func (*recordingSpawner) StopReplica(context.Context, string) error { return nil }
func (*recordingSpawner) SpawnEngine(context.Context, EngineRequest) (string, error) {
	return "/dev/nbd0", nil
}
func (*recordingSpawner) StopEngine(context.Context, string) error { return nil }
func (*recordingSpawner) LocalEngine(string) (EngineHandle, bool)  { return nil, false }

func (r *recordingSpawner) snapshot() []ReplicaRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ReplicaRequest, len(r.calls))
	copy(out, r.calls)
	return out
}

// TestLoop_CloneBootstrap_PropagatesParentAddresses : a Pending replica that
// carries clone provenance (SourceVolumeUUID + SourceSnapshot) is spawned with
// the parent volume's healthy replica addresses + the parent volume name
// attached, so the spawner can dial them via the weftsnap gRPC.
func TestLoop_CloneBootstrap_PropagatesParentAddresses(t *testing.T) {
	ctx := context.Background()
	store := control.NewMemStore()

	const localHost = "host-A"
	const otherHost = "host-B"
	const parentUUID = "parent-vol"
	const childUUID = "child-vol"

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Parent volume + one Running replica on host-B with a known Address.
	must(store.PutVolume(ctx, control.Volume{
		UUID: parentUUID, Name: "parent", SizeBytes: 4 << 20, ReplicaCount: 1,
		State: control.VolumeStateHealthy,
	}))
	must(store.PutReplica(ctx, control.Replica{
		UUID: "p-r1", VolumeUUID: parentUUID, HostUUID: otherHost,
		Address: "tcp://10.9.0.2:30001",
		State:   control.ReplicaStateRunning,
	}))

	// Child (clone) volume + one Pending replica on host-A tagged with the
	// clone provenance. This is what control.CreateFromSnapshot writes.
	must(store.PutVolume(ctx, control.Volume{
		UUID: childUUID, Name: "child", SizeBytes: 4 << 20, ReplicaCount: 1,
		State:            control.VolumeStateProvisioning,
		SourceVolumeUUID: parentUUID,
		SourceSnapshot:   "snap-1",
	}))
	must(store.PutReplica(ctx, control.Replica{
		UUID: "c-r1", VolumeUUID: childUUID, HostUUID: localHost,
		State:            control.ReplicaStatePending,
		SourceVolumeUUID: parentUUID,
		SourceSnapshot:   "snap-1",
	}))

	rec := newRecordingSpawner("10.0.0.1:30001")
	loop := New(localHost, store, rec)
	loop.Interval = 10 * time.Millisecond
	loop.Start(ctx)
	defer loop.Stop()

	// Wait until the spawner has been called for the local clone replica.
	deadline := time.Now().Add(500 * time.Millisecond)
	var hit ReplicaRequest
	for time.Now().Before(deadline) {
		for _, c := range rec.snapshot() {
			if c.ReplicaUUID == "c-r1" {
				hit = c
				goto done
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
done:
	if hit.ReplicaUUID != "c-r1" {
		t.Fatalf("SpawnReplica was never called for c-r1 ; spawner saw %+v", rec.snapshot())
	}

	// The loop must have forwarded the clone provenance.
	if hit.SourceVolumeUUID != parentUUID {
		t.Errorf("SourceVolumeUUID = %q, want %q", hit.SourceVolumeUUID, parentUUID)
	}
	if hit.SourceSnapshot != "snap-1" {
		t.Errorf("SourceSnapshot = %q, want %q", hit.SourceSnapshot, "snap-1")
	}
	if hit.SourceVolumeName != "parent" {
		t.Errorf("SourceVolumeName = %q, want %q", hit.SourceVolumeName, "parent")
	}
	if len(hit.SourceReplicaAddresses) != 1 || hit.SourceReplicaAddresses[0] != "tcp://10.9.0.2:30001" {
		t.Errorf("SourceReplicaAddresses = %v, want [tcp://10.9.0.2:30001]", hit.SourceReplicaAddresses)
	}
}

// TestLoop_CloneBootstrap_DefersUntilParentRunning : when a clone replica
// lands while its parent's replicas are still Pending (no Address yet), the
// loop must NOT spawn an empty replica — that would silently shadow the
// bootstrap intent. Instead it defers until at least one parent replica is
// Running.
func TestLoop_CloneBootstrap_DefersUntilParentRunning(t *testing.T) {
	ctx := context.Background()
	store := control.NewMemStore()

	const localHost = "host-A"
	const otherHost = "host-B"
	const parentUUID = "parent-vol"
	const childUUID = "child-vol"

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(store.PutVolume(ctx, control.Volume{UUID: parentUUID, Name: "parent", SizeBytes: 4 << 20, ReplicaCount: 1, State: control.VolumeStateProvisioning}))
	// Parent replica is still Pending — no Address yet.
	must(store.PutReplica(ctx, control.Replica{
		UUID: "p-r1", VolumeUUID: parentUUID, HostUUID: otherHost,
		State: control.ReplicaStatePending,
	}))
	must(store.PutVolume(ctx, control.Volume{UUID: childUUID, Name: "child", SizeBytes: 4 << 20, ReplicaCount: 1, State: control.VolumeStateProvisioning, SourceVolumeUUID: parentUUID, SourceSnapshot: "snap-1"}))
	must(store.PutReplica(ctx, control.Replica{
		UUID: "c-r1", VolumeUUID: childUUID, HostUUID: localHost,
		State:            control.ReplicaStatePending,
		SourceVolumeUUID: parentUUID,
		SourceSnapshot:   "snap-1",
	}))

	rec := newRecordingSpawner("10.0.0.1:30001")
	loop := New(localHost, store, rec)
	loop.Interval = 10 * time.Millisecond
	loop.Start(ctx)
	defer loop.Stop()

	// Run a few ticks. The spawner must NOT see c-r1 yet.
	time.Sleep(80 * time.Millisecond)
	for _, c := range rec.snapshot() {
		if c.ReplicaUUID == "c-r1" {
			t.Fatalf("SpawnReplica was called for c-r1 while parent replica had no Address ; would silently start an empty replica")
		}
	}
	// The child replica must still be Pending in the store.
	cr, err := store.GetReplica(ctx, "c-r1")
	if err != nil {
		t.Fatal(err)
	}
	if cr.State != control.ReplicaStatePending {
		t.Errorf("child replica state = %q, want Pending (deferred)", cr.State)
	}

	// Promote the parent replica to Running with an address. The loop must
	// then call SpawnReplica for c-r1 on the next tick.
	pr, _ := store.GetReplica(ctx, "p-r1")
	pr.State = control.ReplicaStateRunning
	pr.Address = "tcp://10.9.0.2:30001"
	must(store.PutReplica(ctx, pr))

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, c := range rec.snapshot() {
			if c.ReplicaUUID == "c-r1" && len(c.SourceReplicaAddresses) == 1 {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("SpawnReplica was never invoked for c-r1 after parent reached Running ; spawner saw %+v", rec.snapshot())
}

// TestLoop_CloneBootstrap_NoProvenance_SkipsLookup : the non-clone path
// (empty SourceSnapshot) must continue to work — the loop calls SpawnReplica
// straight away without touching the store for parent lookups. Verified
// indirectly by checking the spawned request carries no clone fields.
func TestLoop_CloneBootstrap_NoProvenance_SkipsLookup(t *testing.T) {
	ctx := context.Background()
	store := control.NewMemStore()
	const localHost = "host-A"
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(store.PutVolume(ctx, control.Volume{UUID: "v", Name: "v", SizeBytes: 4 << 20, ReplicaCount: 1}))
	must(store.PutReplica(ctx, control.Replica{UUID: "r", VolumeUUID: "v", HostUUID: localHost, State: control.ReplicaStatePending}))

	rec := newRecordingSpawner("10.0.0.1:30001")
	loop := New(localHost, store, rec)
	loop.Interval = 10 * time.Millisecond
	loop.Start(ctx)
	defer loop.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	var hit ReplicaRequest
	for time.Now().Before(deadline) {
		for _, c := range rec.snapshot() {
			if c.ReplicaUUID == "r" {
				hit = c
				goto done
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
done:
	if hit.ReplicaUUID != "r" {
		t.Fatalf("SpawnReplica was never called for r")
	}
	if hit.SourceSnapshot != "" || hit.SourceVolumeUUID != "" || len(hit.SourceReplicaAddresses) != 0 {
		t.Errorf("non-clone path leaked clone fields : %+v", hit)
	}
}
