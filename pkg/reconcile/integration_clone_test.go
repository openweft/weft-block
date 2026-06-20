// Copyright 2026 openweft contributors. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license that can be
// found in the LICENSE file.

//go:build linux && integration

package reconcile

// integration_clone_test.go is the end-to-end multi-host clone test promised
// as the V0.6 follow-up of inproc_bootstrap.go. The unit tests
// (clone_bootstrap_test.go) cover the loop's request-shape behaviour with a
// recording fake Spawner ; this test exercises the real byte-streaming path :
// two InProcessSpawner instances acting as two "hosts" sharing the same
// localhost loopback but rooted at distinct state directories, the parent
// holds a snapshot, the clone bootstraps from it through the existing
// weftsnap.SnapshotReader gRPC, and we assert byte-for-byte equivalence of
// the snapshot's contents in the clone's local chain.
//
// Build-tag : `integration` keeps this out of `go test ./...` because it
// spawns real gRPC servers on dynamic ports + writes ~MiBs of test data to a
// temp dir (slower than the unit tests by an order of magnitude). Run via
// `go test -tags integration ./pkg/reconcile/`. Also linux-only because it
// reuses the same Linux-only data-plane stack the InProcessSpawner needs
// (pkg/replica is gated on `//go:build linux`).
//
// Self-contained : no sudo, no /dev/nbd, no network outside loopback. Both
// "hosts" bind on 127.0.0.1 via freeTriplet() — same primitive the real
// spawner uses to allocate replica ports. State directories are
// t.TempDir() so the host filesystem cleans up automatically.

import (
	"context"
	"testing"
	"time"

	"github.com/openweft/weft-block/pkg/replica"
	"github.com/openweft/weft-block/pkg/util"
)

// TestIntegration_CloneAcrossHosts spins up two InProcessSpawners, writes a
// known pattern to the parent + snapshots it, then asks the clone spawner to
// SpawnReplica with that snapshot as bootstrap source. The clone is then
// re-opened locally and read back ; the bytes must match what we wrote into
// the parent before the snapshot was sealed.
//
// Why call SpawnReplica directly instead of going through reconcile.Loop ?
// The loop is already covered by clone_bootstrap_test.go for the request-
// shape contract. Driving the spawner directly here isolates the data-plane
// half of the clone (the streaming bootstrap) from the polling state-machine
// of the loop — when this test fails, the failure is unambiguously in the
// bootstrap path, not in the reconciler.
func TestIntegration_CloneAcrossHosts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: spins up two replica gRPC servers + writes MiBs of data")
	}

	const (
		// 16 MiB volume : large enough to span multiple 4 MiB bootstrap
		// chunks (see inproc_bootstrap.go::bootstrapBlockSize) yet small
		// enough to finish under a second on any dev host.
		volumeSize int64 = 16 * 1024 * 1024
		sectorSize int64 = 4096
		volumeName       = "clone-it-parent" // parent + child share name : the
		// weftsnap interceptor + handler validate it against the source
		// replica's server-side volumeName, which is set at Spawn time
		// from req.VolumeName. The child's bootstrap call sets
		// req.SourceVolumeName to the PARENT's volume name (this value).
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Two "hosts" on loopback. Distinct state dirs ⇒ distinct on-disk
	// chains. HostAddr=127.0.0.1 ⇒ the dataAddr the spawner advertises
	// is reachable from the other spawner without any plumbing.
	hostADir := t.TempDir()
	hostBDir := t.TempDir()
	spawnerA := NewInProcessSpawner("127.0.0.1", hostADir)
	spawnerB := NewInProcessSpawner("127.0.0.1", hostBDir)
	defer func() {
		// Stop replicas best-effort on both spawners. The replicaUUIDs we
		// care about are deterministic below ; an unknown UUID is a no-op.
		_ = spawnerA.StopReplica(context.Background(), "parent-r1")
		_ = spawnerB.StopReplica(context.Background(), "clone-r1")
	}()

	// 1. Spawn the parent replica on host A. SpawnReplica leaves the
	//    underlying replica.Server in Closed state (Create ends with
	//    Close — see inproc.go::SpawnReplica) ; we have to Open it back
	//    up before we can WriteAt + Snapshot.
	parentAddr, err := spawnerA.SpawnReplica(ctx, ReplicaRequest{
		ReplicaUUID: "parent-r1",
		VolumeUUID:  "parent-vol-uuid",
		VolumeName:  volumeName,
		SizeBytes:   volumeSize,
		SectorSize:  sectorSize,
	})
	if err != nil {
		t.Fatalf("SpawnReplica(parent): %v", err)
	}
	if parentAddr == "" {
		t.Fatalf("SpawnReplica(parent) returned empty address")
	}

	parentServer := lookupServer(t, spawnerA, "parent-r1")
	if err := parentServer.Open(false, volumeSize); err != nil {
		t.Fatalf("parent Open: %v", err)
	}

	// 2. Write a deterministic test pattern to the parent's volume head.
	//    The pattern is `i % 251` per byte — a non-trivial sequence that
	//    catches off-by-one byte-shuffling bugs (251 is a prime so the
	//    pattern doesn't align with any 4-KiB or 4-MiB chunk boundary).
	pattern := makePattern(volumeSize)
	if _, err := parentServer.WriteAt(pattern, 0); err != nil {
		t.Fatalf("parent WriteAt: %v", err)
	}

	// 3. Seal the pattern as a named snapshot in the parent's chain. The
	//    snapshot's on-disk file is `volume-snap-<name>.img` — the same
	//    name the weftsnap server resolves on the read path
	//    (weftsnap_server.go::snapshotDiskName).
	const snapName = "clone-it-snap-1"
	if err := parentServer.Snapshot(snapName, true /* userCreated */, util.Now(), nil); err != nil {
		t.Fatalf("parent Snapshot(%s): %v", snapName, err)
	}

	// IMPORTANT : leave the parent server OPEN. The weftsnap handler
	// (weftsnap_server.go::Read line 77) returns
	// "replica server has no open replica" when sr.s.Replica() is nil,
	// which is the state Close would leave us in. The bootstrap path
	// reads exclusively through the snapshot RPC, so the open parent
	// is the seam that lets the data flow.

	// 4. Spawn the clone replica on host B with the parent's data
	//    address wired in. This is the exact ReplicaRequest shape the
	//    reconcile loop builds for a Pending Replica carrying clone
	//    provenance (see loop.go::reconcileReplica) — except we plug
	//    SourceReplicaAddresses directly here instead of going through
	//    the store.
	if _, err := spawnerB.SpawnReplica(ctx, ReplicaRequest{
		ReplicaUUID:            "clone-r1",
		VolumeUUID:             "clone-vol-uuid",
		VolumeName:             "clone-it-child", // child's own volume name ; distinct from parent
		SizeBytes:              volumeSize,
		SectorSize:             sectorSize,
		SourceVolumeUUID:       "parent-vol-uuid",
		SourceSnapshot:         snapName,
		SourceVolumeName:       volumeName, // parent's volume name — the source replica validates this
		SourceReplicaAddresses: []string{parentAddr},
	}); err != nil {
		t.Fatalf("SpawnReplica(clone): %v", err)
	}

	// 5. SpawnReplica(clone) returned with bootstrap already done — the
	//    clone's chain has the snapshot bytes in it, sealed under
	//    "clone-bootstrap-<snapName>" (see inproc_bootstrap.go line 160).
	//    Re-open the clone's server so we can ReadAt + verify.
	cloneServer := lookupServer(t, spawnerB, "clone-r1")
	if err := cloneServer.Open(false, volumeSize); err != nil {
		t.Fatalf("clone Open: %v", err)
	}

	// 6. Read the volume back through the clone's local server. Its chain
	//    is : head (empty) → clone-bootstrap-<snap> (the imported bytes)
	//    → base. ReadAt traverses the chain top-down, so the imported
	//    bytes are what we get back.
	got := make([]byte, volumeSize)
	if _, err := cloneServer.ReadAt(got, 0); err != nil {
		t.Fatalf("clone ReadAt: %v", err)
	}

	// 7. Byte-for-byte equality with what we wrote to the parent. The
	//    test catches any chunk re-ordering, off-by-one offsets, or
	//    partial-chunk truncation in the bootstrap loop.
	if !bytesEqual(got, pattern) {
		// Find the first divergent byte for a useful failure message.
		idx := firstDiff(got, pattern)
		t.Fatalf("clone bytes differ from parent at offset %d (clone=0x%02x, parent=0x%02x)",
			idx, got[idx], pattern[idx])
	}

	// 8. The clone's chain must include the synthesised bootstrap
	//    snapshot, named after the source. List the chain through the
	//    open server's underlying Replica and look for the entry.
	cloneReplica := cloneServer.Replica()
	if cloneReplica == nil {
		t.Fatalf("clone server has no open replica after Open")
	}
	disks := cloneReplica.ListDisks()
	wantBootstrap := "volume-snap-clone-bootstrap-" + snapName + ".img"
	if _, ok := disks[wantBootstrap]; !ok {
		names := make([]string, 0, len(disks))
		for n := range disks {
			names = append(names, n)
		}
		t.Errorf("clone chain missing bootstrap snapshot %q ; chain has %v", wantBootstrap, names)
	}
}

// lookupServer reaches into the InProcessSpawner's running-replicas map and
// returns the underlying *replica.Server for the named UUID. Test-only seam :
// the spawner doesn't expose this on its public API because production
// callers don't need it, but the integration test needs to drive WriteAt /
// Snapshot / ReadAt directly on the parent + clone. The same-package
// boundary makes this safe.
func lookupServer(t *testing.T, sp *InProcessSpawner, replicaUUID string) *replica.Server {
	t.Helper()
	sp.mu.Lock()
	defer sp.mu.Unlock()
	r, ok := sp.replicas[replicaUUID]
	if !ok {
		t.Fatalf("spawner has no running replica %q (have %d)", replicaUUID, len(sp.replicas))
	}
	return r.server
}

// makePattern fills `n` bytes with a deterministic sequence chosen to
// catch chunk re-ordering bugs. `i % 251` (251 is prime) doesn't align
// with any 4-KiB sector or 4-MiB bootstrap chunk boundary, so a chunk
// emitted at the wrong offset surfaces as a mismatching byte well before
// the full volume is compared.
func makePattern(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// bytesEqual is a tiny stand-in for bytes.Equal that avoids dragging the
// bytes package into the integration test's imports just for one call.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// firstDiff returns the index of the first differing byte between a and b.
// Caller has already confirmed they differ.
func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
