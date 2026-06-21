package control

import (
	"context"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
)

// quietLog returns a logrus logger that swallows output — keeps
// the test runner clean while the policy still exercises every
// log call path.
func quietLog() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// TestRereplicate_NoopWhenAllHostsLive : the trivial case. Every
// replica's host is in the live set ; nothing should be touched.
func TestRereplicate_NoopWhenAllHostsLive(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	mustPutVolume(ctx, s, Volume{UUID: "vol-1", ProjectUUID: "p", Name: "n", ReplicaCount: 3, State: VolumeStateHealthy})
	for _, h := range []string{"h1", "h2", "h3"} {
		mustPutReplica(ctx, s, Replica{UUID: "r-" + h, VolumeUUID: "vol-1", HostUUID: h, State: ReplicaStateRunning})
	}
	stats, err := RereplicateOrphans(ctx, s, HostLiveness{
		LiveHostUUIDs:       []string{"h1", "h2", "h3"},
		PlacementCandidates: []HostCandidate{{UUID: "h1", DC: "dc1"}, {UUID: "h2", DC: "dc2"}, {UUID: "h3", DC: "dc3"}, {UUID: "h4", DC: "dc4"}},
	}, "h1", quietLog())
	if err != nil {
		t.Fatalf("RereplicateOrphans: %v", err)
	}
	if stats.OrphansFaulted != 0 || stats.ReplicasScheduled != 0 {
		t.Errorf("expected no action ; got %+v", stats)
	}
}

// TestRereplicate_FaultsOrphansAndSchedulesReplacement : the main
// promise. A 3-replica volume with one host dead → that replica
// is marked Faulted AND a new Pending replica is created on a
// fresh candidate host.
func TestRereplicate_FaultsOrphansAndSchedulesReplacement(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	mustPutVolume(ctx, s, Volume{UUID: "vol-1", ProjectUUID: "p", Name: "n", ReplicaCount: 3, State: VolumeStateHealthy})
	mustPutReplica(ctx, s, Replica{UUID: "r-h1", VolumeUUID: "vol-1", HostUUID: "h1", State: ReplicaStateRunning, DC: "dc1"})
	mustPutReplica(ctx, s, Replica{UUID: "r-h2", VolumeUUID: "vol-1", HostUUID: "h2", State: ReplicaStateRunning, DC: "dc2"})
	mustPutReplica(ctx, s, Replica{UUID: "r-h3", VolumeUUID: "vol-1", HostUUID: "h3", State: ReplicaStateRunning, DC: "dc3"}) // h3 dies

	live := HostLiveness{
		LiveHostUUIDs: []string{"h1", "h2", "h4"}, // h3 missing
		PlacementCandidates: []HostCandidate{
			{UUID: "h1", DC: "dc1"},
			{UUID: "h2", DC: "dc2"},
			{UUID: "h4", DC: "dc4"},
		},
	}
	// Owner = lowest live replica host = h1.
	stats, err := RereplicateOrphans(ctx, s, live, "h1", quietLog())
	if err != nil {
		t.Fatalf("RereplicateOrphans: %v", err)
	}
	if stats.OrphansFaulted != 1 {
		t.Errorf("OrphansFaulted = %d, want 1 ; stats=%+v", stats.OrphansFaulted, stats)
	}
	if stats.ReplicasScheduled != 1 {
		t.Errorf("ReplicasScheduled = %d, want 1 ; stats=%+v", stats.ReplicasScheduled, stats)
	}
	// Inspect store : r-h3 must be Faulted ; a new replica on h4 must exist.
	got, _ := s.GetReplica(ctx, "r-h3")
	if got.State != ReplicaStateFaulted {
		t.Errorf("r-h3 state = %q, want faulted", got.State)
	}
	rs, _ := s.ListReplicasFor(ctx, "vol-1")
	var spawnedOnH4 bool
	for _, r := range rs {
		if r.HostUUID == "h4" && r.State == ReplicaStatePending {
			spawnedOnH4 = true
		}
	}
	if !spawnedOnH4 {
		t.Errorf("no Pending replica on h4 ; have :")
		for _, r := range rs {
			t.Logf("  %+v", r)
		}
	}
}

// TestRereplicate_OnlyOwnerActs : the non-owner agent must do
// nothing for the volume even though it sees the same orphans.
// Critical for race-safety in a 3-host cluster where all three
// agents run RereplicateOrphans every tick.
func TestRereplicate_OnlyOwnerActs(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	mustPutVolume(ctx, s, Volume{UUID: "vol-1", ProjectUUID: "p", Name: "n", ReplicaCount: 3, State: VolumeStateHealthy})
	mustPutReplica(ctx, s, Replica{UUID: "r-h2", VolumeUUID: "vol-1", HostUUID: "h2", State: ReplicaStateRunning})
	mustPutReplica(ctx, s, Replica{UUID: "r-h3", VolumeUUID: "vol-1", HostUUID: "h3", State: ReplicaStateRunning})
	mustPutReplica(ctx, s, Replica{UUID: "r-h4", VolumeUUID: "vol-1", HostUUID: "h4", State: ReplicaStateRunning}) // h4 dies

	live := HostLiveness{
		LiveHostUUIDs: []string{"h2", "h3", "h5"},
		PlacementCandidates: []HostCandidate{
			{UUID: "h2", DC: "dc2"}, {UUID: "h3", DC: "dc3"}, {UUID: "h5", DC: "dc5"},
		},
	}
	// owner = lowest among (h2, h3) = h2
	stats, err := RereplicateOrphans(ctx, s, live, "h3", quietLog())
	if err != nil {
		t.Fatalf("RereplicateOrphans on non-owner: %v", err)
	}
	if stats.OrphansFaulted != 0 || stats.ReplicasScheduled != 0 {
		t.Errorf("non-owner should not act ; got %+v", stats)
	}
	if stats.VolumesSkippedNotOwner != 1 {
		t.Errorf("VolumesSkippedNotOwner = %d, want 1", stats.VolumesSkippedNotOwner)
	}
	// Now the owner runs : MUST act.
	stats, err = RereplicateOrphans(ctx, s, live, "h2", quietLog())
	if err != nil {
		t.Fatalf("RereplicateOrphans on owner: %v", err)
	}
	if stats.OrphansFaulted != 1 || stats.ReplicasScheduled != 1 {
		t.Errorf("owner should heal ; got %+v", stats)
	}
}

// TestRereplicate_DeterministicNewReplicaUUID : the helper produces
// the same UUID for the same (volume, host) pair on every call.
// Tests the race-safety belt-and-braces guarantee.
func TestRereplicate_DeterministicNewReplicaUUID(t *testing.T) {
	if a, b := newReplicaUUID("vol-1", "host-X"), newReplicaUUID("vol-1", "host-X"); a != b {
		t.Errorf("newReplicaUUID not deterministic : %q vs %q", a, b)
	}
	if a, b := newReplicaUUID("vol-1", "host-X"), newReplicaUUID("vol-2", "host-X"); a == b {
		t.Errorf("newReplicaUUID should differentiate volumes")
	}
}

// TestRereplicate_NoCandidatesSchedulesNothing : when the placement
// pool excludes every live host (e.g. all live hosts already host
// a replica of this volume), no spawn fires. VolumesShortNoHost
// is bumped so operators see the resource-pressure.
func TestRereplicate_NoCandidatesSchedulesNothing(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	mustPutVolume(ctx, s, Volume{UUID: "vol-1", ReplicaCount: 3})
	mustPutReplica(ctx, s, Replica{UUID: "r-h1", VolumeUUID: "vol-1", HostUUID: "h1", State: ReplicaStateRunning})
	mustPutReplica(ctx, s, Replica{UUID: "r-h2", VolumeUUID: "vol-1", HostUUID: "h2", State: ReplicaStateRunning})
	mustPutReplica(ctx, s, Replica{UUID: "r-h3", VolumeUUID: "vol-1", HostUUID: "h3", State: ReplicaStateRunning})
	live := HostLiveness{
		LiveHostUUIDs: []string{"h1", "h2"}, // h3 dead, only h1+h2 left → both already used
		PlacementCandidates: []HostCandidate{
			{UUID: "h1", DC: "dc1"}, {UUID: "h2", DC: "dc2"},
		},
	}
	stats, err := RereplicateOrphans(ctx, s, live, "h1", quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if stats.OrphansFaulted != 1 {
		t.Errorf("orphan should still be faulted ; got %+v", stats)
	}
	if stats.ReplicasScheduled != 0 {
		t.Errorf("no candidate → no scheduling ; got %+v", stats)
	}
	if stats.VolumesShortNoHost != 1 {
		t.Errorf("VolumesShortNoHost = %d, want 1", stats.VolumesShortNoHost)
	}
}

func mustPutVolume(ctx context.Context, s *MemStore, v Volume) {
	if err := s.PutVolume(ctx, v); err != nil {
		panic(err)
	}
}

func mustPutReplica(ctx context.Context, s *MemStore, r Replica) {
	if err := s.PutReplica(ctx, r); err != nil {
		panic(err)
	}
}
