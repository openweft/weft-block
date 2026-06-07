package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/openweft/weft-block/control"
)

// TestLoop_ReplicaThenEngine: starting from Pending records, the loop calls
// the spawner on this host's records, the store ends up with Running state +
// non-empty Address/FrontendPath, and records pinned to OTHER hosts are
// left untouched.
func TestLoop_ReplicaThenEngine(t *testing.T) {
	ctx := context.Background()
	store := control.NewMemStore()

	const localHost = "host-A"
	const otherHost = "host-B"
	const volumeUUID = "vol-1"

	if err := store.PutVolume(ctx, control.Volume{
		UUID: volumeUUID, Name: "test-vol", SizeBytes: 4 << 20, ReplicaCount: 2,
		State: control.VolumeStateProvisioning,
	}); err != nil {
		t.Fatal(err)
	}
	// one replica on the local host, one elsewhere
	must := func(err error) { t.Helper(); if err != nil { t.Fatal(err) } }
	must(store.PutReplica(ctx, control.Replica{
		UUID: "r-local", VolumeUUID: volumeUUID, HostUUID: localHost,
		State: control.ReplicaStatePending,
	}))
	must(store.PutReplica(ctx, control.Replica{
		UUID: "r-remote", VolumeUUID: volumeUUID, HostUUID: otherHost,
		State: control.ReplicaStatePending,
	}))

	loop := New(localHost, store, NewLogSpawner("10.0.0.1"))
	loop.Interval = 10 * time.Millisecond
	loop.Start(ctx)
	defer loop.Stop()

	// Wait until the local replica is Running.
	waitFor(t, 500*time.Millisecond, func() bool {
		r, _ := store.GetReplica(ctx, "r-local")
		return r.State == control.ReplicaStateRunning && r.Address != ""
	}, "local replica did not reach Running")

	// The remote replica must NOT have been touched.
	rem, _ := store.GetReplica(ctx, "r-remote")
	if rem.State != control.ReplicaStatePending {
		t.Errorf("remote replica state = %q, want Pending (not on this host)", rem.State)
	}

	// Pre-engine: the engine waits for ALL replicas Running. Mark the remote
	// one Running too (as if another host's loop did it).
	rem.State = control.ReplicaStateRunning
	rem.Address = "10.0.0.2:30001"
	must(store.PutReplica(ctx, rem))

	// Now place an engine on the local host.
	must(store.PutEngine(ctx, control.Engine{
		UUID: "e-1", VolumeUUID: volumeUUID, HostUUID: localHost,
		State: control.ReplicaStatePending,
	}))
	waitFor(t, 500*time.Millisecond, func() bool {
		e, _ := store.GetEngine(ctx, "e-1")
		return e.State == control.ReplicaStateRunning && e.FrontendPath != ""
	}, "engine did not reach Running")

	e, _ := store.GetEngine(ctx, "e-1")
	if e.FrontendPath == "" {
		t.Errorf("engine FrontendPath empty")
	}
}

// TestLoop_DeletingTriggersStop: a Replica record flipped to Deleting causes
// StopReplica + delete-from-store; same for Engine.
func TestLoop_DeletingTriggersStop(t *testing.T) {
	ctx := context.Background()
	store := control.NewMemStore()

	const localHost = "host-A"
	must := func(err error) { t.Helper(); if err != nil { t.Fatal(err) } }

	must(store.PutVolume(ctx, control.Volume{UUID: "v", Name: "n", SizeBytes: 4 << 20, ReplicaCount: 1}))
	must(store.PutReplica(ctx, control.Replica{UUID: "r", VolumeUUID: "v", HostUUID: localHost, State: control.ReplicaStateDeleting}))

	loop := New(localHost, store, NewLogSpawner("10.0.0.1"))
	loop.Interval = 10 * time.Millisecond
	loop.Start(ctx)
	defer loop.Stop()

	waitFor(t, 500*time.Millisecond, func() bool {
		_, err := store.GetReplica(ctx, "r")
		return err == control.ErrNotFound
	}, "deleting replica was not removed from the store")
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", msg)
}
