package main

import (
	"context"
	"strings"
	"sync"
	"testing"

	drivers "github.com/openweft/weft-drivers"
)

// fakeFSCoordinator records every freeze/thaw call. Used by the
// driver tests to assert the labels-→-coordinator wiring works.
type fakeFSCoordinator struct {
	mu          sync.Mutex
	freezeCalls int
	thawCalls   int
	lastVM      string
}

func (f *fakeFSCoordinator) Freeze(_ context.Context, vmUUID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.freezeCalls++
	f.lastVM = vmUUID
	return nil
}

func (f *fakeFSCoordinator) Thaw(_ context.Context, vmUUID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thawCalls++
	f.lastVM = vmUUID
	return nil
}

func TestShouldFreezeFS_RecognisesTruthyValues(t *testing.T) {
	cases := []struct {
		name       string
		labels     map[string]string
		wantOn     bool
		wantVMUUID string
	}{
		{"nil-map", nil, false, ""},
		{"empty-map", map[string]string{}, false, ""},
		{"absent", map[string]string{"other": "v"}, false, ""},
		{"false-string", map[string]string{"freeze_fs": "false", "vm_uuid": "abc"}, false, ""},
		{"true-lower", map[string]string{"freeze_fs": "true", "vm_uuid": "abc"}, true, "abc"},
		{"TRUE-upper", map[string]string{"freeze_fs": "TRUE", "vm_uuid": "abc"}, true, "abc"},
		{"yes", map[string]string{"freeze_fs": "yes", "vm_uuid": "abc"}, true, "abc"},
		{"one", map[string]string{"freeze_fs": "1", "vm_uuid": "abc"}, true, "abc"},
		{"on", map[string]string{"freeze_fs": "on", "vm_uuid": "abc"}, true, "abc"},
		{"true-but-missing-vm", map[string]string{"freeze_fs": "true"}, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			on, vm := shouldFreezeFS(tc.labels)
			if on != tc.wantOn {
				t.Errorf("on : got %v, want %v", on, tc.wantOn)
			}
			if vm != tc.wantVMUUID {
				t.Errorf("vm : got %q, want %q", vm, tc.wantVMUUID)
			}
		})
	}
}

func TestNoopFSCoordinator_AlwaysSucceeds(t *testing.T) {
	c := NoopFSCoordinator{}
	if err := c.Freeze(context.Background(), "anything"); err != nil {
		t.Errorf("Freeze: %v", err)
	}
	if err := c.Thaw(context.Background(), "anything"); err != nil {
		t.Errorf("Thaw: %v", err)
	}
}

func TestNATSFSCoordinator_RejectsWhenMisconfigured(t *testing.T) {
	c := &NATSFSCoordinator{} // no NC
	if err := c.Freeze(context.Background(), "abc"); err == nil {
		t.Error("Freeze must error when NATS conn is nil")
	}
	if err := c.Thaw(context.Background(), "abc"); err == nil {
		t.Error("Thaw must error when NATS conn is nil")
	}
}

func TestFakeCoordinator_CountsCalls(t *testing.T) {
	// Sanity check on the test double itself.
	f := &fakeFSCoordinator{}
	_ = f.Freeze(context.Background(), "vm-a")
	_ = f.Freeze(context.Background(), "vm-b")
	_ = f.Thaw(context.Background(), "vm-b")
	if f.freezeCalls != 2 {
		t.Errorf("freezeCalls : got %d, want 2", f.freezeCalls)
	}
	if f.thawCalls != 1 {
		t.Errorf("thawCalls : got %d, want 1", f.thawCalls)
	}
	if f.lastVM != "vm-b" {
		t.Errorf("lastVM : got %q, want vm-b", f.lastVM)
	}
}

// The label-validation tests below use the volumeDriver directly
// with a fake coordinator. They short-circuit BEFORE touching the
// engine (no spawner/store wiring needed), proving the freeze
// pre-check sequence is correct.

func TestVolumeDriver_FreezeLabel_RequiresVMUUID(t *testing.T) {
	v := &volumeDriver{fsCoord: &fakeFSCoordinator{}}
	_, err := v.CreateSnapshot(context.Background(), drivers.SnapshotSpec{
		VolumeUUID: "doesnt-matter",
		Name:       "snap-1",
		Labels:     map[string]string{"freeze_fs": "true"},
	})
	if err == nil {
		t.Fatal("expected error when freeze_fs=true without vm_uuid")
	}
	if !strings.Contains(err.Error(), "vm_uuid") {
		t.Errorf("error should mention vm_uuid, got: %v", err)
	}
}

func TestVolumeDriver_FreezeLabel_RequiresCoordinator(t *testing.T) {
	v := &volumeDriver{fsCoord: nil}
	_, err := v.CreateSnapshot(context.Background(), drivers.SnapshotSpec{
		VolumeUUID: "doesnt-matter",
		Name:       "snap-1",
		Labels: map[string]string{
			"freeze_fs": "true",
			"vm_uuid":   "vm-1",
		},
	})
	if err == nil {
		t.Fatal("expected error when coordinator is nil")
	}
	if !strings.Contains(err.Error(), "FS coordinator") {
		t.Errorf("error should mention FS coordinator, got: %v", err)
	}
}

// Freeze-error propagation is covered end-to-end by the multihost
// integration tests where a real engine + spawner are wired ; a
// unit test would need to mock the engine handle in addition to
// the coordinator. The Freeze error path is one line
// (return Snapshot{}, fmt.Errorf("weft-block: guest fs freeze: %w", err))
// so the loss of unit coverage is minimal and the validation
// pre-checks above already exercise the early-exit branches.

func TestSetFSCoordinator_NilDefaultsToNoop(t *testing.T) {
	v := &volumeDriver{}
	v.SetFSCoordinator(nil)
	if _, ok := v.fsCoord.(NoopFSCoordinator); !ok {
		t.Errorf("nil coord should default to NoopFSCoordinator, got %T", v.fsCoord)
	}
}
