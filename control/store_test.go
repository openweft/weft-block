package control

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemStore_VolumeCRUD(t *testing.T) {
	s := NewMemStore()
	frozen := time.Unix(1700000000, 0)
	s.SetClock(func() time.Time { return frozen })
	ctx := context.Background()

	if _, err := s.GetVolume(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing volume should ErrNotFound, got %v", err)
	}

	v := Volume{UUID: "v1", Name: "data", SizeBytes: 1 << 30, ReplicaCount: 3, State: VolumeStateProvisioning}
	if err := s.PutVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetVolume(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != VolumeStateProvisioning || got.SizeBytes != 1<<30 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if !got.CreatedAt.Equal(frozen) || !got.UpdatedAt.Equal(frozen) {
		t.Errorf("timestamps not stamped: %+v", got)
	}

	// Update bumps UpdatedAt but preserves CreatedAt.
	later := frozen.Add(time.Hour)
	s.SetClock(func() time.Time { return later })
	got.State = VolumeStateHealthy
	if err := s.PutVolume(ctx, got); err != nil {
		t.Fatal(err)
	}
	got2, _ := s.GetVolume(ctx, "v1")
	if !got2.CreatedAt.Equal(frozen) || !got2.UpdatedAt.Equal(later) {
		t.Errorf("update timestamps wrong: created=%v updated=%v", got2.CreatedAt, got2.UpdatedAt)
	}

	if err := s.DeleteVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetVolume(ctx, "v1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("post-delete should ErrNotFound, got %v", err)
	}
	if err := s.DeleteVolume(ctx, "v1"); err != nil {
		t.Errorf("delete missing should be idempotent: %v", err)
	}
}

func TestMemStore_PutVolumeRejectsEmpty(t *testing.T) {
	s := NewMemStore()
	if err := s.PutVolume(context.Background(), Volume{}); err == nil {
		t.Error("empty UUID should error")
	}
}

func TestMemStore_ReplicasFilterByVolume(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	_ = s.PutReplica(ctx, Replica{UUID: "r1", VolumeUUID: "vA", HostUUID: "h1"})
	_ = s.PutReplica(ctx, Replica{UUID: "r2", VolumeUUID: "vA", HostUUID: "h2"})
	_ = s.PutReplica(ctx, Replica{UUID: "r3", VolumeUUID: "vB", HostUUID: "h1"})

	rA, _ := s.ListReplicasFor(ctx, "vA")
	if len(rA) != 2 {
		t.Errorf("vA replicas = %d, want 2", len(rA))
	}
	rB, _ := s.ListReplicasFor(ctx, "vB")
	if len(rB) != 1 {
		t.Errorf("vB replicas = %d, want 1", len(rB))
	}
	rC, _ := s.ListReplicasFor(ctx, "missing")
	if len(rC) != 0 {
		t.Errorf("unknown volume should yield 0 replicas, got %d", len(rC))
	}
}
