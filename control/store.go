package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNotFound is returned by VolumeStore.GetVolume / GetReplica when the UUID
// is unknown. Callers can errors.Is on it to distinguish lookup misses from
// other (transient) store failures.
var ErrNotFound = errors.New("weft-block: not found")

// VolumeStore is the persistence boundary for the control plane. The
// production binding is etcd (the embedded one weft already runs); this
// interface exists so the driver, the placement logic, and the reconcile
// loops are testable against an in-memory store, and so the etcd binding
// drops in later without touching call sites.
//
// Contract:
//   - PutVolume / PutReplica / PutEngine upsert by UUID and bump UpdatedAt.
//   - List* return a snapshot (callers must not mutate the slices).
//   - Delete* are idempotent — deleting a missing UUID returns nil.
//   - Reads/writes inside one method are atomic, but the store does NOT
//     provide multi-record transactions; the driver's convergence logic
//     handles intermediate states (Pending replicas, …).
type VolumeStore interface {
	PutVolume(ctx context.Context, v Volume) error
	GetVolume(ctx context.Context, uuid string) (Volume, error)
	DeleteVolume(ctx context.Context, uuid string) error
	ListVolumes(ctx context.Context) ([]Volume, error)

	PutReplica(ctx context.Context, r Replica) error
	GetReplica(ctx context.Context, uuid string) (Replica, error)
	DeleteReplica(ctx context.Context, uuid string) error
	ListReplicasFor(ctx context.Context, volumeUUID string) ([]Replica, error)

	PutEngine(ctx context.Context, e Engine) error
	GetEngine(ctx context.Context, uuid string) (Engine, error)
	DeleteEngine(ctx context.Context, uuid string) error
	ListEnginesFor(ctx context.Context, volumeUUID string) ([]Engine, error)
}

// MemStore is an in-memory VolumeStore. It exists for tests and for the
// single-process dev path; production wires the etcd-backed store next slice.
type MemStore struct {
	mu       sync.Mutex
	now      func() time.Time
	volumes  map[string]Volume
	replicas map[string]Replica
	engines  map[string]Engine
}

func NewMemStore() *MemStore {
	return &MemStore{
		now:      time.Now,
		volumes:  map[string]Volume{},
		replicas: map[string]Replica{},
		engines:  map[string]Engine{},
	}
}

// SetClock lets tests freeze time. The production code never calls this.
func (s *MemStore) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

var _ VolumeStore = (*MemStore)(nil)

func (s *MemStore) PutVolume(_ context.Context, v Volume) error {
	if v.UUID == "" {
		return fmt.Errorf("PutVolume: empty UUID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v.UpdatedAt = s.now()
	if cur, ok := s.volumes[v.UUID]; ok {
		v.CreatedAt = cur.CreatedAt
	} else {
		v.CreatedAt = v.UpdatedAt
	}
	s.volumes[v.UUID] = v
	return nil
}

func (s *MemStore) GetVolume(_ context.Context, uuid string) (Volume, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.volumes[uuid]
	if !ok {
		return Volume{}, ErrNotFound
	}
	return v, nil
}

func (s *MemStore) DeleteVolume(_ context.Context, uuid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.volumes, uuid)
	return nil
}

func (s *MemStore) ListVolumes(_ context.Context) ([]Volume, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Volume, 0, len(s.volumes))
	for _, v := range s.volumes {
		out = append(out, v)
	}
	return out, nil
}

func (s *MemStore) PutReplica(_ context.Context, r Replica) error {
	if r.UUID == "" || r.VolumeUUID == "" {
		return fmt.Errorf("PutReplica: UUID and VolumeUUID required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r.UpdatedAt = s.now()
	if cur, ok := s.replicas[r.UUID]; ok {
		r.CreatedAt = cur.CreatedAt
	} else {
		r.CreatedAt = r.UpdatedAt
	}
	s.replicas[r.UUID] = r
	return nil
}

func (s *MemStore) GetReplica(_ context.Context, uuid string) (Replica, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.replicas[uuid]
	if !ok {
		return Replica{}, ErrNotFound
	}
	return r, nil
}

func (s *MemStore) DeleteReplica(_ context.Context, uuid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.replicas, uuid)
	return nil
}

func (s *MemStore) ListReplicasFor(_ context.Context, volumeUUID string) ([]Replica, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Replica
	for _, r := range s.replicas {
		if r.VolumeUUID == volumeUUID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *MemStore) PutEngine(_ context.Context, e Engine) error {
	if e.UUID == "" || e.VolumeUUID == "" {
		return fmt.Errorf("PutEngine: UUID and VolumeUUID required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e.UpdatedAt = s.now()
	if cur, ok := s.engines[e.UUID]; ok {
		e.CreatedAt = cur.CreatedAt
	} else {
		e.CreatedAt = e.UpdatedAt
	}
	s.engines[e.UUID] = e
	return nil
}

func (s *MemStore) GetEngine(_ context.Context, uuid string) (Engine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.engines[uuid]
	if !ok {
		return Engine{}, ErrNotFound
	}
	return e, nil
}

func (s *MemStore) DeleteEngine(_ context.Context, uuid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.engines, uuid)
	return nil
}

func (s *MemStore) ListEnginesFor(_ context.Context, volumeUUID string) ([]Engine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Engine
	for _, e := range s.engines {
		if e.VolumeUUID == volumeUUID {
			out = append(out, e)
		}
	}
	return out, nil
}
