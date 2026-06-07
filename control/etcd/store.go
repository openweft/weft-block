// Package etcd is the production binding of control.VolumeStore: every Put /
// Get / Delete / List is one etcd v3 KV operation against the same etcd
// cluster weft already runs (embedded in single-node dev mode, external in
// HA). Records are JSON-serialised at stable per-type prefixes:
//
//	/weft-block/volumes/<uuid>   →  control.Volume
//	/weft-block/replicas/<uuid>  →  control.Replica
//	/weft-block/engines/<uuid>   →  control.Engine
//
// Multiple weft-block plugin instances (one per host) share the same store —
// reconcile loops on each host see the records placed there by any plugin's
// EnsureVolume / AttachVolume and converge independently. List operations
// that filter by VolumeUUID (ListReplicasFor / ListEnginesFor) do a prefix-
// range read and filter in memory — fine up to tens of thousands of records;
// a secondary index can land later if needed.
package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/openweft/weft-block/control"
)

// Key prefixes. Trailing slash is deliberate so a Range with the prefix
// matches all UUIDs under it but not /weft-block/<type>-* siblings.
const (
	PrefixVolumes  = "/weft-block/volumes/"
	PrefixReplicas = "/weft-block/replicas/"
	PrefixEngines  = "/weft-block/engines/"
)

// Store is the etcd-backed VolumeStore. One client is enough for the
// lifetime of a weft-block plugin process; reads + writes share it.
type Store struct {
	cli *clientv3.Client
	now func() time.Time // overridable for tests
}

// New wraps cli in a Store. The caller owns cli (closes it at shutdown).
func New(cli *clientv3.Client) *Store { return &Store{cli: cli, now: time.Now} }

// SetClock lets tests freeze time. Production code never calls this.
func (s *Store) SetClock(now func() time.Time) { s.now = now }

var _ control.VolumeStore = (*Store)(nil)

// ----- Volume ----------------------------------------------------------------

func volumeKey(uuid string) string { return PrefixVolumes + uuid }

func (s *Store) PutVolume(ctx context.Context, v control.Volume) error {
	if v.UUID == "" {
		return fmt.Errorf("PutVolume: empty UUID")
	}
	v.UpdatedAt = s.now()
	cur, err := s.GetVolume(ctx, v.UUID)
	if err == nil {
		v.CreatedAt = cur.CreatedAt
	} else if err == control.ErrNotFound {
		v.CreatedAt = v.UpdatedAt
	} else {
		return err
	}
	return s.putJSON(ctx, volumeKey(v.UUID), v)
}

func (s *Store) GetVolume(ctx context.Context, uuid string) (control.Volume, error) {
	var v control.Volume
	if err := s.getJSON(ctx, volumeKey(uuid), &v); err != nil {
		return control.Volume{}, err
	}
	return v, nil
}

func (s *Store) DeleteVolume(ctx context.Context, uuid string) error {
	_, err := s.cli.Delete(ctx, volumeKey(uuid))
	return err
}

func (s *Store) ListVolumes(ctx context.Context) ([]control.Volume, error) {
	resp, err := s.cli.Get(ctx, PrefixVolumes, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]control.Volume, 0, resp.Count)
	for _, kv := range resp.Kvs {
		var v control.Volume
		if err := json.Unmarshal(kv.Value, &v); err != nil {
			return nil, fmt.Errorf("decode %s: %w", kv.Key, err)
		}
		out = append(out, v)
	}
	return out, nil
}

// ----- Replica ---------------------------------------------------------------

func replicaKey(uuid string) string { return PrefixReplicas + uuid }

func (s *Store) PutReplica(ctx context.Context, r control.Replica) error {
	if r.UUID == "" || r.VolumeUUID == "" {
		return fmt.Errorf("PutReplica: UUID and VolumeUUID required")
	}
	r.UpdatedAt = s.now()
	cur, err := s.GetReplica(ctx, r.UUID)
	if err == nil {
		r.CreatedAt = cur.CreatedAt
	} else if err == control.ErrNotFound {
		r.CreatedAt = r.UpdatedAt
	} else {
		return err
	}
	return s.putJSON(ctx, replicaKey(r.UUID), r)
}

func (s *Store) GetReplica(ctx context.Context, uuid string) (control.Replica, error) {
	var r control.Replica
	if err := s.getJSON(ctx, replicaKey(uuid), &r); err != nil {
		return control.Replica{}, err
	}
	return r, nil
}

func (s *Store) DeleteReplica(ctx context.Context, uuid string) error {
	_, err := s.cli.Delete(ctx, replicaKey(uuid))
	return err
}

func (s *Store) ListReplicasFor(ctx context.Context, volumeUUID string) ([]control.Replica, error) {
	resp, err := s.cli.Get(ctx, PrefixReplicas, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []control.Replica
	for _, kv := range resp.Kvs {
		var r control.Replica
		if err := json.Unmarshal(kv.Value, &r); err != nil {
			return nil, fmt.Errorf("decode %s: %w", kv.Key, err)
		}
		if r.VolumeUUID == volumeUUID {
			out = append(out, r)
		}
	}
	return out, nil
}

// ----- Engine ----------------------------------------------------------------

func engineKey(uuid string) string { return PrefixEngines + uuid }

func (s *Store) PutEngine(ctx context.Context, e control.Engine) error {
	if e.UUID == "" || e.VolumeUUID == "" {
		return fmt.Errorf("PutEngine: UUID and VolumeUUID required")
	}
	e.UpdatedAt = s.now()
	cur, err := s.GetEngine(ctx, e.UUID)
	if err == nil {
		e.CreatedAt = cur.CreatedAt
	} else if err == control.ErrNotFound {
		e.CreatedAt = e.UpdatedAt
	} else {
		return err
	}
	return s.putJSON(ctx, engineKey(e.UUID), e)
}

func (s *Store) GetEngine(ctx context.Context, uuid string) (control.Engine, error) {
	var e control.Engine
	if err := s.getJSON(ctx, engineKey(uuid), &e); err != nil {
		return control.Engine{}, err
	}
	return e, nil
}

func (s *Store) DeleteEngine(ctx context.Context, uuid string) error {
	_, err := s.cli.Delete(ctx, engineKey(uuid))
	return err
}

func (s *Store) ListEnginesFor(ctx context.Context, volumeUUID string) ([]control.Engine, error) {
	resp, err := s.cli.Get(ctx, PrefixEngines, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []control.Engine
	for _, kv := range resp.Kvs {
		var e control.Engine
		if err := json.Unmarshal(kv.Value, &e); err != nil {
			return nil, fmt.Errorf("decode %s: %w", kv.Key, err)
		}
		if e.VolumeUUID == volumeUUID {
			out = append(out, e)
		}
	}
	return out, nil
}

// ----- internals -------------------------------------------------------------

func (s *Store) putJSON(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	_, err = s.cli.Put(ctx, key, string(b))
	return err
}

func (s *Store) getJSON(ctx context.Context, key string, dst any) error {
	resp, err := s.cli.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return control.ErrNotFound
	}
	if err := json.Unmarshal(resp.Kvs[0].Value, dst); err != nil {
		return fmt.Errorf("decode %s: %w", key, err)
	}
	return nil
}

// KeyHasPrefix is exported for the dev/ops tooling that lists or repairs
// keys directly via etcdctl wrappers; the production reconcile path doesn't
// need it.
func KeyHasPrefix(k, prefix string) bool { return strings.HasPrefix(k, prefix) }
