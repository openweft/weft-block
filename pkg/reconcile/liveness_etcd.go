package reconcile

// liveness_etcd.go implements the etcd binding of LivenessProvider.
// weft-agent writes a host-liveness lease key at /weft/coord/hosts/
// <host_uuid> ; this scanner reads the prefix on every call and
// returns the surviving host UUIDs. The PlacementCandidates list is
// derived the same way — we don't have host-capacity metadata
// available in the same prefix yet, so the v1 binding just hands
// every live host as a candidate with empty DC ; PlaceReplicas's
// fallback path handles that (single-DC placement, no AZ-aware
// spread, but the volume IS re-replicated).
//
// Wiring : cmd/weft-block/main.go constructs an *EtcdLiveness when
// WEFT_BLOCK_ETCD_ENDPOINTS is set + assigns the bound function to
// Loop.Liveness. MemStore deployments (single-host dev) skip this
// — Loop.Liveness stays nil, the rereplicate sweep is disabled,
// behaviour is bit-identical to v0.1.x.

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/openweft/weft-block/control"
)

// EtcdLiveness reads /weft/coord/hosts/ from the cluster's etcd
// (the same one weft-agent uses for its own liveness leases). The
// per-call timeout caps how long the rereplicate sweep waits for
// etcd on a slow control plane ; defaults to 2s, configurable via
// the Timeout field.
type EtcdLiveness struct {
	Client  *clientv3.Client
	Prefix  string        // defaults to "/weft/coord/hosts/"
	Timeout time.Duration // defaults to 2s
}

// NewEtcdLiveness builds a LivenessProvider closure suitable for
// Loop.Liveness. The closure reads the prefix on every call ; the
// host-list is small (operator-scale clusters have at most a few
// thousand) so a fresh read per sweep keeps the implementation
// trivial. A future caching layer can sit in front if needed.
func NewEtcdLiveness(cli *clientv3.Client) LivenessProvider {
	if cli == nil {
		return nil
	}
	e := &EtcdLiveness{Client: cli}
	return e.Get
}

// Get is the LivenessProvider entry point.
func (e *EtcdLiveness) Get(ctx context.Context) (control.HostLiveness, error) {
	prefix := e.Prefix
	if prefix == "" {
		prefix = "/weft/coord/hosts/"
	}
	timeout := e.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := e.Client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return control.HostLiveness{}, fmt.Errorf("etcd get %s: %w", prefix, err)
	}
	uuids := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		uuid := strings.TrimPrefix(key, prefix)
		// Defensive against accidental nesting under the prefix.
		if uuid == "" || strings.Contains(uuid, "/") {
			continue
		}
		uuids = append(uuids, uuid)
	}
	candidates := make([]control.HostCandidate, len(uuids))
	for i, u := range uuids {
		candidates[i] = control.HostCandidate{UUID: u}
	}
	return control.HostLiveness{
		LiveHostUUIDs:       uuids,
		PlacementCandidates: candidates,
	}, nil
}
