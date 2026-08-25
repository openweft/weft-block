package control

// rereplicate.go closes the v0.1.x gap : when a host dies, its
// replicas are orphaned. The per-host reconcile loop on each
// surviving host iterates over `HostUUID == loop.HostUUID` ; it
// never even SEES the dead host's replicas. Without an external
// trigger they sit Faulted forever and the volume stays Degraded
// until an operator intervenes manually.
//
// RereplicateOrphans is the policy : sweep the volume store,
// detect replicas pinned to non-live hosts, mark them Faulted,
// and for each volume that now has too few healthy replicas
// spawn a fresh Pending replica on a candidate host that isn't
// already hosting one.
//
// Owner election : each agent calls this in its reconcile tick,
// but only acts on volumes for which it's the LEXICALLY-LOWEST
// surviving host among the volume's running replicas. That gives
// a deterministic single-driver per volume without any etcd
// coordination — same surviving topology = same driver = no
// duplicate Pending replicas. Three agents running this in
// parallel reach the same conclusion ; only one writes.

import (
	"context"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
)

// HostLiveness is the narrow snapshot RereplicateOrphans consumes.
// Decoupled from any concrete source (etcd lease scan, gRPC pull,
// hand-rolled stub for tests) — tests inject a literal slice + the
// production agent populates it from /weft/coord/hosts/ scan in its
// reconcile tick.
type HostLiveness struct {
	// LiveHostUUIDs is the set of hosts that currently hold an
	// active liveness lease. Order doesn't matter ; uniqueness
	// isn't required either (defensive de-dup inside the policy).
	LiveHostUUIDs []string
	// PlacementCandidates is the pool the policy considers when
	// it needs to schedule a NEW replica to replace an orphaned
	// one. Caller decides whether to pass HostCandidate records
	// with capacity / DC labels — PlaceReplicas runs on what we
	// hand it. A nil / empty slice means "skip the spawn step"
	// (we still mark faulted replicas, just don't schedule
	// replacements) so a partial-knowledge reconciler is safe.
	PlacementCandidates []HostCandidate
}

// liveSet builds a string-set from LiveHostUUIDs for O(1) lookups.
func (h HostLiveness) liveSet() map[string]struct{} {
	out := make(map[string]struct{}, len(h.LiveHostUUIDs))
	for _, u := range h.LiveHostUUIDs {
		out[u] = struct{}{}
	}
	return out
}

// RereplicateStats summarises what one Sweep call did. Caller logs
// it ; metrics surface it to Prometheus.
type RereplicateStats struct {
	VolumesScanned         int
	OrphansFaulted         int
	ReplicasScheduled      int
	VolumesSkippedNotOwner int // we weren't the lexically-lowest survivor
	VolumesShortNoHost     int // no placement candidate available
}

// RereplicateOrphans is the entry point. Pure function over the
// VolumeStore + liveness snapshot — testable without a real etcd.
//
// localHostUUID identifies the caller for the owner-election step.
// Pass the host this agent is bound to ; an empty string means
// "act on everything" (test-only escape hatch).
func RereplicateOrphans(
	ctx context.Context,
	store VolumeStore,
	live HostLiveness,
	localHostUUID string,
	log logrus.FieldLogger,
) (RereplicateStats, error) {
	var stats RereplicateStats
	vols, err := store.ListVolumes(ctx)
	if err != nil {
		return stats, fmt.Errorf("list volumes: %w", err)
	}
	stats.VolumesScanned = len(vols)
	liveSet := live.liveSet()
	for _, v := range vols {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		processVolume(ctx, store, v, liveSet, live.PlacementCandidates, localHostUUID, log, &stats)
	}
	return stats, nil
}

// processVolume is the per-volume slice of the sweep. Extracted so
// the loop above stays linear ; the owner-election + faulting +
// scheduling steps share the same `stats` shape.
func processVolume(
	ctx context.Context,
	store VolumeStore,
	v Volume,
	liveSet map[string]struct{},
	placement []HostCandidate,
	localHostUUID string,
	log logrus.FieldLogger,
	stats *RereplicateStats,
) {
	rs, err := store.ListReplicasFor(ctx, v.UUID)
	if err != nil {
		log.WithError(err).WithField("volume", v.UUID).Warn("rereplicate: list replicas")
		return
	}

	// Partition replicas into "alive on a live host" vs. "orphaned"
	// (Running or Pending state, but the host is gone). Faulted
	// replicas already faulted are left as-is — they're handled
	// by the standard reconcile state machine.
	var aliveReplicas []Replica
	var orphans []Replica
	usedHosts := map[string]struct{}{}
	for _, r := range rs {
		_, hostLive := liveSet[r.HostUUID]
		switch r.State {
		case ReplicaStateRunning, ReplicaStatePending:
			if hostLive {
				aliveReplicas = append(aliveReplicas, r)
				usedHosts[r.HostUUID] = struct{}{}
			} else {
				orphans = append(orphans, r)
			}
		case ReplicaStateFaulted:
			// Faulted on a now-dead host : still counts as
			// occupying that slot, but we don't re-fault it.
			if !hostLive {
				orphans = append(orphans, r)
			}
		case ReplicaStateDeleting:
			// Intentional teardown ; leave alone.
		}
	}

	if len(orphans) == 0 {
		return
	}

	// Owner election : the lexically-lowest LIVE host among the
	// alive replicas drives the rebuild. Localhost might not host
	// ANY replica for this volume ; in that case we still want
	// SOMEONE to drive — pick the lexically-lowest live host
	// overall as a fallback. This deterministic choice means
	// every agent reaches the same conclusion ; only one writes.
	var ownerHost string
	if len(aliveReplicas) > 0 {
		hosts := make([]string, 0, len(usedHosts))
		for h := range usedHosts {
			hosts = append(hosts, h)
		}
		sort.Strings(hosts)
		ownerHost = hosts[0]
	} else {
		// All replicas orphaned. Fallback : lexically-lowest live
		// host cluster-wide ; that's the only deterministic choice.
		live := make([]string, 0, len(liveSet))
		for h := range liveSet {
			live = append(live, h)
		}
		sort.Strings(live)
		if len(live) == 0 {
			// No live host at all — nobody can recover. Caller
			// already saw it (or will). Skip.
			return
		}
		ownerHost = live[0]
	}

	if localHostUUID != "" && localHostUUID != ownerHost {
		stats.VolumesSkippedNotOwner++
		return
	}

	// Mark orphaned non-faulted replicas as Faulted so the rest of
	// the state machine and operator queries reflect reality.
	for _, r := range orphans {
		if r.State == ReplicaStateFaulted {
			continue
		}
		r.State = ReplicaStateFaulted
		if err := store.PutReplica(ctx, r); err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"volume": v.UUID, "replica": r.UUID,
			}).Warn("rereplicate: mark faulted")
			continue
		}
		stats.OrphansFaulted++
	}

	// Decide how many new replicas to schedule. We aim to bring the
	// count of (Running|Pending on a live host) up to v.ReplicaCount,
	// not above. Volumes that already have at least the desired
	// count (e.g. operator over-replicated by hand) get nothing new.
	deficit := v.ReplicaCount - len(aliveReplicas)
	if deficit <= 0 {
		return
	}

	// Pick fresh hosts : exclude every host that's already hosting
	// any kind of replica for this volume (live or orphaned). That
	// keeps the volume's host-set diverse + avoids landing a new
	// replica on the same host as a Faulted-but-recoverable one.
	excluded := map[string]struct{}{}
	for _, r := range rs {
		excluded[r.HostUUID] = struct{}{}
	}
	candidates := make([]HostCandidate, 0, len(placement))
	for _, c := range placement {
		if _, blocked := excluded[c.UUID]; blocked {
			continue
		}
		if _, live := liveSet[c.UUID]; !live {
			continue
		}
		candidates = append(candidates, c)
	}
	if len(candidates) < deficit {
		// Not enough room to fully heal. Schedule what we CAN ;
		// the next sweep will re-run when more hosts come up.
		stats.VolumesShortNoHost++
		if len(candidates) == 0 {
			return
		}
	}

	picks, err := PlaceReplicas(candidates, min(deficit, len(candidates)))
	if err != nil {
		// PlaceReplicas refuses when there aren't enough candidates ;
		// already accounted for via VolumesShortNoHost above.
		log.WithError(err).WithField("volume", v.UUID).Warn("rereplicate: place replicas")
		return
	}
	for _, p := range picks {
		newReplica := Replica{
			UUID:       newReplicaUUID(v.UUID, p.UUID),
			VolumeUUID: v.UUID,
			HostUUID:   p.UUID,
			DC:         p.DC,
			State:      ReplicaStatePending,
			// Bootstrap from the volume's CoW source if any.
			SourceVolumeUUID: v.SourceVolumeUUID,
			SourceSnapshot:   v.SourceSnapshot,
		}
		if err := store.PutReplica(ctx, newReplica); err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"volume": v.UUID, "host": p.UUID,
			}).Warn("rereplicate: put new replica")
			continue
		}
		stats.ReplicasScheduled++
		log.WithFields(logrus.Fields{
			"volume":  v.UUID,
			"host":    p.UUID,
			"replica": newReplica.UUID,
			"dc":      p.DC,
		}).Info("rereplicate: scheduled replacement replica")
	}
}

// newReplicaUUID is a deterministic per-(volume, host) replica id.
// Determinism matters : if two agents race RereplicateOrphans for
// the same volume in the brief window before the owner-election
// settles, they'd both compute the SAME UUID for the SAME placement
// pick and PutReplica idempotently. The owner-election makes the
// race extremely unlikely ; this is belt-and-braces.
func newReplicaUUID(volumeUUID, hostUUID string) string {
	return fmt.Sprintf("%s-replacement-%s", volumeUUID, hostUUID)
}

// min returns the smaller of a, b. Standalone helper — Go 1.26
// has builtin min(), but we keep this for clarity at the call site
// (`min(deficit, len(candidates))` reads either way).
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
