package control

import (
	"fmt"
	"sort"
)

// HostCandidate describes a host the scheduler may place a replica on. The
// adapter populates this from the Host registry (DC = availability zone).
type HostCandidate struct {
	UUID string
	DC   string
}

// PlaceReplicas picks `count` hosts to spread the replicas across, preferring
// distinct DCs. This is the Longhorn rule, simplified: never put two replicas
// on the same host; spread across DCs first, fall back to additional hosts in
// the same DCs only when count > len(distinct DCs).
//
// Deterministic: candidates are sorted by (DC, UUID) before picking, so the
// same input always yields the same placement. That's what makes the planner
// in driver.go safe to re-run as a convergence loop.
func PlaceReplicas(candidates []HostCandidate, count int) ([]HostCandidate, error) {
	if count <= 0 {
		return nil, fmt.Errorf("placement: replica count must be >= 1, got %d", count)
	}
	if len(candidates) < count {
		return nil, fmt.Errorf("placement: need %d hosts, have %d", count, len(candidates))
	}

	pool := append([]HostCandidate(nil), candidates...)
	sort.Slice(pool, func(i, j int) bool {
		if pool[i].DC != pool[j].DC {
			return pool[i].DC < pool[j].DC
		}
		return pool[i].UUID < pool[j].UUID
	})

	// First pass: one per DC, in (DC, UUID) order — spreads replicas across AZs.
	usedDC := map[string]bool{}
	usedHost := map[string]bool{}
	picked := make([]HostCandidate, 0, count)
	for _, h := range pool {
		if len(picked) == count {
			break
		}
		if usedDC[h.DC] {
			continue
		}
		picked = append(picked, h)
		usedDC[h.DC] = true
		usedHost[h.UUID] = true
	}

	// Second pass (only if count > #distinct DCs): fill from remaining hosts,
	// still excluding already-picked ones, preserving the (DC, UUID) order so
	// the result is deterministic.
	for _, h := range pool {
		if len(picked) == count {
			break
		}
		if usedHost[h.UUID] {
			continue
		}
		picked = append(picked, h)
		usedHost[h.UUID] = true
	}

	if len(picked) != count {
		return nil, fmt.Errorf("placement: could only place %d/%d replicas", len(picked), count)
	}
	return picked, nil
}
