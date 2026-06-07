package control

import (
	"strings"
	"testing"
)

func cand(uuid, dc string) HostCandidate { return HostCandidate{UUID: uuid, DC: dc} }

// uuids returns the placement as a sorted, comma-joined string for stable
// comparison — placement is deterministic, but ordering inside a DC level is
// alphabetic, which matters for the assertions.
func uuids(in []HostCandidate) string {
	var s []string
	for _, c := range in {
		s = append(s, c.UUID)
	}
	return strings.Join(s, ",")
}

func TestPlaceReplicas_OnePerDCFirst(t *testing.T) {
	hs := []HostCandidate{cand("h1", "dc1"), cand("h2", "dc2"), cand("h3", "dc3")}
	got, err := PlaceReplicas(hs, 3)
	if err != nil {
		t.Fatal(err)
	}
	if uuids(got) != "h1,h2,h3" {
		t.Errorf("got %s, want h1,h2,h3", uuids(got))
	}
}

func TestPlaceReplicas_SingleHostSingleNode(t *testing.T) {
	got, err := PlaceReplicas([]HostCandidate{cand("solo", "dc1")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if uuids(got) != "solo" {
		t.Errorf("got %s", uuids(got))
	}
}

// When count exceeds DC count, fall back to filling extra hosts within DCs —
// still deterministic by (DC, UUID).
func TestPlaceReplicas_FallbackWithinDC(t *testing.T) {
	hs := []HostCandidate{
		cand("h1a", "dc1"), cand("h1b", "dc1"),
		cand("h2a", "dc2"),
	}
	got, err := PlaceReplicas(hs, 3)
	if err != nil {
		t.Fatal(err)
	}
	// First pass picks one per DC: h1a (dc1), h2a (dc2). Second pass fills with
	// the remaining dc1 host (h1b).
	if uuids(got) != "h1a,h2a,h1b" {
		t.Errorf("got %s, want h1a,h2a,h1b", uuids(got))
	}
}

func TestPlaceReplicas_NotEnoughHosts(t *testing.T) {
	if _, err := PlaceReplicas([]HostCandidate{cand("h1", "dc1")}, 3); err == nil {
		t.Error("expected error when fewer hosts than replicas")
	}
}

func TestPlaceReplicas_InvalidCount(t *testing.T) {
	if _, err := PlaceReplicas([]HostCandidate{cand("h1", "dc1")}, 0); err == nil {
		t.Error("count=0 should error")
	}
}

// Determinism: re-running with a permuted input must yield the same placement.
func TestPlaceReplicas_Deterministic(t *testing.T) {
	a := []HostCandidate{cand("h3", "dc3"), cand("h1", "dc1"), cand("h2", "dc2")}
	b := []HostCandidate{cand("h1", "dc1"), cand("h2", "dc2"), cand("h3", "dc3")}
	ra, _ := PlaceReplicas(a, 3)
	rb, _ := PlaceReplicas(b, 3)
	if uuids(ra) != uuids(rb) {
		t.Errorf("non-deterministic: %s vs %s", uuids(ra), uuids(rb))
	}
}
