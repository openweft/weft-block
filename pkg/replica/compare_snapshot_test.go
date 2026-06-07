//go:build linux

package replica

// compare_snapshot_test.go — Linux-only test for the public
// Replica.CompareSnapshot helper added alongside the weftsnap Compare
// RPC wiring. Validates the diff-walking behaviour end-to-end against
// the real sector→file location table :
//
//   1. Construct a Replica with a small sector size + a small head.
//   2. Snapshot empty state ("snap-0") + write 1 sector of A's.
//   3. Snapshot ("snap-1") + overwrite the same sector with B's,
//      write another sector with C's, leave the rest untouched.
//   4. Call CompareSnapshot("volume-snap-snap-1.img",
//      "volume-snap-snap-0.img", blockSize).
//   5. Verify the returned ranges cover exactly the modified sectors
//      and nothing else.
//
// Build-tagged "linux" because pkg/replica brings in sparse-tools +
// fibmap which use Linux-only syscalls (Stat_t.Ctim, Fallocate).

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/openweft/weft-block/pkg/types"
)

func TestCompareSnapshot_DiffWindows(t *testing.T) {
	dir, err := os.MkdirTemp("", "replica-compare-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	const (
		sectorSize = int64(4096) // one block = one sector here for predictable mapping
		size       = sectorSize * 16
		blockSize  = sectorSize
	)
	r, err := New(context.Background(), size, sectorSize, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, size)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = r.Close() }()

	now := "2026-06-05T00:00:00Z"

	// Snap-0 : empty volume (every sector is hole / backing-file).
	if err := r.Snapshot("snap-0", true, now, nil); err != nil {
		t.Fatalf("Snapshot snap-0: %v", err)
	}

	// Write sector 0 with A's, then snapshot snap-1. snap-1 should
	// see one sector different from snap-0.
	a := bytes.Repeat([]byte{'A'}, int(sectorSize))
	if _, err := r.WriteAt(a, 0); err != nil {
		t.Fatalf("WriteAt sector 0: %v", err)
	}
	if err := r.Snapshot("snap-1", true, now, nil); err != nil {
		t.Fatalf("Snapshot snap-1: %v", err)
	}

	// Modify sector 0 (B's) + write sector 4 (C's). The head-future
	// snap-2 will show two sectors different from snap-1.
	b := bytes.Repeat([]byte{'B'}, int(sectorSize))
	c := bytes.Repeat([]byte{'C'}, int(sectorSize))
	if _, err := r.WriteAt(b, 0); err != nil {
		t.Fatalf("WriteAt sector 0 again: %v", err)
	}
	if _, err := r.WriteAt(c, 4*sectorSize); err != nil {
		t.Fatalf("WriteAt sector 4: %v", err)
	}
	if err := r.Snapshot("snap-2", true, now, nil); err != nil {
		t.Fatalf("Snapshot snap-2: %v", err)
	}

	// Close the live replica so NewReadOnly can re-open it on each
	// snapshot below — pkg/replica only allows one open handle per
	// dir at a time.
	if err := r.Close(); err != nil {
		t.Fatalf("Close live replica: %v", err)
	}

	// Helper : open the snapshot read-only, run CompareSnapshot,
	// close. Mirrors the production flow (weftsnap_server.Compare
	// opens NewReadOnly on the snapshot before walking the diff).
	compare := func(t *testing.T, snap, parent string) []SnapshotRange {
		t.Helper()
		ro, err := NewReadOnly(context.Background(), dir, snap, nil)
		if err != nil {
			t.Fatalf("NewReadOnly %s: %v", snap, err)
		}
		defer func() { _ = ro.Close() }()
		got, err := ro.CompareSnapshot(snap, parent, blockSize)
		if err != nil {
			t.Fatalf("CompareSnapshot %s vs %s: %v", snap, parent, err)
		}
		return got
	}

	// snap-2 vs snap-1 : only sectors 0 + 4 differ (written between
	// snap-1 and snap-2).
	got := compare(t, "volume-snap-snap-2.img", "volume-snap-snap-1.img")
	wantOffsets := map[int64]bool{0: true, 4 * sectorSize: true}
	if len(got) != len(wantOffsets) {
		t.Fatalf("snap-2 vs snap-1 returned %d ranges, want %d: %+v", len(got), len(wantOffsets), got)
	}
	for _, rg := range got {
		if !wantOffsets[rg.Offset] {
			t.Errorf("unexpected range offset %d (want one of %v)", rg.Offset, mapKeys(wantOffsets))
		}
		if rg.Size != blockSize {
			t.Errorf("range size = %d, want %d", rg.Size, blockSize)
		}
	}

	// snap-2 vs snap-0 : sectors 0 + 4 still differ (cumulative
	// since snap-0 = empty). sector 0 has been written twice but
	// CompareSnapshot reports the FINAL state, so one entry per block.
	got = compare(t, "volume-snap-snap-2.img", "volume-snap-snap-0.img")
	if len(got) != 2 {
		t.Fatalf("snap-2 vs snap-0 returned %d ranges, want 2: %+v", len(got), got)
	}

	// snap-1 vs snap-0 : only sector 0 (A's, written between snap-0
	// and snap-1). Opens snap-1 read-only — the chain stops there, so
	// the location table reflects "as of snap-1" not "as of head".
	got = compare(t, "volume-snap-snap-1.img", "volume-snap-snap-0.img")
	if len(got) != 1 || got[0].Offset != 0 || got[0].Size != blockSize {
		t.Fatalf("snap-1 vs snap-0 = %+v, want [{0, %d}]", got, blockSize)
	}

	// Empty parent ("full footprint of snap-1") — same single sector.
	got = compare(t, "volume-snap-snap-1.img", "")
	if len(got) != 1 || got[0].Offset != 0 {
		t.Fatalf("snap-1 full = %+v, want [{0, %d}]", got, blockSize)
	}
}

func TestCompareSnapshot_Errors(t *testing.T) {
	dir, err := os.MkdirTemp("", "replica-compare-err-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	r, err := New(context.Background(), 4096*8, 4096, dir, nil, false, false, 250, 0, false, false, types.ReplicaStateInitial, 4096*8)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = r.Close() }()

	if err := r.Snapshot("only", true, "2026-06-05T00:00:00Z", nil); err != nil {
		t.Fatalf("Snapshot only: %v", err)
	}

	// Empty snapshot name → InvalidArgument-shaped error.
	if _, err := r.CompareSnapshot("", "", 4096); err == nil {
		t.Errorf("empty snapshot name should return an error")
	}

	// blockSize ≤ 0 → error.
	if _, err := r.CompareSnapshot("volume-snap-only.img", "", 0); err == nil {
		t.Errorf("zero block size should return an error")
	}

	// Unknown snapshot → not found.
	if _, err := r.CompareSnapshot("volume-snap-nope.img", "", 4096); err == nil {
		t.Errorf("unknown snapshot should return an error")
	}

	// Backwards diff (parent above snapshot) — we don't set up the
	// chain here so the parent miss surfaces as not-found first ;
	// that's the same operator-visible behaviour.
}

func mapKeys(m map[int64]bool) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// _ ensures the test file imports fmt at top for future debug prints
// without churning the import block. Removing is fine if fmt isn't
// used elsewhere here.
var _ = fmt.Sprintf
