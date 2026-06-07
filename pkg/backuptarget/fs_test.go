package backuptarget

// fs_test.go — round-trip tests for the local-filesystem backup target.
// Two layers :
//   - direct Push/Pull/List/Delete contract on the fs:// Target.
//   - end-to-end "encode → push → pull → decode" pipeline composing
//     backupcrypto's Encoder/Decoder with the fs:// Target — the closest
//     thing we can get to an integration test of the encrypted-backup
//     story without spinning a real replica.
//
// Driver-layer integration (Compare → Read with ranges → ciphertext →
// fs:// → restore) lives in the higher-level driver test suite ; here
// we keep to the seam between BackupTarget + backupcrypto.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openweft/weft-block/pkg/backupcrypto"
)

// newTestFSTarget builds a Target rooted in t.TempDir() and the URL
// the operator would type for the same root. Cleanup is handled by
// t.TempDir().
func newTestFSTarget(t *testing.T) (Target, string) {
	t.Helper()
	root := t.TempDir()
	rootURL := "fs://" + root
	tgt, err := New(rootURL)
	if err != nil {
		t.Fatalf("New(%q): %v", rootURL, err)
	}
	if tgt.Scheme() != SchemeFS {
		t.Fatalf("scheme = %q, want %q", tgt.Scheme(), SchemeFS)
	}
	return tgt, rootURL
}

// makeFullURL composes a per-object URL under the test root, matching
// what driver.go's CreateBackup would emit. Keeps tests readable.
func makeFullURL(rootURL, name string) string {
	u, _ := url.Parse(rootURL)
	u.Path = filepath.Join(u.Path, name)
	return u.String()
}

func TestFS_PushPullRoundTrip(t *testing.T) {
	ctx := context.Background()
	tgt, rootURL := newTestFSTarget(t)

	// Sizes spanning the chunk-buffering boundaries io.Copy uses :
	// empty, sub-page, page, multi-page, multi-MiB.
	cases := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"tiny", 17},
		{"page", 4096},
		{"odd", 4096*3 + 7},
		{"multi_mib", 5 * 1024 * 1024},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload := make([]byte, c.size)
			_, _ = rand.Read(payload)
			obj := makeFullURL(rootURL, "obj-"+c.name)
			if err := tgt.Push(ctx, obj, int64(len(payload)), bytes.NewReader(payload)); err != nil {
				t.Fatalf("Push: %v", err)
			}
			var got bytes.Buffer
			n, err := tgt.Pull(ctx, obj, &got)
			if err != nil {
				t.Fatalf("Pull: %v", err)
			}
			if n != int64(len(payload)) {
				t.Fatalf("Pull returned %d, want %d", n, len(payload))
			}
			if !bytes.Equal(payload, got.Bytes()) {
				t.Fatalf("payload mismatch")
			}
		})
	}
}

func TestFS_PushIsAtomic(t *testing.T) {
	// Atomicity contract : interrupted Push leaves no readable file
	// at the destination URL (Push opens .tmp, fsyncs, renames). We
	// simulate "no destination yet" by attempting Pull on a name
	// that's never been pushed — must surface a clear os.ErrNotExist
	// rather than half-written bytes.
	ctx := context.Background()
	tgt, rootURL := newTestFSTarget(t)
	obj := makeFullURL(rootURL, "never-pushed")
	var sink bytes.Buffer
	_, err := tgt.Pull(ctx, obj, &sink)
	if err == nil {
		t.Fatalf("Pull on missing object should fail")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Pull err = %v, want wraps os.ErrNotExist", err)
	}

	// And Push of overlapping URLs is last-writer-wins (rename
	// onto same dst replaces atomically).
	if err := tgt.Push(ctx, obj, 0, strings.NewReader("first")); err != nil {
		t.Fatalf("Push 1: %v", err)
	}
	if err := tgt.Push(ctx, obj, 0, strings.NewReader("second-and-longer")); err != nil {
		t.Fatalf("Push 2: %v", err)
	}
	sink.Reset()
	if _, err := tgt.Pull(ctx, obj, &sink); err != nil {
		t.Fatalf("Pull after rewrite: %v", err)
	}
	if got := sink.String(); got != "second-and-longer" {
		t.Fatalf("after rewrite, Pull = %q, want %q", got, "second-and-longer")
	}
}

func TestFS_List_DirTree(t *testing.T) {
	ctx := context.Background()
	tgt, rootURL := newTestFSTarget(t)

	// Drop a tree : <root>/<vol>/<backup>.bin + sidecar + a
	// sibling volume's backup. List must surface every leaf and
	// sort lexicographically.
	objs := []string{"vol-a/snap1.bin", "vol-a/snap1.json", "vol-a/snap2.bin", "vol-b/snap1.bin"}
	for _, o := range objs {
		if err := tgt.Push(ctx, makeFullURL(rootURL, o), 4, strings.NewReader("data")); err != nil {
			t.Fatalf("Push %s: %v", o, err)
		}
	}

	// List under the root → every entry.
	entries, err := tgt.List(ctx, rootURL)
	if err != nil {
		t.Fatalf("List root: %v", err)
	}
	if len(entries) != len(objs) {
		t.Fatalf("List root returned %d entries, want %d", len(entries), len(objs))
	}
	for i, e := range entries {
		want := makeFullURL(rootURL, objs[i])
		if e.URL != want {
			t.Errorf("entry[%d].URL = %q, want %q", i, e.URL, want)
		}
		if e.SizeBytes != 4 {
			t.Errorf("entry[%d].SizeBytes = %d, want 4", i, e.SizeBytes)
		}
	}

	// List scoped to one volume's prefix → just that subset.
	scoped, err := tgt.List(ctx, makeFullURL(rootURL, "vol-a"))
	if err != nil {
		t.Fatalf("List vol-a: %v", err)
	}
	if len(scoped) != 3 {
		t.Fatalf("List vol-a returned %d entries, want 3", len(scoped))
	}

	// List of a missing prefix → empty + nil error (NOT a hard
	// failure ; mirrors what an empty bucket should do).
	missing, err := tgt.List(ctx, makeFullURL(rootURL, "vol-missing"))
	if err != nil {
		t.Fatalf("List missing: unexpected err %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("List missing returned %d entries, want 0", len(missing))
	}
}

func TestFS_Delete_Idempotent(t *testing.T) {
	ctx := context.Background()
	tgt, rootURL := newTestFSTarget(t)
	obj := makeFullURL(rootURL, "del-me")

	// Delete-before-push is a no-op (idempotency).
	if err := tgt.Delete(ctx, obj); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
	if err := tgt.Push(ctx, obj, 4, strings.NewReader("data")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := tgt.Delete(ctx, obj); err != nil {
		t.Fatalf("Delete present: %v", err)
	}
	// Second Delete after physical removal — still no error.
	if err := tgt.Delete(ctx, obj); err != nil {
		t.Fatalf("Delete twice: %v", err)
	}
	// Pull on the deleted URL should now surface NotExist.
	var sink bytes.Buffer
	if _, err := tgt.Pull(ctx, obj, &sink); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Pull after delete: err = %v, want wraps os.ErrNotExist", err)
	}
}

func TestFS_URLValidation(t *testing.T) {
	// Each entry is its own subtest — keeps the failure message
	// pointed at the exact URL shape that broke.
	cases := []struct {
		name    string
		url     string
		wantErr string
	}{
		{"relative_path", "fs://relative/path", "non-empty host"},
		{"host_is_not_ok", "fs://hostname/abs", "non-empty host"},
		{"unknown_scheme", "ftp://host/path", "unsupported scheme"},
		{"malformed", "::not-a-url", "parse"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(c.url)
			if err == nil {
				t.Fatalf("New(%q) = nil, want error", c.url)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("New(%q) err = %v, want substring %q", c.url, err, c.wantErr)
			}
		})
	}
}

// TestEncryptedRoundTrip_FS proves the integration of the
// backupcrypto Encoder/Decoder pipeline with the fs:// BackupTarget.
// This is the closest unit-level proof that "operator runs
// `weft volume backup create --target=fs:///… ` with
// WEFT_BACKUP_PASSPHRASE set" will round-trip cleanly back through
// `weft volume backup restore` — the same byte stream the driver
// would push lands in the file, and the same byte stream the driver
// would pull comes back out.
func TestEncryptedRoundTrip_FS(t *testing.T) {
	ctx := context.Background()
	tgt, rootURL := newTestFSTarget(t)

	for _, algo := range []string{backupcrypto.AlgChaCha20Poly1305, backupcrypto.AlgAES256GCM} {
		t.Run(algo, func(t *testing.T) {
			key := make([]byte, 32)
			if _, err := rand.Read(key); err != nil {
				t.Fatalf("rand: %v", err)
			}
			params := backupcrypto.Params{Algorithm: algo, Key: key}

			// 6 MiB payload — spans 2 full chunks (4 MiB default)
			// plus a short tail, the same shape a real driver
			// ships for a small volume's snapshot.
			plaintext := make([]byte, 6*1024*1024)
			if _, err := rand.Read(plaintext); err != nil {
				t.Fatalf("rand: %v", err)
			}
			wantHash := sha256.Sum256(plaintext)

			// 1. Encrypt into a buffer that simulates the driver's
			// staging area. Capture the ciphertext for the Push.
			var ciphertext bytes.Buffer
			enc, err := backupcrypto.NewEncoder(&ciphertext, params)
			if err != nil {
				t.Fatalf("NewEncoder: %v", err)
			}
			if _, err := enc.Write(plaintext); err != nil {
				t.Fatalf("Encoder.Write: %v", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatalf("Encoder.Close: %v", err)
			}
			if ciphertext.Len() <= len(plaintext) {
				t.Fatalf("ciphertext (%d) should grow vs plaintext (%d) — nonce+tag per chunk",
					ciphertext.Len(), len(plaintext))
			}

			// 2. Push ciphertext via the fs:// target. The target
			// must never see plaintext — verify by reading the
			// on-disk file later.
			obj := makeFullURL(rootURL, fmt.Sprintf("vol-x/snap-%s.bin", algo))
			pushBody := bytes.NewReader(ciphertext.Bytes())
			if err := tgt.Push(ctx, obj, int64(ciphertext.Len()), pushBody); err != nil {
				t.Fatalf("Push: %v", err)
			}

			// 3. The on-disk file should hold exactly the ciphertext,
			// not the plaintext. (Operators inspecting the fs:// dir
			// must not see anything readable about the volume.)
			u, _ := url.Parse(obj)
			onDisk, err := os.ReadFile(u.Path)
			if err != nil {
				t.Fatalf("read on-disk file: %v", err)
			}
			if !bytes.Equal(onDisk, ciphertext.Bytes()) {
				t.Fatalf("on-disk bytes != pushed ciphertext (len on-disk=%d, len pushed=%d)",
					len(onDisk), ciphertext.Len())
			}
			if bytes.Contains(onDisk, plaintext[:1024]) {
				t.Fatalf("on-disk file contains plaintext window — encryption is a no-op")
			}

			// 4. Pull from the target into a buffer ; pipe through
			// the Decoder ; verify plaintext SHA-256 matches.
			var pulled bytes.Buffer
			pulledN, err := tgt.Pull(ctx, obj, &pulled)
			if err != nil {
				t.Fatalf("Pull: %v", err)
			}
			if pulledN != int64(ciphertext.Len()) {
				t.Fatalf("Pull returned %d, want %d", pulledN, ciphertext.Len())
			}
			dec, err := backupcrypto.NewDecoder(&pulled, params)
			if err != nil {
				t.Fatalf("NewDecoder: %v", err)
			}
			restored, err := io.ReadAll(dec)
			if err != nil {
				t.Fatalf("Decoder.ReadAll: %v", err)
			}
			if gotHash := sha256.Sum256(restored); gotHash != wantHash {
				t.Fatalf("plaintext hash mismatch ; want %s got %s",
					hex.EncodeToString(wantHash[:]), hex.EncodeToString(gotHash[:]))
			}
			if !bytes.Equal(restored, plaintext) {
				t.Fatalf("restored != plaintext (len got=%d len want=%d)",
					len(restored), len(plaintext))
			}
		})
	}
}

// TestEncryptedRoundTrip_WrongKey_FS proves the operator-friendly
// failure mode : a passphrase mismatch on restore surfaces as a
// decryption error from the Decoder, NOT a silent corruption. This
// guards against operator confusion ("did I lose the data?") on
// post-rotation passphrase mismatches.
func TestEncryptedRoundTrip_WrongKey_FS(t *testing.T) {
	ctx := context.Background()
	tgt, rootURL := newTestFSTarget(t)
	key1 := make([]byte, 32)
	_, _ = rand.Read(key1)
	key2 := make([]byte, 32)
	_, _ = rand.Read(key2)
	if bytes.Equal(key1, key2) {
		t.Skip("rand collision — extremely improbable, just retry")
	}
	plain := make([]byte, 64*1024)
	_, _ = rand.Read(plain)
	var ciphertext bytes.Buffer
	enc, _ := backupcrypto.NewEncoder(&ciphertext, backupcrypto.Params{
		Algorithm: backupcrypto.AlgChaCha20Poly1305, Key: key1,
	})
	_, _ = enc.Write(plain)
	_ = enc.Close()
	obj := makeFullURL(rootURL, "wrong-key.bin")
	if err := tgt.Push(ctx, obj, int64(ciphertext.Len()), bytes.NewReader(ciphertext.Bytes())); err != nil {
		t.Fatalf("Push: %v", err)
	}
	var pulled bytes.Buffer
	if _, err := tgt.Pull(ctx, obj, &pulled); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	dec, err := backupcrypto.NewDecoder(&pulled, backupcrypto.Params{
		Algorithm: backupcrypto.AlgChaCha20Poly1305, Key: key2,
	})
	if err != nil {
		// Decoder may fail early on header tag check ; that's fine.
		return
	}
	out, err := io.ReadAll(dec)
	if err == nil {
		t.Fatalf("decrypt with wrong key succeeded — encryption is broken (out len=%d)", len(out))
	}
}
