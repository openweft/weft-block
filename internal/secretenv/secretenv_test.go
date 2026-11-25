package secretenv

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setup(t *testing.T, ref func(string) ([]byte, error)) *bytes.Buffer {
	t.Helper()
	reset()
	var buf bytes.Buffer
	oldW, oldO := Warn, open
	Warn = &buf
	open = func(_ context.Context, r string) ([]byte, error) { return ref(r) }
	t.Cleanup(func() { Warn, open = oldW, oldO; reset() })
	return &buf
}

func noRef(string) ([]byte, error) { return nil, errors.New("unexpected open") }

func TestPlaintextWorksWarnsOnceAndIsUnset(t *testing.T) {
	buf := setup(t, noRef)
	t.Setenv("X_PASS", "hunter2")
	for i := 0; i < 2; i++ {
		v, err := Load(context.Background(), "X_PASS")
		if err != nil || string(v) != "hunter2" {
			t.Fatalf("load %d = %q, %v", i, v, err)
		}
	}
	if got := strings.Count(buf.String(), "X_PASS is deprecated"); got != 1 {
		t.Fatalf("want one warning, got %d: %q", got, buf.String())
	}
	if !strings.Contains(buf.String(), "X_PASS_FROM") {
		t.Fatalf("warning must name _FROM: %q", buf.String())
	}
	if strings.Contains(buf.String(), "hunter2") {
		t.Fatal("warning echoes the value")
	}
	if got := os.Getenv("X_PASS"); got != "" {
		t.Fatalf("plaintext variable not unset: %q", got)
	}
}

func TestFromResolvesTrimsAndCaches(t *testing.T) {
	buf := setup(t, func(r string) ([]byte, error) {
		if r != "file:/p" {
			t.Fatalf("ref = %q", r)
		}
		return []byte("s3cret\n"), nil
	})
	t.Setenv("X_PASS_FROM", "file:/p")
	v, err := Load(context.Background(), "X_PASS")
	if err != nil || string(v) != "s3cret" {
		t.Fatalf("= %q, %v", v, err)
	}
	if buf.Len() != 0 {
		t.Fatalf("no warning expected: %q", buf.String())
	}
	// the returned slice is a copy: wiping it must not corrupt the cache.
	clear(v)
	v, _ = Load(context.Background(), "X_PASS")
	if string(v) != "s3cret" {
		t.Fatalf("cache corrupted by caller wipe: %q", v)
	}
}

func TestBothSetIsAnError(t *testing.T) {
	setup(t, noRef)
	t.Setenv("X_PASS", "a")
	t.Setenv("X_PASS_FROM", "file:/p")
	_, err := Load(context.Background(), "X_PASS")
	if err == nil || !strings.Contains(err.Error(), "both set") {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "file:/p") {
		t.Fatal("error echoes the reference")
	}
}

func TestUnresolvableRefNeverEchoesValue(t *testing.T) {
	setup(t, func(string) ([]byte, error) { return nil, errors.New("no such credential") })
	t.Setenv("X_PASS_FROM", "systemd-creds:x")
	_, err := Load(context.Background(), "X_PASS")
	if err == nil || !strings.Contains(err.Error(), "X_PASS_FROM") {
		t.Fatalf("err = %v", err)
	}
}

func TestEmptyFromIsAnError(t *testing.T) {
	setup(t, func(string) ([]byte, error) { return []byte("\n"), nil })
	t.Setenv("X_PASS_FROM", "file:/p")
	_, err := Load(context.Background(), "X_PASS")
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("err = %v", err)
	}
}

func TestUnset(t *testing.T) {
	setup(t, noRef)
	_, err := Load(context.Background(), "X_PASS")
	if !errors.Is(err, ErrUnset) {
		t.Fatalf("err = %v", err)
	}
}

func TestDeprecatedEnvWarnsOnce(t *testing.T) {
	buf := setup(t, noRef)
	DeprecatedEnv("OLD", "NEW_FROM")
	DeprecatedEnv("OLD", "NEW_FROM")
	if got := strings.Count(buf.String(), "OLD is deprecated"); got != 1 {
		t.Fatalf("got %d: %q", got, buf.String())
	}
	if !strings.Contains(buf.String(), "NEW_FROM") {
		t.Fatalf("%q", buf.String())
	}
}

func TestDefaultOpenReadsAFileReference(t *testing.T) {
	reset()
	t.Cleanup(reset)
	p := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(p, []byte("from-disk\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("X_REAL_FROM", "file:"+p)
	v, err := Load(context.Background(), "X_REAL")
	if err != nil || string(v) != "from-disk" {
		t.Fatalf("= %q, %v", v, err)
	}
	t.Setenv("X_MISSING_FROM", "file:"+filepath.Join(t.TempDir(), "absent"))
	if _, err := Load(context.Background(), "X_MISSING"); err == nil || !strings.Contains(err.Error(), "X_MISSING_FROM") {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultOpenRefusesALooseFile(t *testing.T) {
	reset()
	t.Cleanup(reset)
	p := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("X_LOOSE_FROM", "file:"+p)
	if _, err := Load(context.Background(), "X_LOOSE"); err == nil {
		t.Fatal("a world-readable secret file must be refused")
	}
}
