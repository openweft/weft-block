package secretenv

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestLoadFromOnlyReadsTheReference(t *testing.T) {
	buf := setup(t, func(r string) ([]byte, error) { return []byte("v-" + r + "\n"), nil })
	t.Setenv("Y_SECRET", "plain-must-be-ignored")
	if _, err := LoadFrom(context.Background(), "Y_SECRET"); !errors.Is(err, ErrUnset) {
		t.Fatalf("plaintext must not count: %v", err)
	}
	t.Setenv("Y_SECRET_FROM", "r1")
	v, err := LoadFrom(context.Background(), "Y_SECRET")
	if err != nil || string(v) != "v-r1" {
		t.Fatalf("= %q, %v", v, err)
	}
	if buf.Len() != 0 {
		t.Fatalf("no warning expected: %q", buf.String())
	}
	// the cache is per reference: a changed reference is read again.
	t.Setenv("Y_SECRET_FROM", "r2")
	v, _ = LoadFrom(context.Background(), "Y_SECRET")
	if string(v) != "v-r2" {
		t.Fatalf("stale cache: %q", v)
	}
}

func TestAReferenceIsOpenedOnce(t *testing.T) {
	opens := 0
	setup(t, func(string) ([]byte, error) { opens++; return []byte("v"), nil })
	t.Setenv("Y_SECRET_FROM", "r")
	for i := 0; i < 3; i++ {
		if _, err := LoadFrom(context.Background(), "Y_SECRET"); err != nil {
			t.Fatal(err)
		}
	}
	if opens != 1 {
		t.Fatalf("opened %d times", opens)
	}
}

func TestLoadFromErrors(t *testing.T) {
	setup(t, func(string) ([]byte, error) { return nil, errors.New("boom") })
	t.Setenv("Y_SECRET_FROM", "r")
	if _, err := LoadFrom(context.Background(), "Y_SECRET"); err == nil || !strings.Contains(err.Error(), "Y_SECRET_FROM") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
	setup(t, func(string) ([]byte, error) { return []byte("\r\n"), nil })
	if _, err := LoadFrom(context.Background(), "Y_SECRET"); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadHintNamesTheReplacement(t *testing.T) {
	buf := setup(t, noRef)
	t.Setenv("LIBVAR", "x")
	if _, err := LoadHint(context.Background(), "LIBVAR", "OURS_FROM"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "use OURS_FROM=") {
		t.Fatalf("%q", buf.String())
	}
	// captured then unset: a later call still answers.
	v, err := LoadHint(context.Background(), "LIBVAR", "OURS_FROM")
	if err != nil || string(v) != "x" {
		t.Fatalf("= %q, %v", v, err)
	}
}

func TestWarnOnce(t *testing.T) {
	buf := setup(t, noRef)
	WarnOnce("k", "hello")
	WarnOnce("k", "hello")
	WarnOnce("k2", "again")
	if buf.String() != "hello\nagain\n" {
		t.Fatalf("%q", buf.String())
	}
}
