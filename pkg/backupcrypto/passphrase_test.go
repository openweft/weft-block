package backupcrypto

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func secretFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "pass")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadPassphrase_FromReference(t *testing.T) {
	t.Setenv("TPASS_A_FROM", "file:"+secretFile(t, "from-file\n"))
	got, err := LoadPassphrase("TPASS_A")
	if err != nil || string(got) != "from-file" {
		t.Fatalf("= %q, %v", got, err)
	}
}

func TestLoadPassphrase_PlaintextStillWorksAndIsUnset(t *testing.T) {
	t.Setenv("TPASS_B", "plain")
	got, err := LoadPassphrase("TPASS_B")
	if err != nil || string(got) != "plain" {
		t.Fatalf("= %q, %v", got, err)
	}
	if v := os.Getenv("TPASS_B"); v != "" {
		t.Fatalf("plaintext variable still set: %q", v)
	}
	again, err := LoadPassphrase("TPASS_B")
	if err != nil || string(again) != "plain" {
		t.Fatalf("second load = %q, %v", again, err)
	}
}

func TestLoadPassphrase_BothSetIsAnError(t *testing.T) {
	t.Setenv("TPASS_C", "plain")
	t.Setenv("TPASS_C_FROM", "file:"+secretFile(t, "x"))
	_, err := LoadPassphrase("TPASS_C")
	if err == nil || !strings.Contains(err.Error(), "both set") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadPassphrase_UnresolvableNeverEchoes(t *testing.T) {
	t.Setenv("TPASS_D_FROM", "file:/nonexistent/very-secret-path-value")
	_, err := LoadPassphrase("TPASS_D")
	if err == nil || !strings.Contains(err.Error(), "TPASS_D_FROM") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadPassphrase_UnsetAndEmptyName(t *testing.T) {
	if _, err := LoadPassphrase("TPASS_E_NEVER_SET"); err == nil || !strings.Contains(err.Error(), "TPASS_E_NEVER_SET_FROM") {
		t.Fatalf("err = %v", err)
	}
	if _, err := LoadPassphrase(""); err == nil || !strings.Contains(err.Error(), "passphrase_env is empty") {
		t.Fatal("empty name must error")
	}
}

func TestDeriveKeyAndWipe(t *testing.T) {
	pass := []byte("correct horse")
	want, err := DeriveKey([]byte("correct horse"), make([]byte, 16), KDFArgon2id, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DeriveKeyAndWipe(pass, make([]byte, 16), KDFArgon2id, nil)
	if err != nil || string(got) != string(want) {
		t.Fatalf("key differs or err: %v", err)
	}
	for i, b := range pass {
		if b != 0 {
			t.Fatalf("passphrase byte %d not wiped", i)
		}
	}
	// wiped on failure too
	bad := []byte("zz")
	if _, err := DeriveKeyAndWipe(bad, nil, "nope", nil); err == nil {
		t.Fatal("unknown kdf must error")
	}
	if bad[0] != 0 || bad[1] != 0 {
		t.Fatal("passphrase not wiped on error")
	}
}
