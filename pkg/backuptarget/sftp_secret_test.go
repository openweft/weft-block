package backuptarget

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSFTPPasswordFromReference(t *testing.T) {
	p := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(p, []byte("pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSH_AUTH_SOCK", "")
	t.Setenv(envSFTPKeyFile, "")
	t.Setenv(envSFTPPassword+"_FROM", "file:"+p)
	m, err := (&sftpTarget{}).collectAuthMethods()
	if err != nil || len(m) != 1 {
		t.Fatalf("methods=%d err=%v", len(m), err)
	}
}

func TestSFTPPasswordPlaintextDeprecatedButWorks(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	t.Setenv(envSFTPKeyFile, "")
	t.Setenv(envSFTPPassword, "legacy-pw")
	m, err := (&sftpTarget{}).collectAuthMethods()
	if err != nil || len(m) != 1 {
		t.Fatalf("methods=%d err=%v", len(m), err)
	}
}

func TestSFTPPasswordBothSetIsAnError(t *testing.T) {
	t.Setenv(envSFTPPassword, "a")
	t.Setenv(envSFTPPassword+"_FROM", "file:/x")
	_, err := (&sftpTarget{}).collectAuthMethods()
	if err == nil || !strings.Contains(err.Error(), "both set") {
		t.Fatalf("err = %v", err)
	}
}

func TestSFTPKeyPassphraseFromBadReferenceErrors(t *testing.T) {
	key := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(key, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSH_AUTH_SOCK", "")
	t.Setenv(envSFTPPassword, "")
	t.Setenv(envSFTPKeyFile, key)
	t.Setenv(envSFTPKeyPassphrase+"_FROM", "file:/nonexistent")
	_, err := (&sftpTarget{}).collectAuthMethods()
	if err == nil || !strings.Contains(err.Error(), envSFTPKeyPassphrase+"_FROM") {
		t.Fatalf("err = %v", err)
	}
}

func TestSFTPKeyPassphraseFromReferenceIsUsed(t *testing.T) {
	key := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(key, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	pp := filepath.Join(t.TempDir(), "pp")
	if err := os.WriteFile(pp, []byte("phrase"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSH_AUTH_SOCK", "")
	t.Setenv(envSFTPPassword, "")
	t.Setenv(envSFTPKeyFile, key)
	t.Setenv(envSFTPKeyPassphrase+"_FROM", "file:"+pp)
	// The key is junk, so parsing fails; reaching the parse step proves the
	// reference resolved (a resolution error would say "_FROM").
	_, err := (&sftpTarget{}).collectAuthMethods()
	if err == nil || !strings.Contains(err.Error(), "parse key") {
		t.Fatalf("err = %v", err)
	}
}
