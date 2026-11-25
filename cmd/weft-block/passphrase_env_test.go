package main

import "testing"

func TestRestorePassphraseEnvName(t *testing.T) {
	t.Setenv("WEFT_BACKUP_PASSPHRASE_ENV", "")
	if got := restorePassphraseEnvName(); got != "WEFT_BACKUP_PASSPHRASE" {
		t.Fatalf("default = %q", got)
	}
	t.Setenv("WEFT_BACKUP_PASSPHRASE_ENV", "MY_PASS")
	if got := restorePassphraseEnvName(); got != "MY_PASS" {
		t.Fatalf("override = %q", got)
	}
}
