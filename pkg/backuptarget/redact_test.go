package backuptarget

import (
	"strings"
	"testing"

	"github.com/openweft/weft-block/internal/secretenv"
)

func TestRedact(t *testing.T) {
	cases := map[string]string{
		"sftp://alice:s3cr3t@host:22/p": "sftp://alice:xxxxx@host:22/p",
		"sftp://alice@host/p":           "sftp://alice@host/p",
		"s3://bucket@eu-west-1/x":       "s3://bucket@eu-west-1/x",
		"fs:///a/b":                     "fs:///a/b",
		"sftp://alice:pa ss@host/p":     "sftp://<unparseable>",
		"%zz:secret":                    "<unparseable>",
	}
	for in, want := range cases {
		got := Redact(in)
		if got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(got, "s3cr3t") || strings.Contains(got, "pa ss") || strings.Contains(got, "secret") {
			t.Errorf("Redact(%q) leaks the password: %q", in, got)
		}
	}
}

func TestErrorsNeverQuoteAURLPassword(t *testing.T) {
	const pw = "hunter2"
	bad := "sftp://alice:" + pw + "@ho st/p" // a space in the host makes url.Parse fail
	_, err := New(bad)
	if err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("New err = %v", err)
	}
	st := &sftpTarget{}
	if _, err := st.resolvePath(bad); err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("resolvePath(bad) err = %v", err)
	}
	if _, err := st.resolvePath("s3://alice:" + pw + "@h/p"); err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("resolvePath(non-sftp) err = %v", err)
	}
	s3t := &s3Target{}
	if _, err := s3t.keyFor(bad); err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("keyFor(bad) err = %v", err)
	}
	if _, err := s3t.keyFor("sftp://alice:" + pw + "@h/p"); err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("keyFor(non-s3) err = %v", err)
	}
	if _, err := New("sftp://alice:" + pw + "@/p"); err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("missing host err = %v", err)
	}
	if _, err := New("sftp://:" + pw + "@host/p"); err == nil || strings.Contains(err.Error(), pw) {
		t.Fatalf("missing user err = %v", err)
	}
}

func TestSFTPURLPasswordWarnsButWorks(t *testing.T) {
	secretenv.Reset()
	var warn strings.Builder
	old := secretenv.Warn
	secretenv.Warn = &warn
	t.Cleanup(func() { secretenv.Warn = old; secretenv.Reset() })
	tg, err := New("sftp://alice:hunter2@host:2222/base")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(warn.String(), "WEFT_SFTP_PASSWORD_FROM") || strings.Contains(warn.String(), "hunter2") {
		t.Fatalf("warning = %q", warn.String())
	}
	warn.Reset()
	if _, err := New("sftp://alice@host/base"); err != nil || warn.Len() != 0 {
		t.Fatalf("no password, no warning: %v %q", err, warn.String())
	}
	st := tg.(*sftpTarget)
	if st.password != "hunter2" || st.user != "alice" || st.host != "host:2222" {
		t.Fatalf("target = %+v", st)
	}
}
