package backuptarget

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openweft/weft-block/internal/secretenv"
)

func credFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return "file:" + p
}

func clearS3Env(t *testing.T) {
	secretenv.Reset()
	t.Cleanup(secretenv.Reset)
	for _, n := range []string{
		envS3AccessKeyID, envS3SecretAccessKey, envS3SessionToken,
		envAWSAccessKeyID, envAWSSecretAccessKey, envAWSSessionToken,
	} {
		t.Setenv(n, "")
		t.Setenv(n+"_FROM", "")
	}
}

func TestS3CredentialsNothingSetKeepsDefaultChain(t *testing.T) {
	clearS3Env(t)
	p, err := s3Credentials(context.Background())
	if err != nil || p != nil {
		t.Fatalf("provider=%v err=%v", p, err)
	}
}

func TestS3CredentialsFromReferences(t *testing.T) {
	clearS3Env(t)
	t.Setenv(envS3AccessKeyID+"_FROM", credFile(t, "AKIDX\n"))
	t.Setenv(envS3SecretAccessKey+"_FROM", credFile(t, "sekret\n"))
	t.Setenv(envS3SessionToken+"_FROM", credFile(t, "tok\n"))
	p, err := s3Credentials(context.Background())
	if err != nil || p == nil {
		t.Fatalf("provider=%v err=%v", p, err)
	}
	c, err := p.Retrieve(context.Background())
	if err != nil || c.AccessKeyID != "AKIDX" || c.SecretAccessKey != "sekret" || c.SessionToken != "tok" {
		t.Fatalf("creds=%+v err=%v", c, err)
	}
}

func TestS3CredentialsPlaintextAWSIsCapturedAndUnset(t *testing.T) {
	clearS3Env(t)
	t.Setenv(envAWSAccessKeyID, "AKPLAIN")
	t.Setenv(envAWSSecretAccessKey, "plainsecret")
	t.Setenv(envAWSSessionToken, "plaintok")
	p, err := s3Credentials(context.Background())
	if err != nil || p == nil {
		t.Fatalf("provider=%v err=%v", p, err)
	}
	c, _ := p.Retrieve(context.Background())
	if c.AccessKeyID != "AKPLAIN" || c.SecretAccessKey != "plainsecret" || c.SessionToken != "plaintok" {
		t.Fatalf("creds=%+v", c)
	}
	if os.Getenv(envAWSSecretAccessKey) != "" || os.Getenv(envAWSSessionToken) != "" {
		t.Fatal("plaintext secret variables must be unset")
	}
}

func TestS3CredentialsSecretWithoutIDIsAnError(t *testing.T) {
	clearS3Env(t)
	t.Setenv(envS3SecretAccessKey+"_FROM", credFile(t, "sekret"))
	_, err := s3Credentials(context.Background())
	if err == nil || strings.Contains(err.Error(), "sekret") {
		t.Fatalf("err = %v", err)
	}
}

func TestS3CredentialsIDWithoutSecretIsAnError(t *testing.T) {
	clearS3Env(t)
	t.Setenv(envS3AccessKeyID+"_FROM", credFile(t, "AKID"))
	if _, err := s3Credentials(context.Background()); err == nil {
		t.Fatal("an access key id alone must be refused")
	}
}

func TestS3CredentialsBadReferenceNeverEchoes(t *testing.T) {
	clearS3Env(t)
	t.Setenv(envS3SecretAccessKey+"_FROM", "file:/nonexistent/x")
	_, err := s3Credentials(context.Background())
	if err == nil || !strings.Contains(err.Error(), envS3SecretAccessKey+"_FROM") {
		t.Fatalf("err = %v", err)
	}
	clearS3Env(t)
	t.Setenv(envS3SecretAccessKey+"_FROM", credFile(t, "s"))
	t.Setenv(envS3AccessKeyID+"_FROM", "file:/nonexistent/y")
	if _, err := s3Credentials(context.Background()); err == nil || !strings.Contains(err.Error(), envS3AccessKeyID+"_FROM") {
		t.Fatalf("id err = %v", err)
	}
	clearS3Env(t)
	t.Setenv(envS3SecretAccessKey+"_FROM", credFile(t, "s"))
	t.Setenv(envS3AccessKeyID+"_FROM", credFile(t, "i"))
	t.Setenv(envS3SessionToken+"_FROM", "file:/nonexistent/z")
	if _, err := s3Credentials(context.Background()); err == nil || !strings.Contains(err.Error(), envS3SessionToken+"_FROM") {
		t.Fatalf("token err = %v", err)
	}
}

func TestClientRefusesWhenCredentialsAreIncomplete(t *testing.T) {
	clearS3Env(t)
	t.Setenv(envS3SecretAccessKey+"_FROM", credFile(t, "sekret\n"))
	tg := &s3Target{bucket: "b", region: "r"}
	if c, err := tg.client(context.Background()); err == nil || c != nil {
		t.Fatalf("client=%v err=%v", c, err)
	}
}
