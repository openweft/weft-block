package backuptarget

// sftp.go : SFTP backup target implementation. Talks to sftpgo / OpenSSH
// sshd over the standard SSH protocol via golang.org/x/crypto/ssh +
// github.com/pkg/sftp.
//
// Auth precedence :
//
//   1. URL-embedded password : "sftp://user:pass@host:port/path" (NOT
//      recommended — leaks into etcd / logs).
//   2. $WEFT_SFTP_PASSWORD for password auth (per-process secret).
//   3. $SSH_AUTH_SOCK for agent-mediated key auth (preferred).
//   4. $WEFT_SFTP_KEY_FILE for an explicit private-key path
//      (with optional $WEFT_SFTP_KEY_PASSPHRASE for encrypted keys).
//
// Host key verification :
//
//   * $WEFT_SFTP_KNOWN_HOSTS points at a known_hosts file (default
//     "$HOME/.ssh/known_hosts"). Unknown / mismatched hosts cause a
//     clear error rather than silently accepting (no insecure default).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// sftpTarget binds one SFTP endpoint. Each call opens a fresh SSH session ;
// callers needing high-throughput should batch through Push streaming
// rather than poking many small files.
type sftpTarget struct {
	host     string // host:port
	user     string
	password string // optional, may be empty
	basePath string // path from URL ; subsequent calls' paths are joined to this
}

// envSFTPPassword names the password env var. Exposed as a const so the
// CLI / docs reference one source of truth.
const (
	envSFTPPassword      = "WEFT_SFTP_PASSWORD"
	envSFTPKeyFile       = "WEFT_SFTP_KEY_FILE"
	envSFTPKeyPassphrase = "WEFT_SFTP_KEY_PASSPHRASE"
	envSFTPKnownHosts    = "WEFT_SFTP_KNOWN_HOSTS"
	envSSHAuthSock       = "SSH_AUTH_SOCK"
	envHome              = "HOME"

	defaultSFTPPort = "22"
)

func newSFTPTarget(u *url.URL) (Target, error) {
	if u.Host == "" {
		return nil, fmt.Errorf("backuptarget: sftp URL %q missing host", u.String())
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = defaultSFTPPort
	}
	user := u.User.Username()
	if user == "" {
		return nil, fmt.Errorf("backuptarget: sftp URL %q missing user", u.String())
	}
	pwd, _ := u.User.Password()
	return &sftpTarget{
		host:     net.JoinHostPort(host, port),
		user:     user,
		password: pwd,
		basePath: u.Path,
	}, nil
}

func (t *sftpTarget) Scheme() string { return SchemeSFTP }

// dial opens a fresh SSH connection + SFTP session. Caller is responsible
// for closing the returned *sftp.Client (which transitively closes the SSH
// connection via the cleanup func).
func (t *sftpTarget) dial(ctx context.Context) (*sftp.Client, func(), error) {
	auth, err := t.collectAuthMethods()
	if err != nil {
		return nil, nil, fmt.Errorf("collect sftp auth: %w", err)
	}
	hostKeyCB, err := t.hostKeyCallback()
	if err != nil {
		return nil, nil, fmt.Errorf("sftp known_hosts: %w", err)
	}
	cfg := &ssh.ClientConfig{
		User:            t.user,
		Auth:            auth,
		HostKeyCallback: hostKeyCB,
	}
	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", t.host)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", t.host, err)
	}
	sshConn, chans, reqs, err := ssh.NewClientConn(netConn, t.host, cfg)
	if err != nil {
		_ = netConn.Close()
		return nil, nil, fmt.Errorf("ssh handshake %s: %w", t.host, err)
	}
	cli := ssh.NewClient(sshConn, chans, reqs)
	s, err := sftp.NewClient(cli)
	if err != nil {
		_ = cli.Close()
		return nil, nil, fmt.Errorf("sftp subsystem on %s: %w", t.host, err)
	}
	cleanup := func() {
		_ = s.Close()
		_ = cli.Close()
	}
	return s, cleanup, nil
}

// collectAuthMethods follows the auth precedence documented in the file
// header. An empty result is an error — the caller has nothing to try.
func (t *sftpTarget) collectAuthMethods() ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod
	// 1. URL password.
	if t.password != "" {
		methods = append(methods, ssh.Password(t.password))
	}
	// 2. Env password.
	if pwd := os.Getenv(envSFTPPassword); pwd != "" {
		methods = append(methods, ssh.Password(pwd))
	}
	// 3. SSH agent (preferred).
	if sock := os.Getenv(envSSHAuthSock); sock != "" {
		if c, err := net.Dial("unix", sock); err == nil {
			ag := agent.NewClient(c)
			methods = append(methods, ssh.PublicKeysCallback(ag.Signers))
		}
	}
	// 4. Explicit private-key file.
	if kf := os.Getenv(envSFTPKeyFile); kf != "" {
		raw, err := os.ReadFile(kf)
		if err != nil {
			return nil, fmt.Errorf("read key %s: %w", kf, err)
		}
		var signer ssh.Signer
		if pp := os.Getenv(envSFTPKeyPassphrase); pp != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(raw, []byte(pp))
		} else {
			signer, err = ssh.ParsePrivateKey(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("parse key %s: %w", kf, err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}
	if len(methods) == 0 {
		return nil, errors.New("no SSH auth method available (set " + envSFTPPassword + ", " + envSSHAuthSock + ", or " + envSFTPKeyFile + ")")
	}
	return methods, nil
}

// hostKeyCallback resolves the known_hosts file to a verification callback.
// Missing file is an explicit error — we don't silently fall back to
// InsecureIgnoreHostKey because that defeats the point.
func (t *sftpTarget) hostKeyCallback() (ssh.HostKeyCallback, error) {
	path := os.Getenv(envSFTPKnownHosts)
	if path == "" {
		home := os.Getenv(envHome)
		if home == "" {
			return nil, fmt.Errorf("$HOME unset and " + envSFTPKnownHosts + " unset")
		}
		path = home + "/.ssh/known_hosts"
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("known_hosts %s: %w", path, err)
	}
	return knownhosts.New(path)
}

// resolvePath joins the URL's path with the Target's basePath. URLs handed
// to Push/Pull/List/Delete carry a path ; the basePath from the original
// construction acts as a prefix anchor (mostly informational since each
// call already brings a full URL).
func (t *sftpTarget) resolvePath(fullURL string) (string, error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", fmt.Errorf("parse url %q: %w", fullURL, err)
	}
	if u.Scheme != SchemeSFTP {
		return "", fmt.Errorf("sftp target got non-sftp URL %q", fullURL)
	}
	return u.Path, nil
}

func (t *sftpTarget) Push(ctx context.Context, fullURL string, size int64, body io.Reader) error {
	p, err := t.resolvePath(fullURL)
	if err != nil {
		return err
	}
	s, cleanup, err := t.dial(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := s.MkdirAll(path.Dir(p)); err != nil {
		return fmt.Errorf("mkdir %s: %w", path.Dir(p), err)
	}
	// Write to a sibling tmp file + rename for atomicity. Avoids partial
	// readers seeing a half-uploaded backup.
	tmp := p + ".tmp"
	f, err := s.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := io.Copy(f, body); err != nil {
		_ = f.Close()
		_ = s.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = s.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := s.PosixRename(tmp, p); err != nil {
		// Fall back to non-atomic rename when the server doesn't speak
		// posix-rename@openssh.com (some older sftpgo versions).
		_ = s.Remove(p)
		if rErr := s.Rename(tmp, p); rErr != nil {
			_ = s.Remove(tmp)
			return fmt.Errorf("rename %s → %s: posix=%v fallback=%v", tmp, p, err, rErr)
		}
	}
	return nil
}

func (t *sftpTarget) Pull(ctx context.Context, fullURL string, dst io.Writer) (int64, error) {
	p, err := t.resolvePath(fullURL)
	if err != nil {
		return 0, err
	}
	s, cleanup, err := t.dial(ctx)
	if err != nil {
		return 0, err
	}
	defer cleanup()
	f, err := s.Open(p)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", p, err)
	}
	defer f.Close()
	return io.Copy(dst, f)
}

func (t *sftpTarget) List(ctx context.Context, prefixURL string) ([]Entry, error) {
	p, err := t.resolvePath(prefixURL)
	if err != nil {
		return nil, err
	}
	s, cleanup, err := t.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	// Walk recursively under p so the abstraction matches S3's prefix-list
	// semantics (everything under the prefix, not just direct children).
	var out []Entry
	walker := s.Walk(p)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			// Treat "not found" the same as S3 : empty list, no error.
			if isNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("walk %s: %w", walker.Path(), err)
		}
		info := walker.Stat()
		if info.IsDir() {
			continue
		}
		u := *mustParse(prefixURL)
		u.Path = walker.Path()
		out = append(out, Entry{
			URL:          u.String(),
			SizeBytes:    info.Size(),
			LastModified: info.ModTime(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out, nil
}

func (t *sftpTarget) Delete(ctx context.Context, fullURL string) error {
	p, err := t.resolvePath(fullURL)
	if err != nil {
		return err
	}
	s, cleanup, err := t.dial(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := s.Remove(p); err != nil {
		if isNotExist(err) {
			return nil
		}
		return fmt.Errorf("remove %s: %w", p, err)
	}
	return nil
}

// mustParse panics on parse failure — only called for URLs we already
// validated through resolvePath, so a failure here is a bug.
func mustParse(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(fmt.Sprintf("backuptarget: re-parse of validated URL %q failed: %v", s, err))
	}
	return u
}

// isNotExist reports whether err is the SFTP server's "no such file" error.
// Different SFTP servers wrap the code in different error types, so we
// match on the canonical string.
func isNotExist(err error) bool {
	return err != nil && strings.Contains(err.Error(), "does not exist")
}
