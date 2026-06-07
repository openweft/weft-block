// Package backuptarget abstracts the "where do we ship a backup to" concern
// behind a single Target interface. Two concrete drivers ship today :
//
//   - s3.go : versitygw / native S3 / any S3-compatible object store.
//     URL shape : "s3://bucket@region/prefix" with credentials passed via
//     standard AWS env vars (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) or
//     the SDK's default chain. Endpoint override via AWS_ENDPOINT_URL_S3.
//
//   - sftp.go : sftpgo / OpenSSH sshd. URL shape :
//     "sftp://user@host:port/path", credentials via WEFT_SFTP_PASSWORD or
//     ssh-agent. Host key verification is on by default ; expected key
//     comes from WEFT_SFTP_KNOWN_HOSTS (defaults to ~/.ssh/known_hosts).
//
// The interface is intentionally tiny — Push streams a Reader, Pull writes
// to a Writer, List + Delete operate on URLs. Backupstore-style structured
// metadata lives one layer up (the weft-block driver wraps the Target with
// snapshot-chunk encoding / chunk indexing on top).
package backuptarget

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

// Scheme* identify the backup-target URL prefixes weft-block ships.
// OCI is the openweft default for the snapshot+backup story — see the
// header of pkg/backuptarget/oci.go for the rationale (content-addressed
// by design, shared auth surface with driver/kernel pulls, cosign-
// signable, mirrors via standard OCI tooling).
const (
	SchemeOCI  = "oci"
	SchemeS3   = "s3"   // versitygw / CubeFS objectnode / AWS S3 (MinIO deliberately excluded — see no-minio policy)
	SchemeSFTP = "sftp" // sftpgo / OpenSSH sshd
	SchemeFS   = "fs"   // local filesystem — dev / tests only, no off-host durability
)

// Target is the abstract sink a backup ships to. All methods are safe for
// concurrent use ; implementations must validate URLs each call (one Target
// may legitimately route many backups to different prefixes).
type Target interface {
	// Scheme returns the URL scheme this Target binds, e.g. "s3" or "sftp".
	Scheme() string

	// Push uploads an object's content to the target at the given URL.
	// The Reader is consumed sequentially ; size is informational (some
	// backends need it for chunked transfer encoding, others stream).
	// Returns when the upload is durable on the remote (S3 PutObject ACK,
	// SFTP fsync). Idempotent in the sense that an identical URL re-upload
	// overwrites cleanly.
	Push(ctx context.Context, fullURL string, size int64, body io.Reader) error

	// Pull downloads an object from the target into the given Writer.
	// Returns the size pulled. Truncates short on context cancellation.
	Pull(ctx context.Context, fullURL string, dst io.Writer) (int64, error)

	// List enumerates every object under the given URL prefix. The "prefix"
	// is treated as a directory-like boundary — listing "sftp://h/x/" finds
	// "sftp://h/x/a" and "sftp://h/x/b/c" but NOT "sftp://h/y". Returns
	// driver-stable order (typically lexicographic).
	List(ctx context.Context, prefixURL string) ([]Entry, error)

	// Delete removes one object. Idempotent : deleting a missing URL is
	// a no-op, not an error.
	Delete(ctx context.Context, fullURL string) error
}

// Entry describes one object as returned by List(). SizeBytes is the
// on-target size (post-compression / encryption — the abstraction doesn't
// know what the bytes mean to weft-block).
type Entry struct {
	URL          string
	SizeBytes    int64
	LastModified time.Time
}

// New routes a target URL to its concrete Target driver. Caller-owned
// configuration (S3 creds, SFTP known_hosts) is loaded lazily by the driver
// from env on first use ; New itself is cheap and only validates the URL
// scheme is one we ship.
//
// Unknown / malformed URLs fail fast with a clear error — callers shouldn't
// have to disambiguate "unsupported scheme" from "the network is wrong" in
// their error handling.
func New(targetURL string) (Target, error) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("backuptarget: parse %q: %w", targetURL, err)
	}
	switch strings.ToLower(u.Scheme) {
	case SchemeOCI:
		return newOCITarget(u)
	case SchemeS3:
		return newS3Target(u)
	case SchemeSFTP:
		return newSFTPTarget(u)
	case SchemeFS:
		return newFSTarget(u)
	default:
		return nil, fmt.Errorf("backuptarget: unsupported scheme %q (want oci, s3, sftp, or fs)", u.Scheme)
	}
}
