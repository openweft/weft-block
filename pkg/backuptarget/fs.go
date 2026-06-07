package backuptarget

// fs.go : local-filesystem backup target. Strictly for dev / tests /
// the "I'm just trying things out before standing up an OCI registry"
// case. Production deployments should use oci:// (default), s3://, or
// sftp:// — fs:// has no replication, no encryption-at-rest, no off-host
// durability, and lives on a single disk.
//
// URL shape : "fs://<absolute_path>" — e.g. fs:///var/lib/weft-block-backups.
// The path MUST be absolute (a relative path here would resolve against
// the daemon's CWD, which is usually not what the operator expects).
//
// Layout : one file per URL, atomic rename through a sibling .tmp ;
// listing walks the directory tree under a prefix URL ; delete is
// idempotent. No metadata layer — every Target consumer (CreateBackup's
// .bin + .json pair) lives as two distinct files.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
)

type fsTarget struct {
	root string // /absolute/path/from/url
}

func newFSTarget(u *url.URL) (Target, error) {
	// url.Parse on "fs:///abs/path" puts /abs/path in u.Path with u.Host="".
	// "fs://host/path" puts host in u.Host — reject that to avoid silent
	// network-vs-local confusion.
	if u.Host != "" {
		return nil, fmt.Errorf("backuptarget: fs URL %q has a non-empty host (use fs:///abs/path)", u.String())
	}
	if u.Path == "" {
		return nil, fmt.Errorf("backuptarget: fs URL %q missing path", u.String())
	}
	if !filepath.IsAbs(u.Path) {
		return nil, fmt.Errorf("backuptarget: fs URL path %q must be absolute", u.Path)
	}
	return &fsTarget{root: u.Path}, nil
}

func (t *fsTarget) Scheme() string { return SchemeFS }

// pathFor converts a per-call URL into the on-disk file path. The URL's
// path is taken verbatim (the BackupTarget abstraction's contract says
// every per-call URL is fully qualified, not relative-to-root).
func (t *fsTarget) pathFor(fullURL string) (string, error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", err
	}
	if u.Scheme != SchemeFS {
		return "", fmt.Errorf("fs target got non-fs URL %q", fullURL)
	}
	if !filepath.IsAbs(u.Path) {
		return "", fmt.Errorf("fs URL path %q must be absolute", u.Path)
	}
	return u.Path, nil
}

func (t *fsTarget) Push(ctx context.Context, fullURL string, size int64, body io.Reader) error {
	p, err := t.pathFor(fullURL)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(p), err)
	}
	tmp := p + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := io.Copy(f, body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s → %s: %w", tmp, p, err)
	}
	_ = size
	return nil
}

func (t *fsTarget) Pull(ctx context.Context, fullURL string, dst io.Writer) (int64, error) {
	p, err := t.pathFor(fullURL)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(p)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", p, err)
	}
	defer f.Close()
	return io.Copy(dst, f)
}

func (t *fsTarget) List(ctx context.Context, prefixURL string) ([]Entry, error) {
	p, err := t.pathFor(prefixURL)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("stat %s: %w", p, err)
	}
	if !info.IsDir() {
		// Single-file listing — emit one Entry.
		return []Entry{{
			URL:          fullURLFromTemplate(prefixURL, p),
			SizeBytes:    info.Size(),
			LastModified: info.ModTime(),
		}}, nil
	}
	var out []Entry
	walkErr := filepath.WalkDir(p, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, Entry{
			URL:          fullURLFromTemplate(prefixURL, path),
			SizeBytes:    fi.Size(),
			LastModified: fi.ModTime(),
		})
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("walk %s: %w", p, walkErr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out, nil
}

func (t *fsTarget) Delete(ctx context.Context, fullURL string) error {
	p, err := t.pathFor(fullURL)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove %s: %w", p, err)
	}
	return nil
}

// fullURLFromTemplate builds a per-entry URL with the prefixURL's scheme
// + the entry's actual path. We keep the prefix's URL bytes (so any
// scheme-side decoration the operator typed comes through) and override
// the path component.
func fullURLFromTemplate(prefixURL, entryPath string) string {
	u, err := url.Parse(prefixURL)
	if err != nil {
		return "fs://" + entryPath
	}
	u.Path = entryPath
	return u.String()
}
