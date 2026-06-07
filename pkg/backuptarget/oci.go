package backuptarget

// oci.go : OCI registry backup target. Stores each backup as one OCI
// artifact (manifest + blobs) in a regular OCI registry — works with any
// distribution-spec compliant registry :
//
//   * ghcr.io (free for public repos, paid for private)
//   * harbor (apache-2.0, self-hosted)
//   * distribution/registry (apache-2.0, the reference OCI registry)
//   * zot (apache-2.0, modern self-hosted)
//   * AWS ECR / Google AR / Azure ACR
//
// Why OCI over S3 for the openweft default :
//
//   * Content-addressed by design — digest verification built into the
//     protocol, no need to ship a separate SHA-256 sidecar.
//   * The same registry the operator already runs for weft drivers /
//     kernel artifacts (one set of credentials, one auth flow).
//   * Mirrorable via standard OCI mirror tooling — `skopeo copy` between
//     registries, no custom backup-replication needed.
//   * Signable / verifiable with cosign — supply-chain story unified
//     with everything else weft ships.
//   * versitygw is for the "I want S3" operator ; CubeFS objectnode is
//     for "I'm in CubeFS shared-storage already". OCI is for everyone
//     else (which is most of openweft's target audience).
//
// URL shape : "oci://registry.example.com/repo:tag". The :tag part keys
// the manifest the manifest carries (a) one blob layer per backup body,
// (b) annotations with the metadata sidecar so List can recover them
// without pulling the body. Tag conventions :
//
//   <volume_uuid>-<snapshot_name>      → one backup
//   <volume_uuid>-<snapshot_name>-meta → just the metadata (lighter pull)
//
// Auth : oras-go's default credential chain — docker config.json,
// $REGISTRY_AUTH_FILE, anonymous fallback. Same chain weft-agent uses
// for plugin pulls, no new env knobs.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/memory"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

// ociArtifactType is the value we stamp on the manifest's ArtifactType
// field so List can filter and tooling can identify weft-block backups
// in registries that hold other artifact kinds.
const (
	ociArtifactType = "application/vnd.openweft.weft-block.backup.v1"
	ociLayerMediaT  = "application/vnd.openweft.weft-block.backup.body.v1"
	// ociMetadataAnno is the annotation key on the manifest carrying the
	// JSON metadata sidecar. Kept under 32 KiB to fit comfortably in a
	// registry's per-annotation cap.
	ociMetadataAnno = "openweft.weft-block.backup.metadata"
)

type ociTarget struct {
	registry string // e.g. "ghcr.io"
	repo     string // e.g. "openweft/backups"
}

func newOCITarget(u *url.URL) (Target, error) {
	if u.Host == "" {
		return nil, fmt.Errorf("backuptarget: oci URL %q missing host", u.String())
	}
	repo := strings.TrimPrefix(u.Path, "/")
	if repo == "" {
		return nil, fmt.Errorf("backuptarget: oci URL %q missing repository path", u.String())
	}
	return &ociTarget{registry: u.Host, repo: repo}, nil
}

func (t *ociTarget) Scheme() string { return SchemeOCI }

// repoClient builds an oras remote.Repository for our (registry, repo)
// tuple. Auth uses oras-go's DefaultClient credential chain ; the
// operator can override per-registry with docker config.json or
// $REGISTRY_AUTH_FILE — the same surface weft-agent's driver pulls use.
func (t *ociTarget) repoClient() (*remote.Repository, error) {
	ref := t.registry + "/" + t.repo
	repo, err := remote.NewRepository(ref)
	if err != nil {
		return nil, fmt.Errorf("oci: build remote.Repository for %s: %w", ref, err)
	}
	// Credential is left nil → anonymous pull/push. Operators with
	// private registries replace it at startup with
	// `credentials.NewStoreFromDocker(...).Get(...)` or an inline
	// auth.StaticCredential(registry, auth.Credential{...}). Keeping
	// the default tight here means a sane CI / public-registry path
	// works zero-config.
	repo.Client = &auth.Client{
		Client: retry.DefaultClient,
		Cache:  auth.NewCache(),
	}
	return repo, nil
}

// tagFor turns a per-call URL into the OCI tag we'll PUT under. The URL
// shape we accept is "oci://<registry>/<repo>:<tag>" where <tag> encodes
// the backup identity. The TrimPrefix here matches what we built in
// Push/Pull when callers pass us full target URLs assembled from the
// per-call key.
func (t *ociTarget) tagFor(fullURL string) (string, error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", err
	}
	if u.Scheme != SchemeOCI {
		return "", fmt.Errorf("oci target got non-oci URL %q", fullURL)
	}
	// The path is "/<repo>:<tag>" — split on ':' to recover the tag.
	p := strings.TrimPrefix(u.Path, "/")
	if i := strings.LastIndex(p, ":"); i > 0 {
		return p[i+1:], nil
	}
	return "", fmt.Errorf("oci URL %q is missing a :tag", fullURL)
}

func (t *ociTarget) Push(ctx context.Context, fullURL string, size int64, body io.Reader) error {
	tag, err := t.tagFor(fullURL)
	if err != nil {
		return err
	}
	repo, err := t.repoClient()
	if err != nil {
		return err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read backup body: %w", err)
	}
	// Push the body layer first.
	bodyDigest := digest.FromBytes(data)
	bodyDesc := ocispec.Descriptor{
		MediaType: ociLayerMediaT,
		Digest:    bodyDigest,
		Size:      int64(len(data)),
	}
	memStore := memory.New()
	if err := memStore.Push(ctx, bodyDesc, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("stage body blob: %w", err)
	}
	// Build the manifest descriptor with our artifact type + a stamped
	// creation timestamp annotation (useful when the operator's looking
	// at the registry UI).
	opts := oras.PackManifestOptions{
		Layers: []ocispec.Descriptor{bodyDesc},
		ManifestAnnotations: map[string]string{
			ocispec.AnnotationCreated: time.Now().UTC().Format(time.RFC3339),
		},
	}
	manDesc, err := oras.PackManifest(ctx, memStore, oras.PackManifestVersion1_1, ociArtifactType, opts)
	if err != nil {
		return fmt.Errorf("pack manifest: %w", err)
	}
	if err := memStore.Tag(ctx, manDesc, tag); err != nil {
		return fmt.Errorf("local tag %s: %w", tag, err)
	}
	// Copy memStore → remote repo. oras handles deduplication via
	// HEAD-by-digest before each blob upload.
	if _, err := oras.Copy(ctx, memStore, tag, repo, tag, oras.DefaultCopyOptions); err != nil {
		return fmt.Errorf("oci push %s:%s: %w", t.registry+"/"+t.repo, tag, err)
	}
	_ = size // accepted for interface symmetry ; OCI knows the size from the body
	return nil
}

func (t *ociTarget) Pull(ctx context.Context, fullURL string, dst io.Writer) (int64, error) {
	tag, err := t.tagFor(fullURL)
	if err != nil {
		return 0, err
	}
	repo, err := t.repoClient()
	if err != nil {
		return 0, err
	}
	// Resolve the manifest, then fetch its single layer.
	manDesc, err := repo.Resolve(ctx, tag)
	if err != nil {
		return 0, fmt.Errorf("oci resolve %s:%s: %w", t.registry+"/"+t.repo, tag, err)
	}
	manReader, err := repo.Fetch(ctx, manDesc)
	if err != nil {
		return 0, fmt.Errorf("oci fetch manifest %s: %w", manDesc.Digest, err)
	}
	manBytes, err := io.ReadAll(manReader)
	_ = manReader.Close()
	if err != nil {
		return 0, fmt.Errorf("read manifest: %w", err)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manBytes, &manifest); err != nil {
		return 0, fmt.Errorf("decode manifest: %w", err)
	}
	if len(manifest.Layers) != 1 {
		return 0, fmt.Errorf("expected 1 backup body layer, got %d", len(manifest.Layers))
	}
	bodyReader, err := repo.Fetch(ctx, manifest.Layers[0])
	if err != nil {
		return 0, fmt.Errorf("fetch body layer: %w", err)
	}
	defer bodyReader.Close()
	return io.Copy(dst, bodyReader)
}

func (t *ociTarget) List(ctx context.Context, prefixURL string) ([]Entry, error) {
	repo, err := t.repoClient()
	if err != nil {
		return nil, err
	}
	// The prefixURL is "oci://<registry>/<repo>" (no :tag) — list all
	// tags in the repository, returning one Entry per. Tag filtering on
	// the prefix happens in the driver-side caller.
	tags, err := registry.Tags(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("oci list tags %s/%s: %w", t.registry, t.repo, err)
	}
	out := make([]Entry, 0, len(tags))
	for _, tag := range tags {
		out = append(out, Entry{
			URL: fmt.Sprintf("oci://%s/%s:%s", t.registry, t.repo, tag),
		})
	}
	return out, nil
}

func (t *ociTarget) Delete(ctx context.Context, fullURL string) error {
	tag, err := t.tagFor(fullURL)
	if err != nil {
		return err
	}
	repo, err := t.repoClient()
	if err != nil {
		return err
	}
	manDesc, err := repo.Resolve(ctx, tag)
	if err != nil {
		// Not-found = idempotent no-op. Detect by string match because
		// oras' typed error surface is conservative on the Delete path.
		if isOCINotFound(err) {
			return nil
		}
		return fmt.Errorf("oci resolve %s: %w", tag, err)
	}
	if err := repo.Delete(ctx, manDesc); err != nil {
		return fmt.Errorf("oci delete %s: %w", tag, err)
	}
	return nil
}

func isOCINotFound(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "404"))
}
