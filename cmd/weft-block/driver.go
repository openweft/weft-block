package main

// driver.go implements drivers.VolumeDriver on top of the weft-block control
// plane (control.MemStore + placement). EnsureVolume / DestroyVolume operate
// on the store; AttachVolume / DetachVolume are stubbed pending the agent's
// per-host reconcile loop (which is what actually spawns weft-block-engine on
// the chosen host and brings up /dev/nbdN).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	drivers "github.com/openweft/weft-drivers"

	"github.com/openweft/weft-block/control"
	"github.com/openweft/weft-block/pkg/backupcrypto"
	"github.com/openweft/weft-block/pkg/backuptarget"
	"github.com/openweft/weft-block/pkg/reconcile"
)

// hostInfo is the cached identity the plugin advertises (env-supplied).
type hostInfo struct {
	uuid     string
	hostname string
	az       string
}

func (h hostInfo) driversHostInfo() drivers.HostInfo {
	return drivers.HostInfo{
		UUID:         h.uuid,
		Hostname:     h.hostname,
		AZ:           h.az,
		Hypervisor:   "block",
		Architecture: runtime.GOARCH,
	}
}

// volumeDriver is the cluster-wide block backend. Local() = false means a
// volume can be attached on any host — the engine binds to the volume on the
// requested host and dials the replicas across the overlay.
type volumeDriver struct {
	hi       hostInfo
	stateDir string

	mu    sync.Mutex
	store control.VolumeStore // shared with the reconcile loop
	// spawner is the Spawner the reconcile loop uses on this host. The
	// driver consults it (via LocalEngine) to invoke snapshot / backup
	// operations on engines this host actually runs. Cross-host calls
	// land here because the control plane routes through the dispatch
	// table — when an engine isn't local we return ErrNotFound so the
	// caller knows to re-dispatch.
	spawner reconcile.Spawner

	// hosts is the candidate pool placement may use. Defaults to [self] for
	// single-host dev; SetHostCandidates injects the cluster-wide set (the
	// weft Host registry feeds this in production, tests inject directly).
	hosts []control.HostCandidate

	// fsCoord coordinates guest-side filesystem freeze around
	// CreateSnapshot when the spec labels opt in (freeze_fs=true
	// + vm_uuid). Defaults to NoopFSCoordinator ; main.go can
	// swap a NATSFSCoordinator when WEFT_BLOCK_NATS_URL is set.
	fsCoord FSCoordinator
}

func newVolumeDriver(hi hostInfo, stateDir string, store control.VolumeStore, spawner reconcile.Spawner) *volumeDriver {
	v := &volumeDriver{hi: hi, stateDir: stateDir, store: store, spawner: spawner}
	v.hosts = []control.HostCandidate{{UUID: hi.uuid, DC: hi.az}}
	v.fsCoord = NoopFSCoordinator{}
	return v
}

// SetFSCoordinator swaps the freeze/thaw coordinator. Called from
// main.go after the NATS connection is established ; tests inject
// fakes directly.
func (v *volumeDriver) SetFSCoordinator(c FSCoordinator) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if c == nil {
		v.fsCoord = NoopFSCoordinator{}
		return
	}
	v.fsCoord = c
}

// SetHostCandidates replaces the placement pool. Called by the cluster-side
// integration once the weft Host registry is reachable; tests use it to
// configure multi-host scenarios.
func (v *volumeDriver) SetHostCandidates(hosts []control.HostCandidate) {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]control.HostCandidate, len(hosts))
	copy(out, hosts)
	v.hosts = out
}

var _ drivers.VolumeDriver = (*volumeDriver)(nil)

func (v *volumeDriver) Name() string { return "block" }
func (v *volumeDriver) Local() bool  { return false }

func (v *volumeDriver) HostInfo(context.Context) (drivers.HostInfo, error) {
	return v.hi.driversHostInfo(), nil
}

// EnsureVolume records the Volume + assigns N Replica placements in the
// store. Idempotent: re-running with the same spec is a no-op once the volume
// is Healthy; growing the size is allowed (grow-only contract).
//
// The actual per-host replica processes are brought up by the agent's
// reconcile loop watching the store — not by this RPC. This call returns once
// the desired state is persisted.
func (v *volumeDriver) EnsureVolume(ctx context.Context, spec drivers.VolumeSpec) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	cur, err := v.store.GetVolume(ctx, spec.UUID)
	if err != nil && err != control.ErrNotFound {
		return fmt.Errorf("weft-block: load volume %s: %w", spec.UUID, err)
	}
	candidates := v.hostCandidates()
	replicaCount := effectiveReplicaCount(len(candidates))
	desired := control.Volume{
		UUID:         spec.UUID,
		ProjectUUID:  spec.ProjectUUID,
		Name:         spec.Name,
		SizeBytes:    int64(spec.SizeGiB) * (1 << 30),
		ReplicaCount: replicaCount,
		State:        control.VolumeStateProvisioning,
		UpdatedAt:    time.Now(),
	}
	if err == control.ErrNotFound {
		if err := v.store.PutVolume(ctx, desired); err != nil {
			return fmt.Errorf("weft-block: persist volume: %w", err)
		}
		// Initial replica records (Pending; the agent fills Address/State once
		// it's actually spawned weft-block-replica on each host).
		picks, perr := control.PlaceReplicas(candidates, replicaCount)
		if perr != nil {
			return fmt.Errorf("weft-block: placement: %w", perr)
		}
		for i, h := range picks {
			r := control.Replica{
				UUID:       fmt.Sprintf("%s-r%d", spec.UUID, i),
				VolumeUUID: spec.UUID,
				HostUUID:   h.UUID,
				DC:         h.DC,
				State:      control.ReplicaStatePending,
			}
			if err := v.store.PutReplica(ctx, r); err != nil {
				return fmt.Errorf("weft-block: persist replica %s: %w", r.UUID, err)
			}
		}
		return nil
	}
	// Volume exists: enforce grow-only on size.
	if desired.SizeBytes < cur.SizeBytes {
		return fmt.Errorf("weft-block: shrink %d → %d not allowed (grow-only)", cur.SizeBytes, desired.SizeBytes)
	}
	cur.SizeBytes = desired.SizeBytes
	cur.Name = desired.Name
	return v.store.PutVolume(ctx, cur)
}

// DestroyVolume marks the Volume + its Replicas for deletion. Idempotent;
// missing UUID returns nil. The actual on-disk cleanup happens in the agent
// reconcile loop when it observes the Deleting state.
func (v *volumeDriver) DestroyVolume(ctx context.Context, volumeUUID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	rs, _ := v.store.ListReplicasFor(ctx, volumeUUID)
	for _, r := range rs {
		r.State = control.ReplicaStateDeleting
		_ = v.store.PutReplica(ctx, r)
	}
	cur, err := v.store.GetVolume(ctx, volumeUUID)
	if err == control.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	cur.State = control.VolumeStateDeleting
	return v.store.PutVolume(ctx, cur)
}

// AttachVolume creates an Engine record on hostUUID and polls the store until
// the per-host reconcile loop spawns the controller + NBD frontend and writes
// State=Running + FrontendPath back. Returns FrontendPath (e.g. "/dev/nbd0")
// as the BackingPath the hypervisor opens. Idempotent: a re-attach on the same
// host reuses the existing engine record.
func (v *volumeDriver) AttachVolume(ctx context.Context, volumeUUID, hostUUID string) (drivers.AttachedVolume, error) {
	v.mu.Lock()
	vol, err := v.store.GetVolume(ctx, volumeUUID)
	if err != nil {
		v.mu.Unlock()
		return drivers.AttachedVolume{}, fmt.Errorf("weft-block: volume %s: %w", volumeUUID, err)
	}
	engines, _ := v.store.ListEnginesFor(ctx, volumeUUID)
	var engineUUID string
	for _, e := range engines {
		if e.HostUUID == hostUUID {
			engineUUID = e.UUID
			break
		}
	}
	if engineUUID == "" {
		engineUUID = fmt.Sprintf("%s@%s", volumeUUID, hostUUID)
		if err := v.store.PutEngine(ctx, control.Engine{
			UUID:       engineUUID,
			VolumeUUID: volumeUUID,
			HostUUID:   hostUUID,
			State:      control.ReplicaStatePending,
		}); err != nil {
			v.mu.Unlock()
			return drivers.AttachedVolume{}, err
		}
		vol.AttachedTo = hostUUID
		vol.State = control.VolumeStateAttached
		_ = v.store.PutVolume(ctx, vol)
	}
	v.mu.Unlock()

	// Poll until Running. The deadline is intentionally generous — for a real
	// engine spawn the controller has to dial every replica, which can take
	// seconds when there are pending rebuilds. The caller's context cancels
	// the wait if it doesn't want to block that long.
	pollUntil := time.Now().Add(attachTimeout)
	for time.Now().Before(pollUntil) {
		e, err := v.store.GetEngine(ctx, engineUUID)
		if err != nil {
			return drivers.AttachedVolume{}, err
		}
		switch e.State {
		case control.ReplicaStateRunning:
			if e.FrontendPath == "" {
				return drivers.AttachedVolume{}, fmt.Errorf("weft-block: engine %s reached Running but FrontendPath empty (spawner bug)", e.UUID)
			}
			return drivers.AttachedVolume{BackingPath: e.FrontendPath, ReadOnly: false}, nil
		case control.ReplicaStateFaulted:
			return drivers.AttachedVolume{}, fmt.Errorf("weft-block: engine %s reached Faulted state", e.UUID)
		}
		select {
		case <-ctx.Done():
			return drivers.AttachedVolume{}, ctx.Err()
		case <-time.After(attachPollInterval):
		}
	}
	return drivers.AttachedVolume{}, fmt.Errorf("weft-block: AttachVolume(%s, %s) timed out waiting for engine to reach Running", volumeUUID, hostUUID)
}

// DetachVolume marks the engine for deletion on hostUUID; the reconcile loop
// stops the controller, tears down NBD, and removes the record. Idempotent.
func (v *volumeDriver) DetachVolume(ctx context.Context, volumeUUID, hostUUID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	engines, _ := v.store.ListEnginesFor(ctx, volumeUUID)
	for _, e := range engines {
		if e.HostUUID != hostUUID {
			continue
		}
		e.State = control.ReplicaStateDeleting
		if err := v.store.PutEngine(ctx, e); err != nil {
			return err
		}
	}
	if vol, err := v.store.GetVolume(ctx, volumeUUID); err == nil && vol.AttachedTo == hostUUID {
		vol.AttachedTo = ""
		vol.State = control.VolumeStateHealthy
		_ = v.store.PutVolume(ctx, vol)
	}
	return nil
}

// attachTimeout / attachPollInterval govern AttachVolume's wait for the
// reconcile loop to drive the engine to Running.
const (
	attachTimeout      = 60 * time.Second
	attachPollInterval = 200 * time.Millisecond
)

// ----- Snapshots -----

// localEngineFor finds the engine for volumeUUID that this host runs (if any)
// and returns its handle. ErrNotFound when no engine is local — the control
// plane should re-dispatch the call to the right host.
func (v *volumeDriver) localEngineFor(ctx context.Context, volumeUUID string) (reconcile.EngineHandle, error) {
	engines, _ := v.store.ListEnginesFor(ctx, volumeUUID)
	for _, e := range engines {
		if e.HostUUID != v.hi.uuid {
			continue
		}
		if e.State != control.ReplicaStateRunning {
			return nil, fmt.Errorf("engine %s not yet Running (state=%s)", e.UUID, e.State)
		}
		h, ok := v.spawner.LocalEngine(e.UUID)
		if !ok {
			return nil, fmt.Errorf("engine %s missing from local spawner", e.UUID)
		}
		return h, nil
	}
	return nil, drivers.ErrNotFound
}

// CreateSnapshot triggers a snapshot on the engine. The host receiving the
// call must own the engine (Local() = false on weft-block means the control
// plane dispatches to the right host via the driver table).
//
// Filesystem-freeze : when spec.Labels carry `freeze_fs=true` + `vm_uuid=<id>`,
// we call the configured FSCoordinator to ask the guest to FIFREEZE before the
// snapshot and FITHAW after. Thaw runs in a defer so the guest doesn't stay
// frozen if the snapshot call panics or returns an error.
func (v *volumeDriver) CreateSnapshot(ctx context.Context, spec drivers.SnapshotSpec) (drivers.Snapshot, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Validate the freeze opt-in FIRST so misconfigured specs fail
	// fast without touching the engine. The flag is fail-loud : if
	// freeze is requested but vm_uuid is missing or the coordinator
	// is unwired, we error rather than silently downgrade — the
	// operator explicitly asked for filesystem-consistent semantics
	// and a quiet downgrade would produce a snapshot that LOOKS
	// consistent but isn't.
	freeze, vmUUID := shouldFreezeFS(spec.Labels)
	if freeze {
		if vmUUID == "" {
			return drivers.Snapshot{}, fmt.Errorf("weft-block: freeze_fs requires labels[%q]=<uuid>", labelKeyVMUUID)
		}
		if v.fsCoord == nil {
			return drivers.Snapshot{}, fmt.Errorf("weft-block: freeze_fs requested but no FS coordinator wired (set WEFT_BLOCK_NATS_URL)")
		}
	}

	h, err := v.localEngineFor(ctx, spec.VolumeUUID)
	if err != nil {
		return drivers.Snapshot{}, err
	}

	if freeze {
		if err := v.fsCoord.Freeze(ctx, vmUUID); err != nil {
			return drivers.Snapshot{}, fmt.Errorf("weft-block: guest fs freeze: %w", err)
		}
		// Thaw EVERY exit path, including the panic case. The
		// per-call ctx may already be cancelled by the time we
		// thaw, so we use a fresh background ctx with its own
		// timeout — letting the guest stay frozen because the
		// dispatch ctx expired is the worst-case outcome.
		defer func() {
			thawCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = v.fsCoord.Thaw(thawCtx, vmUUID)
		}()
	}

	// freezeFS=true passed to the controller is the LEGACY path
	// (longhorn's k8s container-namespace mount-utils trick). Our
	// guest-side freeze above already took care of consistency, so
	// we pass false here and let the controller's sync() fallback
	// handle the dirty-pages flush on the host side.
	name, err := h.CreateSnapshot(spec.Name, spec.Labels, false)
	if err != nil {
		return drivers.Snapshot{}, fmt.Errorf("weft-block: CreateSnapshot: %w", err)
	}
	return drivers.Snapshot{
		VolumeUUID:      spec.VolumeUUID,
		Name:            name,
		Labels:          spec.Labels,
		UserCreated:     true,
		CreatedAtUnixNs: time.Now().UnixNano(),
	}, nil
}

// ListSnapshots returns the engine's snapshot chain, oldest to youngest.
func (v *volumeDriver) ListSnapshots(ctx context.Context, volumeUUID string) ([]drivers.Snapshot, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	h, err := v.localEngineFor(ctx, volumeUUID)
	if err != nil {
		return nil, err
	}
	infos, err := h.ListSnapshots()
	if err != nil {
		return nil, fmt.Errorf("weft-block: ListSnapshots: %w", err)
	}
	out := make([]drivers.Snapshot, 0, len(infos))
	for _, s := range infos {
		// Skip snapshots marked Removed — they're tombstones the purger
		// will reclaim. UI / CLI care about the live chain.
		if s.Removed {
			continue
		}
		var createdNs int64
		if t, err := time.Parse(time.RFC3339, s.Created); err == nil {
			createdNs = t.UnixNano()
		}
		out = append(out, drivers.Snapshot{
			VolumeUUID:      volumeUUID,
			Name:            s.Name,
			Parent:          s.Parent,
			SizeBytes:       s.SizeBytes,
			CreatedAtUnixNs: createdNs,
			Labels:          s.Labels,
			UserCreated:     s.UserCreated,
		})
	}
	return out, nil
}

// DeleteSnapshot marks a snapshot for removal and triggers purge across
// replicas. Returns once the mark phase is durable on every healthy replica
// — the physical merge runs in background.
func (v *volumeDriver) DeleteSnapshot(ctx context.Context, volumeUUID, snapshotName string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	h, err := v.localEngineFor(ctx, volumeUUID)
	if err != nil {
		return err
	}
	if err := h.DeleteSnapshot(snapshotName); err != nil {
		return fmt.Errorf("weft-block: DeleteSnapshot %s: %w", snapshotName, err)
	}
	return nil
}

// RevertSnapshot rolls the volume back to a previous snapshot. The volume
// must be detached first — the engine returns an error otherwise (caller
// surfaces it as drivers.ErrInUse for the control plane to handle).
func (v *volumeDriver) RevertSnapshot(ctx context.Context, volumeUUID, snapshotName string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	// Reject when the volume is still attached — the engine refuses anyway
	// but we short-circuit here for a clearer error message.
	if vol, err := v.store.GetVolume(ctx, volumeUUID); err == nil && vol.AttachedTo != "" {
		return fmt.Errorf("weft-block: RevertSnapshot %s: %w (detach %s first)", snapshotName, drivers.ErrInUse, volumeUUID)
	}
	h, err := v.localEngineFor(ctx, volumeUUID)
	if err != nil {
		return err
	}
	if err := h.RevertSnapshot(snapshotName); err != nil {
		return fmt.Errorf("weft-block: RevertSnapshot %s: %w", snapshotName, err)
	}
	return nil
}

// ----- Clone (CoW from snapshot) -----

// CreateFromSnapshot is the V0.5.1 high-level CoW-clone RPC. Allocates a new
// Volume + Replica records keyed to (newVolumeUUID, newName), each tagged
// with the parent's (VolumeUUID, SnapshotName) so the per-host reconcile
// loops can bootstrap the new replica chain from the parent's snapshot
// instead of from an empty head — the longhorn pattern : the new replica's
// initial disk is a CoW pointer at the source snapshot, not a byte copy.
//
// Returns the new Volume so the caller can persist its UUID and (optionally)
// AttachVolume it. Idempotent : a retry with the same newVolumeUUID returns
// the already-persisted Volume rather than re-allocating replicas.
//
// V0.5.1 scope : control-plane records only. The actual per-replica
// bootstrap-from-snapshot byte plumbing — which the longhorn data plane
// already implements via CompareSnapshot/ReadSnapshot in pkg/replica — gets
// wired into the reconcile-loop's SpawnReplica path in V0.6 ; until then a
// cloned volume's replicas come up empty and the operator can populate them
// via RestoreBackup. The control-plane records are forward-compatible :
// SourceVolumeUUID + SourceSnapshot are durably persisted so the V0.6
// upgrade is in-place.
func (v *volumeDriver) CreateFromSnapshot(ctx context.Context, parentVolumeUUID, snapshotName, newVolumeUUID, newName string) (control.Volume, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return control.CreateFromSnapshot(ctx, v.store, v.hostCandidates(), control.CloneRequest{
		ParentVolumeUUID: parentVolumeUUID,
		SnapshotName:     snapshotName,
		NewVolumeUUID:    newVolumeUUID,
		NewVolumeName:    newName,
	}, time.Now)
}

// ----- Backups -----
//
// Backups ship a snapshot's contents to a Target abstracted behind
// pkg/backuptarget (today : s3 / sftp). The wire format is intentionally
// trivial — the snapshot's bytes are streamed verbatim alongside a JSON
// metadata sidecar — so restores work without any backupstore-specific
// indexing layer. Future enhancement : chunked / dedup format for
// space-efficient deltas between snapshots (longhorn-style).
//
// On-target layout (one snapshot per backup) :
//
//   <target>/<volume_uuid>/<snapshot_uuid>.bin   ← raw snapshot bytes
//   <target>/<volume_uuid>/<snapshot_uuid>.json  ← metadata (size, sha256, labels…)
//
// The backup URL returned by CreateBackup is the .bin URL ; List walks the
// JSON sidecars to reconstruct the full Backup descriptor.

// backupMeta is the sidecar JSON. Stored next to the .bin so a single
// List walk gives us everything List needs without HEADing every blob.
//
// The Encryption field, when populated, carries everything restore needs
// to re-derive the key EXCEPT the passphrase (algorithm + KDF + KDF
// params + per-backup salt). The SHA256 field reflects the bytes ACTUALLY
// uploaded to the target — so for encrypted backups it's the SHA256 of
// the ciphertext, which is what an OCI registry's digest also matches.
type backupMeta struct {
	VolumeUUID   string `json:"volume_uuid"`
	SnapshotName string `json:"snapshot_name"`
	// SizeBytes is the on-target size — what we actually uploaded. Equal
	// to the snapshot's plaintext bytes for unencrypted backups ; equal
	// to the AEAD-wrapped stream size (header + per-chunk overhead) for
	// encrypted ones. SHA256 covers exactly these bytes, so it matches
	// what an OCI registry's digest would carry.
	SizeBytes int64 `json:"size_bytes"`
	// PlaintextBytes is the snapshot's original size in bytes, used by
	// restore to size the ImportBytes loop. For unencrypted backups it's
	// equal to SizeBytes ; for encrypted ones it's the pre-AEAD count.
	// For incremental backups it's the SUM of all range sizes (NOT the
	// volume's full size — the parent backup covered the rest).
	PlaintextBytes  int64                        `json:"plaintext_bytes,omitempty"`
	SHA256          string                       `json:"sha256"`
	CreatedAtUnixNs int64                        `json:"created_at_unix_ns"`
	Labels          map[string]string            `json:"labels,omitempty"`
	ParentURL       string                       `json:"parent_url,omitempty"`
	Encryption      *drivers.BackupEncryptionInfo `json:"encryption,omitempty"`
	// Ranges, when non-empty, marks this as an incremental backup whose
	// body only carries the listed (offset, size) windows. The plaintext
	// stream is the concatenation of those ranges in this order. Restore
	// places each chunk back at its offset on the volume.
	Ranges []backupRange `json:"ranges,omitempty"`
}

// backupRange mirrors reconcile.Range on the JSON wire — tinier name
// for sidecar compactness across many ranges.
type backupRange struct {
	Offset int64 `json:"o"`
	Size   int64 `json:"s"`
}

// backupBinURL returns the .bin URL where a backup's raw bytes live.
func backupBinURL(target, volumeUUID, snapshotName string) string {
	u, _ := url.Parse(target)
	u.Path = path.Join(u.Path, volumeUUID, snapshotName+".bin")
	return u.String()
}

// backupMetaURL returns the .json sidecar URL.
func backupMetaURL(target, volumeUUID, snapshotName string) string {
	u, _ := url.Parse(target)
	u.Path = path.Join(u.Path, volumeUUID, snapshotName+".json")
	return u.String()
}

// readBackupMetaSibling pulls the .json sidecar that lives next to a
// backup's .bin URL. Convention : same path, same prefix, ".json" suffix.
// Used by CreateBackup when ParentURL is set : we need the parent's
// SnapshotName to feed into the engine's Compare call.
func readBackupMetaSibling(ctx context.Context, tgt backuptarget.Target, binURL string) (backupMeta, error) {
	metaURL := strings.TrimSuffix(binURL, ".bin") + ".json"
	var buf strings.Builder
	if _, err := tgt.Pull(ctx, metaURL, &builderWriter{&buf}); err != nil {
		return backupMeta{}, fmt.Errorf("pull %s: %w", metaURL, err)
	}
	var m backupMeta
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		return backupMeta{}, fmt.Errorf("parse %s: %w", metaURL, err)
	}
	return m, nil
}

// CreateBackup streams a snapshot's contents to the target. The snapshot
// referenced by spec.SnapshotName must already exist (taken via
// CreateSnapshot). Returns the backup descriptor once the upload is durable.
func (v *volumeDriver) CreateBackup(ctx context.Context, spec drivers.BackupSpec) (drivers.Backup, error) {
	if spec.Target == "" {
		return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: empty target")
	}
	if spec.SnapshotName == "" {
		return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: empty snapshot_name")
	}
	tgt, err := backuptarget.New(spec.Target)
	if err != nil {
		return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: %w", err)
	}
	v.mu.Lock()
	h, err := v.localEngineFor(ctx, spec.VolumeUUID)
	v.mu.Unlock()
	if err != nil {
		return drivers.Backup{}, err
	}
	// Resolve encryption first so a missing passphrase fails BEFORE we
	// touch the engine or open a pipe — clearer error, no half state.
	var encInfo *drivers.BackupEncryptionInfo
	var encParams backupcrypto.Params
	var encryptEnabled bool
	if spec.Encryption.Algorithm != "" {
		passphrase, err := backupcrypto.LoadPassphrase(spec.Encryption.PassphraseEnv)
		if err != nil {
			return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: %w", err)
		}
		salt, err := backupcrypto.NewSalt()
		if err != nil {
			return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: salt: %w", err)
		}
		key, err := backupcrypto.DeriveKey(passphrase, salt, spec.Encryption.KDF, spec.Encryption.KDFParams)
		if err != nil {
			return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: derive key: %w", err)
		}
		encParams = backupcrypto.Params{Algorithm: spec.Encryption.Algorithm, Key: key}
		encInfo = &drivers.BackupEncryptionInfo{
			Algorithm: spec.Encryption.Algorithm,
			KDF:       firstNonEmpty(spec.Encryption.KDF, backupcrypto.KDFArgon2id),
			KDFParams: spec.Encryption.KDFParams,
			SaltHex:   hex.EncodeToString(salt),
		}
		encryptEnabled = true
	}
	// Stream the snapshot's bytes through a SHA-256 hash so the .json
	// sidecar carries an integrity tag for the bytes actually uploaded
	// (== ciphertext when encryption is on, matching what an OCI digest
	// would also reflect). io.Pipe joins the engine reader to the target
	// uploader so the transfer happens in one pass without buffering
	// the full snapshot.
	r, w := io.Pipe()
	hasher := sha256.New()
	// uploadCounter measures bytes that hit the BackupTarget — same as
	// what SHA256 sees (== ciphertext for encrypted, plaintext otherwise).
	// We track it separately from StreamSnapshotBytes's plaintext-byte
	// return so the sidecar can record both numbers (restore uses the
	// plaintext one to size ImportBytes).
	uploadCounter := &countingWriter{w: w}
	teeUpload := &teeByteWriter{primary: uploadCounter, secondary: hasher}
	// targetSink is what BackupTarget.Push actually consumes : the pipe
	// writer wrapped with SHA-256 tee, optionally wrapped by an AEAD
	// encoder when encryption is on. The encoder writes its 32-byte
	// header + per-chunk (nonce|ct|tag) onto the tee, so the sha+pipe
	// see only ciphertext.
	var (
		targetSink reconcile.SnapshotByteWriter
		encoder    *backupcrypto.Encoder
	)
	if encryptEnabled {
		enc, err := backupcrypto.NewEncoder(teeUpload, encParams)
		if err != nil {
			return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: aead init: %w", err)
		}
		encoder = enc
		targetSink = &writerByteSink{w: enc}
	} else {
		targetSink = teeUpload
	}
	binURL := backupBinURL(spec.Target, spec.VolumeUUID, spec.SnapshotName)
	metaURL := backupMetaURL(spec.Target, spec.VolumeUUID, spec.SnapshotName)
	uploadErr := make(chan error, 1)
	go func() {
		uploadErr <- tgt.Push(ctx, binURL, 0, r)
	}()
	// Stream from the engine. Closing the pipe writer signals the
	// uploader goroutine to finish ; we then wait for its result.
	const streamBlockSize = 4 * 1024 * 1024 // 4 MiB
	// Incremental dispatch : if ParentURL is set, ask the engine for the
	// delta ranges. On Unimplemented we log + fall back to full so the
	// caller still gets a backup. On other errors we surface them.
	var (
		ranges      []reconcile.Range
		incremental bool
	)
	if spec.ParentURL != "" {
		parentMeta, err := readBackupMetaSibling(ctx, tgt, spec.ParentURL)
		if err != nil {
			return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: read parent meta: %w", err)
		}
		parentSnapName := parentMeta.SnapshotName
		got, err := h.CompareSnapshots(spec.SnapshotName, parentSnapName, streamBlockSize)
		switch {
		case err == nil && len(got) > 0:
			ranges = got
			incremental = true
		case err == nil && len(got) == 0:
			// Two snapshots are identical : create a zero-range "no-op"
			// incremental rather than a full backup. The body will be
			// empty (just the AEAD header if encrypted) ; restore is a
			// no-op too.
			ranges = []reconcile.Range{}
			incremental = true
		case errors.Is(err, reconcile.ErrCompareUnimplemented):
			// Logged at the caller's discretion ; the spec.Labels can carry
			// a note. The fallback is full backup.
			incremental = false
		default:
			return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: compare snapshots: %w", err)
		}
	}
	var (
		total      int64
		streamErr  error
	)
	if incremental {
		total, streamErr = h.StreamSnapshotRanges(spec.SnapshotName, streamBlockSize, ranges, targetSink)
	} else {
		total, streamErr = h.StreamSnapshotBytes(spec.SnapshotName, streamBlockSize, targetSink)
	}
	if encoder != nil {
		// Flush the final AEAD chunk before closing the pipe — otherwise
		// the uploader gets EOF on a truncated last chunk and the
		// decoder fails to authenticate.
		if cerr := encoder.Close(); cerr != nil && streamErr == nil {
			streamErr = cerr
		}
	}
	_ = w.Close()
	upErr := <-uploadErr
	if streamErr != nil {
		_ = tgt.Delete(ctx, binURL)
		return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: stream snapshot: %w", streamErr)
	}
	if upErr != nil {
		_ = tgt.Delete(ctx, binURL)
		return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: upload: %w", upErr)
	}
	// SizeBytes is what landed on the target (== ciphertext size for
	// encrypted backups). PlaintextBytes is the snapshot's original size
	// — only emitted when it differs from SizeBytes (i.e. when encryption
	// is on OR when incremental), keeping the sidecar minimal for the
	// common unencrypted-full case.
	plaintextBytes := int64(0)
	if encryptEnabled || incremental {
		plaintextBytes = total
	}
	var metaRanges []backupRange
	if incremental {
		metaRanges = make([]backupRange, 0, len(ranges))
		for _, r := range ranges {
			metaRanges = append(metaRanges, backupRange{Offset: r.Offset, Size: r.Size})
		}
	}
	meta := backupMeta{
		VolumeUUID:      spec.VolumeUUID,
		SnapshotName:    spec.SnapshotName,
		SizeBytes:       uploadCounter.n,
		PlaintextBytes:  plaintextBytes,
		SHA256:          hex.EncodeToString(hasher.Sum(nil)),
		CreatedAtUnixNs: time.Now().UnixNano(),
		Labels:          spec.Labels,
		ParentURL:       spec.ParentURL,
		Encryption:      encInfo,
		Ranges:          metaRanges,
	}
	metaJSON, _ := json.Marshal(meta)
	if err := tgt.Push(ctx, metaURL, int64(len(metaJSON)), strings.NewReader(string(metaJSON))); err != nil {
		// Roll back the .bin so the listing doesn't show a half-uploaded
		// backup. Idempotent : Delete on a fresh URL is fine if Push
		// failed before any bytes landed.
		_ = tgt.Delete(ctx, binURL)
		return drivers.Backup{}, fmt.Errorf("weft-block: CreateBackup: write meta: %w", err)
	}
	return drivers.Backup{
		VolumeUUID:      spec.VolumeUUID,
		SnapshotName:    spec.SnapshotName,
		URL:             binURL,
		SizeBytes:       meta.SizeBytes,
		CreatedAtUnixNs: meta.CreatedAtUnixNs,
		Labels:          spec.Labels,
		State:           "complete",
	}, nil
}

// ListBackups walks the target's metadata sidecars + reconstructs the
// Backup descriptors. Scoped to volumeUUID when non-empty.
func (v *volumeDriver) ListBackups(ctx context.Context, target, volumeUUID string) ([]drivers.Backup, error) {
	if target == "" {
		return nil, fmt.Errorf("weft-block: ListBackups: empty target")
	}
	tgt, err := backuptarget.New(target)
	if err != nil {
		return nil, fmt.Errorf("weft-block: ListBackups: %w", err)
	}
	// Build the prefix URL : either the volume-scoped subdir or the
	// target root for a full inventory.
	prefixURL := target
	if volumeUUID != "" {
		u, _ := url.Parse(target)
		u.Path = path.Join(u.Path, volumeUUID)
		prefixURL = u.String()
	}
	entries, err := tgt.List(ctx, prefixURL)
	if err != nil {
		return nil, fmt.Errorf("weft-block: ListBackups: %w", err)
	}
	var out []drivers.Backup
	for _, e := range entries {
		if !strings.HasSuffix(e.URL, ".json") {
			continue
		}
		var buf strings.Builder
		if _, err := tgt.Pull(ctx, e.URL, &builderWriter{&buf}); err != nil {
			continue // best-effort : skip unreadable sidecars
		}
		var meta backupMeta
		if err := json.Unmarshal([]byte(buf.String()), &meta); err != nil {
			continue
		}
		out = append(out, drivers.Backup{
			VolumeUUID:      meta.VolumeUUID,
			SnapshotName:    meta.SnapshotName,
			URL:             strings.TrimSuffix(e.URL, ".json") + ".bin",
			SizeBytes:       meta.SizeBytes,
			CreatedAtUnixNs: meta.CreatedAtUnixNs,
			Labels:          meta.Labels,
			State:           "complete",
		})
	}
	return out, nil
}

// DeleteBackup removes both the .bin and the .json sidecar. Idempotent.
func (v *volumeDriver) DeleteBackup(ctx context.Context, backupURL string) error {
	if backupURL == "" {
		return fmt.Errorf("weft-block: DeleteBackup: empty url")
	}
	tgt, err := backuptarget.New(backupURL)
	if err != nil {
		return fmt.Errorf("weft-block: DeleteBackup: %w", err)
	}
	metaURL := strings.TrimSuffix(backupURL, ".bin") + ".json"
	if err := tgt.Delete(ctx, backupURL); err != nil {
		return fmt.Errorf("weft-block: DeleteBackup: drop .bin: %w", err)
	}
	if err := tgt.Delete(ctx, metaURL); err != nil {
		return fmt.Errorf("weft-block: DeleteBackup: drop .json: %w", err)
	}
	return nil
}

// restoreOneBackup applies a single backup (full or incremental) to a
// live engine. Pulls the .bin via the target, runs it through the
// SHA-256 verifier + optional AEAD decoder, then either
// ImportBytes (full) or ImportBytesAtRanges (incremental). Used as the
// per-link primitive inside RestoreBackup's parent-chain walk.
func (v *volumeDriver) restoreOneBackup(ctx context.Context, h reconcile.EngineHandle, backupURL string, meta backupMeta) error {
	tgt, err := backuptarget.New(backupURL)
	if err != nil {
		return fmt.Errorf("backuptarget.New: %w", err)
	}
	var (
		decoderParams  backupcrypto.Params
		decryptEnabled bool
	)
	if meta.Encryption != nil && meta.Encryption.Algorithm != "" {
		envName := os.Getenv("WEFT_BACKUP_PASSPHRASE_ENV")
		if envName == "" {
			envName = "WEFT_BACKUP_PASSPHRASE"
		}
		passphrase, err := backupcrypto.LoadPassphrase(envName)
		if err != nil {
			return fmt.Errorf("%w (encrypted backup ; set %s)", err, envName)
		}
		salt, err := hex.DecodeString(meta.Encryption.SaltHex)
		if err != nil {
			return fmt.Errorf("decode salt: %w", err)
		}
		key, err := backupcrypto.DeriveKey(passphrase, salt, meta.Encryption.KDF, meta.Encryption.KDFParams)
		if err != nil {
			return fmt.Errorf("derive key: %w", err)
		}
		decoderParams = backupcrypto.Params{Algorithm: meta.Encryption.Algorithm, Key: key}
		decryptEnabled = true
	}
	r, w := io.Pipe()
	hasher := sha256.New()
	pullErr := make(chan error, 1)
	go func() {
		_, e := tgt.Pull(ctx, backupURL, &writerWriter{w})
		_ = w.Close()
		pullErr <- e
	}()
	teeForSHA := &teeByteReader{primary: r, secondary: hasher}
	var importReader reconcile.SnapshotByteReader
	if decryptEnabled {
		dec, err := backupcrypto.NewDecoder(&readerToReader{r: teeForSHA}, decoderParams)
		if err != nil {
			return fmt.Errorf("aead init: %w", err)
		}
		importReader = &readerByteSource{r: dec}
	} else {
		importReader = teeForSHA
	}
	expectedPlaintext := meta.SizeBytes
	if decryptEnabled || len(meta.Ranges) > 0 {
		expectedPlaintext = meta.PlaintextBytes
	}
	const streamBlockSize = 4 * 1024 * 1024
	var (
		written    int64
		importErr  error
	)
	if len(meta.Ranges) > 0 {
		// Incremental backup : write each chunk at the matching volume
		// offset rather than sequentially from 0. The engine's ranges
		// MUST be in the same order they were emitted at create time
		// (sidecar JSON preserves order).
		ranges := make([]reconcile.Range, 0, len(meta.Ranges))
		for _, r := range meta.Ranges {
			ranges = append(ranges, reconcile.Range{Offset: r.Offset, Size: r.Size})
		}
		written, importErr = h.ImportBytesAtRanges(streamBlockSize, ranges, importReader)
	} else {
		written, importErr = h.ImportBytes(streamBlockSize, expectedPlaintext, importReader)
	}
	if upErr := <-pullErr; upErr != nil {
		return fmt.Errorf("pull bin: %w", upErr)
	}
	if importErr != nil {
		return fmt.Errorf("import bytes: %w", importErr)
	}
	if written != expectedPlaintext {
		return fmt.Errorf("size mismatch (wrote %d, meta says %d)", written, expectedPlaintext)
	}
	if got := hex.EncodeToString(hasher.Sum(nil)); meta.SHA256 != "" && got != meta.SHA256 {
		return fmt.Errorf("SHA256 mismatch (got %s, meta says %s — backup corrupted or wrong key)", got, meta.SHA256)
	}
	return nil
}

// walkBackupChain collects (backupURL, meta) tuples from the leaf URL
// back to the chain's root (where ParentURL == ""). The returned slice
// is FULL FIRST (root → leaf), the order RestoreBackup needs to apply
// the deltas correctly. A cycle protection cap stops a malformed chain
// from looping forever.
func (v *volumeDriver) walkBackupChain(ctx context.Context, leafURL string) ([]chainLink, error) {
	const maxChain = 256 // 1 full + 255 incrementals → plenty for any retention policy
	tgt, err := backuptarget.New(leafURL)
	if err != nil {
		return nil, err
	}
	var chain []chainLink
	current := leafURL
	for i := 0; i < maxChain; i++ {
		if current == "" {
			break
		}
		meta, err := readBackupMetaSibling(ctx, tgt, current)
		if err != nil {
			return nil, fmt.Errorf("walk chain at %s: %w", current, err)
		}
		chain = append(chain, chainLink{url: current, meta: meta})
		if meta.ParentURL == "" {
			break
		}
		current = meta.ParentURL
	}
	if len(chain) == maxChain {
		return nil, fmt.Errorf("backup chain at %s exceeds %d links — refusing to walk further (likely a cycle)", leafURL, maxChain)
	}
	// Reverse the chain so the caller applies root → leaf.
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

type chainLink struct {
	url  string
	meta backupMeta
}

// RestoreBackup populates a fresh volume from a backup's bytes. The caller
// MUST have already called EnsureVolume(spec) so the engine + replicas are
// up and waiting for data on the volume head. The .json sidecar's SHA256
// is checked against what we actually pulled — mismatched checksums abort
// the restore (the volume is left in whatever state WriteAt got to ; the
// caller cleans up by destroying the volume).
//
// Limitations : single-pass streamed restore, no chunked / dedup. Future
// optimisation : align with backup chunk boundaries to enable delta
// restore (skip blocks that already match the parent snapshot).
func (v *volumeDriver) RestoreBackup(ctx context.Context, backupURL string, spec drivers.VolumeSpec) error {
	if backupURL == "" {
		return fmt.Errorf("weft-block: RestoreBackup: empty url")
	}
	v.mu.Lock()
	h, err := v.localEngineFor(ctx, spec.UUID)
	v.mu.Unlock()
	if err != nil {
		return fmt.Errorf("weft-block: RestoreBackup: %w (call EnsureVolume first)", err)
	}
	// Walk the parent chain leaf→root, then apply each link root→leaf.
	// The root MUST be a full backup (meta.Ranges empty) ; we fail fast
	// if the walker landed on an orphan incremental.
	chain, err := v.walkBackupChain(ctx, backupURL)
	if err != nil {
		return fmt.Errorf("weft-block: RestoreBackup: %w", err)
	}
	if len(chain) == 0 {
		return fmt.Errorf("weft-block: RestoreBackup: empty chain at %s", backupURL)
	}
	if len(chain[0].meta.Ranges) > 0 {
		return fmt.Errorf("weft-block: RestoreBackup: chain root at %s is an incremental — its parent is missing from the target", chain[0].url)
	}
	for i, link := range chain {
		if err := v.restoreOneBackup(ctx, h, link.url, link.meta); err != nil {
			return fmt.Errorf("weft-block: RestoreBackup: apply chain[%d] %s: %w", i, link.url, err)
		}
	}
	return nil
}


// teeByteReader satisfies SnapshotByteReader by reading from a primary
// source and forwarding every chunk to a secondary writer (for SHA256
// verification). Mirrors teeByteWriter on the read side.
type teeByteReader struct {
	primary   io.Reader
	secondary io.Writer
}

func (t *teeByteReader) Read(p []byte) (int, error) {
	n, err := t.primary.Read(p)
	if n > 0 {
		_, _ = t.secondary.Write(p[:n])
	}
	return n, err
}

// writerWriter wraps an io.Writer (like the pipe writer) so it satisfies
// what backuptarget.Target.Pull expects. Pull takes io.Writer ; we already
// have one — this just keeps the type narrow.
type writerWriter struct{ w io.Writer }

func (w *writerWriter) Write(p []byte) (int, error) { return w.w.Write(p) }

// builderWriter adapts strings.Builder to io.Writer (Pull's signature).
type builderWriter struct{ b *strings.Builder }

func (w *builderWriter) Write(p []byte) (int, error) {
	return w.b.Write(p)
}

// teeByteWriter satisfies SnapshotByteWriter by writing every chunk to
// both a primary (the pipe writer that feeds the upload) and a secondary
// (the sha256 hasher) sink. Used by CreateBackup to integrity-tag the
// stream without re-reading it.
type teeByteWriter struct {
	primary   io.Writer
	secondary io.Writer
}

func (t *teeByteWriter) Write(p []byte) (int, error) {
	if n, err := t.primary.Write(p); err != nil {
		return n, err
	}
	// secondary (sha256) never short-writes, but propagate errors anyway.
	_, _ = t.secondary.Write(p)
	return len(p), nil
}

// hostCandidates returns the configured placement pool (defaults to [self]).
// Must be called with v.mu held.
func (v *volumeDriver) hostCandidates() []control.HostCandidate {
	out := make([]control.HostCandidate, len(v.hosts))
	copy(out, v.hosts)
	return out
}

// effectiveReplicaCount mirrors cluster.hcl's Block.EffectiveReplicaCount
// policy: 3 if the candidate pool has 3+ hosts (3-DC cluster), the host count
// otherwise (single-host dev → 1). Will be replaced by an explicit
// VolumeSpec.ReplicaCount field once weft-drivers carries it.
func effectiveReplicaCount(hostCount int) int {
	if hostCount >= 3 {
		return 3
	}
	if hostCount > 0 {
		return hostCount
	}
	return 1
}

// firstNonEmpty returns the first non-empty string. Used for KDF default
// fallback (sidecar must record what was actually used, even when the
// operator left the field empty + the encoder filled in defaults).
func firstNonEmpty(s ...string) string {
	for _, x := range s {
		if x != "" {
			return x
		}
	}
	return ""
}

// writerByteSink adapts any io.Writer to the reconcile.SnapshotByteWriter
// surface — used to plug a backupcrypto.Encoder (io.Writer) into the
// engine's chunk-emitting path without a per-chunk type assertion.
type writerByteSink struct{ w io.Writer }

func (s *writerByteSink) Write(p []byte) (int, error) { return s.w.Write(p) }

// countingWriter counts bytes written through it. Used to measure how
// many bytes hit the BackupTarget (== ciphertext size for encrypted
// backups, == plaintext size otherwise) — separately from the plaintext
// byte count StreamSnapshotBytes reports.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// readerToReader adapts a reconcile.SnapshotByteReader (Read-only
// interface) to io.Reader — used to plug a teeByteReader into the
// backupcrypto.Decoder constructor which expects io.Reader.
type readerToReader struct{ r reconcile.SnapshotByteReader }

func (r *readerToReader) Read(p []byte) (int, error) { return r.r.Read(p) }

// readerByteSource adapts an io.Reader to the reconcile.SnapshotByteReader
// surface — symmetric with writerByteSink, used to plug the AEAD decoder
// (io.Reader) into EngineHandle.ImportBytes.
type readerByteSource struct{ r io.Reader }

func (s *readerByteSource) Read(p []byte) (int, error) { return s.r.Read(p) }
