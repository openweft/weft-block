package main

// fs_coordinator.go — guest-side filesystem-freeze coordination for
// crash-consistent snapshots.
//
// Background : weft-block creates snapshots at the BLOCK level. A
// snapshot is always crash-consistent (it's an atomic copy of the
// on-disk state at T). It is NOT necessarily file-system-consistent :
// any data sitting in the guest's page cache at T is gone, and any
// half-finished file-system transaction (journal replay required) is
// captured mid-flight. For Postgres / etcd / a Linux ext4 root, that's
// fine — they recover on next boot. For workloads that need consistent
// in-flight buffers (database backups in particular), the guest must
// FREEZE its filesystem before the snapshot, then THAW it after.
//
// FREEZE / THAW must happen in the GUEST. Linux exposes the FIFREEZE
// ioctl ; weft's in-guest agent (weft-microvm-agent) is the only
// thing inside the VM that can call it. Coordination crosses the
// guest/host boundary, so we use NATS (the same transport the agent
// already uses for dynamic config — see
// memory/project_guest_dynamic_config.md). One subject per VM ;
// publish+request a "freeze" before snapshot, "thaw" after.
//
// Opt-in : the operator marks the snapshot spec with
//
//   labels["freeze_fs"] = "true"
//
// and, because weft-block doesn't know which VM owns the volume, the
// spec must ALSO carry the VM's UUID :
//
//   labels["vm_uuid"] = "<uuid>"
//
// Both labels come from `weft volume snapshot --freeze-fs --vm <uuid>`
// (the CLI flag is the source of truth ; the labels are how they
// reach this layer through the proto-stable SnapshotSpec.Labels map).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// labelKeyFreezeFS = sentinel label name on SnapshotSpec.Labels that
// requests guest-side filesystem freeze. Value "true" / "1" enables it.
const labelKeyFreezeFS = "freeze_fs"

// labelKeyVMUUID = sentinel label name on SnapshotSpec.Labels carrying
// the guest VM's UUID. Required when freeze_fs is set ; the coordinator
// uses it to address the per-VM NATS subject.
const labelKeyVMUUID = "vm_uuid"

// FSCoordinator coordinates with the guest agent to freeze + thaw the
// guest filesystem around a snapshot. Implementations are responsible
// for guaranteeing the THAW happens even if the SNAPSHOT call fails ;
// a guest left frozen is worse than a non-consistent snapshot.
type FSCoordinator interface {
	// Freeze tells the guest agent for vmUUID to call FIFREEZE on
	// the mounted root (and any extra mountpoints the agent
	// surfaces). Returns when the agent acks the freeze, or an
	// error if the agent isn't reachable / refuses.
	Freeze(ctx context.Context, vmUUID string) error
	// Thaw releases a previously-frozen guest. Calls are idempotent
	// from the guest side (FITHAW is a no-op on a non-frozen FS).
	Thaw(ctx context.Context, vmUUID string) error
}

// NoopFSCoordinator is the default ; it skips the freeze/thaw dance
// entirely. The driver uses it when no NATS URL is configured in the
// environment. Snapshots are still crash-consistent ; they just
// aren't filesystem-quiesced. This is the right default for the
// vast majority of workloads (postgres, etcd, etc. recover on boot).
type NoopFSCoordinator struct {
	Log *slog.Logger
}

// Freeze logs at debug and returns nil.
func (n NoopFSCoordinator) Freeze(_ context.Context, vmUUID string) error {
	if n.Log != nil {
		n.Log.Debug("weft-block: freeze_fs requested but no FS coordinator wired ; skipping", "vm", vmUUID)
	}
	return nil
}

// Thaw is symmetric : log + skip.
func (n NoopFSCoordinator) Thaw(_ context.Context, vmUUID string) error { return nil }

// NATSFSCoordinator publishes freeze / thaw requests on a per-VM NATS
// subject and awaits an ack from weft-microvm-agent inside the guest.
//
// Subjects (mirror project_guest_dynamic_config.md's per-VM addressing) :
//
//	weft.vm.<uuid>.fs.freeze   — payload: {"timeout_ms": ...}, reply: {"frozen": true/false, "err": "..."}
//	weft.vm.<uuid>.fs.thaw     — payload: {}, reply: {"thawed": true, "err": "..."}
//
// FreezeTimeout bounds the request — if the guest doesn't ack, we
// FAIL the snapshot rather than silently skipping the freeze (the
// operator opted in explicitly, so silent fallback would be wrong).
type NATSFSCoordinator struct {
	NC            *nats.Conn
	FreezeTimeout time.Duration
	ThawTimeout   time.Duration
	Log           *slog.Logger
}

// fsFreezeRequest is the request payload published on the freeze subject.
type fsFreezeRequest struct {
	TimeoutMs int64 `json:"timeout_ms,omitempty"`
}

// fsFreezeReply is what the guest agent replies. Frozen=true means
// FIFREEZE returned 0 on every mountpoint the agent tracks.
type fsFreezeReply struct {
	Frozen bool   `json:"frozen"`
	Err    string `json:"err,omitempty"`
}

// fsThawReply mirrors the freeze reply for thaw.
type fsThawReply struct {
	Thawed bool   `json:"thawed"`
	Err    string `json:"err,omitempty"`
}

// Freeze publishes a freeze request on the per-VM subject.
func (n *NATSFSCoordinator) Freeze(ctx context.Context, vmUUID string) error {
	if n == nil || n.NC == nil {
		return errors.New("weft-block: NATSFSCoordinator has no NATS connection")
	}
	if vmUUID == "" {
		return errors.New("weft-block: freeze_fs requested but vm_uuid label is missing")
	}
	timeout := n.FreezeTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	body, err := json.Marshal(fsFreezeRequest{TimeoutMs: timeout.Milliseconds()})
	if err != nil {
		return fmt.Errorf("encode freeze request: %w", err)
	}
	subj := fmt.Sprintf("weft.vm.%s.fs.freeze", vmUUID)
	rctx, cancel := withTimeout(ctx, timeout)
	defer cancel()
	msg, err := n.NC.RequestWithContext(rctx, subj, body)
	if err != nil {
		return fmt.Errorf("freeze request on %s: %w", subj, err)
	}
	var reply fsFreezeReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return fmt.Errorf("decode freeze reply from %s: %w", subj, err)
	}
	if reply.Err != "" {
		return fmt.Errorf("guest refused freeze: %s", reply.Err)
	}
	if !reply.Frozen {
		return fmt.Errorf("guest did not confirm freeze on %s", subj)
	}
	if n.Log != nil {
		n.Log.Info("weft-block: guest filesystem frozen", "vm", vmUUID)
	}
	return nil
}

// Thaw publishes a thaw request on the per-VM subject. Thaw is
// best-effort : we log errors but don't surface them as a snapshot
// failure (the snapshot itself succeeded by the time we thaw, and a
// guest that doesn't thaw will recover on next FIFREEZE deadline or
// on reboot).
func (n *NATSFSCoordinator) Thaw(ctx context.Context, vmUUID string) error {
	if n == nil || n.NC == nil {
		return errors.New("weft-block: NATSFSCoordinator has no NATS connection")
	}
	if vmUUID == "" {
		return nil
	}
	timeout := n.ThawTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	subj := fmt.Sprintf("weft.vm.%s.fs.thaw", vmUUID)
	rctx, cancel := withTimeout(ctx, timeout)
	defer cancel()
	msg, err := n.NC.RequestWithContext(rctx, subj, []byte(`{}`))
	if err != nil {
		if n.Log != nil {
			n.Log.Warn("weft-block: thaw request failed", "vm", vmUUID, "err", err)
		}
		return fmt.Errorf("thaw request on %s: %w", subj, err)
	}
	var reply fsThawReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		if n.Log != nil {
			n.Log.Warn("weft-block: thaw reply undecodable", "vm", vmUUID, "err", err)
		}
		return nil
	}
	if reply.Err != "" {
		if n.Log != nil {
			n.Log.Warn("weft-block: guest reported thaw error", "vm", vmUUID, "err", reply.Err)
		}
	}
	return nil
}

// shouldFreezeFS returns (true, vmUUID) when the snapshot spec labels
// request guest-side freeze and supply the VM identifier.
// Mirrors how the agent reads labels elsewhere — case-sensitive keys,
// "true" / "1" / "yes" all enable the flag.
func shouldFreezeFS(labels map[string]string) (bool, string) {
	if len(labels) == 0 {
		return false, ""
	}
	v, ok := labels[labelKeyFreezeFS]
	if !ok {
		return false, ""
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes", "on":
		return true, strings.TrimSpace(labels[labelKeyVMUUID])
	}
	return false, ""
}

// withTimeout layers a fresh deadline onto ctx if the existing one is
// looser (or absent). Snapshots inherit a long-running ctx from the
// driver dispatch ; we don't want freeze to block forever even if
// the parent waits patiently. The caller MUST defer the returned
// cancel — when the parent deadline is already tighter we return a
// no-op cancel so the call site stays uniform.
func withTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if dl, ok := parent.Deadline(); ok {
		remaining := time.Until(dl)
		if remaining < d {
			return parent, func() {}
		}
	}
	return context.WithTimeout(parent, d)
}
