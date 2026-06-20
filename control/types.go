package control

import "time"

// VolumeState is the current lifecycle phase of a Volume. The state machine
// mirrors Longhorn's: a volume is Provisioning while its replicas are being
// laid down, Healthy once N replicas are reachable, Degraded when at least one
// is down but a quorum remains, Attached while an engine is bound to a host,
// and Detaching/Deleting on teardown.
type VolumeState string

const (
	VolumeStateProvisioning VolumeState = "provisioning"
	VolumeStateHealthy      VolumeState = "healthy"
	VolumeStateDegraded     VolumeState = "degraded"
	VolumeStateAttached     VolumeState = "attached"
	VolumeStateDetaching    VolumeState = "detaching"
	VolumeStateDeleting     VolumeState = "deleting"
)

// ReplicaState is the lifecycle of one replica record. The reconciling agent
// drives Pending → Running on a successful local spawn, Faulted on a probe
// failure, Deleting when the volume is being torn down.
type ReplicaState string

const (
	ReplicaStatePending  ReplicaState = "pending"
	ReplicaStateRunning  ReplicaState = "running"
	ReplicaStateFaulted  ReplicaState = "faulted"
	ReplicaStateDeleting ReplicaState = "deleting"
)

// Volume is the cluster-wide record of one block volume — what would be a
// Longhorn `Volume` CRD. Persisted in etcd; reconciled by the agents.
type Volume struct {
	UUID        string      // stable identity (matches drivers.VolumeSpec.UUID)
	ProjectUUID string      // tenant
	Name        string
	SizeBytes   int64       // exact volume size; SizeGiB on the wire converts here
	ReplicaCount int        // desired replica count (1 single-node, 3 default 3-DC)
	State       VolumeState
	AttachedTo  string      // host UUID currently running the engine, "" when detached
	// SourceVolumeUUID + SourceSnapshot mark this Volume as a CoW clone of
	// another Volume's snapshot. Set by CreateFromSnapshot ; empty for
	// freshly-provisioned volumes. The reconcile loop on each placed-on host
	// uses (SourceVolumeUUID, SourceSnapshot) on the Replica to bootstrap the
	// new replica's chain from the parent snapshot (chain pointer, not a byte
	// copy — the Longhorn replica chain naturally implements CoW).
	SourceVolumeUUID string
	SourceSnapshot   string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Replica is one replica placement on one host — what would be a Longhorn
// `Replica` CRD. The agent on HostUUID owns spawning/probing it. Address is
// the overlay-reachable endpoint the engine dials (host:port).
type Replica struct {
	UUID       string       // stable per-replica id
	VolumeUUID string       // backref
	HostUUID   string       // pinned to this host
	DC         string       // for AZ-aware placement decisions + later moves
	Address    string       // overlay endpoint (host:port); set by the agent after spawn
	State      ReplicaState
	// SourceVolumeUUID + SourceSnapshot, when non-empty, instruct the spawner
	// to bootstrap this replica's chain from the named snapshot on the parent
	// volume rather than from an empty head. Mirrors the parent Volume's
	// provenance fields ; carried per-replica so the per-host reconcile loop
	// can see the bootstrap intent without re-reading the Volume record.
	SourceVolumeUUID string
	SourceSnapshot   string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Engine is the per-volume controller placement — what would be a Longhorn
// `Engine` CRD. It exists only while the volume is attached. The agent on
// HostUUID spawns the local weft-block-engine process for VolumeUUID and
// configures its frontend (NBD); the frontend path lands in FrontendPath once
// the engine is up.
type Engine struct {
	UUID         string
	VolumeUUID   string
	HostUUID     string
	FrontendPath string // e.g. "/dev/nbd0" — what AttachVolume returns to the hypervisor
	State        ReplicaState // reused: Pending/Running/Faulted/Deleting
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
