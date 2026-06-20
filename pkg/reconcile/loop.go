package reconcile

// loop.go is the per-host polling reconciler. It periodically reads the
// store, finds Replica + Engine records that belong to this host
// (HostUUID == loop.hostUUID), and dispatches the appropriate spawn / stop
// calls to the Spawner. State is written back to the store so the rest of the
// cluster (and the driver's AttachVolume RPC, which polls for Running) can
// observe progress. Polling is good enough for the in-memory + embedded-etcd
// store; the etcd binding can switch to a Watch later without changing the
// reconciliation logic.

import (
	"context"
	"sync"
	"time"

	"github.com/openweft/weft-block/control"
	"github.com/sirupsen/logrus"
)

// DefaultPollInterval is how often the loop re-reads the store. Short enough
// that AttachVolume's poll-for-Running returns promptly, long enough that the
// store isn't hammered. Override via Loop.Interval.
const DefaultPollInterval = 1 * time.Second

// Loop runs the reconciliation tick for one host.
type Loop struct {
	HostUUID string
	Store    control.VolumeStore
	Spawner  Spawner
	Interval time.Duration

	mu     sync.Mutex
	known  map[string]string // replicaUUID/engineUUID -> last-seen state
	cancel context.CancelFunc
	done   chan struct{}
}

// New builds a Loop with the default poll interval.
func New(hostUUID string, store control.VolumeStore, spawner Spawner) *Loop {
	return &Loop{
		HostUUID: hostUUID,
		Store:    store,
		Spawner:  spawner,
		Interval: DefaultPollInterval,
		known:    map[string]string{},
	}
}

// Start launches the loop in a goroutine. Stop() blocks until it has drained.
func (l *Loop) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	l.mu.Lock()
	l.cancel = cancel
	l.done = make(chan struct{})
	l.mu.Unlock()

	go func() {
		defer close(l.done)
		t := time.NewTicker(l.Interval)
		defer t.Stop()
		// Run one pass immediately so AttachVolume sees Running on the first
		// poll after Spawner-initiated startup, rather than waiting a tick.
		l.tick(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				l.tick(ctx)
			}
		}
	}()
}

// Stop cancels the loop and waits for the goroutine to exit.
func (l *Loop) Stop() {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.cancel = nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// tick is one reconciliation pass: scan all volumes, then for each volume
// process its replicas (HostUUID==l.HostUUID) and its engine (HostUUID==
// l.HostUUID), spawning / stopping as needed.
func (l *Loop) tick(ctx context.Context) {
	vols, err := l.Store.ListVolumes(ctx)
	if err != nil {
		logrus.WithError(err).Warn("reconcile: list volumes")
		return
	}
	for _, v := range vols {
		l.reconcileVolume(ctx, v)
	}
}

func (l *Loop) reconcileVolume(ctx context.Context, v control.Volume) {
	log := logrus.WithField("volume", v.UUID)
	rs, _ := l.Store.ListReplicasFor(ctx, v.UUID)
	for _, r := range rs {
		if r.HostUUID != l.HostUUID {
			continue
		}
		l.reconcileReplica(ctx, log, v, r)
	}
	es, _ := l.Store.ListEnginesFor(ctx, v.UUID)
	for _, e := range es {
		if e.HostUUID != l.HostUUID {
			continue
		}
		l.reconcileEngine(ctx, log, v, e, rs)
	}
}

func (l *Loop) reconcileReplica(ctx context.Context, log logrus.FieldLogger, v control.Volume, r control.Replica) {
	switch r.State {
	case control.ReplicaStatePending:
		req := ReplicaRequest{
			ReplicaUUID: r.UUID,
			VolumeUUID:  v.UUID,
			VolumeName:  v.Name,
			SizeBytes:   v.SizeBytes,
			SectorSize:  defaultSectorSize,
		}
		// CoW-clone bootstrap : when the replica record carries provenance
		// (SourceVolumeUUID + SourceSnapshot), look up the parent volume's
		// healthy replicas and hand their addresses to the spawner. The
		// spawner dials them via the existing weftsnap.SnapshotReader gRPC
		// to populate the new replica's chain from the named snapshot
		// before it returns. Empty SourceSnapshot = standard empty-replica
		// path ; SourceReplicaAddresses falls back to empty and the
		// spawner can short-circuit.
		if r.SourceSnapshot != "" && r.SourceVolumeUUID != "" {
			req.SourceVolumeUUID = r.SourceVolumeUUID
			req.SourceSnapshot = r.SourceSnapshot
			if parent, perr := l.Store.GetVolume(ctx, r.SourceVolumeUUID); perr == nil {
				req.SourceVolumeName = parent.Name
			} else {
				log.WithField("source_volume", r.SourceVolumeUUID).WithError(perr).
					Warn("reconcile: clone bootstrap : parent volume not found ; spawner will short-circuit")
			}
			if parentReplicas, perr := l.Store.ListReplicasFor(ctx, r.SourceVolumeUUID); perr == nil {
				for _, pr := range parentReplicas {
					if pr.State != control.ReplicaStateRunning || pr.Address == "" {
						continue
					}
					req.SourceReplicaAddresses = append(req.SourceReplicaAddresses, pr.Address)
				}
			}
			if len(req.SourceReplicaAddresses) == 0 {
				// Parent replicas are not yet Running. Defer spawn until
				// they reach Running — try again on the next tick rather
				// than spawning an empty replica that would silently
				// shadow the bootstrap intent.
				log.WithFields(logrus.Fields{
					"replica":       r.UUID,
					"source_volume": r.SourceVolumeUUID,
					"snapshot":      r.SourceSnapshot,
				}).Debug("reconcile: clone bootstrap : parent has no Running replicas yet ; deferring")
				return
			}
		}
		addr, err := l.Spawner.SpawnReplica(ctx, req)
		if err != nil {
			log.WithField("replica", r.UUID).WithError(err).Warn("reconcile: spawn replica failed; marking Faulted")
			r.State = control.ReplicaStateFaulted
			_ = l.Store.PutReplica(ctx, r)
			return
		}
		r.Address = addr
		r.State = control.ReplicaStateRunning
		if err := l.Store.PutReplica(ctx, r); err != nil {
			log.WithError(err).Warn("reconcile: persist running replica")
		}
	case control.ReplicaStateDeleting:
		if err := l.Spawner.StopReplica(ctx, r.UUID); err != nil {
			log.WithField("replica", r.UUID).WithError(err).Warn("reconcile: stop replica")
		}
		_ = l.Store.DeleteReplica(ctx, r.UUID)
	}
}

func (l *Loop) reconcileEngine(ctx context.Context, log logrus.FieldLogger, v control.Volume, e control.Engine, replicas []control.Replica) {
	switch e.State {
	case control.ReplicaStatePending:
		// Wait for ALL replicas of this volume to be Running before bringing
		// the engine up — Longhorn requires every replica reachable at
		// Controller.Start to register the data path.
		addrs := make([]string, 0, len(replicas))
		for _, r := range replicas {
			if r.State != control.ReplicaStateRunning || r.Address == "" {
				log.Debug("reconcile: engine waits for replicas Running")
				return
			}
			addrs = append(addrs, r.Address)
		}
		path, err := l.Spawner.SpawnEngine(ctx, EngineRequest{
			EngineUUID:       e.UUID,
			VolumeUUID:       v.UUID,
			VolumeName:       v.Name,
			SizeBytes:        v.SizeBytes,
			SectorSize:       defaultSectorSize,
			ReplicaAddresses: addrs,
		})
		if err != nil {
			log.WithField("engine", e.UUID).WithError(err).Warn("reconcile: spawn engine failed; marking Faulted")
			e.State = control.ReplicaStateFaulted
			_ = l.Store.PutEngine(ctx, e)
			return
		}
		e.FrontendPath = path
		e.State = control.ReplicaStateRunning
		if err := l.Store.PutEngine(ctx, e); err != nil {
			log.WithError(err).Warn("reconcile: persist running engine")
		}
	case control.ReplicaStateDeleting:
		if err := l.Spawner.StopEngine(ctx, e.UUID); err != nil {
			log.WithField("engine", e.UUID).WithError(err).Warn("reconcile: stop engine")
		}
		_ = l.Store.DeleteEngine(ctx, e.UUID)
	}
}

// defaultSectorSize matches diskutil.ReplicaSectorSize (4 KiB) without
// dragging the pkg/util/disk dep into this package. Spawners are free to
// override per-replica if the backing volume reports something else.
const defaultSectorSize int64 = 4096
