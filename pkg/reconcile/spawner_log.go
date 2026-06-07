package reconcile

// LogSpawner is the bootstrap Spawner: it does NOT actually start any process,
// only logs the intent and synthesises addresses / frontend paths so the rest
// of the control plane (Loop → store updates → AttachVolume polling) drives
// end-to-end. Real replica + engine startup land in the next slice as an
// InProcessSpawner that calls replica.NewServer + replicarpc.New*Server +
// controller.NewController + nbd.Startup.
//
// The address it returns is "<host>:<port>" with a deterministic port
// derived from the replica UUID — that lets the reconcile loop ALSO be
// exercised against the real Spawner with no API change.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/sirupsen/logrus"
)

// LogSpawner is a no-op Spawner that logs every call. Used by default in
// cmd/weft-block until the real in-process spawner lands.
type LogSpawner struct {
	HostAddr string // overlay-reachable address of this host, e.g. "10.9.0.1"
	Log      logrus.FieldLogger
}

// NewLogSpawner returns a LogSpawner with the standard logrus logger.
func NewLogSpawner(hostAddr string) *LogSpawner {
	return &LogSpawner{HostAddr: hostAddr, Log: logrus.WithField("spawner", "log")}
}

func (s *LogSpawner) SpawnReplica(_ context.Context, req ReplicaRequest) (string, error) {
	port := portFromUUID(req.ReplicaUUID, 30000, 5000)
	addr := fmt.Sprintf("%s:%d", s.HostAddr, port)
	s.Log.WithFields(logrus.Fields{
		"replica": req.ReplicaUUID, "volume": req.VolumeUUID,
		"size_bytes": req.SizeBytes, "addr": addr,
	}).Info("LogSpawner.SpawnReplica (no process started; real spawn pending next slice)")
	return addr, nil
}

func (s *LogSpawner) StopReplica(_ context.Context, replicaUUID string) error {
	s.Log.WithField("replica", replicaUUID).Info("LogSpawner.StopReplica (no-op)")
	return nil
}

func (s *LogSpawner) SpawnEngine(_ context.Context, req EngineRequest) (string, error) {
	// The frontend path the real engine would expose: /dev/nbdN. LogSpawner
	// uses N = sha-derived for determinism without actually touching /dev.
	n := portFromUUID(req.EngineUUID, 0, 16)
	path := fmt.Sprintf("/dev/nbd%d", n)
	s.Log.WithFields(logrus.Fields{
		"engine": req.EngineUUID, "volume": req.VolumeUUID,
		"replicas": req.ReplicaAddresses, "frontend": path,
	}).Info("LogSpawner.SpawnEngine (no process started; real spawn pending next slice)")
	return path, nil
}

func (s *LogSpawner) StopEngine(_ context.Context, engineUUID string) error {
	s.Log.WithField("engine", engineUUID).Info("LogSpawner.StopEngine (no-op)")
	return nil
}

// LocalEngine always reports "not local" since LogSpawner doesn't actually
// run any engine. Callers route snapshot / backup operations elsewhere.
func (s *LogSpawner) LocalEngine(engineUUID string) (EngineHandle, bool) {
	s.Log.WithField("engine", engineUUID).Debug("LogSpawner.LocalEngine (no engine running)")
	return nil, false
}

// portFromUUID maps a UUID to a stable integer in [base, base+span) so two
// calls with the same UUID get the same port / device number, without any
// shared state. Trivial hash; collisions just mean two volumes try the same
// port — the real spawner will scan for a free one.
func portFromUUID(uuid string, base, span int) int {
	if span <= 0 {
		return base
	}
	h := sha256.Sum256([]byte(uuid))
	n := int(binary.BigEndian.Uint32(h[:4])) & 0x7fffffff
	return base + (n % span)
}
