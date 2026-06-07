package main

// multihost_test.go: end-to-end proof that cross-host AttachVolume works
// through the shared EtcdStore. Two volumeDriver instances (host-A, host-B)
// share one embedded etcd and each runs its own reconcile loop with
// LogSpawner. EnsureVolume on driver A places one replica per host;
// AttachVolume(volume, hostB) on driver A creates an Engine pinned to host B,
// driverA polls etcd, driverB's loop picks up the engine and writes State=
// Running + FrontendPath, driverA returns the path. Equivalent to two
// weft-block plugin processes sharing the cluster's etcd, minus the gRPC
// front door (which is just plumbing on top of the same driver instance).

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	drivers "github.com/openweft/weft-drivers"

	"github.com/openweft/weft-block/control"
	controletcd "github.com/openweft/weft-block/control/etcd"
	"github.com/openweft/weft-block/pkg/reconcile"
)

func startEmbeddedEtcdForTest(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(dir, "etcd-data")
	listenURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", freePortForTest(t)))
	peerURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", freePortForTest(t)))
	cfg.ListenClientUrls = []url.URL{*listenURL}
	cfg.AdvertiseClientUrls = []url.URL{*listenURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.LogLevel = "error"
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start embedded etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		e.Server.Stop()
		t.Fatal("embedded etcd never became leader within 20s")
	}
	t.Cleanup(func() { e.Close() })
	return listenURL.String()
}

func freePortForTest(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestCrossHostAttachVolume_ThroughEtcdStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: spins up an embedded etcd")
	}
	clientURL := startEmbeddedEtcdForTest(t)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	store := controletcd.New(cli)

	hostA := hostInfo{uuid: "host-A", hostname: "10.9.0.1", az: "dc1"}
	hostB := hostInfo{uuid: "host-B", hostname: "10.9.0.2", az: "dc2"}
	candidates := []control.HostCandidate{
		{UUID: hostA.uuid, DC: hostA.az},
		{UUID: hostB.uuid, DC: hostB.az},
	}

	drvA := newVolumeDriver(hostA, t.TempDir(), store, reconcile.NewLogSpawner(hostA.hostname))
	drvA.SetHostCandidates(candidates)
	drvB := newVolumeDriver(hostB, t.TempDir(), store, reconcile.NewLogSpawner(hostB.hostname))
	drvB.SetHostCandidates(candidates)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	loopA := reconcile.New(hostA.uuid, store, reconcile.NewLogSpawner(hostA.hostname))
	loopA.Interval = 50 * time.Millisecond
	loopA.Start(ctx)
	defer loopA.Stop()

	loopB := reconcile.New(hostB.uuid, store, reconcile.NewLogSpawner(hostB.hostname))
	loopB.Interval = 50 * time.Millisecond
	loopB.Start(ctx)
	defer loopB.Stop()

	// 1. driver A persists the volume + 2 Pending replicas (one per host).
	spec := drivers.VolumeSpec{
		UUID:        "vol-cross",
		ProjectUUID: "p",
		Name:        "shared",
		SizeGiB:     1,
		Format:      "raw",
	}
	if err := drvA.EnsureVolume(ctx, spec); err != nil {
		t.Fatalf("EnsureVolume: %v", err)
	}

	// 2. Both loops should drive their respective replicas to Running.
	waitUntil(t, 5*time.Second, func() bool {
		rs, _ := store.ListReplicasFor(ctx, spec.UUID)
		if len(rs) != 2 {
			return false
		}
		runningOnA := false
		runningOnB := false
		for _, r := range rs {
			if r.HostUUID == hostA.uuid && r.State == control.ReplicaStateRunning && r.Address != "" {
				runningOnA = true
			}
			if r.HostUUID == hostB.uuid && r.State == control.ReplicaStateRunning && r.Address != "" {
				runningOnB = true
			}
		}
		return runningOnA && runningOnB
	}, "expected one Running replica per host within 5s")

	// 3. driver A asks to attach the volume on HOST B. The Engine record gets
	//    written, driver A starts polling etcd, host B's loop spawns the
	//    engine and writes Running+FrontendPath back.
	got, err := drvA.AttachVolume(ctx, spec.UUID, hostB.uuid)
	if err != nil {
		t.Fatalf("AttachVolume(host-B): %v", err)
	}
	if got.BackingPath == "" {
		t.Fatalf("AttachVolume returned empty BackingPath: %+v", got)
	}
	if !strings.HasPrefix(got.BackingPath, "/dev/nbd") {
		t.Errorf("BackingPath = %q, want /dev/nbdN", got.BackingPath)
	}

	// 4. The Engine record in etcd should be pinned to host B with the same
	//    FrontendPath the caller received. Anyone else inspecting the store
	//    (etcdctl, another plugin) sees the same truth.
	es, _ := store.ListEnginesFor(ctx, spec.UUID)
	if len(es) != 1 {
		t.Fatalf("engines for vol-cross = %d, want 1", len(es))
	}
	if es[0].HostUUID != hostB.uuid || es[0].FrontendPath != got.BackingPath {
		t.Errorf("engine record = %+v; want HostUUID=%s FrontendPath=%s", es[0], hostB.uuid, got.BackingPath)
	}

	// 5. DetachVolume on driver A marks the Engine Deleting; host B's loop
	//    must remove it from the store.
	if err := drvA.DetachVolume(ctx, spec.UUID, hostB.uuid); err != nil {
		t.Fatalf("DetachVolume: %v", err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		es, _ := store.ListEnginesFor(ctx, spec.UUID)
		return len(es) == 0
	}, "engine record was not deleted after Detach")
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", msg)
}
