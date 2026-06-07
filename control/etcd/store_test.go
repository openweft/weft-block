package etcd

// store_test.go validates the etcd-backed VolumeStore against a real
// embedded etcd. The fixture mirrors weft's storage_etcd_embedded_test.go
// pattern: start embed.Etcd on a free loopback port, wait for ReadyNotify,
// run the contract, tear down on t.Cleanup. ~1-2s of startup so the test
// honours `-short` and skips when called there.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/openweft/weft-block/control"
)

// startEmbeddedEtcd brings up a single-node etcd on free loopback ports and
// returns the client URL the test should dial. Tears down on t.Cleanup.
func startEmbeddedEtcd(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(dir, "etcd-data")
	listenURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", freePort(t)))
	if err != nil {
		t.Fatalf("parse listen url: %v", err)
	}
	cfg.ListenClientUrls = []url.URL{*listenURL}
	cfg.AdvertiseClientUrls = []url.URL{*listenURL}
	peerURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", freePort(t)))
	if err != nil {
		t.Fatalf("parse peer url: %v", err)
	}
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

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// newTestStore spins up an embedded etcd, dials it with a fresh client, and
// returns a Store ready to use. Closes both on cleanup.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping: brings up an embedded etcd (~1-2s startup)")
	}
	clientURL := startEmbeddedEtcd(t)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return New(cli)
}

func TestEtcdStore_Volume_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	v := control.Volume{UUID: "vol-1", Name: "data", SizeBytes: 4 << 30, ReplicaCount: 3, State: control.VolumeStateProvisioning}
	if err := s.PutVolume(ctx, v); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.GetVolume(ctx, "vol-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "data" || got.SizeBytes != 4<<30 || got.State != control.VolumeStateProvisioning {
		t.Errorf("round trip lost fields: %+v", got)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Errorf("CreatedAt/UpdatedAt not set: %+v", got)
	}

	// Update preserves CreatedAt + bumps UpdatedAt.
	createdAt := got.CreatedAt
	time.Sleep(2 * time.Millisecond)
	got.State = control.VolumeStateHealthy
	if err := s.PutVolume(ctx, got); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetVolume(ctx, "vol-1")
	if !got.CreatedAt.Equal(createdAt) {
		t.Errorf("CreatedAt mutated on update: %v → %v", createdAt, got.CreatedAt)
	}
	if !got.UpdatedAt.After(createdAt) {
		t.Errorf("UpdatedAt not bumped on update")
	}
}

func TestEtcdStore_NotFoundSentinel(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.GetVolume(ctx, "missing"); !errors.Is(err, control.ErrNotFound) {
		t.Errorf("Get missing: want ErrNotFound, got %v", err)
	}
	if _, err := s.GetReplica(ctx, "missing"); !errors.Is(err, control.ErrNotFound) {
		t.Errorf("Get missing replica: want ErrNotFound, got %v", err)
	}
	if _, err := s.GetEngine(ctx, "missing"); !errors.Is(err, control.ErrNotFound) {
		t.Errorf("Get missing engine: want ErrNotFound, got %v", err)
	}
}

func TestEtcdStore_DeleteIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Deleting a missing key must not error (matches MemStore contract).
	if err := s.DeleteVolume(ctx, "never-existed"); err != nil {
		t.Errorf("DeleteVolume missing: %v", err)
	}
	if err := s.DeleteReplica(ctx, "never-existed"); err != nil {
		t.Errorf("DeleteReplica missing: %v", err)
	}
	if err := s.DeleteEngine(ctx, "never-existed"); err != nil {
		t.Errorf("DeleteEngine missing: %v", err)
	}
}

func TestEtcdStore_ListReplicasFor_FiltersByVolume(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	mustPutReplica := func(uuid, volUUID, host string) {
		if err := s.PutReplica(ctx, control.Replica{
			UUID: uuid, VolumeUUID: volUUID, HostUUID: host,
			State: control.ReplicaStatePending,
		}); err != nil {
			t.Fatal(err)
		}
	}
	mustPutReplica("r-1-a", "vol-1", "host-A")
	mustPutReplica("r-1-b", "vol-1", "host-B")
	mustPutReplica("r-2-a", "vol-2", "host-A") // different volume

	got, err := s.ListReplicasFor(ctx, "vol-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("vol-1 replicas = %d, want 2: %+v", len(got), got)
	}
	for _, r := range got {
		if r.VolumeUUID != "vol-1" {
			t.Errorf("filter leaked: %+v", r)
		}
	}
}

func TestEtcdStore_ListVolumes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, name := range []string{"a", "b", "c"} {
		if err := s.PutVolume(ctx, control.Volume{UUID: "vol-" + name, Name: name, SizeBytes: 1 << 20}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ListVolumes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("ListVolumes = %d, want 3: %+v", len(got), got)
	}
}

func TestEtcdStore_Engine_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	e := control.Engine{
		UUID: "e-1", VolumeUUID: "v", HostUUID: "host-A",
		FrontendPath: "/dev/nbd0", State: control.ReplicaStateRunning,
	}
	if err := s.PutEngine(ctx, e); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetEngine(ctx, "e-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.FrontendPath != "/dev/nbd0" || got.State != control.ReplicaStateRunning {
		t.Errorf("round trip lost fields: %+v", got)
	}
	// List filters by volume.
	es, err := s.ListEnginesFor(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 1 || es[0].UUID != "e-1" {
		t.Errorf("ListEnginesFor: %+v", es)
	}
}
