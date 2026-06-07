// Command weft-block is the weft volume driver plugin for cluster-wide block
// storage — the Longhorn data plane (engine + replicas + NBD frontend) wrapped
// in a drivers.VolumeDriver and served over the standard weft go-plugin
// transport. Hypervisor / Network / Image services are stubbed (this plugin
// only carries Volume); a host's hypervisor plugin (weft-driver-vz/-qemu)
// covers the other three.
//
// Process model: one weft-block per host. The same process serves the gRPC
// VolumeDriver and runs the per-host reconcile loop, which polls the shared
// control store and brings up the local replica + engine processes via a
// Spawner. The default is InProcessSpawner on Linux (real replicas + engines
// + NBD), LogSpawner elsewhere (no real processes — the data plane is Linux-
// only by design, so non-Linux dev hosts get the structural state machine
// without trying to bind /dev/nbdN).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"

	weftplugin "github.com/openweft/weft-driver-plugin"

	"github.com/openweft/weft-block/control"
	controletcd "github.com/openweft/weft-block/control/etcd"
	"github.com/openweft/weft-block/pkg/reconcile"
)

// Env knobs for cmd/weft-block:
//
//   - WEFT_BLOCK_LOG_SPAWNER=1 forces the LogSpawner even on Linux (smoke
//     tests of the plugin handshake without claiming /dev/nbdN).
//   - WEFT_BLOCK_ETCD_ENDPOINTS=ep1,ep2,... selects the etcd-backed store
//     (the multi-host production path). Empty → MemStore (single-host dev).
//   - WEFT_BLOCK_DAEMON=1 runs as a free-standing daemon instead of a
//     go-plugin (skips weftplugin.Serve). The reconcile loop is started the
//     same way; the process blocks until SIGINT/SIGTERM. Use this when weft
//     isn't (yet) the orchestrator — e.g. the 3-DC live validation runs the
//     daemons under nohup and drives state via etcdctl.
const (
	EnvForceLogSpawner = "WEFT_BLOCK_LOG_SPAWNER"
	EnvEtcdEndpoints   = "WEFT_BLOCK_ETCD_ENDPOINTS"
	EnvDaemon          = "WEFT_BLOCK_DAEMON"

	// EnvNATSURL is the operator-visible knob that opts the
	// weft-block plugin into guest-side filesystem freeze on
	// snapshots. When set, the plugin dials the cluster NATS and
	// installs a NATSFSCoordinator ; the driver then honours
	// `freeze_fs=true` + `vm_uuid=<uuid>` labels on snapshot specs.
	// Empty → NoopFSCoordinator (snapshots stay crash-consistent,
	// no quiescing).
	EnvNATSURL = "WEFT_BLOCK_NATS_URL"
)

func main() {
	hi := hostInfo{
		uuid:     os.Getenv(weftplugin.EnvHostUUID),
		hostname: os.Getenv(weftplugin.EnvHostname),
		az:       os.Getenv(weftplugin.EnvAZ),
	}
	stateDir := os.Getenv(weftplugin.EnvStateDir)
	if stateDir == "" {
		stateDir = "/var/lib/weft-block"
	}

	// Single shared store: the volumeDriver writes desired state (volumes,
	// replicas, engines) and the reconcile loop turns it into running
	// processes. WEFT_BLOCK_ETCD_ENDPOINTS selects the etcd-backed store
	// (production / multi-host); empty → in-memory (single-host dev).
	store, closeStore, err := buildStore(os.Getenv(EnvEtcdEndpoints))
	if err != nil {
		fmt.Fprintf(os.Stderr, "weft-block: %v\n", err)
		os.Exit(1)
	}
	defer closeStore()

	var spawner reconcile.Spawner
	if runtime.GOOS == "linux" && os.Getenv(EnvForceLogSpawner) == "" {
		// HostAddr is the address engines on OTHER hosts dial to reach our
		// replicas. Hostname env (the WEFT_DRIVER_HOSTNAME the host plugin
		// receives) is good enough as long as DNS resolves it on the overlay;
		// the cluster-bring-up slice will replace this with the cluster's
		// overlay IP.
		spawner = reconcile.NewInProcessSpawner(hi.hostname, stateDir)
	} else {
		spawner = reconcile.NewLogSpawner(hi.hostname)
	}

	loop := reconcile.New(hi.uuid, store, spawner)
	loop.Start(context.Background())
	defer loop.Stop()

	vol := newVolumeDriver(hi, stateDir, store, spawner)

	// Optional : install the NATS-backed freeze coordinator so
	// snapshot specs carrying `freeze_fs=true` can quiesce the
	// guest filesystem. Default (env unset) is the no-op.
	if natsURL := strings.TrimSpace(os.Getenv(EnvNATSURL)); natsURL != "" {
		nc, ncErr := nats.Connect(natsURL,
			nats.Name(fmt.Sprintf("weft-block/%s", hi.uuid)),
			nats.Timeout(5*time.Second),
			nats.MaxReconnects(-1), // reconnect forever ; snapshots wait
			nats.ReconnectWait(2*time.Second),
		)
		if ncErr != nil {
			// Don't kill the plugin — snapshots without freeze are
			// the safe default. Log and keep going.
			fmt.Fprintf(os.Stderr, "weft-block: NATS dial %s failed (%v) ; freeze_fs snapshots will error until wired\n", natsURL, ncErr)
		} else {
			coord := &NATSFSCoordinator{
				NC:            nc,
				FreezeTimeout: 10 * time.Second,
				ThawTimeout:   10 * time.Second,
				Log:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			}
			vol.SetFSCoordinator(coord)
			defer nc.Close()
			fmt.Fprintf(os.Stderr, "weft-block: NATS connected (%s) ; guest-fs freeze coordinator active\n", natsURL)
		}
	}

	if os.Getenv(EnvDaemon) != "" {
		// Free-standing daemon: reconcile loop already runs in goroutine;
		// block on SIGINT/SIGTERM. weft isn't (yet) the orchestrator here —
		// state is driven via etcdctl by the cluster bring-up.
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		fmt.Fprintf(os.Stderr, "weft-block: daemon mode (host=%s reconcile→etcd=%s), waiting for SIGTERM\n",
			hi.uuid, os.Getenv(EnvEtcdEndpoints))
		<-sig
		return
	}

	weftplugin.Serve(weftplugin.DriverSet{
		Hypervisor: stubHypervisor{hi: hi},
		Network:    stubNetwork{hi: hi},
		Volume:     vol,
		Image:      stubImage{hi: hi},
	})
}

// buildStore picks the VolumeStore based on env. Returns a closer the caller
// defers (no-op for MemStore; client.Close for etcd).
func buildStore(endpoints string) (control.VolumeStore, func(), error) {
	if strings.TrimSpace(endpoints) == "" {
		return control.NewMemStore(), func() {}, nil
	}
	parts := strings.Split(endpoints, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   parts,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("etcd dial %v: %w", parts, err)
	}
	return controletcd.New(cli), func() { _ = cli.Close() }, nil
}
