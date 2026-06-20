<p align="center"><img src="https://raw.githubusercontent.com/openweft/brand/main/social/openweft.png" alt="openweft" width="720"></p>

# weft-block

Distributed block storage for weft — an **adaptation of [Longhorn](https://github.com/longhorn/longhorn-engine)**
into weft's idioms.

## Approach: adapt, not reimplement

The data plane (controller / engine / replica / dataconn / sparse-tools / qcow)
is `longhorn-engine` ported under our module path. Years of storage
engineering — synchronous replication, snapshot diff trees, rebuild after
replica loss, error handling during writes — carry over for free.

The control plane is **weft-native** (etcd + go-plugin) and replaces
`longhorn-manager` (a Kubernetes CRD controller weft has no use for).

## Longhorn → weft mapping

| Longhorn (k8s)                     | weft-block                                                |
|------------------------------------|-----------------------------------------------------------|
| `longhorn-manager` (k8s ctrlr)     | [`control/`](control/) (this repo) — types, placement, etcd-backed store |
| Volume / Replica / Engine CRDs     | `control.{Volume,Replica,Engine}` records                 |
| `longhorn-engine`'s `pkg/controller` | kept verbatim — synchronous replica fan-out, error handling |
| `longhorn-engine`'s `pkg/replica`  | kept verbatim — sparse files + snapshot chain             |
| `longhorn-engine`'s `pkg/dataconn` | kept verbatim — fast binary block protocol                |
| `instance-manager` (DaemonSet)     | the weft agent — watches etcd, spawns local processes     |
| iSCSI tgt frontend                 | **dropped** in favour of NBD (kernel client, no userspace target daemon) |
| Backups to S3/NFS                  | kept (`pkg/backup`) — same shape, later slices            |

## Status

Phase 0 — repo bootstrap (current):

* [x] longhorn-engine source rehomed under `github.com/openweft/weft-block`
* [x] `go.mod` rewritten (Go 1.25, our toolchain; weft-driver-plugin + weft-drivers replaces wired)
* [x] `control/` weft-native control-plane scaffold (types, deterministic placement, in-memory store + tests)
* [ ] Build `pkg/replica` + `pkg/controller` after thinning incompatible deps
      (k8s.io/{apimachinery,client-go,mount-utils} pulled by `go-common-libs`; the
      `frontend/tgt` iSCSI path; `backupstore`'s AWS/Azure transitives — defer)
* [ ] `cmd/weft-block/` go-plugin entrypoint wrapping engine+replica as `drivers.VolumeDriver`
* [ ] NBD frontend in lieu of iSCSI (in-kernel NBD client = simpler integration with weft VMs)
* [ ] etcd-backed `VolumeStore` impl
* [ ] Per-host reconcile loop in weft-agent

## Module layout

```
weft-block/
  go.mod                      module: github.com/openweft/weft-block
  control/                    weft-native control plane (the longhorn-manager replacement)
    types.go                  Volume / Replica / Engine records
    placement.go              deterministic N-replica spread across DCs
    store.go                  VolumeStore interface + MemStore
    *_test.go                 unit tests for placement + store
  pkg/                        the imported longhorn-engine data plane (ported in place)
    controller/               engine: dial replicas, fan-out writes, quorum reads
    replica/                  replica: sparse file + RPC server
    dataconn/                 custom binary data protocol (fast block I/O)
    frontend/                 frontend abstractions (tgt to drop, socket to keep, NBD to add)
    backup/ backingfile/ qcow/ sync/ types/ util/   — kept verbatim where they build
  app/                        their CLI commands (we'll keep the engine + replica subcommands;
                              longhorn-manager-leaning bits will be pruned)
  cmd/weft-block/             (TODO) go-plugin executable exposing drivers.VolumeDriver
```

## License

Code under `pkg/`, `app/`, and other paths copied from longhorn-engine is
Apache-2.0, © The Longhorn Authors. See [LICENSE](LICENSE). Control-plane code
under `control/` and any future weft-native additions are MIT/Apache-2.0 per
the wider openweft project.
