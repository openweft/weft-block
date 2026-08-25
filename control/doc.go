// Package control is the weft-native control plane for the block storage
// driver: the bits Longhorn would put in longhorn-manager (a Kubernetes
// CRD controller). It owns the Volume/Replica/Engine state and the
// placement policy; the data plane (engine, replica, sparse-file ops)
// stays in the imported pkg/controller, pkg/replica, etc. ported from
// longhorn-engine.
//
//   - types.go      : Volume, Replica, Engine records (~ Longhorn CRDs)
//   - placement.go  : deterministic N-replica spread across DCs
//   - store.go      : VolumeStore interface + in-memory impl
//     (etcd binding lives a slice further)
//
// The package depends only on the standard library — production binds
// run against the embedded etcd via a different VolumeStore impl, and
// tests can drive the whole control plane with MemStore.
package control
