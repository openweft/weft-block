package main

// stubs.go provides Hypervisor / Network / Image stubs so this plugin
// satisfies the four-service Bundle the weft go-plugin contract serves. Only
// Volume is real here; the host's hypervisor plugin (weft-driver-vz/-qemu)
// owns the other three. Every stub method either returns the cached
// HostInfo or drivers.ErrUnsupported with a clear "this plugin only serves
// the Volume backend" message.

import (
	"context"
	"fmt"

	drivers "github.com/openweft/weft-drivers"
)

func unsupported(method string) error {
	return fmt.Errorf("weft-block: %s not served by this plugin (Volume-only backend): %w", method, drivers.ErrUnsupported)
}

// ----- Hypervisor stub -----

type stubHypervisor struct{ hi hostInfo }

func (s stubHypervisor) HostInfo(context.Context) (drivers.HostInfo, error) {
	return s.hi.driversHostInfo(), nil
}
func (stubHypervisor) CreateVM(context.Context, drivers.VMSpec) error { return unsupported("CreateVM") }
func (stubHypervisor) StartVM(context.Context, string) error          { return unsupported("StartVM") }
func (stubHypervisor) StopVM(context.Context, string) error           { return unsupported("StopVM") }
func (stubHypervisor) DeleteVM(context.Context, string) error         { return unsupported("DeleteVM") }
func (stubHypervisor) AttachDisk(context.Context, string, drivers.DiskSpec) error {
	return unsupported("AttachDisk")
}
func (stubHypervisor) DetachDisk(context.Context, string, string) error { return unsupported("DetachDisk") }
func (stubHypervisor) AttachNIC(context.Context, string, drivers.NICHandle) error {
	return unsupported("AttachNIC")
}
func (stubHypervisor) DetachNIC(context.Context, string, string) error { return unsupported("DetachNIC") }

// ----- Network stub -----

type stubNetwork struct{ hi hostInfo }

func (s stubNetwork) HostInfo(context.Context) (drivers.HostInfo, error) {
	return s.hi.driversHostInfo(), nil
}
func (stubNetwork) EnsureNetwork(context.Context, drivers.NetworkSpec) error {
	return unsupported("EnsureNetwork")
}
func (stubNetwork) DestroyNetwork(context.Context, string) error { return unsupported("DestroyNetwork") }
func (stubNetwork) AttachPort(context.Context, drivers.PortSpec) (drivers.NICHandle, error) {
	return drivers.NICHandle{}, unsupported("AttachPort")
}
func (stubNetwork) DetachPort(context.Context, string) error { return unsupported("DetachPort") }
func (stubNetwork) RotateMeshPeer(context.Context, drivers.PortSpec) error {
	return drivers.ErrNotApplicable
}

// ----- Image stub -----

type stubImage struct{ hi hostInfo }

func (s stubImage) HostInfo(context.Context) (drivers.HostInfo, error) {
	return s.hi.driversHostInfo(), nil
}
func (stubImage) Pull(context.Context, string) error                { return unsupported("Pull") }
func (stubImage) LocalPath(context.Context, string) (string, error) { return "", drivers.ErrNotFound }
func (stubImage) Delete(context.Context, string) error              { return unsupported("Delete") }
func (stubImage) InCache(context.Context, string) (bool, error)     { return false, nil }
