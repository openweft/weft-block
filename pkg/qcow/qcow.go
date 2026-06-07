// Package qcow is the QCOW2 backing-file adapter consumed by pkg/backingfile.
// Originally cgo (linking libqcow), it now shims our pure-Go QCOW2 driver
// github.com/go-diskimages/qcow2 — no cgo, no system library to install.
//
// Surface kept compatible with the upstream Longhorn API so pkg/backingfile
// builds unchanged: Open(file string) (*Qcow, error) returning an object that
// satisfies types.DiffDisk (ReadAt / WriteAt / UnmapAt / Close / Fd / Size).
//
// Backing files are read-only base images: WriteAt and UnmapAt are no-ops
// returning the request length, mirroring the "writes silently ignored on a
// backing layer" expectation. Real writes happen on the volume's head, never
// on the backing file.
package qcow

import (
	"errors"
	"fmt"

	qcow2 "github.com/go-diskimages/qcow2"
)

// Qcow wraps an image_qcow2.Device with the io.WriterAt / UnmapAt extensions
// pkg/types.DiffDisk requires. Read paths go through the pure-Go QCOW2
// reader; write paths are no-ops (backing files are read-only).
type Qcow struct {
	dev *qcow2.Device
}

// Open opens path as a QCOW2 image for backing-file use.
func Open(path string) (*Qcow, error) {
	if path == "" {
		return nil, errors.New("qcow.Open: empty path")
	}
	d, err := qcow2.OpenDevice(path)
	if err != nil {
		return nil, fmt.Errorf("qcow.Open(%s): %w", path, err)
	}
	return &Qcow{dev: d}, nil
}

func (q *Qcow) ReadAt(buf []byte, off int64) (int, error) {
	return q.dev.ReadAt(buf, off)
}

// WriteAt is a no-op on a backing file (writes belong to the volume head).
// We return len(buf), nil so callers that pipeline writes blindly don't error
// — the upstream cgo binding behaved similarly when fed a read-only image.
func (q *Qcow) WriteAt(buf []byte, _ int64) (int, error) {
	return len(buf), nil
}

// UnmapAt (discard / trim) is a no-op on a backing file.
func (q *Qcow) UnmapAt(length uint32, _ int64) (int, error) {
	return int(length), nil
}

func (q *Qcow) Close() error { return q.dev.Close() }

func (q *Qcow) Size() (int64, error) { return q.dev.Size() }

func (q *Qcow) Fd() uintptr { return q.dev.Fd() }
