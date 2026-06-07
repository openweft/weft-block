//go:build !linux

package nbd

import (
	"errors"

	"github.com/openweft/weft-block/pkg/types"
)

// errNBDLinuxOnly is what every backing call returns on non-Linux platforms.
// The frontend type still satisfies types.Frontend (so the rest of weft-block
// compiles on dev hosts) but Startup will fail with this error; the data
// plane is Linux-only by design.
var errNBDLinuxOnly = errors.New("nbd: the NBD frontend is Linux-only (in-kernel NBD client)")

func (n *Nbd) startup(types.ReaderWriterUnmapperAt) error { return errNBDLinuxOnly }
func (n *Nbd) shutdown() error                            { return errNBDLinuxOnly }
func (n *Nbd) expand(int64) error                         { return errNBDLinuxOnly }
