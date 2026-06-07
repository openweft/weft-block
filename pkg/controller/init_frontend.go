package controller

import (
	"fmt"
	"time"

	"github.com/openweft/weft-block/pkg/frontend/nbd"
	"github.com/openweft/weft-block/pkg/frontend/rest"
	"github.com/openweft/weft-block/pkg/frontend/socket"
	"github.com/openweft/weft-block/pkg/types"
	"github.com/sirupsen/logrus"
)

const (
	DefaultEngineReplicaTimeout = 8 * time.Second
	minEngineReplicaTimeout     = 8 * time.Second
	maxEngineReplicaTimeout     = 30 * time.Second
)

// NewFrontend wires a frontend by name. iSCSI/tgt (the upstream Longhorn
// default) is dropped in this fork — its replacement is NBD, which uses the
// in-kernel NBD client and needs no userspace target daemon. "tgt-blockdev"
// and "tgt-iscsi" are aliased to nbd so existing callers keep working.
func NewFrontend(frontendType string, _ time.Duration) (types.Frontend, error) {
	switch frontendType {
	case "rest":
		return rest.New(), nil
	case "socket":
		return socket.New(), nil
	case "nbd", "tgt-blockdev", "tgt-iscsi":
		return nbd.New(), nil
	default:
		return nil, fmt.Errorf("unsupported frontend type: %v", frontendType)
	}
}

func DetermineEngineReplicaTimeout(timeout time.Duration) time.Duration {
	if timeout < minEngineReplicaTimeout || timeout > maxEngineReplicaTimeout {
		logrus.Warnf("Using default engine-replica timeout %v instead since the given value %v is not allowable", DefaultEngineReplicaTimeout, timeout)
		return DefaultEngineReplicaTimeout
	}
	return timeout
}

// DetermineIscsiTargetRequestTimeout is a no-op kept for callsite stability:
// the iSCSI frontend was replaced with NBD, which has no per-request timeout
// of its own (the kernel NBD client manages I/O timeouts). app/cmd/controller
// still computes a value and passes it through; we accept and ignore it.
func DetermineIscsiTargetRequestTimeout(time.Duration) time.Duration { return 0 }
