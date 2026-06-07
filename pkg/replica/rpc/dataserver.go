package rpc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/openweft/weft-block/pkg/dataconn"
	"github.com/openweft/weft-block/pkg/replica"
	"github.com/openweft/weft-block/pkg/types"
)

type DataServer struct {
	protocol types.DataServerProtocol
	address  string
	s        *replica.Server

	mu       sync.Mutex
	listener net.Listener // set once ListenAndServe binds; used by Close
	closed   bool
}

func NewDataServer(protocol types.DataServerProtocol, address string, s *replica.Server) *DataServer {
	return &DataServer{
		protocol: protocol,
		address:  address,
		s:        s,
	}
}

// Close stops the data server: closes the listener (which makes AcceptTCP /
// AcceptUnix return an error and ListenAndServe return cleanly) and marks the
// server closed so a transient Accept error after Close doesn't get logged as
// an error and doesn't trigger a tight retry loop. Idempotent.
func (s *DataServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.listener == nil {
		return nil
	}
	err := s.listener.Close()
	s.listener = nil
	return err
}

func (s *DataServer) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *DataServer) setListener(l net.Listener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listener = l
}

func (s *DataServer) ListenAndServe() error {
	switch s.protocol {
	case types.DataServerProtocolTCP:
		return s.listenAndServeTCP()
	case types.DataServerProtocolUNIX:
		return s.listenAndServeUNIX()
	default:
		return fmt.Errorf("unsupported protocol: %v", s.protocol)
	}
}

func (s *DataServer) listenAndServeTCP() error {
	addr, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	s.setListener(l)

	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			if s.isClosed() {
				// Close() was called — clean exit, not an error.
				return nil
			}
			logrus.WithError(err).Error("failed to accept tcp connection")
			continue
		}

		logrus.Infof("New connection from: %v", conn.RemoteAddr())

		go func(conn net.Conn) {
			defer func() {
				_ = conn.Close()
			}()
			server := dataconn.NewServer(conn, s.s)
			if err := server.Handle(); err != nil {
				if errors.Is(err, io.EOF) {
					// Clean remote close: this is normal on detach, engine restart.
					logrus.WithError(err).Info("Data server connection closed by remote")
					return
				}
				logrus.WithError(err).Warn("Failed to handle data server")
			}
		}(conn)
	}
}

func (s *DataServer) listenAndServeUNIX() error {
	unixAddr, err := net.ResolveUnixAddr("unix", s.address)
	if err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", unixAddr)
	if err != nil {
		return err
	}
	s.setListener(l)

	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			if s.isClosed() {
				return nil
			}
			logrus.WithError(err).Error("failed to accept unix-domain-socket connection")
			continue
		}
		logrus.Infof("New connection from: %v", conn.RemoteAddr())
		go func(conn net.Conn) {
			defer func() {
				_ = conn.Close()
			}()
			server := dataconn.NewServer(conn, s.s)
			if err := server.Handle(); err != nil {
				if errors.Is(err, io.EOF) {
					logrus.WithError(err).Info("Data server connection closed by local peer")
					return
				}
				logrus.WithError(err).Warn("Failed to handle data server")
			}
		}(conn)
	}
}
