package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
)

// Server exposes a small client protocol over TCP in front of a Raft node and
// its state machine. It is deliberately separate from the Raft wire frame
// protocol so the frozen Raft message-type range is untouched.
type Server struct {
	node  raft.NodeAPI
	store kv.Store

	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	closed   bool
	listener net.Listener

	wg sync.WaitGroup
}

// NewServer wires a client listener to a Raft node and its state machine.
func NewServer(node raft.NodeAPI, store kv.Store) *Server {
	return &Server{node: node, store: store, conns: make(map[net.Conn]struct{})}
}

// Listen binds the client address. Call before Serve.
func (s *Server) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	return nil
}

// Addr returns the bound address, or nil before Listen.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Serve accepts connections until ctx is canceled, then closes the listener and
// all live connections and waits for handlers to return.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener == nil {
		return errors.New("client server: Serve before Listen")
	}

	acceptErr := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			if !s.track(conn) {
				_ = conn.Close()
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer s.drop(conn)
				s.handleConn(ctx, conn)
			}()
		}
	}()

	var err error
	select {
	case <-ctx.Done():
		err = nil
	case err = <-acceptErr:
		if isClosedConnErr(err) {
			err = nil
		}
	}

	// Close the listener to unblock Accept, then unblock in-flight handlers by
	// closing their connections, then wait.
	_ = listener.Close()
	s.closeAllConns()
	s.wg.Wait()
	return err
}

// Close stops accepting and drops live connections.
func (s *Server) Close() error {
	s.mu.Lock()
	listener := s.listener
	s.closed = true
	s.mu.Unlock()
	var err error
	if listener != nil {
		err = listener.Close()
	}
	s.closeAllConns()
	s.wg.Wait()
	return err
}

func (s *Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) drop(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	_ = conn.Close()
}

func (s *Server) closeAllConns() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// handleConn reads requests sequentially and dispatches each to its own
// goroutine so a slow op cannot stall the connection, mirroring the Raft
// server's concurrency style. A per-connection write mutex keeps frames whole.
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	reader := newFrameReader(conn)
	var writeMu sync.Mutex
	for {
		var req Request
		if err := readFrame(reader, &req); err != nil {
			return
		}
		s.wg.Add(1)
		go func(req Request) {
			defer s.wg.Done()
			resp := s.process(ctx, req)
			writeMu.Lock()
			defer writeMu.Unlock()
			_ = writeFrame(conn, resp)
		}(req)
	}
}

// process executes one client operation. A non-leader proposal is reported as
// a redirect; a Get is served from the local store (not linearizable).
func (s *Server) process(ctx context.Context, req Request) Response {
	switch req.Op {
	case OpStatus:
		role, err := s.node.Role(ctx)
		if err != nil {
			return Response{Error: err.Error()}
		}
		leader, err := s.node.Leader(ctx)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return Response{OK: true, Role: role, Leader: leader}

	case OpPut, OpDelete:
		cmdType := kv.Put
		if req.Op == OpDelete {
			cmdType = kv.Delete
		}
		cmd := kv.Command{Type: cmdType, Key: req.Key, Value: req.Value}
		encoded, err := cmd.Encode()
		if err != nil {
			return Response{Error: err.Error()}
		}
		result, err := s.node.Propose(ctx, encoded)
		if err != nil {
			return notLeaderResponse(err)
		}
		return Response{OK: true, Result: result}

	case OpGet:
		value, found, err := s.store.Get(ctx, req.Key)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return Response{OK: true, Found: found, Value: value}

	default:
		return Response{Error: fmt.Sprintf("%v: %q", ErrUnknownOp, req.Op)}
	}
}

// notLeaderResponse maps a Raft error to a client redirect when the node knows
// it is not the leader; other errors are reported as-is.
func notLeaderResponse(err error) Response {
	var raftErr *raft.Error
	if errors.As(err, &raftErr) {
		if raftErr.Code == raft.ErrCodeNotLeader {
			resp := Response{Error: notLeaderMessage}
			if raftErr.LeaderID != "" {
				resp.Leader = raftErr.LeaderID
			}
			return resp
		}
		return Response{Error: raftErr.Error()}
	}
	return Response{Error: err.Error()}
}

func isClosedConnErr(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
