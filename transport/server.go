package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

// RPC is the inbound dispatch surface a Server routes requests to. The
// exported *raft.Node methods RequestVote, AppendEntries, and InstallSnapshot
// satisfy it, which is exactly what fault.Network calls directly in the
// deterministic in-memory path. Using the receiver's own methods means the wire
// dispatch engages the same Raft semantics as the test transport, with no
// protocol layer between them.
type RPC interface {
	RequestVote(context.Context, raft.RequestVoteArgs) (raft.RequestVoteReply, error)
	AppendEntries(context.Context, raft.AppendEntriesArgs) (raft.AppendEntriesReply, error)
	InstallSnapshot(context.Context, raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error)
}

var _ RPC = (*raft.Node)(nil)

const defaultServerWriteTimeout = 10 * time.Second

// Server is the inbound half of the TCP transport: it listens, accepts
// connections, routes each request frame to the RPC handler, and returns the
// reply using the request's own ID so a client can correlate it. Replies from
// concurrent handlers may leave out of order; the frame protocol and client
// correlator make that harmless. A request that decodes or dispatches with an
// error closes the connection, so the peer's pending request fails promptly
// instead of hanging, and the peer reconnects on its next RPC attempt.
type Server struct {
	handler      RPC
	listener     net.Listener
	writeTimeout time.Duration

	mu    sync.Mutex
	conns map[*serverConn]struct{}
	wg    sync.WaitGroup
}

// serverConn pairs a socket with a write mutex. A connection's request frames
// are read sequentially on one goroutine; replies are written from the handler
// goroutines, so the mutex keeps individual frames atomic on the wire.
type serverConn struct {
	conn    net.Conn
	writeMu sync.Mutex
}

// NewServer returns a server that dispatches to handler.
func NewServer(handler RPC) *Server {
	return &Server{handler: handler, conns: make(map[*serverConn]struct{})}
}

// SetHandler replaces the RPC handler. It must be attached before Serve so
// accepted requests never dispatch to a nil handler.
func (s *Server) SetHandler(handler RPC) {
	s.handler = handler
}

// SetWriteTimeout bounds a single reply write. Zero restores the default.
func (s *Server) SetWriteTimeout(d time.Duration) {
	s.writeTimeout = d
}

func (s *Server) replyWriteTimeout() time.Duration {
	if s.writeTimeout > 0 {
		return s.writeTimeout
	}
	return defaultServerWriteTimeout
}

// Listen binds the server to addr, e.g. "127.0.0.1:7001" or ":0" for an
// ephemeral port (then read it with Addr).
func (s *Server) Listen(addr string) error {
	if s.listener != nil {
		return errors.New("transport: server is already listening")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

// Addr returns the bound address, for discovering an ephemeral port.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Serve accepts connections until ctx is canceled (graceful shutdown) or the
// listener fails. On shutdown it closes every connection, waits for in-flight
// handlers to return (their ctx is canceled), and returns nil.
func (s *Server) Serve(ctx context.Context) error {
	if s.listener == nil {
		return errors.New("transport: server is not listening")
	}
	type acceptResult struct{ err error }
	accepted := make(chan acceptResult, 1)
	go func() { accepted <- acceptResult{err: s.acceptLoop(ctx)} }()

	var err error
	select {
	case result := <-accepted:
		err = result.err
	case <-ctx.Done():
		s.listener.Close()
		<-accepted
	}
	s.shutdown()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (s *Server) acceptLoop(ctx context.Context) error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		sc := &serverConn{conn: conn}
		s.mu.Lock()
		s.conns[sc] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.drop(sc)
			s.handleConn(ctx, sc)
		}()
	}
}

// handleConn reads request frames sequentially and hands each to its own
// handler goroutine, so a slow request (a snapshot install) never blocks a
// faster one (a heartbeat) behind it on the same connection.
func (s *Server) handleConn(ctx context.Context, sc *serverConn) {
	for {
		msg, err := ReadMessage(sc.conn)
		if err != nil {
			return
		}
		if !msg.Type.IsRequest() {
			continue
		}
		s.wg.Add(1)
		go func(msg Message) {
			defer s.wg.Done()
			s.dispatch(ctx, sc, msg)
		}(msg)
	}
}

func (s *Server) dispatch(ctx context.Context, sc *serverConn, msg Message) {
	replyType, ok := msg.Type.ResponseType()
	if !ok {
		return
	}
	payload, err := s.handle(ctx, msg)
	if err != nil {
		sc.conn.Close()
		return
	}
	frame, err := Encode(Message{Version: ProtocolVersion, Type: replyType, RequestID: msg.RequestID, Payload: payload})
	if err != nil {
		return
	}
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	if err := sc.conn.SetWriteDeadline(time.Now().Add(s.replyWriteTimeout())); err != nil {
		return
	}
	sc.conn.Write(frame)
}

// handle decodes the typed request and invokes the handler, returning the
// encoded reply payload.
func (s *Server) handle(ctx context.Context, msg Message) ([]byte, error) {
	switch msg.Type {
	case MessageTypeVoteRequest:
		args, err := DecodeVoteRequest(msg.Payload)
		if err != nil {
			return nil, err
		}
		reply, err := s.handler.RequestVote(ctx, args)
		if err != nil {
			return nil, err
		}
		return EncodeVoteResponse(reply)
	case MessageTypeAppendRequest:
		args, err := DecodeAppendRequest(msg.Payload)
		if err != nil {
			return nil, err
		}
		reply, err := s.handler.AppendEntries(ctx, args)
		if err != nil {
			return nil, err
		}
		return EncodeAppendResponse(reply)
	case MessageTypeSnapshotRequest:
		args, err := DecodeSnapshotRequest(msg.Payload)
		if err != nil {
			return nil, err
		}
		reply, err := s.handler.InstallSnapshot(ctx, args)
		if err != nil {
			return nil, err
		}
		return EncodeSnapshotResponse(reply)
	default:
		return nil, ErrUnknownMessageType
	}
}

// Close closes the listener, causing a running Serve to shut down gracefully
// once in-flight handlers finish.
func (s *Server) Close() error {
	if s.listener == nil {
		return errors.New("transport: server is not listening")
	}
	return s.listener.Close()
}

// shutdown closes every live connection (unblocking the readers) and waits for
// the connection handlers and request handlers to exit.
func (s *Server) shutdown() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.conns))
	for sc := range s.conns {
		conns = append(conns, sc)
	}
	s.mu.Unlock()
	for _, sc := range conns {
		sc.conn.Close()
	}
	s.wg.Wait()
}

func (s *Server) drop(sc *serverConn) {
	s.mu.Lock()
	delete(s.conns, sc)
	s.mu.Unlock()
}
