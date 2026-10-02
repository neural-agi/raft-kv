package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

var (
	// ErrUnknownPeer is returned by an RPC when the target is absent from the
	// transport's static address map. A transport never guesses an address.
	ErrUnknownPeer = errors.New("transport: unknown peer")
	// ErrTransportClosed is returned by RPC calls after the transport's Close.
	ErrTransportClosed = errors.New("transport: closed")
	// ErrUnexpectedResponse is returned when a response's type does not match
	// the request it answers.
	ErrUnexpectedResponse = errors.New("transport: unexpected response type")
)

const (
	defaultDialTimeout  = 5 * time.Second
	defaultWriteTimeout = 10 * time.Second
)

// Config carries the static peer addressing and connection budgets for a
// TCPTransport. Peers maps a Raft node ID to its host:port, so the NodeID ->
// address mapping is deterministic and supplied by the caller (V9.3 B); a
// transport contains no embedded addresses. Values of zero duration select the
// documented defaults.
type Config struct {
	// Peers maps each peer node ID to a dial address, e.g. "127.0.0.1:7001".
	Peers map[raft.NodeID]string

	// DialTimeout bounds a single connection attempt when the caller's context
	// does not already carry an earlier deadline. Zero means defaultDialTimeout.
	DialTimeout time.Duration

	// WriteTimeout bounds writing one frame once a connection is established.
	// A large InstallSnapshot frame may need more time on slow links. Zero
	// means defaultWriteTimeout.
	WriteTimeout time.Duration
}

func (c Config) dialTimeout() time.Duration {
	if c.DialTimeout > 0 {
		return c.DialTimeout
	}
	return defaultDialTimeout
}

func (c Config) writeTimeout() time.Duration {
	if c.WriteTimeout > 0 {
		return c.WriteTimeout
	}
	return defaultWriteTimeout
}

// TCPTransport is a real multi-process raft.Transport implementation: it dials
// peers over TCP, sends V9.1 framed requests, correlates each response with the
// request that produced it using the V9.2 correlation layer, and decodes the
// typed reply. Alongside the deterministic in-memory fault.Network it is the
// second raft.Transport implementation, providing the same RPC semantics over
// real sockets. Raft calls RPC methods from separate goroutines, so a
// TCPTransport must be safe for concurrent use and never assume a caller.
type TCPTransport struct {
	config Config

	mu     sync.Mutex
	conns  map[raft.NodeID]*peerConn
	closed bool
}

// NewTCPTransport returns a transport that dials the addresses in config.Peers.
// No sockets are opened until the first RPC.
func NewTCPTransport(config Config) *TCPTransport {
	return &TCPTransport{config: config, conns: make(map[raft.NodeID]*peerConn)}
}

// TCPTransport implements raft.Transport. Compile-time assertion.
var _ raft.Transport = (*TCPTransport)(nil)

// RequestVote delivers a RequestVote RPC and waits for the correlated reply.
func (t *TCPTransport) RequestVote(ctx context.Context, target raft.NodeID, request raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	payload, err := EncodeVoteRequest(request)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	msg, err := t.call(ctx, target, MessageTypeVoteRequest, MessageTypeVoteResponse, payload)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	return DecodeVoteResponse(msg.Payload)
}

// AppendEntries delivers an AppendEntries RPC and waits for the correlated
// reply. Heartbeats are AppendEntries calls with empty entries and share this
// path.
func (t *TCPTransport) AppendEntries(ctx context.Context, target raft.NodeID, request raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	payload, err := EncodeAppendRequest(request)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	msg, err := t.call(ctx, target, MessageTypeAppendRequest, MessageTypeAppendResponse, payload)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return DecodeAppendResponse(msg.Payload)
}

// InstallSnapshot delivers an InstallSnapshot RPC. Snapshots are the largest
// payloads the protocol carries; the frame is bounded by MaxFrameSize with the
// payload encoders validating length fields explicitly, so a snapshot that
// would not fit fails with a definite error instead of a partial transfer.
func (t *TCPTransport) InstallSnapshot(ctx context.Context, target raft.NodeID, request raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	payload, err := EncodeSnapshotRequest(request)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	msg, err := t.call(ctx, target, MessageTypeSnapshotRequest, MessageTypeSnapshotResponse, payload)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	return DecodeSnapshotResponse(msg.Payload)
}

// call is the shared request path every RPC uses: obtain a live connection to
// target, allocate the request ID from that connection's correlator, send the
// framed request, and wait for the matching response. The waiter is resolved
// exactly once and the correlation entry is never leaked: a reply, a canceled
// context, a dropped connection, or a closed transport each resolve it.
func (t *TCPTransport) call(ctx context.Context, target raft.NodeID, requestType, responseType MessageType, payload []byte) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	pc, err := t.acquire(ctx, target)
	if err != nil {
		return Message{}, err
	}

	id, replyCh, err := pc.corr.Allocate()
	if err != nil {
		// The correlator is closed, which means this connection's reader is
		// gone; the connection is unusable. Drop it so the next attempt dials
		// a fresh one instead of reusing a dead peer.
		pc.close()
		return Message{}, err
	}

	frame, err := Encode(Message{Version: ProtocolVersion, Type: requestType, RequestID: id, Payload: payload})
	if err != nil {
		pc.corr.Cancel(id)
		return Message{}, err
	}
	if err := pc.writeFrame(ctx, frame); err != nil {
		pc.corr.Cancel(id)
		return Message{}, err
	}

	select {
	case completion := <-replyCh:
		if completion.Err != nil {
			return Message{}, completion.Err
		}
		if completion.Reply.Type != responseType {
			return Message{}, fmt.Errorf("%w: got %v for %v", ErrUnexpectedResponse, completion.Reply.Type, requestType)
		}
		return completion.Reply, nil
	case <-ctx.Done():
		pc.corr.Cancel(id)
		return Message{}, ctx.Err()
	}
}

// acquire returns a live connection to target, dialing one when necessary. The
// result may fail concurrently (the reader may close the connection at any
// moment); callers treat that as a transport error and the next attempt
// reconnects.
func (t *TCPTransport) acquire(ctx context.Context, target raft.NodeID) (*peerConn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrTransportClosed
	}
	if pc := t.conns[target]; pc != nil {
		select {
		case <-pc.closed:
			// stale connection; dial a replacement below
		default:
			t.mu.Unlock()
			return pc, nil
		}
	}
	t.mu.Unlock()

	addr, ok := t.config.Peers[target]
	if !ok {
		return nil, ErrUnknownPeer
	}
	conn, err := t.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	pc := newPeerConn(t, target, addr, conn)
	go pc.readLoop()

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		pc.close()
		return nil, ErrTransportClosed
	}
	if existing := t.conns[target]; existing != nil {
		select {
		case <-existing.closed:
			t.conns[target] = pc
			t.mu.Unlock()
			return pc, nil
		default:
			// Another caller won the race and installed a live connection;
			// discard the duplicate we just dialed and reuse theirs.
			t.mu.Unlock()
			pc.close()
			return existing, nil
		}
	}
	t.conns[target] = pc
	t.mu.Unlock()
	return pc, nil
}

func (t *TCPTransport) dial(ctx context.Context, addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: t.config.dialTimeout()}
	return dialer.DialContext(ctx, "tcp", addr)
}

// Close shuts the transport down: it drops every live connection, which fails
// any in-flight request on it, and rejects all subsequent RPCs.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	conns := make([]*peerConn, 0, len(t.conns))
	for _, pc := range t.conns {
		conns = append(conns, pc)
	}
	t.conns = make(map[raft.NodeID]*peerConn)
	t.mu.Unlock()
	for _, pc := range conns {
		pc.close()
	}
	return nil
}

// peerConn is one established client connection to one peer. It owns a
// per-connection Correlator, a read-loop goroutine that routes every received
// response to the waiter registered for its request ID, and a write mutex so
// concurrent RPC callers cannot interleave frames on the same socket.
type peerConn struct {
	transport *TCPTransport
	target    raft.NodeID
	addr      string
	conn      net.Conn
	corr      *Correlator
	writeMu   sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}
}

func newPeerConn(t *TCPTransport, target raft.NodeID, addr string, conn net.Conn) *peerConn {
	return &peerConn{transport: t, target: target, addr: addr, conn: conn, corr: NewCorrelator(), closed: make(chan struct{})}
}

// readLoop forwards every frame received on the connection to the waiter whose
// request ID it carries. Responses may arrive in any order (V9.3 D); the
// Correlator matches by request ID, so out-of-order and duplicate responses are
// harmless. When the connection fails or closes, the loop exits and close()
// fails every waiter still pending on it.
func (pc *peerConn) readLoop() {
	for {
		msg, err := ReadMessage(pc.conn)
		if err != nil {
			break
		}
		pc.corr.Complete(msg.RequestID, msg)
	}
	pc.close()
}

func (pc *peerConn) writeFrame(ctx context.Context, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(pc.transport.config.writeTimeout())
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()
	if err := pc.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := pc.conn.Write(frame)
	return err
}

// close tears the connection down exactly once: it closes the socket, closes
// the correlation layer so every pending waiter on this connection fails, and
// drops this connection from the transport so the next RPC dials fresh.
func (pc *peerConn) close() {
	pc.closeOnce.Do(func() {
		pc.conn.Close()
		pc.corr.Close()
		close(pc.closed)
		pc.transport.forget(pc)
	})
}

func (t *TCPTransport) forget(pc *peerConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conns[pc.target] == pc {
		delete(t.conns, pc.target)
	}
}
