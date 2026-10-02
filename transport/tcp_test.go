package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

// testHandler is a configurable raft RPC handler for TCP transport tests. Reply
// functions produce deterministic responses, gates block a handler until
// released, and per-term delays force out-of-order replies.
type testHandler struct {
	mu sync.Mutex

	voteFn     func(raft.RequestVoteArgs) raft.RequestVoteReply
	appendFn   func(raft.AppendEntriesArgs) raft.AppendEntriesReply
	snapshotFn func(raft.InstallSnapshotArgs) raft.InstallSnapshotReply

	voteGate     chan struct{}
	appendGate   chan struct{}
	snapshotGate chan struct{}

	appendDelays map[raft.Term]time.Duration

	voteCount    uint64
	appendCount  uint64
	lastAppend   raft.AppendEntriesArgs
	lastSnapshot raft.InstallSnapshotArgs
}

func newTestHandler() *testHandler {
	return &testHandler{appendDelays: make(map[raft.Term]time.Duration)}
}

func (h *testHandler) counts() (votes, appends uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.voteCount, h.appendCount
}

func (h *testHandler) wait(ctx context.Context, gate chan struct{}) error {
	if gate == nil {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *testHandler) RequestVote(ctx context.Context, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	h.mu.Lock()
	h.voteCount++
	fn, gate := h.voteFn, h.voteGate
	h.mu.Unlock()
	if err := h.wait(ctx, gate); err != nil {
		return raft.RequestVoteReply{}, err
	}
	if fn != nil {
		return fn(args), nil
	}
	return raft.RequestVoteReply{}, nil
}

func (h *testHandler) AppendEntries(ctx context.Context, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	h.mu.Lock()
	h.appendCount++
	fn, gate := h.appendFn, h.appendGate
	delay := h.appendDelays[args.Term]
	h.lastAppend = args
	h.mu.Unlock()
	if err := h.wait(ctx, gate); err != nil {
		return raft.AppendEntriesReply{}, err
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return raft.AppendEntriesReply{}, ctx.Err()
		}
	}
	if fn != nil {
		return fn(args), nil
	}
	return raft.AppendEntriesReply{}, nil
}

func (h *testHandler) InstallSnapshot(ctx context.Context, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	h.mu.Lock()
	fn, gate := h.snapshotFn, h.snapshotGate
	h.lastSnapshot = args
	h.mu.Unlock()
	if err := h.wait(ctx, gate); err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	if fn != nil {
		return fn(args), nil
	}
	return raft.InstallSnapshotReply{}, nil
}

func newTestTransport(t *testing.T, peers map[raft.NodeID]string) *TCPTransport {
	t.Helper()
	return NewTCPTransport(Config{Peers: peers})
}

type testServer struct {
	server *Server
	addr   string
	cancel context.CancelFunc
	done   chan error
}

func startTestServer(t *testing.T, handler RPC) *testServer {
	t.Helper()
	server := NewServer(handler)
	if err := server.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	return &testServer{server: server, addr: server.Addr().String(), cancel: cancel, done: done}
}

func (ts *testServer) shutdown(t *testing.T) {
	t.Helper()
	ts.cancel()
	select {
	case err := <-ts.done:
		if err != nil {
			t.Fatalf("server Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}

func waitForRequestCounts(t *testing.T, handler *testHandler, wantVotes, wantAppends uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		votes, appends := handler.counts()
		if votes >= wantVotes && appends >= wantAppends {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	votes, appends := handler.counts()
	t.Fatalf("handler observed %d vote/%d append requests, want >= %d/%d", votes, appends, wantVotes, wantAppends)
}

func TestServerDispatchesAllRPCsRoundTrip(t *testing.T) {
	handler := newTestHandler()
	handler.voteFn = func(args raft.RequestVoteArgs) raft.RequestVoteReply {
		return raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
	}
	handler.appendFn = func(args raft.AppendEntriesArgs) raft.AppendEntriesReply {
		return raft.AppendEntriesReply{Term: args.Term + 1, Success: true}
	}
	handler.snapshotFn = func(args raft.InstallSnapshotArgs) raft.InstallSnapshotReply {
		return raft.InstallSnapshotReply{Term: args.Term + 1}
	}
	ts := startTestServer(t, handler)
	defer ts.shutdown(t)
	tr := newTestTransport(t, map[raft.NodeID]string{"b": ts.addr})
	defer tr.Close()

	ctx := context.Background()

	vote, err := tr.RequestVote(ctx, "b", raft.RequestVoteArgs{Term: 3, CandidateID: "a", LastLogIndex: 7, LastLogTerm: 2})
	if err != nil {
		t.Fatal(err)
	}
	if vote.Term != 4 || !vote.VoteGranted {
		t.Fatalf("vote reply = %+v, want {Term:4 VoteGranted:true}", vote)
	}

	entries := []raft.LogEntry{{Term: 5, Index: 3, Command: []byte("x")}}
	appendReply, err := tr.AppendEntries(ctx, "b", raft.AppendEntriesArgs{Term: 5, LeaderID: "a", PrevLogIndex: 2, PrevLogTerm: 1, Entries: entries, LeaderCommit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if appendReply.Term != 6 || !appendReply.Success {
		t.Fatalf("append reply = %+v, want {Term:6 Success:true}", appendReply)
	}
	if handler.lastAppend.PrevLogIndex != 2 || handler.lastAppend.PrevLogTerm != 1 || handler.lastAppend.LeaderID != "a" || handler.lastAppend.LeaderCommit != 3 {
		t.Fatalf("append args = %+v", handler.lastAppend)
	}
	if len(handler.lastAppend.Entries) != 1 || handler.lastAppend.Entries[0].Term != 5 || handler.lastAppend.Entries[0].Index != 3 || !bytes.Equal(handler.lastAppend.Entries[0].Command, []byte("x")) {
		t.Fatalf("append entries = %+v", handler.lastAppend.Entries)
	}

	snapshot, err := tr.InstallSnapshot(ctx, "b", raft.InstallSnapshotArgs{Term: 9, LeaderID: "a", LastIncludedIndex: 4, LastIncludedTerm: 8, Data: []byte("snap-data")})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Term != 10 {
		t.Fatalf("snapshot reply = %+v, want {Term:10}", snapshot)
	}
	if !bytes.Equal(handler.lastSnapshot.Data, []byte("snap-data")) {
		t.Fatalf("snapshot data = %q, want %q", handler.lastSnapshot.Data, "snap-data")
	}
}

func TestOutOfOrderResponsesReachCorrectCallers(t *testing.T) {
	handler := newTestHandler()
	handler.appendFn = func(args raft.AppendEntriesArgs) raft.AppendEntriesReply {
		return raft.AppendEntriesReply{Term: args.Term + 1, Success: true}
	}
	// Distinct per-request delays force the server to answer 10, then 12, then 11:
	// a reply order that never matches the request order, so routing must be by
	// request ID rather than arrival position.
	handler.appendDelays = map[raft.Term]time.Duration{
		10: 30 * time.Millisecond,
		11: 90 * time.Millisecond,
		12: 50 * time.Millisecond,
	}
	ts := startTestServer(t, handler)
	defer ts.shutdown(t)
	tr := newTestTransport(t, map[raft.NodeID]string{"b": ts.addr})
	defer tr.Close()

	type result struct {
		term  raft.Term
		reply raft.AppendEntriesReply
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 3)
	for _, term := range []raft.Term{10, 11, 12} {
		go func(term raft.Term) {
			<-start
			reply, err := tr.AppendEntries(context.Background(), "b", raft.AppendEntriesArgs{Term: term, LeaderID: "a"})
			results <- result{term: term, reply: reply, err: err}
		}(term)
	}
	close(start)
	for i := 0; i < 3; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("request %d: %v", r.term, r.err)
		}
		if !r.reply.Success || r.reply.Term != r.term+1 {
			t.Fatalf("request %d routed to reply %+v", r.term, r.reply)
		}
	}
}

func TestConcurrentRequestVoteOverSingleConnection(t *testing.T) {
	handler := newTestHandler()
	handler.voteFn = func(args raft.RequestVoteArgs) raft.RequestVoteReply {
		return raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
	}
	ts := startTestServer(t, handler)
	defer ts.shutdown(t)
	tr := newTestTransport(t, map[raft.NodeID]string{"b": ts.addr})
	defer tr.Close()

	const actors = 24
	type result struct {
		term  raft.Term
		reply raft.RequestVoteReply
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, actors)
	for i := 0; i < actors; i++ {
		term := raft.Term(1000 + i)
		go func(term raft.Term) {
			<-start
			reply, err := tr.RequestVote(context.Background(), "b", raft.RequestVoteArgs{Term: term, CandidateID: "a"})
			results <- result{term: term, reply: reply, err: err}
		}(term)
	}
	close(start)
	for i := 0; i < actors; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("term %d: %v", r.term, r.err)
		}
		if !r.reply.VoteGranted || r.reply.Term != r.term+1 {
			t.Fatalf("term %d routed to reply %+v", r.term, r.reply)
		}
	}
}

func TestInstallSnapshotLargePayloadAndOversizedRejection(t *testing.T) {
	handler := newTestHandler()
	data := make([]byte, 2<<20)
	for i := range data {
		data[i] = byte(i % 251)
	}
	handler.snapshotFn = func(args raft.InstallSnapshotArgs) raft.InstallSnapshotReply {
		return raft.InstallSnapshotReply{Term: args.Term + 1}
	}
	ts := startTestServer(t, handler)
	defer ts.shutdown(t)
	tr := newTestTransport(t, map[raft.NodeID]string{"b": ts.addr})
	defer tr.Close()

	reply, err := tr.InstallSnapshot(context.Background(), "b", raft.InstallSnapshotArgs{Term: 2, LeaderID: "a", LastIncludedIndex: 100, LastIncludedTerm: 50, Data: data})
	if err != nil {
		t.Fatalf("large snapshot: %v", err)
	}
	if reply.Term != 3 {
		t.Fatalf("snapshot reply = %+v, want {Term:3}", reply)
	}
	if !bytes.Equal(handler.lastSnapshot.Data, data) {
		t.Fatal("snapshot payload corrupted in transit")
	}

	// A snapshot beyond the frame budget must be rejected by the encoder before
	// any network activity, not partially transferred.
	if _, err := tr.InstallSnapshot(context.Background(), "b", raft.InstallSnapshotArgs{Term: 2, LeaderID: "a", Data: make([]byte, MaxFrameSize)}); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized snapshot err = %v, want ErrFrameTooLarge", err)
	}
}

func TestCanceledRequestCancelsCorrelatorAndConnectionSurvives(t *testing.T) {
	handler := newTestHandler()
	handler.voteFn = func(args raft.RequestVoteArgs) raft.RequestVoteReply {
		return raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
	}
	handler.voteGate = make(chan struct{})
	ts := startTestServer(t, handler)
	defer ts.shutdown(t)
	tr := newTestTransport(t, map[raft.NodeID]string{"b": ts.addr})
	defer tr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := tr.RequestVote(ctx, "b", raft.RequestVoteArgs{Term: 41})
		errCh <- err
	}()
	waitForRequestCounts(t, handler, 1, 0)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled request err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request did not return")
	}

	// The cancellation must not corrupt the connection: release the handler and
	// confirm a later RPC still completes normally.
	close(handler.voteGate)
	reply, err := tr.RequestVote(context.Background(), "b", raft.RequestVoteArgs{Term: 5})
	if err != nil {
		t.Fatalf("request after cancellation: %v", err)
	}
	if reply.Term != 6 || !reply.VoteGranted {
		t.Fatalf("reply after cancellation = %+v", reply)
	}
}

func TestServerShutdownFailsInflightAndClientReconnects(t *testing.T) {
	handler := newTestHandler()
	handler.voteFn = func(args raft.RequestVoteArgs) raft.RequestVoteReply {
		return raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
	}
	ts := startTestServer(t, handler)
	tr := newTestTransport(t, map[raft.NodeID]string{"b": ts.addr})
	defer tr.Close()

	if _, err := tr.RequestVote(context.Background(), "b", raft.RequestVoteArgs{Term: 1}); err != nil {
		t.Fatalf("first request: %v", err)
	}
	// Arm the gate only now so the next request blocks server-side.
	handler.voteGate = make(chan struct{})
	// A second in-flight request is now blocked server-side at the gate.
	errCh := make(chan error, 1)
	go func() {
		_, err := tr.RequestVote(context.Background(), "b", raft.RequestVoteArgs{Term: 2})
		errCh <- err
	}()
	waitForRequestCounts(t, handler, 2, 0)

	// Tearing the server down closes the connection and must fail the in-flight
	// request instead of leaving it blocked.
	ts.shutdown(t)
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("in-flight request succeeded after server shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request blocked forever after server shutdown")
	}

	// A server restarted on the same address must be reachable on the next
	// attempt without restarting the client transport (V9.3 E).
	replacement := newTestHandler()
	replacement.voteFn = func(args raft.RequestVoteArgs) raft.RequestVoteReply {
		return raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
	}
	server2 := NewServer(replacement)
	if err := server2.Listen(ts.addr); err != nil {
		t.Fatalf("rebind listener: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- server2.Serve(ctx2) }()
	defer func() {
		cancel2()
		if err := <-done2; err != nil {
			t.Errorf("replacement server err: %v", err)
		}
	}()

	reply, err := tr.RequestVote(context.Background(), "b", raft.RequestVoteArgs{Term: 3})
	if err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	if reply.Term != 4 || !reply.VoteGranted {
		t.Fatalf("reconnect reply = %+v", reply)
	}
}

func TestTransportCloseFailsInflightAndRejectsNewCalls(t *testing.T) {
	handler := newTestHandler()
	handler.voteFn = func(args raft.RequestVoteArgs) raft.RequestVoteReply {
		return raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
	}
	handler.voteGate = make(chan struct{})
	ts := startTestServer(t, handler)
	defer ts.shutdown(t)
	tr := newTestTransport(t, map[raft.NodeID]string{"b": ts.addr})

	errCh := make(chan error, 1)
	go func() {
		_, err := tr.RequestVote(context.Background(), "b", raft.RequestVoteArgs{Term: 9})
		errCh <- err
	}()
	waitForRequestCounts(t, handler, 1, 0)

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("in-flight request succeeded after transport Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request blocked forever after transport Close")
	}

	if _, err := tr.RequestVote(context.Background(), "b", raft.RequestVoteArgs{Term: 10}); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("request after Close err = %v, want ErrTransportClosed", err)
	}
}

func TestUnknownPeerFailsFast(t *testing.T) {
	tr := newTestTransport(t, map[raft.NodeID]string{"b": "127.0.0.1:1"})
	defer tr.Close()
	if _, err := tr.RequestVote(context.Background(), "z", raft.RequestVoteArgs{Term: 1}); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("err = %v, want ErrUnknownPeer", err)
	}
}

func TestRefusedConnectionReportsError(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	tr := newTestTransport(t, map[raft.NodeID]string{"b": addr})
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := tr.RequestVote(ctx, "b", raft.RequestVoteArgs{Term: 1}); err == nil {
		t.Fatal("dial to a closed listener succeeded")
	}
}

func TestUnexpectedResponseTypeRejected(t *testing.T) {
	// A rogue peer answers an AppendEntries request with a VoteResponse frame.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(conn net.Conn) {
				defer wg.Done()
				defer conn.Close()
				msg, err := ReadMessage(conn)
				if err != nil {
					return
				}
				payload, err := EncodeVoteResponse(raft.RequestVoteReply{Term: 2, VoteGranted: true})
				if err != nil {
					return
				}
				frame, err := Encode(Message{Version: ProtocolVersion, Type: MessageTypeVoteResponse, RequestID: msg.RequestID, Payload: payload})
				if err != nil {
					return
				}
				conn.Write(frame)
			}(conn)
		}
	}()

	tr := newTestTransport(t, map[raft.NodeID]string{"b": listener.Addr().String()})
	defer tr.Close()
	_, err = tr.AppendEntries(context.Background(), "b", raft.AppendEntriesArgs{Term: 1, LeaderID: "a"})
	if err == nil {
		t.Fatal("mismatched response type was accepted")
	}
	listener.Close()
	wg.Wait()
}

func TestServerIgnoresResponseFrames(t *testing.T) {
	handler := newTestHandler()
	handler.voteFn = func(args raft.RequestVoteArgs) raft.RequestVoteReply {
		return raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
	}
	ts := startTestServer(t, handler)
	defer ts.shutdown(t)

	conn, err := net.Dial("tcp", ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// A response-type frame is not a request; the server must ignore it.
	noisePayload, err := EncodeVoteResponse(raft.RequestVoteReply{Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	noiseFrame, err := Encode(Message{Version: ProtocolVersion, Type: MessageTypeVoteResponse, RequestID: 77, Payload: noisePayload})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(noiseFrame); err != nil {
		t.Fatal(err)
	}

	reqPayload, err := EncodeVoteRequest(raft.RequestVoteArgs{Term: 4, CandidateID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	reqFrame, err := Encode(Message{Version: ProtocolVersion, Type: MessageTypeVoteRequest, RequestID: 78, Payload: reqPayload})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(reqFrame); err != nil {
		t.Fatal(err)
	}

	msg, err := ReadMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if msg.RequestID != 78 {
		t.Fatalf("reply request id = %d, want 78", msg.RequestID)
	}
	if msg.Type != MessageTypeVoteResponse {
		t.Fatalf("reply type = %v, want VoteResponse", msg.Type)
	}
}
