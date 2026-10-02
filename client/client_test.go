package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
)

// stubNode is a minimal NodeAPI: a fixed role/leader and a Propose that either
// applies to a store (leader) or returns a not-leader redirect.
type stubNode struct {
	mu       sync.Mutex
	role     raft.Role
	leader   raft.NodeID
	store    *kv.MemoryStore
	applyErr error
	proposed [][]byte
	block    chan struct{}
}

func (s *stubNode) Start(context.Context) error { return nil }
func (s *stubNode) Stop(context.Context) error  { return nil }

func (s *stubNode) Propose(ctx context.Context, command []byte) (raft.ProposalResult, error) {
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	s.proposed = append(s.proposed, append([]byte(nil), command...))
	role, leader, applyErr := s.role, s.leader, s.applyErr
	s.mu.Unlock()
	if applyErr != nil {
		return raft.ProposalResult{}, applyErr
	}
	if role != raft.Leader {
		err := &raft.Error{Code: raft.ErrCodeNotLeader}
		if leader != "" {
			err.LeaderID = leader
		}
		return raft.ProposalResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.store.Apply(ctx, command)
	if err != nil {
		return raft.ProposalResult{}, err
	}
	return raft.ProposalResult{Index: raft.LogIndex(len(s.proposed)), Term: 1, Result: result}, nil
}

func (s *stubNode) Role(context.Context) (raft.Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.role, nil
}

func (s *stubNode) Leader(context.Context) (raft.NodeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader, nil
}

func (s *stubNode) setLeader(leader raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role, s.leader = raft.Leader, leader
}

type testServer struct {
	server  *Server
	client  *Client
	addr    string
	cancel  context.CancelFunc
	done    chan error
	stopped sync.Once
}

func startTestServer(t *testing.T, node *stubNode) *testServer {
	t.Helper()
	server := NewServer(node, node.store)
	if err := server.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()

	ts := &testServer{server: server, addr: server.Addr().String(), cancel: cancel, done: done}
	t.Cleanup(ts.stop)
	ts.client = NewClient(map[raft.NodeID]string{"n1": ts.addr})
	return ts
}

func (ts *testServer) stop() {
	ts.stopped.Do(func() {
		ts.cancel()
		select {
		case <-ts.done:
		case <-time.After(5 * time.Second):
		}
	})
}

func newStub(role raft.Role, leader raft.NodeID) *stubNode {
	return &stubNode{role: role, leader: leader, store: kv.NewMemoryStore()}
}

func TestClientPutGetDeleteRoundTrip(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	ts := startTestServer(t, node)
	ctx := context.Background()

	if err := ts.client.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, found, err := ts.client.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "v" {
		t.Fatalf("Get = %q,%v; want v,true", value, found)
	}
	if err := ts.client.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found, err = ts.client.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if found {
		t.Fatal("key still present after delete")
	}
	if len(node.proposed) != 2 {
		t.Fatalf("proposed %d commands, want 2", len(node.proposed))
	}
}

func TestClientStatusReportsRoleAndLeader(t *testing.T) {
	node := newStub(raft.Follower, "n2")
	ts := startTestServer(t, node)

	role, leader, err := ts.client.Status(context.Background(), "n1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if role != raft.Follower {
		t.Fatalf("role = %v, want follower", role)
	}
	if leader != "n2" {
		t.Fatalf("leader = %q, want n2", leader)
	}
}

func TestClientRedirectsToLeader(t *testing.T) {
	follower := newStub(raft.Follower, "z-leader")
	followerTS := startTestServer(t, follower)

	leader := newStub(raft.Leader, "z-leader")
	leaderServer := NewServer(leader, leader.store)
	if err := leaderServer.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- leaderServer.Serve(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	// Member order is sorted, so "a-follower" is tried before "z-leader" and the
	// redirect path is exercised deterministically.
	client := NewClient(map[raft.NodeID]string{
		"a-follower": followerTS.addr,
		"z-leader":   leaderServer.Addr().String(),
	})
	if err := client.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put with redirect: %v", err)
	}
	if len(leader.proposed) != 1 {
		t.Fatalf("leader proposed %d commands, want 1", len(leader.proposed))
	}
	if len(follower.proposed) != 1 {
		t.Fatalf("follower should have received the proposal attempt once, got %d", len(follower.proposed))
	}
}

func TestClientFailsWhenNoLeaderReachable(t *testing.T) {
	follower := newStub(raft.Follower, "")
	followerTS := startTestServer(t, follower)

	client := NewClient(map[raft.NodeID]string{"n1": followerTS.addr})
	if err := client.Put(context.Background(), []byte("k"), []byte("v")); err == nil {
		t.Fatal("expected error when no leader is known")
	}
}

func TestClientPutTimesOutWhenProposeBlocks(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	node.block = make(chan struct{})
	ts := startTestServer(t, node)

	client := NewClient(map[raft.NodeID]string{"n1": ts.addr})
	client.SetTimeouts(500*time.Millisecond, 300*time.Millisecond, 2*time.Second)
	if err := client.Put(context.Background(), []byte("k"), []byte("v")); err == nil {
		t.Fatal("expected timeout error")
	}
	close(node.block)
}

func TestClientGetSkipsUnreachableMember(t *testing.T) {
	node := newStub(raft.Follower, "n1")
	ts := startTestServer(t, node)
	if _, err := node.store.Apply(context.Background(), mustEncode(t, []byte("k"), []byte("v"))); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	client := NewClient(map[raft.NodeID]string{
		"dead":  "127.0.0.1:1",
		"alive": ts.addr,
	})
	value, found, err := client.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "v" {
		t.Fatalf("Get = %q,%v; want v,true", value, found)
	}
}

func TestClientUnknownMember(t *testing.T) {
	client := NewClient(map[raft.NodeID]string{"n1": "127.0.0.1:1"})
	if _, _, err := client.Status(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for unknown member")
	}
}

func TestServerRejectsUnknownOp(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	ts := startTestServer(t, node)
	client := NewClient(map[raft.NodeID]string{"n1": ts.addr})
	_, err := client.roundTrip(context.Background(), "n1", Request{Op: Op("frobnicate")})
	if err != nil {
		t.Fatalf("roundTrip: %v", err)
	}
}

func TestServerRejectsEmptyKeyPut(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	ts := startTestServer(t, node)
	if err := ts.client.Put(context.Background(), nil, []byte("v")); err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestServerProposeErrorIsReported(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	node.applyErr = errors.New("propose unavailable")
	ts := startTestServer(t, node)
	if err := ts.client.Put(context.Background(), []byte("k"), []byte("v")); err == nil {
		t.Fatal("expected error from Propose failure")
	}
}

func TestServerListenErrorOnBusyAddress(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	ts := startTestServer(t, node)
	second := NewServer(node, node.store)
	if err := second.Listen(ts.addr); err == nil {
		t.Fatal("expected bind error for an in-use address")
	}
}

func TestServerServeBeforeListenFails(t *testing.T) {
	server := NewServer(newStub(raft.Leader, "n1"), kv.NewMemoryStore())
	if err := server.Serve(context.Background()); err == nil {
		t.Fatal("expected error when Serve runs before Listen")
	}
}

func TestServerGracefulShutdownDropsHandlers(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	node.block = make(chan struct{})
	server := NewServer(node, node.store)
	if err := server.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	addr := server.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()

	client := NewClient(map[raft.NodeID]string{"n1": addr})
	client.SetTimeouts(time.Second, time.Second, time.Second)
	putDone := make(chan error, 1)
	go func() { putDone <- client.Put(context.Background(), []byte("k"), []byte("v")) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v on graceful shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after context cancellation")
	}

	select {
	case err := <-putDone:
		if err == nil {
			t.Fatal("expected the in-flight Put to fail after shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight Put never returned")
	}
	close(node.block)
}

func TestServerHandlesMultipleRequestsOnOneConnection(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	ts := startTestServer(t, node)

	conn := dialRaw(t, ts.addr)
	defer conn.Close()
	for i := 0; i < 3; i++ {
		if err := writeFrame(conn, Request{Op: OpPut, Key: []byte("k"), Value: []byte("v")}); err != nil {
			t.Fatalf("write request %d: %v", i, err)
		}
		var resp Response
		if err := readFrame(newFrameReader(conn), &resp); err != nil {
			t.Fatalf("read response %d: %v", i, err)
		}
		if !resp.OK {
			t.Fatalf("response %d not ok: %s", i, resp.Error)
		}
	}
}

func TestFrameRejectsOversizedPayload(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	ts := startTestServer(t, node)
	resp, err := ts.client.roundTrip(context.Background(), "n1", Request{Op: OpPut, Key: make([]byte, maxFrameSize+1)})
	if err == nil && resp.OK {
		t.Fatal("expected oversized frame to be rejected")
	}
}

func TestWriteAndReadFrameRoundTrip(t *testing.T) {
	node := newStub(raft.Leader, "n1")
	ts := startTestServer(t, node)
	conn := dialRaw(t, ts.addr)
	defer conn.Close()

	if err := writeFrame(conn, Request{Op: OpStatus}); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := readFrame(newFrameReader(conn), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.Role != raft.Leader {
		t.Fatalf("status response = %+v", resp)
	}
}

func mustEncode(t *testing.T, key, value []byte) []byte {
	t.Helper()
	encoded, err := kv.Command{Type: kv.Put, Key: key, Value: value}.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return encoded
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}
