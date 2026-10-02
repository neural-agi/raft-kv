package transport

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
)

// memStorage is an in-memory raft.Storage for integration tests. Save and Load
// clone, honoring the adapter contract not to retain caller-owned slices.
type memStorage struct {
	mu    sync.Mutex
	state raft.PersistentState
}

func (m *memStorage) Load(context.Context) (raft.PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clonePersistentState(m.state), nil
}

func (m *memStorage) Save(_ context.Context, state raft.PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = clonePersistentState(state)
	return nil
}

func clonePersistentState(state raft.PersistentState) raft.PersistentState {
	state.Log = append([]raft.LogEntry(nil), state.Log...)
	for i := range state.Log {
		state.Log[i].Command = append([]byte(nil), state.Log[i].Command...)
	}
	return state
}

// memSnapshots is an in-memory raft.SnapshotStorage for integration tests.
type memSnapshots struct {
	mu       sync.Mutex
	snapshot raft.Snapshot
	ok       bool
}

func (s *memSnapshots) LoadSnapshot(context.Context) (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ok {
		return raft.Snapshot{}, nil
	}
	clone := s.snapshot
	clone.StateMachineData = append([]byte(nil), s.snapshot.StateMachineData...)
	return clone, nil
}

func (s *memSnapshots) SaveSnapshot(_ context.Context, snapshot raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snapshot
	s.snapshot.StateMachineData = append([]byte(nil), snapshot.StateMachineData...)
	s.ok = true
	return nil
}

func TestRaftNodesReplicateOverTCP(t *testing.T) {
	const (
		nodeA = raft.NodeID("a")
		nodeB = raft.NodeID("b")
	)

	// Bind both listeners first so their addresses can be wired into each
	// peer's transport before the nodes exist (the receiver methods are the
	// RPC handlers).
	serverA := NewServer(nil)
	if err := serverA.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	serverB := NewServer(nil)
	if err := serverB.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	transportA := newTestTransport(t, map[raft.NodeID]string{nodeB: serverB.Addr().String()})
	transportB := newTestTransport(t, map[raft.NodeID]string{nodeA: serverA.Addr().String()})

	var cleanupOnce sync.Once
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	cancelA, cancelB := context.CancelFunc(nil), context.CancelFunc(nil)
	nodeX, nodeY := (*raft.Node)(nil), (*raft.Node)(nil)

	cleanup := func() {
		cleanupOnce.Do(func() {
			if cancelA != nil {
				cancelA()
				<-doneA
			}
			if cancelB != nil {
				cancelB()
				<-doneB
			}
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if nodeX != nil {
				_ = nodeX.Stop(stopCtx)
			}
			if nodeY != nil {
				_ = nodeY.Stop(stopCtx)
			}
			transportA.Close()
			transportB.Close()
		})
	}
	t.Cleanup(cleanup)

	storeA, storeB := kv.NewMemoryStore(), kv.NewMemoryStore()
	storageA, storageB := &memStorage{}, &memStorage{}
	snapA, snapB := &memSnapshots{}, &memSnapshots{}

	var err error
	nodeX, err = raft.NewNode(raft.Config{
		ID: nodeA, Peers: []raft.NodeID{nodeA, nodeB},
		Storage: storageA, Transport: transportA,
		ElectionTimeoutMin: 300 * time.Millisecond, ElectionTimeoutMax: 500 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond, StateMachine: storeA, SnapshotStorage: snapA,
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeY, err = raft.NewNode(raft.Config{
		ID: nodeB, Peers: []raft.NodeID{nodeA, nodeB},
		Storage: storageB, Transport: transportB,
		ElectionTimeoutMin: 300 * time.Millisecond, ElectionTimeoutMax: 500 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond, StateMachine: storeB, SnapshotStorage: snapB,
	})
	if err != nil {
		t.Fatal(err)
	}

	serverA.SetHandler(nodeX)
	serverB.SetHandler(nodeY)

	if err := nodeX.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize %s: %v", nodeA, err)
	}
	if err := nodeY.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize %s: %v", nodeB, err)
	}
	if err := nodeX.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := nodeY.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	serveCtxA, cancel := context.WithCancel(context.Background())
	serveCtxB, cancel2 := context.WithCancel(context.Background())
	cancelA, cancelB = cancel, cancel2
	go func() { doneA <- serverA.Serve(serveCtxA) }()
	go func() { doneB <- serverB.Serve(serveCtxB) }()

	// A leader must emerge over TCP via RequestVote.
	leader := waitForLeader(t, map[raft.NodeID]*raft.Node{nodeA: nodeX, nodeB: nodeY})
	leaderNode := map[raft.NodeID]*raft.Node{nodeA: nodeX, nodeB: nodeY}[leader]

	// The leader replicates a committed PUT to the follower over the socket.
	command, err := (kv.Command{Type: kv.Put, Key: []byte("hello"), Value: []byte("world")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	proposeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := leaderNode.Propose(proposeCtx, command); err != nil {
		t.Fatalf("propose on leader %s: %v", leader, err)
	}

	// Both state machines must apply the committed entry.
	for _, store := range []*kv.MemoryStore{storeA, storeB} {
		waitForValue(t, store, []byte("hello"), []byte("world"))
	}

	// A second PUT over the same long-lived connections stays healthy.
	command2, err := (kv.Command{Type: kv.Put, Key: []byte("again"), Value: []byte("value")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	proposeCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := leaderNode.Propose(proposeCtx, command2); err != nil {
		t.Fatalf("second propose on leader %s: %v", leader, err)
	}
	for _, store := range []*kv.MemoryStore{storeA, storeB} {
		waitForValue(t, store, []byte("again"), []byte("value"))
	}
}

func waitForLeader(t *testing.T, nodes map[raft.NodeID]*raft.Node) raft.NodeID {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for id, node := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			role, err := node.Role(ctx)
			cancel()
			if err == nil && role == raft.Leader {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader was elected over TCP")
	return ""
}

func waitForValue(t *testing.T, store *kv.MemoryStore, key, want []byte) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, ok, err := store.Get(context.Background(), key)
		if err == nil && ok && bytes.Equal(got, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, ok, _ := store.Get(context.Background(), key)
	t.Fatalf("key %q not applied: ok=%v value=%q", key, ok, got)
}
