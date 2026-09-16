package integration

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/fault"
	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
)

type v7MemoryStorage struct {
	mu    sync.Mutex
	state raft.PersistentState
}

func (s *v7MemoryStorage) Load(context.Context) (raft.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneV7State(s.state), nil
}

func (s *v7MemoryStorage) Save(_ context.Context, state raft.PersistentState) error {
	s.mu.Lock()
	s.state = cloneV7State(state)
	s.mu.Unlock()
	return nil
}

type v7MemorySnapshotStorage struct {
	mu       sync.Mutex
	snapshot raft.Snapshot
}

func (s *v7MemorySnapshotStorage) LoadSnapshot(context.Context) (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot.Clone(), nil
}

func (s *v7MemorySnapshotStorage) SaveSnapshot(_ context.Context, snapshot raft.Snapshot) error {
	s.mu.Lock()
	s.snapshot = snapshot.Clone()
	s.mu.Unlock()
	return nil
}

func cloneV7State(state raft.PersistentState) raft.PersistentState {
	state.Log = append([]raft.LogEntry(nil), state.Log...)
	for i := range state.Log {
		state.Log[i].Command = append([]byte(nil), state.Log[i].Command...)
	}
	return state
}

type v7Member struct {
	node     *raft.Node
	storage  *v7MemoryStorage
	snapshot *v7MemorySnapshotStorage
	store    *kv.MemoryStore
}

func v7Put(t *testing.T, key, value string) []byte {
	t.Helper()
	command, err := (kv.Command{Type: kv.Put, Key: []byte(key), Value: []byte(value)}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func v7Fixture(t *testing.T) (raft.Snapshot, raft.LogEntry) {
	t.Helper()
	seed := kv.NewMemoryStore()
	if _, err := seed.Apply(context.Background(), v7Put(t, "one", "1")); err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Apply(context.Background(), v7Put(t, "two", "2")); err != nil {
		t.Fatal(err)
	}
	data, err := seed.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry := raft.LogEntry{Term: 1, Index: 3, Command: v7Put(t, "three", "3")}
	return raft.Snapshot{Version: raft.SnapshotVersion, LastIncludedIndex: 2, LastIncludedTerm: 1, StateMachineData: data}, entry
}

func newV7Member(t *testing.T, id raft.NodeID, peers []raft.NodeID, storage *v7MemoryStorage, snapshot *v7MemorySnapshotStorage, store *kv.MemoryStore, transport raft.Transport, currentState raft.PersistentState, electionMin, electionMax, heartbeat time.Duration) *v7Member {
	t.Helper()
	storage.state = cloneV7State(currentState)
	node, err := raft.NewNode(raft.Config{
		ID:                 id,
		Peers:              peers,
		Storage:            storage,
		SnapshotStorage:    snapshot,
		Transport:          transport,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		ApplyRetryInterval: heartbeat,
		Random:             rand.New(rand.NewSource(int64(id[0]))),
		StateMachine:       store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &v7Member{node: node, storage: storage, snapshot: snapshot, store: store}
}

func startV7Member(t *testing.T, member *v7Member) {
	t.Helper()
	if err := member.node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func waitV7(t *testing.T, node *raft.Node, check func(raft.DebugState) bool) raft.DebugState {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ticker.C:
			state, err := node.DebugState(context.Background())
			if err == nil && check(state) {
				return state
			}
		case <-timeout.C:
			t.Fatal("timed out waiting for V7 state")
		}
	}
}

func waitV7Pending(t *testing.T, network *fault.Network) []uint64 {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		if pending := network.Pending(); len(pending) != 0 {
			return pending
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("timed out waiting for delayed InstallSnapshot")
		}
	}
}

func startV7Cluster(t *testing.T) (map[raft.NodeID]*v7Member, *fault.Network, *fault.Controller) {
	t.Helper()
	snapshot, entry := v7Fixture(t)
	ids := []raft.NodeID{"a", "b", "c"}
	controller := fault.NewController(&fault.Trace{})
	network := fault.NewNetwork(controller)
	members := make(map[raft.NodeID]*v7Member, len(ids))
	leaderState := raft.PersistentState{CurrentTerm: 1, SnapshotBoundary: raft.LogBoundary{Index: 2, Term: 1}, CommitIndex: 3, Log: []raft.LogEntry{entry}}
	members["a"] = newV7Member(t, "a", []raft.NodeID{"b", "c"}, &v7MemoryStorage{state: leaderState}, &v7MemorySnapshotStorage{snapshot: snapshot}, kv.NewMemoryStore(), network.Transport("a"), leaderState, 20*time.Millisecond, 40*time.Millisecond, 5*time.Millisecond)
	members["b"] = newV7Member(t, "b", []raft.NodeID{"a", "c"}, &v7MemoryStorage{}, &v7MemorySnapshotStorage{}, kv.NewMemoryStore(), network.Transport("b"), raft.PersistentState{}, 2*time.Second, 4*time.Second, 20*time.Millisecond)
	members["c"] = newV7Member(t, "c", []raft.NodeID{"a", "b"}, &v7MemoryStorage{}, &v7MemorySnapshotStorage{}, kv.NewMemoryStore(), network.Transport("c"), raft.PersistentState{}, 2*time.Second, 4*time.Second, 20*time.Millisecond)
	for id, member := range members {
		network.Connect(id, member.node)
		startV7Member(t, member)
	}
	t.Cleanup(func() {
		for _, member := range members {
			_ = member.node.Stop(context.Background())
		}
	})
	return members, network, controller
}

func TestLeaderInstallsSnapshotAndRepairsPostSnapshotSuffix(t *testing.T) {
	members, network, controller := startV7Cluster(t)
	leader := members["a"]
	follower := members["b"]
	controller.Partition("a", "b")
	if err := follower.node.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitV7(t, leader.node, func(state raft.DebugState) bool { return state.Role == raft.Leader })
	controller.Heal("a", "b")
	restarted := newV7Member(t, "b", []raft.NodeID{"a", "c"}, follower.storage, follower.snapshot, kv.NewMemoryStore(), network.Transport("b"), raft.PersistentState{}, 2*time.Second, 4*time.Second, 20*time.Millisecond)
	network.Connect("b", restarted.node)
	startV7Member(t, restarted)
	t.Cleanup(func() { _ = restarted.node.Stop(context.Background()) })
	leaderState := waitV7(t, leader.node, func(state raft.DebugState) bool {
		return state.SnapshotBoundary.Index == 2 && state.NextIndex["b"] == 4 && state.MatchIndex["b"] == 3
	})
	followerState := waitV7(t, restarted.node, func(state raft.DebugState) bool {
		return state.SnapshotBoundary.Index == 2 && len(state.Log) == 1 && state.Log[0].Index == 3 && state.CommitIndex == 3 && state.LastApplied == 3
	})
	if leaderState.Log[0].Index != 3 {
		t.Fatalf("leader retained log = %#v", leaderState.Log)
	}
	if value, ok, err := restarted.store.Get(context.Background(), []byte("three")); err != nil || !ok || string(value) != "3" {
		t.Fatalf("rejoined follower state = %q, %v, %v", value, ok, err)
	}
	if value, ok, err := leader.store.Get(context.Background(), []byte("three")); err != nil || !ok || string(value) != "3" {
		t.Fatalf("leader state = %q, %v, %v", value, ok, err)
	}
	if followerState.SnapshotBoundary != leaderState.SnapshotBoundary {
		t.Fatalf("boundary mismatch: leader=%#v follower=%#v", leaderState.SnapshotBoundary, followerState.SnapshotBoundary)
	}
}

func TestDelayedInstallSnapshotRequiresExplicitRelease(t *testing.T) {
	members, network, controller := startV7Cluster(t)
	leader := members["a"]
	follower := members["b"]
	controller.Partition("a", "b")
	if err := follower.node.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitV7(t, leader.node, func(state raft.DebugState) bool { return state.Role == raft.Leader })
	controller.Set("a", "b", fault.InstallSnapshot, fault.Rule{Delay: true})
	controller.Heal("a", "b")
	restarted := newV7Member(t, "b", []raft.NodeID{"a", "c"}, follower.storage, follower.snapshot, kv.NewMemoryStore(), network.Transport("b"), raft.PersistentState{}, 2*time.Second, 4*time.Second, 20*time.Millisecond)
	network.Connect("b", restarted.node)
	startV7Member(t, restarted)
	t.Cleanup(func() { _ = restarted.node.Stop(context.Background()) })
	pending := waitV7Pending(t, network)
	before, err := restarted.node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before.SnapshotBoundary.Index != 0 {
		t.Fatalf("delayed snapshot was applied before release: %#v", before)
	}
	controller.Clear("a", "b", fault.InstallSnapshot)
	for _, id := range pending {
		if !network.Release(id) {
			t.Fatalf("failed to release delayed request %d", id)
		}
	}
	after := waitV7(t, restarted.node, func(state raft.DebugState) bool { return state.SnapshotBoundary.Index == 2 && state.LastApplied == 3 })
	if after.CommitIndex != 3 || after.Log[0].Index != 3 {
		t.Fatalf("released snapshot state = %#v", after)
	}
}

func TestProposalAfterCompactionUsesAbsoluteIndex(t *testing.T) {
	snapshot, _ := v7Fixture(t)
	storage := &v7MemoryStorage{state: raft.PersistentState{CurrentTerm: 1, SnapshotBoundary: raft.LogBoundary{Index: 2, Term: 1}, CommitIndex: 2}}
	store := kv.NewMemoryStore()
	node, err := raft.NewNode(raft.Config{
		ID:                 "a",
		Storage:            storage,
		SnapshotStorage:    &v7MemorySnapshotStorage{snapshot: snapshot},
		Transport:          raft.NewMemoryTransport(),
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
		ApplyRetryInterval: 5 * time.Millisecond,
		Random:             rand.New(rand.NewSource(1)),
		StateMachine:       store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())
	waitV7(t, node, func(state raft.DebugState) bool { return state.Role == raft.Leader })
	command := v7Put(t, "after", "snapshot")
	result, err := node.Propose(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.Index != 3 || string(result.Result) != "snapshot" {
		t.Fatalf("proposal after compaction = %#v", result)
	}
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.SnapshotBoundary.Index != 2 || len(state.Log) != 1 || state.Log[0].Index != 3 || state.CommitIndex != 3 || state.LastApplied != 3 {
		t.Fatalf("proposal state after compaction = %#v", state)
	}
}
