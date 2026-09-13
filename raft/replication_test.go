package raft

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

func nodeWithState(t *testing.T, id NodeID, peers []NodeID, state PersistentState, transport Transport) (*Node, *testStorage) {
	t.Helper()
	storage := &testStorage{state: state}
	node, err := NewNode(Config{ID: id, Peers: peers, Storage: storage, Transport: transport, ElectionTimeoutMin: 600 * time.Millisecond, ElectionTimeoutMax: 900 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond, Random: rand.New(rand.NewSource(7))})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return node, storage
}

func TestAppendEntriesFollowerRules(t *testing.T) {
	transport := NewMemoryTransport()
	node, storage := nodeWithState(t, "b", []NodeID{"a", "c"}, PersistentState{CurrentTerm: 2, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("old")}, {Term: 1, Index: 2, Command: []byte("conflict")}}}, transport)
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Stop(context.Background()) }()

	stale, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 1, LeaderID: "a"})
	if err != nil || stale.Success || stale.Term != 2 {
		t.Fatalf("stale append = %#v, %v", stale, err)
	}
	bad, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 2, LeaderID: "a", PrevLogIndex: 1, PrevLogTerm: 9})
	if err != nil || bad.Success {
		t.Fatalf("bad previous log append = %#v, %v", bad, err)
	}
	good, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 2, LeaderID: "a", PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Term: 2, Index: 2, Command: []byte("replacement")}, {Term: 2, Index: 3, Command: []byte("tail")}}, LeaderCommit: 10})
	if err != nil || !good.Success {
		t.Fatalf("good append = %#v, %v", good, err)
	}
	debug, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(debug.Log) != 3 || string(debug.Log[1].Command) != "replacement" || debug.CommitIndex != 3 || debug.LastApplied != 0 {
		t.Fatalf("state after append = %#v", debug)
	}
	if len(storage.saves) == 0 {
		t.Fatal("log change was not persisted")
	}
}

func TestLeaderReplicatesOpaqueEntryAndCommitsCurrentTerm(t *testing.T) {
	transport := NewMemoryTransport()
	leader, _ := nodeWithState(t, "a", []NodeID{"b", "c"}, PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("command")}}}, transport)
	followerB, _ := nodeWithState(t, "b", []NodeID{"a", "c"}, PersistentState{}, transport)
	followerC, _ := nodeWithState(t, "c", []NodeID{"a", "b"}, PersistentState{}, transport)
	transport.Connect("a", leader)
	transport.Connect("b", followerB)
	transport.Connect("c", followerC)
	transport.SetDropVote("a", true)
	transport.SetDropVote("b", true)
	transport.SetDropVote("c", true)
	for _, node := range []*Node{leader, followerB, followerC} {
		if err := node.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer func(n *Node) { _ = n.Stop(context.Background()) }(node)
	}
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := appendTestEntryForTest(leader, context.Background(), LogEntry{Term: 2, Index: 2, Command: []byte("current")}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		state, err := leader.DebugState(context.Background())
		if err == nil && state.Role == Leader && state.CommitIndex >= 2 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	state, _ := leader.DebugState(context.Background())
	t.Fatalf("leader did not commit replicated current-term entry: %#v", state)
}

func TestOldTermEntryAloneDoesNotCommit(t *testing.T) {
	transport := NewMemoryTransport()
	storage := &testStorage{state: PersistentState{CurrentTerm: 2, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("old")}}}}
	node, _ := nodeWithState(t, "a", []NodeID{"b", "c"}, storage.state, transport)
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Stop(context.Background()) }()
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.CommitIndex != 0 {
		t.Fatalf("old-term entry committed without current-term entry: %d", state.CommitIndex)
	}
}
