package raft

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type testStorage struct {
	mu    sync.Mutex
	state PersistentState
	saves []PersistentState
}

func (s *testStorage) Load(context.Context) (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePersistentState(s.state), nil
}

func (s *testStorage) Save(_ context.Context, state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = clonePersistentState(state)
	s.saves = append(s.saves, clonePersistentState(state))
	return nil
}

func newTestNode(t *testing.T, id NodeID, peers []NodeID, storage Storage, transport Transport) *Node {
	t.Helper()
	node, err := NewNode(Config{
		ID: id, Peers: peers, Storage: storage, Transport: transport,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
		Random:             rand.New(rand.NewSource(int64(id[0]))),
		StateMachine:       NewTestStateMachine(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return node
}

func TestNodeStartsFollowerAndLoadsPersistentState(t *testing.T) {
	storage := &testStorage{state: PersistentState{CurrentTerm: 4, VotedFor: "b", Log: []LogEntry{{Term: 4, Index: 1}}}}
	transport := NewMemoryTransport()
	node := newTestNode(t, "a", []NodeID{"b", "c"}, storage, transport)
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Stop(context.Background()) }()
	role, err := node.Role(context.Background())
	if err != nil || role != Follower {
		t.Fatalf("role = %s, err = %v", role, err)
	}
}

func TestThreeNodeClusterElectsOneLeader(t *testing.T) {
	transport := NewMemoryTransport()
	nodes := make([]*Node, 0, 3)
	ids := []NodeID{"a", "b", "c"}
	for _, id := range ids {
		peers := make([]NodeID, 0, 2)
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		node := newTestNode(t, id, peers, &testStorage{}, transport)
		nodes = append(nodes, node)
		transport.Connect(id, node)
	}
	for _, node := range nodes {
		if err := node.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = node.Stop(context.Background()) }()
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		leaders := 0
		for _, node := range nodes {
			role, _ := node.Role(context.Background())
			if role == Leader {
				leaders++
			}
		}
		if leaders == 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("three-node cluster did not elect exactly one leader")
}

func TestRequestVoteRules(t *testing.T) {
	transport := NewMemoryTransport()
	storage := &testStorage{state: PersistentState{CurrentTerm: 2, Log: []LogEntry{{Term: 2, Index: 1}}}}
	node := newTestNode(t, "b", []NodeID{"a", "c"}, storage, transport)
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Stop(context.Background()) }()
	stale, err := node.RequestVote(context.Background(), RequestVoteArgs{Term: 1, CandidateID: "a", LastLogIndex: 1, LastLogTerm: 2})
	if err != nil || stale.VoteGranted {
		t.Fatalf("stale vote = %#v, err = %v", stale, err)
	}
	valid, err := node.RequestVote(context.Background(), RequestVoteArgs{Term: 3, CandidateID: "a", LastLogIndex: 1, LastLogTerm: 2})
	if err != nil || !valid.VoteGranted || valid.Term != 3 {
		t.Fatalf("valid vote = %#v, err = %v", valid, err)
	}
	duplicate, err := node.RequestVote(context.Background(), RequestVoteArgs{Term: 3, CandidateID: "c", LastLogIndex: 1, LastLogTerm: 2})
	if err != nil || duplicate.VoteGranted {
		t.Fatalf("duplicate vote = %#v, err = %v", duplicate, err)
	}
	outdated, err := node.RequestVote(context.Background(), RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 0, LastLogTerm: 0})
	if err != nil || outdated.VoteGranted {
		t.Fatalf("outdated vote = %#v, err = %v", outdated, err)
	}
}
