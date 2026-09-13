package raft

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"
)

type failingStorage struct {
	state PersistentState
	fail  bool
}

func (s *failingStorage) Load(context.Context) (PersistentState, error) {
	return clonePersistentState(s.state), nil
}

func (s *failingStorage) Save(_ context.Context, state PersistentState) error {
	if s.fail {
		return errors.New("injected storage failure")
	}
	s.state = clonePersistentState(state)
	return nil
}

func auditNode(t *testing.T, id NodeID, peers []NodeID, storage Storage, transport Transport) *Node {
	t.Helper()
	node, err := NewNode(Config{ID: id, Peers: peers, Storage: storage, Transport: transport, ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 150 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond, Random: rand.New(rand.NewSource(11)), StateMachine: NewTestStateMachine()})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Stop(context.Background()) })
	return node
}

func TestLogReplacementIsAtomicOnInvalidSuffix(t *testing.T) {
	log, err := newRaftLog([]LogEntry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
	if err != nil {
		t.Fatal(err)
	}
	before := log.snapshot()
	if err := log.replaceFrom(2, []LogEntry{{Term: 3, Index: 2}, {Term: 3, Index: 4}}); err == nil {
		t.Fatal("invalid replacement accepted")
	}
	if got := log.snapshot(); len(got) != len(before) || got[1].Term != before[1].Term {
		t.Fatalf("invalid replacement mutated log: %#v", got)
	}
}

func TestVoteLogFreshnessOrdering(t *testing.T) {
	transport := NewMemoryTransport()
	node := auditNode(t, "b", []NodeID{"a", "c"}, &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 2, Index: 1}}}}, transport)
	cases := []struct {
		name  string
		index LogIndex
		term  Term
		grant bool
	}{
		{"higher term shorter log", 99, 3, true},
		{"equal term shorter index", 0, 2, false},
		{"lower term larger index", 99, 1, false},
	}
	for _, tc := range cases {
		reply, err := node.RequestVote(context.Background(), RequestVoteArgs{Term: 3, CandidateID: NodeID(tc.name), LastLogIndex: tc.index, LastLogTerm: tc.term})
		if err != nil {
			t.Fatal(err)
		}
		if reply.VoteGranted != tc.grant {
			t.Fatalf("%s: granted=%v want %v", tc.name, reply.VoteGranted, tc.grant)
		}
		if tc.grant {
			break
		}
	}
}

func TestHigherTermPersistenceFailureDoesNotChangeState(t *testing.T) {
	storage := &failingStorage{state: PersistentState{CurrentTerm: 2}, fail: true}
	node := auditNode(t, "b", []NodeID{"a"}, storage, NewMemoryTransport())
	reply, err := node.RequestVote(context.Background(), RequestVoteArgs{Term: 3, CandidateID: "a"})
	if err != nil || reply.VoteGranted || reply.Term != 2 {
		t.Fatalf("failed durable term transition returned %#v, %v", reply, err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.Term != 2 || state.Role != Follower {
		t.Fatalf("state changed despite failed persistence: %#v, %v", state, err)
	}
}

func TestAppendEntriesPersistenceFailureDoesNotAcknowledgeLog(t *testing.T) {
	storage := &failingStorage{state: PersistentState{CurrentTerm: 1}, fail: true}
	node := auditNode(t, "b", []NodeID{"a"}, storage, NewMemoryTransport())
	reply, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte("x")}}})
	if err != nil || reply.Success {
		t.Fatalf("failed log persistence acknowledged: %#v, %v", reply, err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || len(state.Log) != 0 {
		t.Fatalf("log changed despite failed persistence: %#v, %v", state, err)
	}
}

func TestStopWithLateTransportCompletionDoesNotBlock(t *testing.T) {
	transport := NewMemoryTransport()
	node := auditNode(t, "a", []NodeID{"b"}, &testStorage{}, transport)
	node.enqueueBackground(voteReplyEvent{target: "b", electionTerm: 1, reply: RequestVoteReply{Term: 1}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := node.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	node.enqueueBackground(voteReplyEvent{target: "b", electionTerm: 1, reply: RequestVoteReply{Term: 1}})
}
