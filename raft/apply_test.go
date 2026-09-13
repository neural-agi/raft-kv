package raft

import (
	"context"
	"testing"
)

func TestCommittedEntriesApplyInOrderExactlyOnce(t *testing.T) {
	transport := NewMemoryTransport()
	machine := NewTestStateMachine()
	node, err := NewNode(Config{ID: "a", Peers: []NodeID{"b", "c"}, Storage: &testStorage{}, Transport: transport, ElectionTimeoutMin: 100000000, ElectionTimeoutMax: 200000000, HeartbeatInterval: 10000000, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Stop(context.Background()) }()
	entries := []LogEntry{{Term: 1, Index: 1, Command: []byte("one")}, {Term: 1, Index: 2, Command: []byte("two")}}
	if err := enqueueApplyTestState(node, entries[:2], 0, 2); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.LastApplied != 2 || state.CommitIndex != 2 {
		t.Fatalf("applied state = %#v, %v", state, err)
	}
	commands := machine.Commands()
	if machine.Attempts() != 2 || machine.CommandCount() != 2 || string(commands[0]) != "one" || string(commands[1]) != "two" {
		t.Fatalf("commands = %#v attempts=%d", commands, machine.Attempts())
	}
	if err := node.enqueue(context.Background(), applyCommittedEvent{}); err != nil {
		t.Fatal(err)
	}
	if machine.Attempts() != 2 {
		t.Fatalf("unrelated apply re-applied entries: %d", machine.Attempts())
	}
}

func TestApplyFailureLeavesEntryEligibleForRetry(t *testing.T) {
	transport := NewMemoryTransport()
	machine := NewTestStateMachine()
	machine.SetFailAt(1)
	node, err := NewNode(Config{ID: "a", Peers: []NodeID{"b", "c"}, Storage: &testStorage{}, Transport: transport, ElectionTimeoutMin: 100000000, ElectionTimeoutMax: 200000000, HeartbeatInterval: 10000000, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Stop(context.Background()) }()
	if err := enqueueApplyTestState(node, []LogEntry{{Term: 1, Index: 1, Command: []byte("one")}, {Term: 1, Index: 2, Command: []byte("two")}}, 0, 2); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.LastApplied != 1 || state.CommitIndex != 2 {
		t.Fatalf("failed apply state = %#v, %v", state, err)
	}
	machine.SetFailAt(-1)
	if err := node.enqueue(context.Background(), applyCommittedEvent{}); err != nil {
		t.Fatal(err)
	}
	state, err = node.DebugState(context.Background())
	if err != nil || state.LastApplied != 2 {
		t.Fatalf("retry state = %#v, %v", state, err)
	}
}

type applyTestStateEvent struct {
	entries []LogEntry
	commit  LogIndex
	reply   chan error
}

func (e applyTestStateEvent) handle(s *runtimeState) bool {
	candidate := &raftLog{entries: s.log.snapshot()}
	if err := candidate.append(e.entries...); err != nil {
		e.reply <- err
		return false
	}
	s.log = candidate
	s.commitIndex = e.commit
	s.applyCommitted()
	e.reply <- nil
	return false
}

type applyCommittedEvent struct{}

func (applyCommittedEvent) handle(s *runtimeState) bool {
	s.applyCommitted()
	return false
}

func enqueueApplyTestState(node *Node, entries []LogEntry, lastApplied, commit LogIndex) error {
	reply := make(chan error, 1)
	if err := node.enqueue(context.Background(), applyTestStateEvent{entries: entries, commit: commit, reply: reply}); err != nil {
		return err
	}
	return <-reply
}
