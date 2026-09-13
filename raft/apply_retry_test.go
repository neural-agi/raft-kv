package raft

import (
	"context"
	"testing"
	"time"
)

func TestFailedApplyRetriesWithoutAnotherRaftEvent(t *testing.T) {
	transport := NewMemoryTransport()
	machine := NewTestStateMachine()
	machine.SetFailAt(0)
	node, err := NewNode(Config{ID: "a", Peers: []NodeID{"b", "c"}, Storage: &testStorage{}, Transport: transport, ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 20 * time.Millisecond, ApplyRetryInterval: 2 * time.Millisecond, StateMachine: machine})
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
	if err := enqueueApplyTestState(node, []LogEntry{{Term: 1, Index: 1, Command: []byte("one")}}, 0, 1); err != nil {
		t.Fatal(err)
	}
	initialAttempts := machine.Attempts()
	machine.SetFailAt(-1)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		state, err := node.DebugState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if state.LastApplied == 1 {
			if machine.Attempts() <= initialAttempts {
				t.Fatal("lastApplied advanced without a retry attempt")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("failed Apply was not retried while otherwise idle")
}

func TestFailedEntryBlocksLaterCommittedEntry(t *testing.T) {
	transport := NewMemoryTransport()
	machine := NewTestStateMachine()
	machine.SetFailAt(1)
	node, err := NewNode(Config{ID: "a", Peers: []NodeID{"b", "c"}, Storage: &testStorage{}, Transport: transport, ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 20 * time.Millisecond, ApplyRetryInterval: time.Hour, StateMachine: machine})
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
	if err := enqueueApplyTestState(node, []LogEntry{{Term: 1, Index: 1, Command: []byte("one")}, {Term: 1, Index: 2, Command: []byte("two")}, {Term: 1, Index: 3, Command: []byte("three")}}, 0, 3); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.LastApplied != 1 || machine.CommandCount() != 1 {
		t.Fatalf("later entry bypassed failed entry: %#v commands=%d", state, machine.CommandCount())
	}
	machine.SetFailAt(-1)
	if err := node.enqueue(context.Background(), applyCommittedEvent{}); err != nil {
		t.Fatal(err)
	}
	state, err = node.DebugState(context.Background())
	if err != nil || state.LastApplied != 3 || machine.CommandCount() != 3 {
		t.Fatalf("retry did not finish prefix: %#v commands=%d", state, machine.CommandCount())
	}
}
