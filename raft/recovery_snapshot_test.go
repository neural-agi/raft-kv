package raft

import (
	"context"
	"testing"
	"time"
)

func TestInitializeRestoresSnapshotAndReplaysOnlyPostSnapshotEntries(t *testing.T) {
	machine := NewTestStateMachine()
	snapshotData := []byte{1, 'o'}
	storage := &testStorage{state: PersistentState{
		CurrentTerm:      3,
		SnapshotBoundary: LogBoundary{Index: 2, Term: 1},
		CommitIndex:      3,
		Log:              []LogEntry{{Term: 2, Index: 3, Command: []byte("three")}, {Term: 3, Index: 4, Command: []byte("four")}},
	}}
	snapshots := &memorySnapshotStorage{snapshot: Snapshot{Version: SnapshotVersion, LastIncludedIndex: 2, LastIncludedTerm: 1, StateMachineData: snapshotData}}
	node, err := NewNode(Config{ID: "a", Storage: storage, SnapshotStorage: snapshots, Transport: NewMemoryTransport(), ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 10 * time.Millisecond, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	commands := machine.Commands()
	if len(commands) != 2 || string(commands[0]) != "o" || string(commands[1]) != "three" {
		t.Fatalf("restored and replayed commands = %#v", commands)
	}
}
