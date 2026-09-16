package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

type failingRestoreMachine struct {
	testStateMachine
	fail bool
}

func (m *failingRestoreMachine) Restore(context.Context, []byte) error {
	if m.fail {
		return errors.New("restore failed")
	}
	return nil
}

func TestInstallSnapshotRejectsStaleAndMalformed(t *testing.T) {
	storage := &testStorage{state: PersistentState{CurrentTerm: 3}}
	node := auditNode(t, "b", []NodeID{"a"}, storage, NewMemoryTransport())
	if err := node.enqueue(context.Background(), installSnapshotEvent{request: InstallSnapshotArgs{Term: 2, LeaderID: "a", LastIncludedIndex: 2, LastIncludedTerm: 1}, reply: make(chan InstallSnapshotReply, 1)}); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.Term != 3 || len(state.Log) != 0 {
		t.Fatalf("stale snapshot changed state: %#v %v", state, err)
	}
}

func TestInstallSnapshotRestoreFailureStopsNode(t *testing.T) {
	machine := &failingRestoreMachine{fail: true}
	ss := &memorySnapshotStorage{}
	node, err := NewNode(Config{ID: "b", Peers: []NodeID{"a"}, Storage: &testStorage{state: PersistentState{CurrentTerm: 1}}, SnapshotStorage: ss, Transport: NewMemoryTransport(), ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 10 * time.Millisecond, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err = node.InstallSnapshot(context.Background(), InstallSnapshotArgs{Term: 1, LeaderID: "a", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte{1, 'x'}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Role(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("role after restore failure = %v", err)
	}
}

func TestInstallSnapshotSnapshotStorageFailureDoesNotPublish(t *testing.T) {
	machine := NewTestStateMachine()
	ss := &memorySnapshotStorage{fail: true}
	node, err := NewNode(Config{ID: "b", Peers: []NodeID{"a"}, Storage: &testStorage{state: PersistentState{CurrentTerm: 1}}, SnapshotStorage: ss, Transport: NewMemoryTransport(), ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 10 * time.Millisecond, StateMachine: machine})
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
	_, err = node.InstallSnapshot(context.Background(), InstallSnapshotArgs{Term: 1, LeaderID: "a", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte{1, 'x'}})
	if err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.CommitIndex != 0 || state.SnapshotBoundary.Index != 0 {
		t.Fatalf("failed snapshot save published: %#v", state)
	}
}

func TestInstallSnapshotPersistsBeforePublishing(t *testing.T) {
	machine := NewTestStateMachine()
	ss := &memorySnapshotStorage{}
	storage := &failingStorage{state: PersistentState{CurrentTerm: 1}, fail: true}
	node, err := NewNode(Config{ID: "b", Peers: []NodeID{"a"}, Storage: storage, SnapshotStorage: ss, Transport: NewMemoryTransport(), ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 10 * time.Millisecond, StateMachine: machine})
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
	reply, err := node.InstallSnapshot(context.Background(), InstallSnapshotArgs{Term: 1, LeaderID: "a", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte{1, 'x'}})
	if err != nil || reply.Term != 1 {
		t.Fatalf("install reply = %#v %v", reply, err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.CommitIndex != 0 || len(state.Log) != 0 {
		t.Fatalf("failed install published state: %#v", state)
	}
}
