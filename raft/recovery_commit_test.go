package raft

import "testing"

func TestCommitPersistenceFailureDoesNotAdvanceCommit(t *testing.T) {
	storageStub := &failingStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("one")}}}, fail: true}
	node, err := NewNode(Config{ID: "a", Peers: nil, Storage: storageStub, Transport: NewMemoryTransport(), ElectionTimeoutMin: 1000000000, ElectionTimeoutMax: 2000000000, HeartbeatInterval: 10000000, StateMachine: NewTestStateMachine()})
	if err != nil {
		t.Fatal(err)
	}
	state, err := newRuntimeState(node, node.config, storageStub.state)
	if err != nil {
		t.Fatal(err)
	}
	if state.publishCommit(1) {
		t.Fatal("commit persistence failure reported success")
	}
	if state.commitIndex != 0 || state.persistent.CommitIndex != 0 {
		t.Fatalf("commit advanced despite persistence failure: %#v", state)
	}

}
