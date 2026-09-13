package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

func TestFailedRecoveryReplayCannotBeRetriedOnSameNode(t *testing.T) {
	machine := &countingFailureMachine{}
	store := &recoveryStorage{state: raft.PersistentState{
		CurrentTerm: 1,
		CommitIndex: 1,
		Log:         []raft.LogEntry{{Term: 1, Index: 1, Command: []byte("command")}},
	}}
	node, err := raft.NewNode(raft.Config{
		ID: "a", Peers: nil, Storage: store, Transport: raft.NewMemoryTransport(),
		ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second,
		HeartbeatInterval: 10 * time.Millisecond, StateMachine: machine,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err == nil {
		t.Fatal("failed replay unexpectedly initialized")
	}
	if machine.calls != 1 {
		t.Fatalf("replay calls = %d, want 1", machine.calls)
	}
	if err := node.Initialize(context.Background()); err == nil {
		t.Fatal("failed-replay node accepted a second initialization")
	}
	if machine.calls != 1 {
		t.Fatalf("replay was repeated after failure: %d calls", machine.calls)
	}
}

type countingFailureMachine struct{ calls int }

func (m *countingFailureMachine) Apply(context.Context, []byte) ([]byte, error) {
	m.calls++
	return nil, context.Canceled
}

type recoveryStorage struct{ state raft.PersistentState }

func (s *recoveryStorage) Load(context.Context) (raft.PersistentState, error) { return s.state, nil }
func (*recoveryStorage) Save(context.Context, raft.PersistentState) error     { return nil }
