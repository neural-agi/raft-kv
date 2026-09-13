package raft_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
	"github.com/neural-agi/raft-kv/storage"
)

func TestInitializeReplaysOnlyPersistedCommittedPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.state")
	fileStore, err := storage.NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	putX, _ := (kv.Command{Type: kv.Put, Key: []byte("x"), Value: []byte("1")}).Encode()
	putY, _ := (kv.Command{Type: kv.Put, Key: []byte("y"), Value: []byte("2")}).Encode()
	state := raft.PersistentState{CurrentTerm: 4, VotedFor: "b", CommitIndex: 1, Log: []raft.LogEntry{
		{Term: 3, Index: 1, Command: putX},
		{Term: 4, Index: 2, Command: putY},
	}}
	if err := fileStore.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	machine := kv.NewMemoryStore()
	node, err := raft.NewNode(raft.Config{ID: "a", Peers: nil, Storage: fileStore, Transport: raft.NewMemoryTransport(), ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 10 * time.Millisecond, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if value, ok, _ := machine.Get(context.Background(), []byte("x")); !ok || string(value) != "1" {
		t.Fatalf("committed prefix was not replayed: %q %v", value, ok)
	}
	if _, ok, _ := machine.Get(context.Background(), []byte("y")); ok {
		t.Fatal("uncommitted suffix was replayed")
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stateView, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stateView.CommitIndex != 1 || stateView.LastApplied != 1 || stateView.Role != raft.Follower {
		t.Fatalf("recovered volatile state = %#v", stateView)
	}
	if err := node.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeFailsWhenCommittedReplayFails(t *testing.T) {
	storageStub := &failingStorage{state: raft.PersistentState{CurrentTerm: 1, CommitIndex: 1, Log: []raft.LogEntry{{Term: 1, Index: 1, Command: []byte("bad")}}}}
	machine := raft.NewTestStateMachine()
	machine.SetFailAt(0)
	node, err := raft.NewNode(raft.Config{ID: "a", Peers: nil, Storage: storageStub, Transport: raft.NewMemoryTransport(), ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 10 * time.Millisecond, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err == nil {
		t.Fatal("failed committed replay allowed initialization")
	}
}

type failingStorage struct {
	state raft.PersistentState
}

func (s *failingStorage) Load(context.Context) (raft.PersistentState, error) {
	return s.state, nil
}

func (*failingStorage) Save(context.Context, raft.PersistentState) error { return nil }
