package raft_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/raft"
	"github.com/neural-agi/raft-kv/storage"
)

func TestRaftInitializesWithFilesystemStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.state")
	fileStore, err := storage.NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.NewNode(raft.Config{
		ID: "a", Peers: nil, Storage: fileStore, Transport: raft.NewMemoryTransport(),
		ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second,
		HeartbeatInterval: 10 * time.Millisecond, StateMachine: raft.NewTestStateMachine(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := fileStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentTerm != 0 || state.VotedFor != "" || len(state.Log) != 0 {
		t.Fatalf("fresh filesystem state = %#v", state)
	}
}

func TestFilesystemStoragePreservesRaftPersistentState(t *testing.T) {
	fileStore, err := storage.NewFileStorage(filepath.Join(t.TempDir(), "raft.state"))
	if err != nil {
		t.Fatal(err)
	}
	want := raft.PersistentState{
		CurrentTerm: 4,
		VotedFor:    "a",
		Log:         []raft.LogEntry{{Term: 4, Index: 1, Command: []byte{0, 255}}},
		CommitIndex: 1,
	}
	if err := fileStore.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := fileStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentTerm != want.CurrentTerm || got.VotedFor != want.VotedFor || got.CommitIndex != want.CommitIndex || len(got.Log) != 1 || string(got.Log[0].Command) != string(want.Log[0].Command) {
		t.Fatalf("persisted state = %#v", got)
	}
}
