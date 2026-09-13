package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/neural-agi/raft-kv/raft"
)

func sampleState() raft.PersistentState {
	return raft.PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-b",
		Log: []raft.LogEntry{
			{Term: 3, Index: 1, Command: []byte{0, 1, 2, 255}},
			{Term: 7, Index: 2, Command: []byte{}},
		},
	}
}

func TestFileStorageFreshAndRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "raft.state")
	store, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := store.Load(context.Background())
	if err != nil || fresh.CurrentTerm != 0 || fresh.VotedFor != "" || len(fresh.Log) != 0 {
		t.Fatalf("fresh state = %#v, %v", fresh, err)
	}
	want := sampleState()
	if err := store.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertStateEqual(t, want, got)
}

func TestFileStorageSequentialSavesAndIsolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.state")
	store, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	state := sampleState()
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	state.Log[0].Command[0] = 99
	state.Log = append(state.Log, raft.LogEntry{Term: 8, Index: 3, Command: []byte("later")})
	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Log[0].Command[0] != 0 || len(got.Log) != 2 {
		t.Fatalf("save retained caller buffers: %#v", got)
	}
	got.Log[0].Command[0] = 88
	got2, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got2.Log[0].Command[0] != 0 {
		t.Fatal("load exposed mutable storage buffers")
	}
}

func TestFileStorageRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.state")
	store, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), sampleState()); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{"bad magic", []byte("bad")},
		{"bad version", []byte{'R', 'S', 9, 0}},
		{"truncated", []byte{'R', 'S', 1, 0, 0}},
		{"trailing", append(mustRead(t, path), 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Load(context.Background()); err == nil {
				t.Fatal("corrupt state accepted")
			}
		})
	}
}

func TestFileStorageAtomicFailedSavePreservesPreviousState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.state")
	store, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	first := sampleState()
	if err := store.Save(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.CurrentTerm = 8
	store.ops.rename = func(string, string) error { return errors.New("injected rename failure") }
	if err := store.Save(context.Background(), second); err == nil {
		t.Fatal("injected Save failure returned success")
	}
	store.ops.rename = os.Rename
	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertStateEqual(t, first, got)
}

func TestFileStorageSaveMissingDestinationFails(t *testing.T) {
	store, err := NewFileStorage(filepath.Join(t.TempDir(), "raft.state"))
	if err != nil {
		t.Fatal(err)
	}
	store.ops.mkdirAll = func(string, os.FileMode) error { return errors.New("injected mkdir failure") }
	if err := store.Save(context.Background(), sampleState()); err == nil {
		t.Fatal("Save unexpectedly succeeded")
	}
}

func TestEncodeRejectsInvalidLog(t *testing.T) {
	if _, err := encodeState(raft.PersistentState{Log: []raft.LogEntry{{Term: 0, Index: 1}}}); err == nil {
		t.Fatal("invalid log entry encoded")
	}
}

func assertStateEqual(t *testing.T, want, got raft.PersistentState) {
	t.Helper()
	if want.CurrentTerm != got.CurrentTerm || want.VotedFor != got.VotedFor || len(want.Log) != len(got.Log) {
		t.Fatalf("states differ: want=%#v got=%#v", want, got)
	}
	for i := range want.Log {
		if want.Log[i].Term != got.Log[i].Term || want.Log[i].Index != got.Log[i].Index || string(want.Log[i].Command) != string(got.Log[i].Command) {
			t.Fatalf("log entry %d differs: want=%#v got=%#v", i, want.Log[i], got.Log[i])
		}
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
