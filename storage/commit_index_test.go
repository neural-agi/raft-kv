package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRejectsCommitIndexBeyondLogEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.state")
	store, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeState(sampleState())
	if err != nil {
		t.Fatal(err)
	}
	// The commit index is after the fixed header, term, votedFor, and count.
	pos := headerSize + 8 + 4 + len(sampleState().VotedFor) + 4
	binary.BigEndian.PutUint64(data[pos:pos+8], uint64(len(sampleState().Log)+1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); err == nil {
		t.Fatal("invalid commit index accepted")
	}
}
