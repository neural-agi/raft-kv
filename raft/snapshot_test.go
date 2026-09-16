package raft

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestSnapshotRoundTripAndBinaryPayload(t *testing.T) {
	want := Snapshot{Version: SnapshotVersion, LastIncludedIndex: 7, LastIncludedTerm: 3, StateMachineData: []byte{0, 1, 2, 255}}
	data, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != want.Version || got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm || !bytes.Equal(got.StateMachineData, want.StateMachineData) {
		t.Fatalf("snapshot = %#v, want %#v", got, want)
	}
	data[24] = 99
	if got.StateMachineData[0] != 0 {
		t.Fatal("decoded snapshot aliases encoded data")
	}
}

func TestSnapshotRejectsMalformedData(t *testing.T) {
	valid, err := (Snapshot{Version: SnapshotVersion, LastIncludedIndex: 1, LastIncludedTerm: 1}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{nil, valid[:len(valid)-1], append(append([]byte(nil), valid...), 1)} {
		if _, err := DecodeSnapshot(data); err == nil {
			t.Fatalf("malformed snapshot accepted: %x", data)
		}
	}
	unsupported := append([]byte(nil), valid...)
	unsupported[2] = 2
	if _, err := DecodeSnapshot(unsupported); err == nil {
		t.Fatal("unsupported snapshot version accepted")
	}
}

type memorySnapshotStorage struct {
	snapshot Snapshot
	fail     bool
}

func (s *memorySnapshotStorage) LoadSnapshot(context.Context) (Snapshot, error) {
	if s.snapshot.LastIncludedIndex == 0 {
		return Snapshot{}, nil
	}
	return s.snapshot.Clone(), nil
}
func (s *memorySnapshotStorage) SaveSnapshot(_ context.Context, snapshot Snapshot) error {
	if s.fail {
		return errors.New("snapshot save failed")
	}
	s.snapshot = snapshot.Clone()
	return nil
}
