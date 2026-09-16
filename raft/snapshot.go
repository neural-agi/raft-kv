package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const SnapshotVersion byte = 1

// Snapshot is an opaque state-machine image at a committed log boundary.
type Snapshot struct {
	Version           byte
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	StateMachineData  []byte
}

func (s Snapshot) Validate() error {
	if s.Version != SnapshotVersion {
		return fmt.Errorf("unsupported snapshot version %d", s.Version)
	}
	if s.LastIncludedIndex == 0 {
		return errors.New("snapshot index must be greater than zero")
	}
	if s.LastIncludedTerm == 0 {
		return errors.New("snapshot term must be greater than zero")
	}
	if uint64(len(s.StateMachineData)) > uint64(^uint32(0)) {
		return errors.New("snapshot payload is too large")
	}
	return nil
}

func (s Snapshot) Clone() Snapshot {
	s.StateMachineData = append([]byte(nil), s.StateMachineData...)
	return s
}

// Encode is magic(2) | version(1) | reserved(1) | index(8) | term(8) |
// payloadLength(4) | payload. All integers are big-endian.
func (s Snapshot) Encode() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	data := make([]byte, 24+len(s.StateMachineData))
	data[0], data[1], data[2], data[3] = 'R', 'K', s.Version, 0
	binary.BigEndian.PutUint64(data[4:12], uint64(s.LastIncludedIndex))
	binary.BigEndian.PutUint64(data[12:20], uint64(s.LastIncludedTerm))
	binary.BigEndian.PutUint32(data[20:24], uint32(len(s.StateMachineData)))
	copy(data[24:], s.StateMachineData)
	return data, nil
}

func DecodeSnapshot(data []byte) (Snapshot, error) {
	if len(data) < 24 || data[0] != 'R' || data[1] != 'K' || data[3] != 0 {
		return Snapshot{}, errors.New("invalid snapshot format")
	}
	if data[2] != SnapshotVersion {
		return Snapshot{}, fmt.Errorf("unsupported snapshot version %d", data[2])
	}
	payloadLength := uint64(binary.BigEndian.Uint32(data[20:24]))
	if payloadLength != uint64(len(data)-24) {
		return Snapshot{}, errors.New("snapshot payload length does not match header")
	}
	snapshot := Snapshot{
		Version:           data[2],
		LastIncludedIndex: LogIndex(binary.BigEndian.Uint64(data[4:12])),
		LastIncludedTerm:  Term(binary.BigEndian.Uint64(data[12:20])),
		StateMachineData:  append([]byte(nil), data[24:]...),
	}
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}
