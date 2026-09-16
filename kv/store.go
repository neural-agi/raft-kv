package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
)

// Store is the local KV read boundary. GET is not a Raft command and is not
// guaranteed to be linearizable.
type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
}

// StateMachine combines local reads with committed-command application.
type StateMachine interface {
	Store
	Apply(ctx context.Context, command []byte) ([]byte, error)
}

// MemoryStore is a deterministic in-memory KV state machine. Apply is atomic per
// command and is expected to be called by one Raft event loop; the mutex also makes
// local test/internal reads safe while an application call is in progress.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

func (s *MemoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, errors.New("key must not be empty")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *MemoryStore) Snapshot(_ context.Context) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if uint64(len(keys)) > uint64(^uint32(0)) {
		return nil, errors.New("too many keys in snapshot")
	}
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], 1)
	binary.BigEndian.PutUint32(data[4:8], uint32(len(keys)))
	for _, key := range keys {
		value := s.data[key]
		if uint64(len(key)) > uint64(^uint32(0)) || uint64(len(value)) > uint64(^uint32(0)) {
			return nil, errors.New("snapshot field is too large")
		}
		part := make([]byte, 8+len(key)+len(value))
		binary.BigEndian.PutUint32(part[0:4], uint32(len(key)))
		binary.BigEndian.PutUint32(part[4:8], uint32(len(value)))
		copy(part[8:], key)
		copy(part[8+len(key):], value)
		data = append(data, part...)
	}
	return data, nil
}

func (s *MemoryStore) Restore(_ context.Context, data []byte) error {
	if len(data) < 8 || binary.BigEndian.Uint32(data[0:4]) != 1 {
		return errors.New("invalid KV snapshot")
	}
	count := binary.BigEndian.Uint32(data[4:8])
	pos := 8
	restored := make(map[string][]byte, count)
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 8 {
			return errors.New("truncated KV snapshot")
		}
		keyLen := uint64(binary.BigEndian.Uint32(data[pos : pos+4]))
		valueLen := uint64(binary.BigEndian.Uint32(data[pos+4 : pos+8]))
		pos += 8
		if keyLen == 0 || keyLen+valueLen > uint64(len(data)-pos) {
			return errors.New("invalid KV snapshot lengths")
		}
		key := append([]byte(nil), data[pos:pos+int(keyLen)]...)
		pos += int(keyLen)
		value := append([]byte(nil), data[pos:pos+int(valueLen)]...)
		pos += int(valueLen)
		restored[string(key)] = value
	}
	if pos != len(data) {
		return errors.New("trailing KV snapshot data")
	}
	s.mu.Lock()
	s.data = restored
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Apply(_ context.Context, encoded []byte) ([]byte, error) {
	command, err := Decode(encoded)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch command.Type {
	case Put:
		s.data[string(command.Key)] = append([]byte(nil), command.Value...)
		return append([]byte(nil), command.Value...), nil
	case Delete:
		_, existed := s.data[string(command.Key)]
		delete(s.data, string(command.Key))
		if existed {
			return []byte("deleted"), nil
		}
		return []byte("missing"), nil
	default:
		return nil, errors.New("unsupported command type")
	}
}
