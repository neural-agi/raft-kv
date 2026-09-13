package kv

import (
	"context"
	"errors"
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
