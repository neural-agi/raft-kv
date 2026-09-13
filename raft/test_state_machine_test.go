package raft

import (
	"context"
	"errors"
	"sync"
)

type testStateMachine struct {
	mu       sync.Mutex
	commands [][]byte
	failAt   int
	attempts int
}

func NewTestStateMachine() *testStateMachine {
	return &testStateMachine{failAt: -1}
}

func (s *testStateMachine) SetFailAt(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAt = index
}

func (s *testStateMachine) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *testStateMachine) CommandCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commands)
}

func (s *testStateMachine) Commands() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	commands := make([][]byte, len(s.commands))
	for i := range s.commands {
		commands[i] = append([]byte(nil), s.commands[i]...)
	}
	return commands
}

func (s *testStateMachine) Apply(_ context.Context, command []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.failAt >= 0 && len(s.commands) == s.failAt {
		return nil, errors.New("injected apply failure")
	}
	s.commands = append(s.commands, append([]byte(nil), command...))
	return append([]byte(nil), command...), nil
}
