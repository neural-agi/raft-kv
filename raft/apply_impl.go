package raft

import "context"

// applyCommitted applies exactly the next unapplied committed prefix entry. It
// stops at the first error so later entries cannot bypass a failure.
func (s *runtimeState) applyCommitted() {
	for s.lastApplied < s.commitIndex {
		index := s.lastApplied + 1
		entry, ok := s.log.entry(index)
		if !ok {
			return
		}
		if _, err := s.config.StateMachine.Apply(context.Background(), entry.Command); err != nil {
			return
		}
		s.lastApplied = index
	}
}
