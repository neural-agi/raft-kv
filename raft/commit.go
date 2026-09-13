package raft

import "context"

// publishCommit persists and publishes a monotonic commit-index advance. The
// complete log is included in the candidate state, so persisted commitment can
// never refer to an entry absent from the same durable replacement.
func (s *runtimeState) publishCommit(index LogIndex) bool {
	if index <= s.commitIndex || index > s.log.lastIndex() {
		return false
	}
	candidate := clonePersistentState(s.persistent)
	candidate.Log = s.log.snapshot()
	candidate.CommitIndex = index
	if err := s.config.Storage.Save(context.Background(), candidate); err != nil {
		return false
	}
	s.persistent = candidate
	s.commitIndex = index
	return true
}
