package raft

import "testing"

// AssertStateInvariants provides diagnostic checks reusable by deterministic
// failure scenarios. It intentionally checks local evidence, not a global proof.
func AssertStateInvariants(t testing.TB, states map[NodeID]DebugState) {
	t.Helper()
	leaders := map[Term]NodeID{}
	for id, state := range states {
		lastIndex := state.SnapshotBoundary.Index
		if len(state.Log) > 0 {
			lastIndex = state.Log[len(state.Log)-1].Index
			if state.Log[0].Index != state.SnapshotBoundary.Index+1 {
				t.Fatalf("node %s: retained log does not follow snapshot boundary: boundary=%d first=%d", id, state.SnapshotBoundary.Index, state.Log[0].Index)
			}
		}
		if state.CommitIndex < state.SnapshotBoundary.Index || state.CommitIndex > lastIndex {
			t.Fatalf("node %s: commitIndex=%d outside boundary/log [%d,%d]", id, state.CommitIndex, state.SnapshotBoundary.Index, lastIndex)
		}
		if state.LastApplied > state.CommitIndex {
			t.Fatalf("node %s: lastApplied=%d exceeds commitIndex=%d (term=%d role=%s log=%d)", id, state.LastApplied, state.CommitIndex, state.Term, state.Role, len(state.Log))
		}
		if state.Role == Leader {
			if prior, ok := leaders[state.Term]; ok && prior != id {
				t.Fatalf("election safety violation: leaders %s and %s in term %d", prior, id, state.Term)
			}
			leaders[state.Term] = id
		}
	}
	for id, left := range states {
		for other, right := range states {
			if id >= other {
				continue
			}
			limit := left.CommitIndex
			if right.CommitIndex < limit {
				limit = right.CommitIndex
			}
			for index := LogIndex(1); index <= limit; index++ {
				le, lok := entryAt(left.Log, index)
				re, rok := entryAt(right.Log, index)
				if lok && rok && (le.Term != re.Term || string(le.Command) != string(re.Command)) {
					t.Fatalf("log matching violation: %s/%s index=%d left=%#v right=%#v", id, other, index, le, re)
				}
			}
		}
	}
}

func entryAt(entries []LogEntry, index LogIndex) (LogEntry, bool) {
	for _, entry := range entries {
		if entry.Index == index {
			return entry, true
		}
	}
	return LogEntry{}, false
}
