package raft

import (
	"context"
	"testing"
	"time"
)

func TestAppendEntriesExactConflictReplacement(t *testing.T) {
	transport := NewMemoryTransport()
	node := auditNode(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 3, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}, {Term: 2, Index: 2, Command: []byte("b")}, {Term: 2, Index: 3, Command: []byte("c")}}}}, transport)
	reply, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Term: 3, Index: 2, Command: []byte("d")}}})
	if err != nil || !reply.Success {
		t.Fatalf("append reply = %#v, %v", reply, err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || len(state.Log) != 2 || state.Log[1].Term != 3 || string(state.Log[1].Command) != "d" {
		t.Fatalf("replacement state = %#v, %v", state, err)
	}
}

func TestAppendEntriesMatchingSuffixDoesNotChangeLog(t *testing.T) {
	transport := NewMemoryTransport()
	storage := &testStorage{state: PersistentState{CurrentTerm: 2, Log: []LogEntry{{Term: 2, Index: 1, Command: []byte("a")}, {Term: 2, Index: 2, Command: []byte("b")}}}}
	node := auditNode(t, "f", []NodeID{"l"}, storage, transport)
	reply, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 1, PrevLogTerm: 2, Entries: []LogEntry{{Term: 2, Index: 2, Command: []byte("b")}}})
	if err != nil || !reply.Success {
		t.Fatalf("matching append reply = %#v, %v", reply, err)
	}
	if len(storage.saves) != 0 {
		t.Fatalf("matching suffix caused persistence: %d saves", len(storage.saves))
	}
}

func TestStaleAppendEntriesDoesNotRecordLeader(t *testing.T) {
	transport := NewMemoryTransport()
	node := auditNode(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 4}}, transport)
	reply, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 3, LeaderID: "old"})
	if err != nil || reply.Success {
		t.Fatalf("stale reply = %#v, %v", reply, err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.LeaderID != "" || state.Term != 4 {
		t.Fatalf("stale append changed state: %#v, %v", state, err)
	}
}

// TestStaleReplicationFailureDoesNotRollbackProgress exercises the real
// replication flow: the follower rejects until the leader walks nextIndex down,
// then accepts. A late failure carrying an already-completed request identity
// must not roll that progress back.
func TestStaleReplicationFailureDoesNotRollbackProgress(t *testing.T) {
	gated := newGatedTransport()
	arrived := gated.BlockAppend(1)
	storage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}}
	node := lifecycleNode(t, "l", []NodeID{"f"}, storage, gated, NewTestStateMachine(), nil, time.Second)
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("l", node)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	stale := waitActiveReplication(t, node, "f")
	staleArgs := gated.Append()
	gated.ReleaseAppend()
	waitFor(t, "replication to converge", func() bool {
		state, err := node.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 2 && state.NextIndex["f"] == 3
	})
	if err := node.enqueue(context.Background(), appendEntriesReplyEvent{target: "f", request: stale, entries: staleArgs, reply: AppendEntriesReply{Term: stale.term, Success: false}}); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.MatchIndex["f"] != 2 || state.NextIndex["f"] < 3 {
		t.Fatalf("stale failure rolled back progress: %#v", state)
	}
}

// TestCommitPrefixAfterCurrentTermEntry replicates an entry committed in the
// current term against a real follower and checks that commitment of that entry
// also commits the preceding entry from an older term.
func TestCommitPrefixAfterCurrentTermEntry(t *testing.T) {
	gated := newGatedTransport()
	storage := &testStorage{state: PersistentState{CurrentTerm: 2, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}, {Term: 3, Index: 2, Command: []byte("b")}}}}
	node := lifecycleNode(t, "l", []NodeID{"f", "g"}, storage, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("f", lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 2}}, gated, NewTestStateMachine(), nil, time.Second))
	gated.Connect("g", lifecyclePeer(t, "g", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 2}}, gated, NewTestStateMachine(), nil, time.Second))
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "current-term entry to commit", func() bool {
		state, err := node.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 2 && state.CommitIndex == 2
	})
}
