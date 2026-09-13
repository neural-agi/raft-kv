package raft

import (
	"context"
	"testing"
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

func TestStaleReplicationFailureDoesNotRollbackProgress(t *testing.T) {
	transport := NewMemoryTransport()
	node := auditNode(t, "l", []NodeID{"f"}, &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}}, transport)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.enqueue(context.Background(), appendEntriesReplyEvent{target: "f", leaderTerm: 2, request: AppendEntriesArgs{Term: 2, PrevLogIndex: 0, Entries: []LogEntry{{Term: 2, Index: 1}}}, reply: AppendEntriesReply{Term: 2, Success: true}}); err != nil {
		t.Fatal(err)
	}
	if err := node.enqueue(context.Background(), appendEntriesReplyEvent{target: "f", leaderTerm: 2, request: AppendEntriesArgs{Term: 2, PrevLogIndex: 0}, reply: AppendEntriesReply{Term: 2, Success: false}}); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.MatchIndex["f"] != 1 || state.NextIndex["f"] < 2 {
		t.Fatalf("stale failure rolled back progress: %#v", state)
	}
}

func TestCommitPrefixAfterCurrentTermEntry(t *testing.T) {
	transport := NewMemoryTransport()
	storage := &testStorage{state: PersistentState{CurrentTerm: 2, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}, {Term: 3, Index: 2, Command: []byte("b")}}}}
	node := auditNode(t, "l", []NodeID{"f", "g"}, storage, transport)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.enqueue(context.Background(), appendEntriesReplyEvent{target: "f", leaderTerm: 3, request: AppendEntriesArgs{Term: 3, PrevLogIndex: 1, Entries: []LogEntry{{Term: 3, Index: 2}}}, reply: AppendEntriesReply{Term: 3, Success: true}}); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.CommitIndex != 2 {
		t.Fatalf("current-term entry did not commit prefix: %#v, %v", state, err)
	}
}
