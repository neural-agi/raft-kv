package raft

import (
	"errors"
	"testing"
)

func TestRoleString(t *testing.T) {
	for _, tc := range []struct {
		role Role
		want string
	}{
		{Follower, "follower"},
		{Candidate, "candidate"},
		{Leader, "leader"},
	} {
		if got := tc.role.String(); got != tc.want {
			t.Fatalf("Role(%d).String() = %q, want %q", tc.role, got, tc.want)
		}
	}
}

func TestLifecycleOrdering(t *testing.T) {
	if Created >= Initialized || Initialized >= Running || Running >= Stopped {
		t.Fatal("lifecycle states are not ordered")
	}
}

func TestLifecycleString(t *testing.T) {
	if got := Running.String(); got != "running" {
		t.Fatalf("Running.String() = %q", got)
	}
}

func TestLogEntryValidation(t *testing.T) {
	if err := (LogEntry{Term: 1, Index: 1}).Validate(); err != nil {
		t.Fatalf("valid entry rejected: %v", err)
	}
	if err := (LogEntry{Term: 0, Index: 1}).Validate(); err == nil {
		t.Fatal("zero-term entry accepted")
	}
	if !errors.Is(ErrNotLeader, ErrNotLeader) {
		t.Fatal("sentinel error is not inspectable")
	}
}

func TestRPCContractsExposeRequiredProtocolFields(t *testing.T) {
	vote := RequestVoteArgs{Term: 2, CandidateID: "node-a", LastLogIndex: 4, LastLogTerm: 2}
	if vote.Term != 2 || vote.CandidateID == "" || vote.LastLogIndex != 4 || vote.LastLogTerm != 2 {
		t.Fatalf("unexpected RequestVoteArgs: %#v", vote)
	}
	appendEntries := AppendEntriesArgs{Term: 2, LeaderID: "node-a", PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 2}
	if appendEntries.Term != 2 || appendEntries.LeaderID == "" || appendEntries.PrevLogIndex != 3 || appendEntries.PrevLogTerm != 1 || appendEntries.LeaderCommit != 2 {
		t.Fatalf("unexpected AppendEntriesArgs: %#v", appendEntries)
	}
	if (RequestVoteReply{Term: 2, VoteGranted: true}).Term != 2 || !(AppendEntriesReply{Term: 2, Success: true}).Success {
		t.Fatal("unexpected RPC reply fields")
	}
}

func TestPersistentStateContainsOnlyDurableRaftState(t *testing.T) {
	state := PersistentState{CurrentTerm: 3, VotedFor: "node-a", Log: []LogEntry{{Term: 3, Index: 1}}, CommitIndex: 1}
	if state.CurrentTerm != 3 || state.VotedFor != "node-a" || len(state.Log) != 1 || state.CommitIndex != 1 {
		t.Fatalf("unexpected persistent state: %#v", state)
	}
}
