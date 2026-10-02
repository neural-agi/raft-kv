package raft

import (
	"context"
	"testing"
	"time"
)

func TestHeartbeatAndStaleAppendEntries(t *testing.T) {
	transport := NewMemoryTransport()
	node, _ := nodeWithState(t, "b", []NodeID{"a", "c"}, PersistentState{CurrentTerm: 3}, transport)
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Stop(context.Background()) }()
	stale, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 2, LeaderID: "a"})
	if err != nil || stale.Success {
		t.Fatalf("stale heartbeat = %#v, %v", stale, err)
	}
	valid, err := node.AppendEntries(context.Background(), AppendEntriesArgs{Term: 3, LeaderID: "a"})
	if err != nil || !valid.Success {
		t.Fatalf("valid heartbeat = %#v, %v", valid, err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.LeaderID != "a" || state.Role != Follower {
		t.Fatalf("heartbeat state = %#v, %v", state, err)
	}
}

// TestHigherTermAppendEntriesReplyStepsDown delivers a higher-term reply for a
// request the event loop actually issued, and checks that the leader steps down
// and drops its replication state.
func TestHigherTermAppendEntriesReplyStepsDown(t *testing.T) {
	gated := newGatedTransport()
	arrived := gated.BlockAppend(1)
	storage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}}}}
	node := lifecycleNode(t, "a", []NodeID{"b"}, storage, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("b", lifecyclePeer(t, "b", []NodeID{"a"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, NewTestStateMachine(), nil, time.Second))
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	active := waitActiveReplication(t, node, "b")
	if err := node.enqueue(context.Background(), appendEntriesReplyEvent{target: "b", request: active, reply: AppendEntriesReply{Term: 3}}); err != nil {
		t.Fatal(err)
	}
	state, err := node.DebugState(context.Background())
	if err != nil || state.Role != Follower || state.Term != 3 {
		t.Fatalf("higher-term reply state = %#v, %v", state, err)
	}
	waitNoActiveReplication(t, node, "b")
}

func TestHeartbeatStopsAndFollowerCanTimeOut(t *testing.T) {
	transport := NewMemoryTransport()
	leader, _ := nodeWithState(t, "a", []NodeID{"b"}, PersistentState{}, transport)
	follower, _ := nodeWithState(t, "b", []NodeID{"a"}, PersistentState{}, transport)
	transport.Connect("a", leader)
	transport.Connect("b", follower)
	if err := leader.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := follower.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = leader.Stop(context.Background()) }()
	defer func() { _ = follower.Stop(context.Background()) }()
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	seenLeader := false
	for time.Now().Before(deadline) {
		state, _ := follower.DebugState(context.Background())
		if state.LeaderID == "a" && state.Term == 1 {
			seenLeader = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !seenLeader {
		t.Fatalf("follower never observed leader heartbeat")
	}
	transport.SetDropAppendEntries("b", true)
	if err := triggerElectionTimeoutForTest(follower, context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := follower.DebugState(context.Background())
	if err != nil || state.Role != Candidate || state.Term == 0 {
		t.Fatalf("follower did not start a new election after heartbeats stopped: %#v", state)
	}
}
