package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/kv"
)

func proposalNode(t *testing.T, id NodeID, peers []NodeID, machine StateMachine, transport Transport) *Node {
	t.Helper()
	node, err := NewNode(Config{
		ID: id, Peers: peers, Storage: &testStorage{}, Transport: transport,
		ElectionTimeoutMin: 200 * time.Millisecond, ElectionTimeoutMax: 400 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond, ApplyRetryInterval: 5 * time.Millisecond,
		StateMachine: machine,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Stop(context.Background()) })
	return node
}

func TestProposalRequiresLeaderAndReturnsAppliedResult(t *testing.T) {
	transport := NewMemoryTransport()
	leaderMachine := kv.NewMemoryStore()
	leader := proposalNode(t, "a", []NodeID{"b", "c"}, leaderMachine, transport)
	follower := proposalNode(t, "b", []NodeID{"a", "c"}, kv.NewMemoryStore(), transport)
	transport.Connect("a", leader)
	transport.Connect("b", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	encoded, err := (kv.Command{Type: kv.Put, Key: []byte("foo"), Value: []byte("bar")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	result, err := leader.Propose(context.Background(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	if result.Index != 1 || result.Term == 0 || string(result.Result) != "bar" {
		t.Fatalf("unexpected proposal result: %#v", result)
	}
	value, ok, err := leaderMachine.Get(context.Background(), []byte("foo"))
	if err != nil || !ok || string(value) != "bar" {
		t.Fatalf("leader store = %q, %v, %v", value, ok, err)
	}

	followerResult, err := follower.Propose(context.Background(), encoded)
	if err == nil || followerResult.Index != 0 || followerResult.Term != 0 || len(followerResult.Result) != 0 {
		t.Fatalf("follower proposal = %#v, %v", followerResult, err)
	}
	var raftErr *Error
	if !errors.As(err, &raftErr) || raftErr.Code != ErrCodeNotLeader {
		t.Fatalf("follower error = %v", err)
	}
}

func TestProposalResultUsesExactIndexAndConcurrentCommands(t *testing.T) {
	transport := NewMemoryTransport()
	machine := kv.NewMemoryStore()
	node := proposalNode(t, "a", nil, machine, transport)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	commands := make([][]byte, 4)
	for i := range commands {
		command, err := (kv.Command{Type: kv.Put, Key: []byte{byte('a' + i)}, Value: []byte{byte('0' + i)}}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		commands[i] = command
	}
	type outcome struct {
		result ProposalResult
		err    error
	}
	outcomes := make(chan outcome, len(commands))
	for _, command := range commands {
		go func(command []byte) {
			result, err := node.Propose(context.Background(), command)
			outcomes <- outcome{result: result, err: err}
		}(command)
	}
	seen := make(map[LogIndex]bool)
	for range commands {
		got := <-outcomes
		if got.err != nil {
			t.Fatal(got.err)
		}
		if seen[got.result.Index] || got.result.Index == 0 {
			t.Fatalf("duplicate proposal index: %#v", got.result)
		}
		seen[got.result.Index] = true
	}
	if len(seen) != len(commands) {
		t.Fatalf("indexes = %#v", seen)
	}
}

func TestCanceledProposalDoesNotRemoveAcceptedCommand(t *testing.T) {
	transport := NewMemoryTransport()
	node := proposalNode(t, "a", []NodeID{"b", "c"}, kv.NewMemoryStore(), transport)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.Propose(ctx, []byte("not accepted")); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled proposal error = %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := node.Propose(ctx, []byte("opaque"))
		resultCh <- err
	}()
	select {
	case <-time.After(time.Second):
	case err := <-resultCh:
		t.Fatalf("proposal unexpectedly completed before cancellation: %v", err)
	}
	cancel()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("proposal did not observe cancellation")
	}
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Log) != 1 || string(state.Log[0].Command) != "opaque" {
		t.Fatalf("canceled proposal was removed: %#v", state.Log)
	}
}

func TestPendingProposalCompletesOnShutdown(t *testing.T) {
	transport := NewMemoryTransport()
	node := proposalNode(t, "a", []NodeID{"b", "c"}, kv.NewMemoryStore(), transport)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	resultCh := make(chan error, 1)
	go func() {
		_, err := node.Propose(context.Background(), []byte("pending"))
		resultCh <- err
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		state, err := node.DebugState(context.Background())
		if err == nil && len(state.Log) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := node.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-resultCh:
		if !errors.Is(err, ErrProposalStopped) {
			t.Fatalf("shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending proposal did not terminate")
	}
}
