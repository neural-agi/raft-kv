package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/kv"
)

func TestHigherTermStepDownTerminatesPendingProposal(t *testing.T) {
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
	if _, err := node.AppendEntries(context.Background(), AppendEntriesArgs{
		Term: 2, LeaderID: "b", Entries: []LogEntry{{Term: 2, Index: 1, Command: []byte("replacement")}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-resultCh:
		if !errors.Is(err, ErrProposalLost) {
			t.Fatalf("pending proposal error after step-down = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending proposal was not terminated on step-down")
	}
}

func TestCompletedProposalResultIsReleased(t *testing.T) {
	transport := NewMemoryTransport()
	node := proposalNode(t, "a", nil, kv.NewMemoryStore(), transport)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	command, err := (kv.Command{Type: kv.Put, Key: []byte("k"), Value: []byte("v")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	result, err := node.Propose(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.Index != 1 || string(result.Result) != "v" {
		t.Fatalf("proposal result = %#v", result)
	}
	stateCh := make(chan bool, 1)
	if err := node.enqueue(context.Background(), stateInspectEvent{reply: stateCh}); err != nil {
		t.Fatal(err)
	}
	if <-stateCh {
		t.Fatal("completed apply result was retained")
	}
}

type stateInspectEvent struct {
	reply chan bool
}

func (e stateInspectEvent) handle(s *runtimeState) bool {
	_, ok := s.applyResults[1]
	e.reply <- ok
	return false
}

func TestMalformedProposalFailsDuringApplyWithoutKVMutation(t *testing.T) {
	transport := NewMemoryTransport()
	store := kv.NewMemoryStore()
	node := proposalNode(t, "a", nil, store, transport)
	if err := triggerBecomeLeaderForTest(node, context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := node.Propose(ctx, []byte("malformed"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("malformed proposal error = %v", err)
	}
	if _, ok, getErr := store.Get(context.Background(), []byte("k")); getErr != nil || ok {
		t.Fatalf("store changed after malformed command: ok=%v err=%v", ok, getErr)
	}
	state, err := node.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.CommitIndex != 1 || state.LastApplied != 0 {
		t.Fatalf("malformed apply changed indexes: %#v", state)
	}
}
