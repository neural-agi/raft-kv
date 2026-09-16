package fault

import (
	"context"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

func TestNetworkDropsAndErrorsPerEdge(t *testing.T) {
	controller := NewController(nil)
	network := NewNetwork(controller)
	node := testNode(t, "b")
	network.Connect("b", node)
	transport := network.Transport("a")
	controller.Set("a", "b", RequestVote, Rule{Drop: true})
	if _, err := transport.RequestVote(context.Background(), "b", raft.RequestVoteArgs{}); err == nil {
		t.Fatal("dropped vote returned success")
	}
	controller.Set("a", "b", RequestVote, Rule{Error: true})
	if _, err := transport.RequestVote(context.Background(), "b", raft.RequestVoteArgs{}); err == nil {
		t.Fatal("errored vote returned success")
	}
}

func TestNetworkDelayReleaseAndPartitionHeal(t *testing.T) {
	trace := &Trace{}
	controller := NewController(trace)
	network := NewNetwork(controller)
	node := testNode(t, "b")
	network.Connect("b", node)
	transport := network.Transport("a")
	controller.Set("a", "b", RequestVote, Rule{Delay: true})
	result := make(chan error, 1)
	go func() {
		_, err := transport.RequestVote(context.Background(), "b", raft.RequestVoteArgs{})
		result <- err
	}()
	deadline := time.After(time.Second)
	for len(network.Pending()) == 0 {
		select {
		case <-deadline:
			t.Fatal("delayed request was not queued")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	ids := network.Pending()
	if !network.Release(ids[0]) {
		t.Fatal("release failed")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("released request did not complete")
	}
	controller.Clear("a", "b", RequestVote)
	controller.Partition("a", "b")
	if _, err := transport.RequestVote(context.Background(), "b", raft.RequestVoteArgs{}); err == nil {
		t.Fatal("partitioned request returned success")
	}
	controller.Heal("a", "b")
	if _, err := transport.RequestVote(context.Background(), "b", raft.RequestVoteArgs{}); err != nil {
		t.Fatal(err)
	}
	if len(trace.Events()) == 0 {
		t.Fatal("fault trace is empty")
	}
}

func testNode(t *testing.T, id raft.NodeID) *raft.Node {
	t.Helper()
	node, err := raft.NewNode(raft.Config{ID: id, Storage: testStorage{}, Transport: raft.NewMemoryTransport(), ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 10 * time.Millisecond, StateMachine: testMachine{}})
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

type testStorage struct{}

func (testStorage) Load(context.Context) (raft.PersistentState, error) {
	return raft.PersistentState{}, nil
}
func (testStorage) Save(context.Context, raft.PersistentState) error { return nil }

type testMachine struct{}

func (testMachine) Apply(context.Context, []byte) ([]byte, error) { return nil, nil }
func (testMachine) Snapshot(context.Context) ([]byte, error)      { return nil, nil }
func (testMachine) Restore(context.Context, []byte) error         { return nil }
