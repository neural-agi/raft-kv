package integration

import (
	"context"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
)

type clusterNode struct {
	node  *raft.Node
	store *kv.MemoryStore
}

func newCluster(t testing.TB) []*clusterNode {
	t.Helper()
	transport := raft.NewMemoryTransport()
	ids := []raft.NodeID{"a", "b", "c"}
	cluster := make([]*clusterNode, 0, len(ids))
	for _, id := range ids {
		store := kv.NewMemoryStore()
		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: []raft.NodeID{"a", "b", "c"},
			Storage: &memoryStorage{}, Transport: transport,
			ElectionTimeoutMin: 20 * time.Millisecond, ElectionTimeoutMax: 40 * time.Millisecond,
			HeartbeatInterval: 5 * time.Millisecond, StateMachine: store,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Initialize(context.Background()); err != nil {
			t.Fatal(err)
		}
		transport.Connect(id, node)
		cluster = append(cluster, &clusterNode{node: node, store: store})
	}
	for _, member := range cluster {
		if err := member.node.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = member.node.Stop(context.Background()) })
	}
	return cluster
}

type memoryStorage struct{}

func (*memoryStorage) Load(context.Context) (raft.PersistentState, error) {
	return raft.PersistentState{}, nil
}

func (*memoryStorage) Save(context.Context, raft.PersistentState) error { return nil }

func waitLeader(t testing.TB, cluster []*clusterNode) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, member := range cluster {
			role, err := member.node.Role(context.Background())
			if err == nil && role == raft.Leader {
				return member
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cluster did not elect a leader")
	return nil
}

func TestThreeNodePutDeleteProposalPath(t *testing.T) {
	cluster := newCluster(t)
	leader := waitLeader(t, cluster)
	put, err := (kv.Command{Type: kv.Put, Key: []byte("foo"), Value: []byte("bar")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	putResult, err := leader.node.Propose(context.Background(), put)
	if err != nil {
		t.Fatal(err)
	}
	if string(putResult.Result) != "bar" || putResult.Index == 0 || putResult.Term == 0 {
		t.Fatalf("put result = %#v", putResult)
	}
	deleteCommand, err := (kv.Command{Type: kv.Delete, Key: []byte("foo")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	deleteResult, err := leader.node.Propose(context.Background(), deleteCommand)
	if err != nil {
		t.Fatal(err)
	}
	if string(deleteResult.Result) != "deleted" || deleteResult.Index != putResult.Index+1 {
		t.Fatalf("delete result = %#v after put %#v", deleteResult, putResult)
	}
	for _, member := range cluster {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			value, present, getErr := member.store.Get(context.Background(), []byte("foo"))
			if getErr == nil && !present && len(value) == 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		_, present, err := member.store.Get(context.Background(), []byte("foo"))
		if err != nil || present {
			t.Fatalf("member still has foo: present=%v err=%v", present, err)
		}
	}
}
