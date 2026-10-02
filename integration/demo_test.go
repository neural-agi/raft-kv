package integration

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/fault"
	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
)

// demoNode couples a raft.Node with its durable in-memory storage, snapshot
// storage, and KV state machine so the demo can Stop a leader, restart it
// against the same durable state, and watch it catch up.
type demoNode struct {
	id        raft.NodeID
	node      *raft.Node
	storage   *v7MemoryStorage
	snapshot  *v7MemorySnapshotStorage
	store     *kv.MemoryStore
	transport raft.Transport
}

// demoConfig builds a Raft config with Peers excluding self (a three-node
// cluster commits with a quorum of two: self plus one follower), a deterministic
// per-node RNG, and the KV state machine.
func demoConfig(id raft.NodeID, peers []raft.NodeID, storage *v7MemoryStorage, snapshot *v7MemorySnapshotStorage, store *kv.MemoryStore, transport raft.Transport, electionMin, electionMax, heartbeat time.Duration) raft.Config {
	return raft.Config{
		ID:                 id,
		Peers:              peers,
		Storage:            storage,
		SnapshotStorage:    snapshot,
		Transport:          transport,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		ApplyRetryInterval: heartbeat,
		Random:             rand.New(rand.NewSource(int64(id[0]))),
		StateMachine:       store,
	}
}

func buildDemoNode(t testing.TB, config raft.Config, member *demoNode) *demoNode {
	t.Helper()
	node, err := raft.NewNode(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	member.node = node
	t.Cleanup(func() { _ = node.Stop(context.Background()) })
	return member
}

func newDemoNode(t testing.TB, id raft.NodeID, peers []raft.NodeID, transport raft.Transport, electionMin, electionMax, heartbeat time.Duration) *demoNode {
	t.Helper()
	member := &demoNode{
		id:        id,
		storage:   &v7MemoryStorage{},
		snapshot:  &v7MemorySnapshotStorage{},
		store:     kv.NewMemoryStore(),
		transport: transport,
	}
	return buildDemoNode(t, demoConfig(id, peers, member.storage, member.snapshot, member.store, transport, electionMin, electionMax, heartbeat), member)
}

// restartDemoNode brings member back up against the same durable storage and
// snapshot so its term and log survive, but supplies a fresh state machine:
// Raft replays the persisted committed prefix during Initialize. The caller must
// re-register the returned node with the network and start it.
func restartDemoNode(t testing.TB, member *demoNode, peers []raft.NodeID, electionMin, electionMax, heartbeat time.Duration) *demoNode {
	t.Helper()
	restarted := &demoNode{
		id:        member.id,
		storage:   member.storage,
		snapshot:  member.snapshot,
		store:     kv.NewMemoryStore(),
		transport: member.transport,
	}
	return buildDemoNode(t, demoConfig(member.id, peers, member.storage, member.snapshot, restarted.store, member.transport, electionMin, electionMax, heartbeat), restarted)
}

func startDemoNode(t testing.TB, member *demoNode) {
	t.Helper()
	if err := member.node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func stopDemoNode(t testing.TB, member *demoNode) {
	t.Helper()
	if err := member.node.Stop(context.Background()); err != nil {
		t.Fatalf("stop %s: %v", member.id, err)
	}
}

// waitDemoLeader polls the given live nodes and returns the first one that
// reports itself as the current leader. It polls rather than sleeping, so the
// call duration tracks the actual election, not a fixed guess.
func waitDemoLeader(t testing.TB, nodes []*demoNode) *demoNode {
	t.Helper()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		for _, member := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			leader, err := member.node.Leader(ctx)
			cancel()
			if err == nil && leader == member.id {
				return member
			}
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("timed out waiting for a demo leader")
		}
	}
}

// proposeDemoPut encodes a PUT and blocks on Node.Propose until it is committed
// and applied, then asserts the applied result.
func proposeDemoPut(t testing.TB, node *raft.Node, key, value string) {
	t.Helper()
	command, err := (kv.Command{Type: kv.Put, Key: []byte(key), Value: []byte(value)}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	result, err := node.Propose(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Result) != value {
		t.Fatalf("proposal result = %q, want %q", result.Result, value)
	}
}

// waitStoreObserved polls until the given store's local state machine has key
// mapped to want. Being applied locally is proof that the entry committed and
// the node's log caught up.
func waitStoreObserved(t testing.TB, store *kv.MemoryStore, key, want string) {
	t.Helper()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for {
		value, ok, err := store.Get(context.Background(), []byte(key))
		if err == nil && ok && string(value) == want {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("store never observed %q=%q (last value %q ok=%v err=%v)", key, want, value, ok, err)
		}
	}
}

// waitDemoConverged verifies every node's KV state machine observed both the
// initial and the post-failover write, proving the restarted leader caught up.
func waitDemoConverged(t testing.TB, nodes []*demoNode, entries map[string]string) {
	t.Helper()
	for key, want := range entries {
		for _, member := range nodes {
			waitStoreObserved(t, member.store, key, want)
		}
	}
}

// TestThreeNodeFailoverDemo steps through the canonical three-node story using
// the real Raft code path and in-memory fault/transport helpers:
//
//  1. start a, b, and c; "a" is biased as the first leader (30-60ms election
//     timeout against 300-500ms on b/c);
//  2. propose and verify an initial PUT through that leader;
//  3. Stop the leader, wait for a replacement to be elected, and propose and
//     verify an extra PUT through the replacement;
//  4. restart the stopped node on its retained durable storage with a fresh KV
//     state machine, re-register it, and wait until all three nodes have applied
//     both writes;
//  5. cleanly Stop every node, including the restarted one.
//
// All waiting is poll-based; there are no arbitrary sleeps. Run with:
//
//	go test -v ./integration -run '^TestThreeNodeFailoverDemo$' -count=20
func TestThreeNodeFailoverDemo(t *testing.T) {
	ids := []raft.NodeID{"a", "b", "c"}
	peersOf := func(self raft.NodeID) []raft.NodeID {
		others := make([]raft.NodeID, 0, len(ids)-1)
		for _, id := range ids {
			if id != self {
				others = append(others, id)
			}
		}
		return others
	}

	controller := fault.NewController(&fault.Trace{})
	network := fault.NewNetwork(controller)
	members := make(map[raft.NodeID]*demoNode, len(ids))

	members["a"] = newDemoNode(t, "a", peersOf("a"), network.Transport("a"), 30*time.Millisecond, 60*time.Millisecond, 15*time.Millisecond)
	members["b"] = newDemoNode(t, "b", peersOf("b"), network.Transport("b"), 300*time.Millisecond, 500*time.Millisecond, 50*time.Millisecond)
	members["c"] = newDemoNode(t, "c", peersOf("c"), network.Transport("c"), 300*time.Millisecond, 500*time.Millisecond, 50*time.Millisecond)
	live := []*demoNode{members["a"], members["b"], members["c"]}
	for _, id := range ids {
		network.Connect(id, members[id].node)
		startDemoNode(t, members[id])
	}

	first := waitDemoLeader(t, live)
	if first.id != "a" {
		t.Fatalf("first leader = %s, want a", first.id)
	}
	proposeDemoPut(t, first.node, "initial", "one")

	stopDemoNode(t, first)
	replacement := waitDemoLeader(t, []*demoNode{members["b"], members["c"]})
	if replacement.id == "a" {
		t.Fatal("stopped leader reported itself as replacement")
	}
	proposeDemoPut(t, replacement.node, "extra", "two")

	restarted := restartDemoNode(t, members["a"], peersOf("a"), 200*time.Millisecond, 400*time.Millisecond, 50*time.Millisecond)
	network.Connect("a", restarted.node)
	startDemoNode(t, restarted)
	members["a"] = restarted

	waitDemoConverged(t, []*demoNode{members["a"], members["b"], members["c"]}, map[string]string{"initial": "one", "extra": "two"})

	for _, id := range ids {
		stopDemoNode(t, members[id])
	}
}
