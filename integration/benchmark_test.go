package integration

import (
	"context"
	"testing"

	"github.com/neural-agi/raft-kv/kv"
)

// BenchmarkThreeNodePUT measures the in-process three-node Raft proposal path:
// a PUT is proposed through Node.Propose on the elected leader and blocks until
// the entry is replicated, committed, and applied to the KV state machine. The
// cluster uses the in-memory transport and in-memory storage from the existing
// integration harness, so this is a measure of the Raft code itself, not of a
// production network or of disk durability.
//
// All cluster setup happens before b.ResetTimer() and is excluded from the
// measurement. Each iteration issues a PUT for the same key through the real
// Node.Propose path; there is no mocked proposal path anywhere here.
//
// Run with:
//
//	go test -run '^$' -bench '^BenchmarkThreeNodePUT$' -benchmem -benchtime=100ms ./integration
func BenchmarkThreeNodePUT(b *testing.B) {
	cluster := newCluster(b)
	leader := waitLeader(b, cluster)
	ctx := context.Background()

	put, err := (kv.Command{Type: kv.Put, Key: []byte("benchmark"), Value: []byte("value")}).Encode()
	if err != nil {
		b.Fatal(err)
	}
	if result, err := leader.node.Propose(ctx, put); err != nil || string(result.Result) != "value" {
		b.Fatalf("warm-up proposal failed: result=%#v err=%v", result, err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := leader.node.Propose(ctx, put); err != nil {
			b.Fatalf("proposal failed: %v", err)
		}
	}
}
