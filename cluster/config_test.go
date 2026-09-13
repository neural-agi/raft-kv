package cluster

import (
	"testing"

	"github.com/neural-agi/raft-kv/raft"
)

func TestConfigMembership(t *testing.T) {
	config := Config{Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	if !config.Contains(raft.NodeID("b")) || config.Contains(raft.NodeID("x")) {
		t.Fatal("membership lookup mismatch")
	}
	ids := config.IDs()
	if len(ids) != 3 || ids[0] != "a" || ids[2] != "c" {
		t.Fatalf("IDs() = %#v", ids)
	}
}
