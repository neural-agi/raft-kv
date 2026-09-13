package cluster

import "github.com/neural-agi/raft-kv/raft"

// Member describes one static cluster member. Membership changes are out of scope
// for the MVP; a configuration is supplied at startup and remains fixed.
type Member struct {
	ID      raft.NodeID
	Address string
}

// Config is the static cluster configuration used by a node.
type Config struct {
	Members []Member
}
