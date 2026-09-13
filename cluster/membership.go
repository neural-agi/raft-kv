package cluster

import "github.com/neural-agi/raft-kv/raft"

// IDs returns the configured member identities in configuration order.
func (c Config) IDs() []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(c.Members))
	for _, member := range c.Members {
		ids = append(ids, member.ID)
	}
	return ids
}

// Contains reports whether id is part of the static configuration.
func (c Config) Contains(id raft.NodeID) bool {
	for _, member := range c.Members {
		if member.ID == id {
			return true
		}
	}
	return false
}
