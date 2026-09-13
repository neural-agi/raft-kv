package raft

// VolatileState is rebuilt from persistent state after restart and is never stored
// by Storage. It is kept separate to make crash-safety obligations explicit.
type VolatileState struct {
	CommitIndex LogIndex
	LastApplied LogIndex
}

// LeaderState is meaningful only while the local node is leader. It is volatile and
// must be reconstructed after every leadership change or restart.
type LeaderState struct {
	NextIndex  map[NodeID]LogIndex
	MatchIndex map[NodeID]LogIndex
}
