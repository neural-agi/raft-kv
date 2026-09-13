package raft

import "context"

// Transport sends Raft protocol messages. It owns delivery mechanics, deadlines,
// serialization, and transport-level failures; Raft owns protocol meaning. The
// transport must not call back into Raft while a Raft event-loop operation is active.
type Transport interface {
	RequestVote(ctx context.Context, target NodeID, request RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, target NodeID, request AppendEntriesArgs) (AppendEntriesReply, error)
}

// Storage owns Raft's durable state. Load returns a complete independent state
// replacement, or an error for unreadable/invalid persisted data. A missing
// uninitialized store returns the zero PersistentState. Save replaces the complete
// state and must not return nil until the replacement has reached the adapter's
// documented durability boundary. Implementations must not retain caller-owned
// slices. Raft does not depend on whether an adapter uses files, a WAL, or another
// local mechanism.
type Storage interface {
	Load(ctx context.Context) (PersistentState, error)
	Save(ctx context.Context, state PersistentState) error
}

// PersistentState is exactly the Raft state required to survive a crash:
// current term, current-term vote, and the replicated log. Commit/applied indexes
// are intentionally not persisted by this MVP contract.
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Log         []LogEntry
}

// StateMachine applies committed commands in log order. It must not be called for
// uncommitted entries. Apply is deterministic for the same prior state and command.
type StateMachine interface {
	Apply(ctx context.Context, command []byte) (result []byte, err error)
}

// ProposalResult describes a command that Raft has committed and successfully
// applied. Result is the state-machine result associated with the command.
type ProposalResult struct {
	Index  LogIndex
	Term   Term
	Result []byte
}

// NodeAPI is the application-facing Raft boundary. Start performs recovery and
// activates the event loop; Stop prevents new work and waits for the event loop to
// exit. Propose accepts opaque deterministic command bytes, not KV-specific types.
type NodeAPI interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Propose(ctx context.Context, command []byte) (ProposalResult, error)
	Role(ctx context.Context) (Role, error)
	Leader(ctx context.Context) (NodeID, error)
}
