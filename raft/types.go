// Package raft defines the protocol contracts and state ownership boundaries for a Raft node.
//
// This package intentionally contains no election, replication, networking, or persistence
// implementation yet. The types here are the stable vocabulary used by those layers.
package raft

import "errors"

// NodeID identifies a member of a Raft cluster.
type NodeID string

// Term is a monotonically increasing Raft term number.
type Term uint64

// LogIndex identifies a position in the replicated log. Index zero is reserved as the
// synthetic pre-log position used by AppendEntries' previous-log fields.
type LogIndex uint64

// Role is the local node's Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one replicated command. Entries are immutable once appended.
type LogEntry struct {
	Term    Term
	Index   LogIndex
	Command []byte
}

// RequestVoteArgs and RequestVoteReply are the Raft RequestVote RPC contract.
type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex LogIndex
	LastLogTerm  Term
}

type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

// AppendEntriesArgs and AppendEntriesReply are the Raft AppendEntries RPC contract.
// An empty Entries slice represents a heartbeat; heartbeats are a protocol concept,
// not a separate RPC type.
type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
}

type AppendEntriesReply struct {
	Term    Term
	Success bool
}

// InstallSnapshotArgs carries a complete opaque state-machine snapshot.
type InstallSnapshotArgs struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	Data              []byte
}

type InstallSnapshotReply struct {
	Term Term
}

// ErrCode classifies outcomes exposed by the future Raft client API.
type ErrCode uint8

const (
	ErrCodeUnknown ErrCode = iota
	ErrCodeNotLeader
	ErrCodeUnavailable
	ErrCodeTimeout
	ErrCodeInvalidCommand
)

// Error is a stable, inspectable error returned by the Raft API.
type Error struct {
	Code     ErrCode
	LeaderID NodeID
	Message  string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	switch e.Code {
	case ErrCodeNotLeader:
		return "not leader"
	case ErrCodeUnavailable:
		return "raft unavailable"
	case ErrCodeTimeout:
		return "raft operation timed out"
	case ErrCodeInvalidCommand:
		return "invalid command"
	default:
		return "raft error"
	}
}

var (
	ErrNotLeader   = &Error{Code: ErrCodeNotLeader}
	ErrUnavailable = &Error{Code: ErrCodeUnavailable}
	ErrTimeout     = &Error{Code: ErrCodeTimeout}
)

// Validate performs only structural validation. It does not mutate state or make
// protocol decisions; those belong to the future Raft state machine.
func (e LogEntry) Validate() error {
	if e.Index == 0 {
		return errors.New("log entry index must be greater than zero")
	}
	if e.Term == 0 {
		return errors.New("log entry term must be greater than zero")
	}
	return nil
}
