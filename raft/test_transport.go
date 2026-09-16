package raft

import (
	"context"
	"errors"
	"sync"
)

// MemoryTransport is a deterministic in-memory transport for election and
// replication tests. It is not a production network implementation.
type MemoryTransport struct {
	mu         sync.Mutex
	peers      map[NodeID]*Node
	dropVote   map[NodeID]bool
	dropAppend map[NodeID]bool
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{peers: make(map[NodeID]*Node), dropVote: make(map[NodeID]bool), dropAppend: make(map[NodeID]bool)}
}

func (t *MemoryTransport) Connect(id NodeID, node *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = node
}

func (t *MemoryTransport) SetDrop(id NodeID, drop bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropVote[id] = drop
	t.dropAppend[id] = drop
}

func (t *MemoryTransport) SetDropVote(id NodeID, drop bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropVote[id] = drop
}

func (t *MemoryTransport) SetDropAppendEntries(id NodeID, drop bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropAppend[id] = drop
}

func (t *MemoryTransport) RequestVote(ctx context.Context, target NodeID, request RequestVoteArgs) (RequestVoteReply, error) {
	t.mu.Lock()
	node := t.peers[target]
	dropped := t.dropVote[target]
	t.mu.Unlock()
	if node == nil || dropped {
		return RequestVoteReply{}, errors.New("request vote delivery unavailable")
	}
	return node.RequestVote(ctx, request)
}

func (t *MemoryTransport) InstallSnapshot(ctx context.Context, target NodeID, request InstallSnapshotArgs) (InstallSnapshotReply, error) {
	t.mu.Lock()
	node := t.peers[target]
	dropped := t.dropAppend[target]
	t.mu.Unlock()
	if node == nil || dropped {
		return InstallSnapshotReply{}, errors.New("install snapshot delivery unavailable")
	}
	return node.InstallSnapshot(ctx, request)
}

func (t *MemoryTransport) AppendEntries(ctx context.Context, target NodeID, request AppendEntriesArgs) (AppendEntriesReply, error) {
	t.mu.Lock()
	node := t.peers[target]
	dropped := t.dropAppend[target]
	t.mu.Unlock()
	if node == nil || dropped {
		return AppendEntriesReply{}, errors.New("append entries delivery unavailable")
	}
	return node.AppendEntries(ctx, request)
}
