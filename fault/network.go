package fault

import (
	"context"
	"errors"
	"sync"

	"github.com/neural-agi/raft-kv/raft"
)

// Network is a deterministic per-sender transport factory. Each returned
// Transport preserves the production raft.Transport interface while binding the
// sender identity for edge-specific fault rules.
type Network struct {
	controller *Controller
	mu         sync.Mutex
	peers      map[raft.NodeID]*raft.Node
	pending    map[uint64]*pending
	nextID     uint64
}

type pending struct {
	from, to string
	kind     Kind
	deliver  func() (any, error)
	result   chan result
}

type result struct {
	value any
	err   error
}

func NewNetwork(controller *Controller) *Network {
	return &Network{controller: controller, peers: make(map[raft.NodeID]*raft.Node), pending: make(map[uint64]*pending)}
}

func (n *Network) Connect(id raft.NodeID, node *raft.Node) {
	n.mu.Lock()
	n.peers[id] = node
	n.mu.Unlock()
}
func (n *Network) Transport(from raft.NodeID) raft.Transport {
	return &senderTransport{network: n, from: string(from)}
}

func (n *Network) Release(id uint64) bool {
	n.mu.Lock()
	p, ok := n.pending[id]
	if ok {
		delete(n.pending, id)
	}
	n.mu.Unlock()
	if !ok {
		return false
	}
	value, err := p.deliver()
	p.result <- result{value: value, err: err}
	return true
}

func (n *Network) Pending() []uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]uint64, 0, len(n.pending))
	for id := range n.pending {
		ids = append(ids, id)
	}
	return ids
}

type senderTransport struct {
	network *Network
	from    string
}

func (t *senderTransport) RequestVote(ctx context.Context, target raft.NodeID, request raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	value, err := t.network.call(ctx, t.from, string(target), RequestVote, func(node *raft.Node) (any, error) { return node.RequestVote(ctx, request) })
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	return value.(raft.RequestVoteReply), nil
}

func (t *senderTransport) AppendEntries(ctx context.Context, target raft.NodeID, request raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	value, err := t.network.call(ctx, t.from, string(target), AppendEntries, func(node *raft.Node) (any, error) { return node.AppendEntries(ctx, request) })
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return value.(raft.AppendEntriesReply), nil
}

func (n *Network) call(ctx context.Context, from, to string, kind Kind, deliver func(*raft.Node) (any, error)) (any, error) {
	rule, controlled := n.controller.rule(from, to, kind)
	if controlled && (rule.Drop || rule.Error) {
		return nil, ErrInjected
	}
	n.mu.Lock()
	node := n.peers[raft.NodeID(to)]
	if node == nil {
		n.mu.Unlock()
		return nil, errors.New("peer unavailable")
	}
	if controlled && rule.Delay {
		n.nextID++
		id := n.nextID
		resultCh := make(chan result, 1)
		n.pending[id] = &pending{from: from, to: to, kind: kind, result: resultCh, deliver: func() (any, error) { return deliver(node) }}
		n.mu.Unlock()
		select {
		case r := <-resultCh:
			return r.value, r.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	n.mu.Unlock()
	return deliver(node)
}
