package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrAlreadyStarted = errors.New("raft node already started")
	ErrNotInitialized = errors.New("raft node is not initialized")
	ErrAlreadyStopped = errors.New("raft node already stopped")
)

// defaultReplicationTimeout bounds a single leader->peer replication attempt.
// Every attempt must terminate: the transport call is cancelled once it
// elapses, the attempt reports an error, and the per-peer slot is released so
// the next heartbeat can retry. Without a bound, one undeliverable request
// would occupy its peer's slot forever.
const defaultReplicationTimeout = 10 * time.Second

// Config contains the dependencies and timing policy for a Raft node.
type Config struct {
	ID                 NodeID
	Peers              []NodeID
	Storage            Storage
	Transport          Transport
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	ApplyRetryInterval time.Duration
	ReplicationTimeout time.Duration
	Random             *rand.Rand
	StateMachine       StateMachine
	SnapshotStorage    SnapshotStorage
}

type Node struct {
	config Config

	mu        sync.Mutex
	lifecycle Lifecycle
	state     PersistentState
	snapshot  Snapshot
	done      chan struct{}
	events    chan nodeEvent
}

func NewNode(config Config) (*Node, error) {
	if config.ID == "" {
		return nil, errors.New("raft node ID must not be empty")
	}
	if config.Storage == nil {
		return nil, errors.New("raft storage is required")
	}
	if config.Transport == nil {
		return nil, errors.New("raft transport is required")
	}
	if config.ElectionTimeoutMin <= 0 || config.ElectionTimeoutMax <= config.ElectionTimeoutMin {
		return nil, errors.New("invalid election timeout range")
	}
	if config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.ElectionTimeoutMin {
		return nil, errors.New("invalid heartbeat interval")
	}
	if config.ApplyRetryInterval <= 0 {
		config.ApplyRetryInterval = config.HeartbeatInterval
	}
	if config.ReplicationTimeout <= 0 {
		config.ReplicationTimeout = defaultReplicationTimeout
	}
	if config.StateMachine == nil {
		return nil, errors.New("raft state machine is required")
	}
	return &Node{config: config, lifecycle: Created}, nil
}

func (n *Node) Initialize(ctx context.Context) error {
	n.mu.Lock()
	if n.lifecycle != Created {
		n.mu.Unlock()
		return errors.New("raft node is not in created state")
	}
	n.mu.Unlock()

	state, err := n.config.Storage.Load(ctx)
	if err != nil {
		return err
	}
	log, err := newRaftLog(state.SnapshotBoundary, state.Log)
	if err != nil {
		return err
	}
	if state.CommitIndex > log.lastIndex() || state.CommitIndex < state.SnapshotBoundary.Index {
		return errors.New("persisted commit index is outside durable log")
	}
	if state.SnapshotBoundary.Index > 0 {
		if n.config.SnapshotStorage == nil {
			return errors.New("snapshot storage is required for compacted state")
		}
		snapshot, err := n.config.SnapshotStorage.LoadSnapshot(ctx)
		if err != nil {
			return err
		}
		if snapshot.LastIncludedIndex != state.SnapshotBoundary.Index || snapshot.LastIncludedTerm != state.SnapshotBoundary.Term {
			return errors.New("snapshot metadata does not match persistent state")
		}
		if err := n.config.StateMachine.Restore(ctx, append([]byte(nil), snapshot.StateMachineData...)); err != nil {
			n.mu.Lock()
			n.lifecycle = Stopped
			n.mu.Unlock()
			return fmt.Errorf("restore snapshot: %w", err)
		}
	}
	for index := state.SnapshotBoundary.Index + 1; index <= state.CommitIndex; index++ {
		entry, ok := log.entry(index)
		if !ok {
			return errors.New("persisted committed entry is missing")
		}
		if _, err := n.config.StateMachine.Apply(ctx, append([]byte(nil), entry.Command...)); err != nil {
			n.mu.Lock()
			n.lifecycle = Stopped
			n.mu.Unlock()
			return fmt.Errorf("replay committed entry %d: %w", index, err)
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lifecycle != Created {
		return errors.New("raft node changed state during initialization")
	}
	n.state = clonePersistentState(state)
	n.lifecycle = Initialized
	return nil
}

func (n *Node) Start(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lifecycle == Running {
		return ErrAlreadyStarted
	}
	if n.lifecycle != Initialized {
		if n.lifecycle == Stopped {
			return ErrAlreadyStopped
		}
		return ErrNotInitialized
	}
	n.events = make(chan nodeEvent, 128)
	n.done = make(chan struct{})
	n.lifecycle = Running
	go n.run()
	return nil
}

func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	lifecycle := n.lifecycle
	done := n.done
	events := n.events
	n.mu.Unlock()
	if lifecycle == Stopped {
		return nil
	}
	if lifecycle != Running {
		return ErrNotInitialized
	}
	select {
	case events <- stopEvent{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) Role(ctx context.Context) (Role, error) {
	reply := make(chan roleReply, 1)
	if err := n.enqueue(ctx, roleEvent{reply: reply}); err != nil {
		return Follower, err
	}
	result := <-reply
	return result.role, result.err
}

func (n *Node) Leader(ctx context.Context) (NodeID, error) {
	reply := make(chan leaderReply, 1)
	if err := n.enqueue(ctx, leaderEvent{reply: reply}); err != nil {
		return "", err
	}
	result := <-reply
	return result.id, result.err
}

func (n *Node) DebugState(ctx context.Context) (DebugState, error) {
	reply := make(chan DebugState, 1)
	if err := n.enqueue(ctx, debugEvent{reply: reply}); err != nil {
		return DebugState{}, err
	}
	return <-reply, nil
}

func (n *Node) enqueueBackground(event nodeEvent) {
	n.mu.Lock()
	if n.lifecycle != Running {
		n.mu.Unlock()
		return
	}
	events := n.events
	n.mu.Unlock()
	select {
	case events <- event:
	case <-n.done:
	case <-time.After(time.Second):
	}
}

// enqueueCompletion delivers a replication completion to the event loop without
// discarding it. Discarding a completion would leave the peer's replication slot
// occupied forever, because the slot is only released by the completion itself.
// It blocks until the event is accepted or the node stops; at most one such
// goroutine exists per peer, so the wait is bounded by the peer count.
func (n *Node) enqueueCompletion(event nodeEvent) {
	n.mu.Lock()
	if n.lifecycle != Running {
		n.mu.Unlock()
		return
	}
	events := n.events
	n.mu.Unlock()
	select {
	case events <- event:
	case <-n.done:
	}
}

func (n *Node) enqueue(ctx context.Context, event nodeEvent) error {
	n.mu.Lock()
	if n.lifecycle != Running {
		n.mu.Unlock()
		return ErrUnavailable
	}
	events := n.events
	n.mu.Unlock()
	select {
	case events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) run() {
	state, err := newRuntimeState(n, n.config, n.state)
	if err != nil {
		close(n.done)
		n.mu.Lock()
		n.lifecycle = Stopped
		n.mu.Unlock()
		return
	}
	electionTimer := time.NewTimer(state.nextElectionTimeout())
	heartbeatTimer := time.NewTimer(time.Hour)
	applyRetryTimer := time.NewTimer(time.Hour)
	if !heartbeatTimer.Stop() {
		<-heartbeatTimer.C
	}
	if !applyRetryTimer.Stop() {
		<-applyRetryTimer.C
	}
	defer electionTimer.Stop()
	defer heartbeatTimer.Stop()
	defer applyRetryTimer.Stop()
	resetElection := func() {
		if !electionTimer.Stop() {
			select {
			case <-electionTimer.C:
			default:
			}
		}
		electionTimer.Reset(state.nextElectionTimeout())
	}
	resetApplyRetry := func() {
		if !applyRetryTimer.Stop() {
			select {
			case <-applyRetryTimer.C:
			default:
			}
		}
		if state.lastApplied < state.commitIndex {
			applyRetryTimer.Reset(state.config.ApplyRetryInterval)
		}
	}
	// The heartbeat is armed only when leadership starts and re-arms itself on
	// each expiry, so it keeps a fixed cadence regardless of how many events the
	// loop is processing. Re-arming on every event would let a busy event
	// stream (a client proposal or replication reply arriving faster than
	// HeartbeatInterval) starve the timer indefinitely, and a leader that stops
	// heartbeating strands every peer it can no longer reach.
	heartbeatArmed := false
	armHeartbeat := func() {
		if heartbeatArmed {
			return
		}
		heartbeatTimer.Reset(state.nextHeartbeatTimeout())
		heartbeatArmed = true
	}
	disarmHeartbeat := func() {
		if !heartbeatArmed {
			return
		}
		if !heartbeatTimer.Stop() {
			select {
			case <-heartbeatTimer.C:
			default:
			}
		}
		heartbeatArmed = false
	}
	trackLeadership := func() {
		if state.role == Leader {
			armHeartbeat()
		} else {
			disarmHeartbeat()
		}
	}
	defer close(n.done)
	defer func() {
		n.mu.Lock()
		n.lifecycle = Stopped
		n.mu.Unlock()
	}()

	trackLeadership()

	for {
		select {
		case <-electionTimer.C:
			state.onElectionTimeout()
			if state.resetElectionTimer {
				state.resetElectionTimer = false
				resetElection()
			}
			trackLeadership()
		case <-applyRetryTimer.C:
			state.applyCommitted()
			resetApplyRetry()
		case <-heartbeatTimer.C:
			heartbeatArmed = false
			if state.role == Leader {
				state.sendAppendEntries()
				armHeartbeat()
			}
		case event := <-n.events:
			if event.handle(state) {
				return
			}
			state.applyCommitted()
			resetApplyRetry()
			if state.resetElectionTimer {
				state.resetElectionTimer = false
				resetElection()
			}
			trackLeadership()
		}
	}
}

func clonePersistentState(state PersistentState) PersistentState {
	state.Log = append([]LogEntry(nil), state.Log...)
	for i := range state.Log {
		state.Log[i].Command = append([]byte(nil), state.Log[i].Command...)
	}
	return state
}
