package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrAlreadyStarted = errors.New("raft node already started")
	ErrNotInitialized = errors.New("raft node is not initialized")
	ErrAlreadyStopped = errors.New("raft node already stopped")
)

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
	Random             *rand.Rand
	StateMachine       StateMachine
}

type Node struct {
	config Config

	mu        sync.Mutex
	lifecycle Lifecycle
	state     PersistentState
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
	if _, err := newRaftLog(state.Log); err != nil {
		return err
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
	resetHeartbeat := func() {
		if !heartbeatTimer.Stop() {
			select {
			case <-heartbeatTimer.C:
			default:
			}
		}
		if state.role == Leader {
			heartbeatTimer.Reset(state.nextHeartbeatTimeout())
		}
	}
	defer close(n.done)
	defer func() {
		n.mu.Lock()
		n.lifecycle = Stopped
		n.mu.Unlock()
	}()

	for {
		select {
		case <-electionTimer.C:
			state.onElectionTimeout()
			if state.resetElectionTimer {
				state.resetElectionTimer = false
				resetElection()
			}
			if state.role == Leader {
				resetHeartbeat()
			}
		case <-applyRetryTimer.C:
			state.applyCommitted()
			resetApplyRetry()
		case <-heartbeatTimer.C:
			if state.role == Leader {
				state.sendAppendEntries()
				resetHeartbeat()
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
			if state.role == Leader {
				resetHeartbeat()
			}
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
