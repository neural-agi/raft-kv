package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func randSourceForLifecycle(id NodeID) *rand.Rand {
	var seed int64
	for i := 0; i < len(id); i++ {
		seed = seed*131 + int64(id[i])
	}
	return rand.New(rand.NewSource(seed + 1))
}

// gatedTransport is a controllable Transport for replication lifecycle tests.
// It can hold a call in flight until the test releases it, so a test can
// observe the real per-peer replication request the event loop created while
// the request is still active. Tests never construct replication state
// themselves; they only read back what production code allocated.
type gatedTransport struct {
	mu        sync.Mutex
	peers     map[NodeID]*Node
	failErr   error
	appends   int
	snapshots int
	lastSent  AppendEntriesArgs
	holdApp   int
	holdSnap  int
	parked    []chan struct{}

	appendArrived chan struct{}
	snapArrived   chan struct{}
}

func newGatedTransport() *gatedTransport {
	return &gatedTransport{
		peers:         make(map[NodeID]*Node),
		appendArrived: make(chan struct{}, 64),
		snapArrived:   make(chan struct{}, 64),
	}
}

func (t *gatedTransport) Connect(id NodeID, node *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = node
}

// BlockAppend parks the next n AppendEntries calls until the test releases
// them, so a test can hold a real in-flight request and observe the per-peer
// slot the event loop created for it.
func (t *gatedTransport) BlockAppend(n int) <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.holdApp += n
	return t.appendArrived
}

// BlockSnapshot parks the next n InstallSnapshot calls until released.
func (t *gatedTransport) BlockSnapshot(n int) <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.holdSnap += n
	return t.snapArrived
}

func (t *gatedTransport) releaseAll() {
	t.mu.Lock()
	gates := t.parked
	t.parked = nil
	t.mu.Unlock()
	for _, gate := range gates {
		close(gate)
	}
}

func (t *gatedTransport) ReleaseAppend()   { t.releaseAll() }
func (t *gatedTransport) ReleaseSnapshot() { t.releaseAll() }

func (t *gatedTransport) FailWith(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failErr = err
}

func (t *gatedTransport) Counts() (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.appends, t.snapshots
}

func (t *gatedTransport) Append() AppendEntriesArgs {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastSent
}

func (t *gatedTransport) RequestVote(ctx context.Context, target NodeID, request RequestVoteArgs) (RequestVoteReply, error) {
	t.mu.Lock()
	node := t.peers[target]
	t.mu.Unlock()
	if node == nil {
		return RequestVoteReply{}, errors.New("peer unavailable")
	}
	return node.RequestVote(ctx, request)
}

func (t *gatedTransport) AppendEntries(ctx context.Context, target NodeID, request AppendEntriesArgs) (AppendEntriesReply, error) {
	t.mu.Lock()
	t.appends++
	t.lastSent = request
	arrived := t.appendArrived
	failure := t.failErr
	node := t.peers[target]
	var gate chan struct{}
	if t.holdApp > 0 {
		t.holdApp--
		gate = make(chan struct{})
		t.parked = append(t.parked, gate)
	}
	t.mu.Unlock()
	select {
	case arrived <- struct{}{}:
	default:
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return AppendEntriesReply{}, ctx.Err()
		}
	}
	if failure != nil {
		return AppendEntriesReply{}, failure
	}
	if node == nil {
		return AppendEntriesReply{}, errors.New("peer unavailable")
	}
	return node.AppendEntries(ctx, request)
}

func (t *gatedTransport) InstallSnapshot(ctx context.Context, target NodeID, request InstallSnapshotArgs) (InstallSnapshotReply, error) {
	t.mu.Lock()
	t.snapshots++
	arrived := t.snapArrived
	failure := t.failErr
	node := t.peers[target]
	var gate chan struct{}
	if t.holdSnap > 0 {
		t.holdSnap--
		gate = make(chan struct{})
		t.parked = append(t.parked, gate)
	}
	t.mu.Unlock()
	select {
	case arrived <- struct{}{}:
	default:
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return InstallSnapshotReply{}, ctx.Err()
		}
	}
	if failure != nil {
		return InstallSnapshotReply{}, failure
	}
	if node == nil {
		return InstallSnapshotReply{}, errors.New("peer unavailable")
	}
	return node.InstallSnapshot(ctx, request)
}

// testActiveReplicationEvent reads the active replication request for a peer.
// It is an observer only: it never allocates or mutates replication state.
type testActiveReplicationEvent struct {
	peer  NodeID
	reply chan testActiveReplicationResult
}

type testActiveReplicationResult struct {
	request replicationRequest
	active  bool
}

func (e testActiveReplicationEvent) handle(s *runtimeState) bool {
	request, active := s.replication[e.peer]
	e.reply <- testActiveReplicationResult{request: request, active: active}
	return false
}

func activeReplicationForTest(node *Node, ctx context.Context, peer NodeID) (replicationRequest, bool, error) {
	reply := make(chan testActiveReplicationResult, 1)
	if err := node.enqueue(ctx, testActiveReplicationEvent{peer: peer, reply: reply}); err != nil {
		return replicationRequest{}, false, err
	}
	result := <-reply
	return result.request, result.active, nil
}

// lifecycleNode builds a started node wired to the supplied transport. Its
// election timeouts suit a node that is expected to win elections.
func lifecycleNode(t *testing.T, id NodeID, peers []NodeID, storage Storage, transport Transport, machine StateMachine, snapshots SnapshotStorage, replicationTimeout time.Duration) *Node {
	t.Helper()
	return lifecycleNodeWith(t, id, peers, storage, transport, machine, snapshots, 200*time.Millisecond, 400*time.Millisecond, replicationTimeout)
}

// lifecyclePeer builds a started peer that tolerates a stalled leader. While a
// replication request is held in flight the leader is deliberately not sending
// that peer anything, so a peer with a short election timeout would start its
// own election and the test would observe a leadership change rather than the
// replication behaviour it is checking.
func lifecyclePeer(t *testing.T, id NodeID, peers []NodeID, storage Storage, transport Transport, machine StateMachine, snapshots SnapshotStorage, replicationTimeout time.Duration) *Node {
	t.Helper()
	return lifecycleNodeWith(t, id, peers, storage, transport, machine, snapshots, 5*time.Second, 10*time.Second, replicationTimeout)
}

func lifecycleNodeWith(t *testing.T, id NodeID, peers []NodeID, storage Storage, transport Transport, machine StateMachine, snapshots SnapshotStorage, electionMin, electionMax, replicationTimeout time.Duration) *Node {
	t.Helper()
	config := Config{
		ID:                 id,
		Peers:              peers,
		Storage:            storage,
		SnapshotStorage:    snapshots,
		Transport:          transport,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  5 * time.Millisecond,
		ApplyRetryInterval: 5 * time.Millisecond,
		ReplicationTimeout: replicationTimeout,
		Random:             randSourceForLifecycle(id),
		StateMachine:       machine,
	}
	node, err := NewNode(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Stop(context.Background()) })
	return node
}

func waitForOK(t *testing.T, what string, check func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func waitFor(t *testing.T, what string, check func() bool) {
	t.Helper()
	if !waitForOK(t, what, check) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitActiveReplication(t *testing.T, node *Node, peer NodeID) replicationRequest {
	t.Helper()
	var observed replicationRequest
	waitFor(t, "active replication request for "+string(peer), func() bool {
		request, active, err := activeReplicationForTest(node, context.Background(), peer)
		if err != nil || !active {
			return false
		}
		observed = request
		return true
	})
	return observed
}

func waitNoActiveReplication(t *testing.T, node *Node, peer NodeID) {
	t.Helper()
	waitFor(t, "released replication slot for "+string(peer), func() bool {
		_, active, err := activeReplicationForTest(node, context.Background(), peer)
		return err == nil && !active
	})
}

// TestRepeatedHeartbeatsDoNotDuplicateInFlightReplication proves the per-peer
// slot is real: many heartbeats elapse while one request is held, yet only one
// call is ever launched. The control node, without a gate, proves that the same
// window really does produce repeated launches.
func TestRepeatedHeartbeatsDoNotDuplicateInFlightReplication(t *testing.T) {
	gated := newGatedTransport()
	arrived := gated.BlockAppend(1)
	leaderStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}}
	followerStorage := &testStorage{state: PersistentState{CurrentTerm: 1}}
	leader := lifecycleNode(t, "a", []NodeID{"b"}, leaderStorage, gated, NewTestStateMachine(), nil, time.Second)
	follower := lifecyclePeer(t, "b", []NodeID{"a"}, followerStorage, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("a", leader)
	gated.Connect("b", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	held, _ := gated.Counts()
	active := waitActiveReplication(t, leader, "b")
	if active.kind != replicationAppend {
		t.Fatalf("active request kind = %v", active.kind)
	}
	time.Sleep(200 * time.Millisecond)
	if after, _ := gated.Counts(); after != held {
		t.Fatalf("held peer launched %d extra calls: %d -> %d", after-held, held, after)
	}
	if _, stillActive, err := activeReplicationForTest(leader, context.Background(), "b"); err != nil || !stillActive {
		t.Fatalf("held request lost its slot: active=%v err=%v", stillActive, err)
	}
	gated.ReleaseAppend()
	waitFor(t, "held replication to commit", func() bool {
		state, err := leader.DebugState(context.Background())
		return err == nil && state.MatchIndex["b"] == 2
	})

	control := newGatedTransport()
	controlStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}}
	controlFollowerStorage := &testStorage{state: PersistentState{CurrentTerm: 1}}
	controlLeader := lifecycleNode(t, "a2", []NodeID{"b2"}, controlStorage, control, NewTestStateMachine(), nil, time.Second)
	controlFollower := lifecyclePeer(t, "b2", []NodeID{"a2"}, controlFollowerStorage, control, NewTestStateMachine(), nil, time.Second)
	control.Connect("a2", controlLeader)
	control.Connect("b2", controlFollower)
	if err := triggerBecomeLeaderForTest(controlLeader, context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "control replication to commit", func() bool {
		state, err := controlLeader.DebugState(context.Background())
		return err == nil && state.MatchIndex["b2"] == 2
	})
	baseline, _ := control.Counts()
	time.Sleep(200 * time.Millisecond)
	if grown, _ := control.Counts(); grown <= baseline {
		t.Fatalf("control node did not relaunch on heartbeats: %d -> %d", baseline, grown)
	}
}

// TestStaleCompletionCannotClearNewerReplicationRequest drives the real
// rejection path: the first request is held, released as a rejection, and the
// loop immediately issues a second request. Re-delivering the first request's
// completion must not release or mutate the second request.
func TestStaleCompletionCannotClearNewerReplicationRequest(t *testing.T) {
	gated := newGatedTransport()
	// Hold the first request and the follow-up request the loop issues once the
	// first is rejected, so both generations can be observed.
	arrived := gated.BlockAppend(2)
	// The leader has a longer log than the follower, so the first AppendEntries
	// is rejected and the loop walks nextIndex back down.
	leaderStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, leaderStorage, gated, NewTestStateMachine(), nil, time.Second)
	machine := NewTestStateMachine()
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, machine, nil, time.Second)
	gated.Connect("l", leader)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	first := waitActiveReplication(t, leader, "f")
	firstArgs := gated.Append()
	gated.ReleaseAppend()

	// The rejection walks nextIndex back and immediately issues a new request,
	// which parks on a fresh gate.
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("follow-up AppendEntries was not issued")
	}
	second := waitActiveReplication(t, leader, "f")
	if second.generation <= first.generation {
		t.Fatalf("generation did not advance: first=%d second=%d", first.generation, second.generation)
	}
	before, err := leader.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stale := appendEntriesReplyEvent{
		target:  "f",
		request: first,
		entries: firstArgs,
		reply:   AppendEntriesReply{Term: second.term, Success: true},
	}
	if err := leader.enqueue(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if err := leader.enqueue(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	after, err := leader.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.MatchIndex["f"] != before.MatchIndex["f"] || after.NextIndex["f"] != before.NextIndex["f"] {
		t.Fatalf("stale completion mutated replication bookkeeping: before=%#v after=%#v", before, after)
	}
	observed, active, err := activeReplicationForTest(leader, context.Background(), "f")
	if err != nil || !active {
		t.Fatalf("stale completion released the newer slot: active=%v err=%v", active, err)
	}
	if observed != second {
		t.Fatalf("newer slot was replaced: got %#v want %#v", observed, second)
	}
	gated.ReleaseAppend()
	waitFor(t, "replication to converge after stale completions", func() bool {
		state, err := leader.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 2
	})
	// The fixture's entries are from a previous term, so nothing is committable
	// and the state machine must not have been advanced. What the stale
	// completions must not have done is corrupt the log the follower received.
	final, err := follower.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Log) != 2 {
		t.Fatalf("follower log has %d entries, want 2: %#v", len(final.Log), final.Log)
	}
	for i, entry := range final.Log {
		if entry.Index != LogIndex(i+1) || entry.Term != 1 {
			t.Fatalf("follower log entry %d is %#v, want term 1", i, entry)
		}
	}
	if count := machine.CommandCount(); count != 0 {
		t.Fatalf("stale completions advanced the state machine: %d commands", count)
	}
}

// TestDuplicateCompletionIsIdempotent delivers the same successful completion
// twice and asserts neither the follower's applied state nor the replication
// bookkeeping moves a second time.
func TestDuplicateCompletionIsIdempotent(t *testing.T) {
	gated := newGatedTransport()
	arrived := gated.BlockAppend(1)
	leaderStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}}}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, leaderStorage, gated, NewTestStateMachine(), nil, time.Second)
	machine := NewTestStateMachine()
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, machine, nil, time.Second)
	gated.Connect("l", leader)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	active := waitActiveReplication(t, leader, "f")
	args := gated.Append()
	gated.ReleaseAppend()
	waitFor(t, "initial replication to commit", func() bool {
		state, err := leader.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 1
	})
	duplicate := appendEntriesReplyEvent{
		target:  "f",
		request: active,
		entries: args,
		reply:   AppendEntriesReply{Term: active.term, Success: true},
	}
	for i := 0; i < 3; i++ {
		if err := leader.enqueue(context.Background(), duplicate); err != nil {
			t.Fatal(err)
		}
	}
	state, err := leader.DebugState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.MatchIndex["f"] != 1 || state.NextIndex["f"] != 2 {
		t.Fatalf("duplicate completion moved bookkeeping: %#v", state)
	}
	if _, active, err := activeReplicationForTest(leader, context.Background(), "f"); err != nil || active {
		t.Fatalf("duplicate completion left a phantom slot: active=%v err=%v", active, err)
	}
}

// TestTransportFailureRetriesOnNextHeartbeat proves a failed attempt releases
// the peer so the following heartbeat can retry, rather than wedging it.
func TestTransportFailureRetriesOnNextHeartbeat(t *testing.T) {
	gated := newGatedTransport()
	gated.FailWith(errors.New("injected transport failure"))
	leaderStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}}}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, leaderStorage, gated, NewTestStateMachine(), nil, time.Second)
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("l", leader)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retry after transport failure", func() bool {
		appends, _ := gated.Counts()
		return appends >= 3
	})
	waitNoActiveReplication(t, leader, "f")
	state, err := leader.DebugState(context.Background())
	if err != nil || state.Role != Leader {
		t.Fatalf("leader did not survive repeated failures: %#v %v", state, err)
	}
	gated.FailWith(nil)
	var lastState DebugState
	var lastErr error
	var lastActive replicationRequest
	var lastActiveOK bool
	recovered := waitForOK(t, "recovery once transport heals", func() bool {
		lastState, lastErr = leader.DebugState(context.Background())
		lastActive, lastActiveOK, _ = activeReplicationForTest(leader, context.Background(), "f")
		return lastErr == nil && lastState.MatchIndex["f"] == 1
	})
	if !recovered {
		fState, _ := follower.DebugState(context.Background())
		a, sn := gated.Counts()
		t.Fatalf("recovery: appends=%d snapshots=%d leader=%#v err=%v active=%v %+v lastAppend=%#v follower=%#v", a, sn, lastState, lastErr, lastActiveOK, lastActive, gated.Append(), fState)
	}
}

// TestLeaderHeartbeatSurvivesBusyEventStream covers a leader that must keep
// replicating while its own event loop is saturated. The heartbeat is armed once
// when leadership starts and re-arms on expiry, so a stream of events arriving
// faster than HeartbeatInterval cannot starve it. If the timer were re-armed per
// event instead, replication to the peer would stop entirely and this test would
// time out.
func TestLeaderHeartbeatSurvivesBusyEventStream(t *testing.T) {
	gated := newGatedTransport()
	leaderStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}}}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, leaderStorage, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("l", leader)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	// Saturate the event loop with events that arrive far faster than the 5ms
	// heartbeat interval.
	stop := make(chan struct{})
	saturated := make(chan struct{})
	go func() {
		defer close(saturated)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := leader.DebugState(context.Background()); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-saturated })
	waitFor(t, "replication while the event loop is saturated", func() bool {
		appends, _ := gated.Counts()
		return appends >= 3
	})
	waitNoActiveReplication(t, leader, "f")
}

// TestReplicationTimeoutReleasesBlockedPeer proves an attempt that never
// completes still terminates: the bounded attempt reports an error, the slot is
// released, and replication to that peer resumes.
func TestReplicationTimeoutReleasesBlockedPeer(t *testing.T) {
	gated := newGatedTransport()
	arrived := gated.BlockAppend(2)
	leaderStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}}}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, leaderStorage, gated, NewTestStateMachine(), nil, 30*time.Millisecond)
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("l", leader)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	first := waitActiveReplication(t, leader, "f")
	// The gate is never released, so the attempt can only end by timeout.
	waitNoActiveReplication(t, leader, "f")
	waitFor(t, "retry after attempt timeout", func() bool {
		appends, _ := gated.Counts()
		return appends >= 2
	})
	second := waitActiveReplication(t, leader, "f")
	if second.generation <= first.generation {
		t.Fatalf("retry reused a generation: first=%d second=%d", first.generation, second.generation)
	}
	gated.ReleaseAppend()
	waitFor(t, "replication after timeout recovery", func() bool {
		state, err := leader.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 1
	})
}

// TestNewLeadershipTenureStartsWithNoStaleReplication proves a step-down
// discards outstanding requests, so the next tenure can immediately replicate
// to the same peer and an old completion cannot affect it.
func TestNewLeadershipTenureStartsWithNoStaleReplication(t *testing.T) {
	gated := newGatedTransport()
	arrived := gated.BlockAppend(2)
	leaderStorage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}}}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, leaderStorage, gated, NewTestStateMachine(), nil, time.Second)
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("l", leader)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	old := waitActiveReplication(t, leader, "f")

	// A higher-term candidate forces this node down.
	if _, err := leader.RequestVote(context.Background(), RequestVoteArgs{Term: old.term + 5, CandidateID: "f"}); err != nil {
		t.Fatal(err)
	}
	state, err := leader.DebugState(context.Background())
	if err != nil || state.Role != Follower {
		t.Fatalf("higher-term vote did not force step-down: %#v %v", state, err)
	}
	waitNoActiveReplication(t, leader, "f")

	before, _ := gated.Counts()
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "new tenure to launch replication", func() bool {
		after, _ := gated.Counts()
		return after > before
	})
	fresh := waitActiveReplication(t, leader, "f")
	if fresh.generation <= old.generation {
		t.Fatalf("new tenure reused an old generation: old=%d new=%d", old.generation, fresh.generation)
	}
	gated.ReleaseAppend()
	waitFor(t, "replication in the new tenure", func() bool {
		state, err := leader.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 1
	})
}

// TestStopWithOutstandingReplicationDoesNotBlockRestart stops a node while a
// request is in flight and verifies the restart is not wedged by it.
func TestStopWithOutstandingReplicationDoesNotBlockRestart(t *testing.T) {
	gated := newGatedTransport()
	arrived := gated.BlockAppend(1)
	storage := &testStorage{state: PersistentState{CurrentTerm: 1, Log: []LogEntry{{Term: 1, Index: 1}}}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, storage, gated, NewTestStateMachine(), nil, time.Second)
	followerStorage := &testStorage{state: PersistentState{CurrentTerm: 1}}
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, followerStorage, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("l", leader)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no AppendEntries call reached the transport")
	}
	waitActiveReplication(t, leader, "f")
	if err := leader.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Restart against the same durable state while the old call is still parked.
	restarted := lifecycleNode(t, "l", []NodeID{"f"}, storage, gated, NewTestStateMachine(), nil, time.Second)
	gated.Connect("l", restarted)
	if err := triggerBecomeLeaderForTest(restarted, context.Background()); err != nil {
		t.Fatal(err)
	}
	waitActiveReplication(t, restarted, "f")
	// Releasing the old call delivers a completion to the stopped node, which
	// must be discarded rather than mutating the restarted node.
	gated.ReleaseAppend()
	waitFor(t, "restarted node to replicate", func() bool {
		state, err := restarted.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 1
	})
}

// TestHeldSnapshotDrivesSuffixExactlyOnce holds InstallSnapshot while heartbeats
// tick, then releases it and checks that the post-snapshot suffix is replicated
// immediately without any further duplicate snapshot.
func TestHeldSnapshotDrivesSuffixExactlyOnce(t *testing.T) {
	// A snapshot holding two already-applied commands, plus a retained suffix.
	data := []byte{1, 'a', 1, 'b'}
	snapshotStore := &memorySnapshotStorage{snapshot: Snapshot{Version: SnapshotVersion, LastIncludedIndex: 2, LastIncludedTerm: 1, StateMachineData: data}}
	gated := newGatedTransport()
	arrived := gated.BlockSnapshot(1)
	leaderStorage := &testStorage{state: PersistentState{
		CurrentTerm:      1,
		SnapshotBoundary: LogBoundary{Index: 2, Term: 1},
		CommitIndex:      3,
		Log:              []LogEntry{{Term: 1, Index: 3, Command: []byte("c")}},
	}}
	leader := lifecycleNode(t, "l", []NodeID{"f"}, leaderStorage, gated, NewTestStateMachine(), snapshotStore, time.Second)
	machine := NewTestStateMachine()
	follower := lifecyclePeer(t, "f", []NodeID{"l"}, &testStorage{state: PersistentState{CurrentTerm: 1}}, gated, machine, &memorySnapshotStorage{}, time.Second)
	gated.Connect("l", leader)
	gated.Connect("f", follower)
	if err := triggerBecomeLeaderForTest(leader, context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("no InstallSnapshot call reached the transport")
	}
	_, held := gated.Counts()
	if held != 1 {
		t.Fatalf("snapshot call count = %d", held)
	}
	active := waitActiveReplication(t, leader, "f")
	if active.kind != replicationSnapshot || active.index != 2 {
		t.Fatalf("active snapshot request = %#v", active)
	}
	time.Sleep(200 * time.Millisecond)
	if _, after := gated.Counts(); after != held {
		t.Fatalf("held snapshot was relaunched: %d -> %d", held, after)
	}
	gated.ReleaseSnapshot()
	waitFor(t, "snapshot suffix repair", func() bool {
		state, err := leader.DebugState(context.Background())
		return err == nil && state.MatchIndex["f"] == 3 && state.NextIndex["f"] == 4
	})
	if _, snapshotsSent := gated.Counts(); snapshotsSent != 1 {
		t.Fatalf("snapshot was sent %d times", snapshotsSent)
	}
	waitFor(t, "follower to install snapshot and suffix", func() bool {
		state, err := follower.DebugState(context.Background())
		return err == nil && state.SnapshotBoundary.Index == 2 && state.CommitIndex == 3 && state.LastApplied == 3
	})
}
