package raft

import (
	"context"
	"errors"
)

type nodeEvent interface {
	handle(*runtimeState) bool
}

type stopEvent struct{}

func (stopEvent) handle(s *runtimeState) bool {
	s.clearActiveReplication()
	s.completeAllProposals(ErrProposalStopped)
	return true
}

type electionTimeoutEvent struct{}

func (electionTimeoutEvent) handle(s *runtimeState) bool {
	s.onElectionTimeout()
	return false
}

type becomeLeaderEvent struct{}

func (becomeLeaderEvent) handle(s *runtimeState) bool {
	s.persistent.CurrentTerm++
	s.role = Candidate
	s.becomeLeader()
	return false
}

type compactEvent struct {
	index LogIndex
	reply chan error
}

func (e compactEvent) handle(s *runtimeState) bool {
	if e.index == 0 || e.index > s.lastApplied || e.index > s.commitIndex {
		e.reply <- errors.New("snapshot index must be committed and applied")
		return false
	}
	entry, ok := s.log.entry(e.index)
	if !ok {
		e.reply <- errors.New("snapshot index is not retained")
		return false
	}
	data, err := s.config.StateMachine.Snapshot(context.Background())
	if err != nil {
		e.reply <- err
		return false
	}
	snapshot := Snapshot{Version: SnapshotVersion, LastIncludedIndex: e.index, LastIncludedTerm: entry.Term, StateMachineData: append([]byte(nil), data...)}
	if s.config.SnapshotStorage == nil {
		e.reply <- errors.New("snapshot storage is required")
		return false
	}
	if err := s.config.SnapshotStorage.SaveSnapshot(context.Background(), snapshot); err != nil {
		e.reply <- err
		return false
	}
	candidateLog := &raftLog{boundary: LogBoundary{Index: e.index, Term: entry.Term}, entries: s.log.snapshot()}
	candidateLog.truncateFrom(e.index + 1)
	candidate := clonePersistentState(s.persistent)
	candidate.Log = candidateLog.snapshot()
	candidate.SnapshotBoundary = candidateLog.boundary
	if err := s.config.Storage.Save(context.Background(), candidate); err != nil {
		e.reply <- err
		return false
	}
	s.log = candidateLog
	s.persistent = candidate
	e.reply <- nil
	return false
}

type appendTestEntryEvent struct {
	entry LogEntry
	reply chan error
}

func (e appendTestEntryEvent) handle(s *runtimeState) bool {
	if s.role != Leader {
		e.reply <- ErrNotLeader
		return false
	}
	if err := s.log.append(e.entry); err != nil {
		e.reply <- err
		return false
	}
	state := clonePersistentState(s.persistent)
	state.Log = s.log.snapshot()
	err := s.config.Storage.Save(context.Background(), state)
	if err == nil {
		s.persistent = state
		s.matchIndex[s.config.ID] = s.log.lastIndex()
		s.sendAppendEntries()
	}
	e.reply <- err
	return false
}

type roleReply struct {
	role Role
	err  error
}
type roleEvent struct{ reply chan roleReply }

func (e roleEvent) handle(s *runtimeState) bool {
	e.reply <- roleReply{role: s.role}
	return false
}

type leaderReply struct {
	id  NodeID
	err error
}
type leaderEvent struct{ reply chan leaderReply }

func (e leaderEvent) handle(s *runtimeState) bool {
	e.reply <- leaderReply{id: s.leaderID}
	return false
}

type DebugState struct {
	Role             Role
	Term             Term
	LeaderID         NodeID
	Log              []LogEntry
	SnapshotBoundary LogBoundary
	CommitIndex      LogIndex
	LastApplied      LogIndex
	NextIndex        map[NodeID]LogIndex
	MatchIndex       map[NodeID]LogIndex
}
type debugEvent struct{ reply chan DebugState }

func (e debugEvent) handle(s *runtimeState) bool {
	state := DebugState{Role: s.role, Term: s.persistent.CurrentTerm, LeaderID: s.leaderID, Log: s.log.snapshot(), SnapshotBoundary: s.log.boundary, CommitIndex: s.commitIndex, LastApplied: s.lastApplied, NextIndex: map[NodeID]LogIndex{}, MatchIndex: map[NodeID]LogIndex{}}
	for id, index := range s.nextIndex {
		state.NextIndex[id] = index
	}
	for id, index := range s.matchIndex {
		state.MatchIndex[id] = index
	}
	e.reply <- state
	return false
}

type requestVoteEvent struct {
	request RequestVoteArgs
	reply   chan RequestVoteReply
}

func (e requestVoteEvent) handle(s *runtimeState) bool {
	e.reply <- s.handleVoteRequest(e.request)
	return false
}

type voteReplyEvent struct {
	target       NodeID
	electionTerm Term
	reply        RequestVoteReply
}

func (e voteReplyEvent) handle(s *runtimeState) bool {
	s.handleVoteReply(e.target, e.electionTerm, e.reply)
	return false
}

type installSnapshotEvent struct {
	request InstallSnapshotArgs
	reply   chan InstallSnapshotReply
}

func (e installSnapshotEvent) handle(s *runtimeState) bool {
	reply, terminal := s.handleInstallSnapshot(e.request)
	e.reply <- reply
	return terminal
}

type appendEntriesEvent struct {
	request AppendEntriesArgs
	reply   chan AppendEntriesReply
}

func (e appendEntriesEvent) handle(s *runtimeState) bool {
	e.reply <- s.handleAppendEntries(e.request)
	return false
}

type appendEntriesReplyEvent struct {
	target  NodeID
	request replicationRequest
	entries AppendEntriesArgs
	reply   AppendEntriesReply
	err     error
}

func (e appendEntriesReplyEvent) handle(s *runtimeState) bool {
	s.handleAppendEntriesReply(e)
	return false
}

func (n *Node) RequestVote(ctx context.Context, request RequestVoteArgs) (RequestVoteReply, error) {
	reply := make(chan RequestVoteReply, 1)
	if err := n.enqueue(ctx, requestVoteEvent{request: request, reply: reply}); err != nil {
		return RequestVoteReply{}, err
	}
	select {
	case result := <-reply:
		return result, nil
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	}
}

func (n *Node) InstallSnapshot(ctx context.Context, request InstallSnapshotArgs) (InstallSnapshotReply, error) {
	reply := make(chan InstallSnapshotReply, 1)
	if err := n.enqueue(ctx, installSnapshotEvent{request: request, reply: reply}); err != nil {
		return InstallSnapshotReply{}, err
	}
	select {
	case result := <-reply:
		return result, nil
	case <-ctx.Done():
		return InstallSnapshotReply{}, ctx.Err()
	}
}

func (n *Node) AppendEntries(ctx context.Context, request AppendEntriesArgs) (AppendEntriesReply, error) {
	reply := make(chan AppendEntriesReply, 1)
	if err := n.enqueue(ctx, appendEntriesEvent{request: request, reply: reply}); err != nil {
		return AppendEntriesReply{}, err
	}
	select {
	case result := <-reply:
		return result, nil
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	}
}
