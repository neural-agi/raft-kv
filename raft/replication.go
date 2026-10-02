package raft

import "context"

type replicationKind uint8

const (
	replicationAppend replicationKind = iota + 1
	replicationSnapshot
)

type replicationRequest struct {
	generation uint64
	kind       replicationKind
	term       Term
	index      LogIndex
}

func (s *runtimeState) beginReplication(peer NodeID, kind replicationKind, term Term, index LogIndex) (replicationRequest, bool) {
	if _, ok := s.replication[peer]; ok {
		return replicationRequest{}, false
	}
	s.nextReplication++
	request := replicationRequest{generation: s.nextReplication, kind: kind, term: term, index: index}
	s.replication[peer] = request
	return request, true
}

func (s *runtimeState) finishReplication(peer NodeID, request replicationRequest) bool {
	active, ok := s.replication[peer]
	if !ok || active != request {
		return false
	}
	delete(s.replication, peer)
	return true
}

// clearActiveReplication releases every outstanding per-peer replication
// request. It is event-loop owned and must be called whenever this node stops
// leading, so that the next leadership tenure starts with no occupied slot.
// Completions belonging to a discarded request are rejected by generation
// matching, because generations are never reused.
func (s *runtimeState) clearActiveReplication() {
	clear(s.replication)
}

func (s *runtimeState) sendAppendEntries() {
	if s.role != Leader {
		return
	}
	for _, peer := range s.config.Peers {
		if peer == s.config.ID {
			continue
		}
		s.sendAppendEntriesTo(peer)
	}
}

func (s *runtimeState) snapshotRequest() (InstallSnapshotArgs, bool) {
	if s.log.boundary.Index == 0 || s.config.SnapshotStorage == nil {
		return InstallSnapshotArgs{}, false
	}
	snapshot, err := s.config.SnapshotStorage.LoadSnapshot(context.Background())
	if err != nil || snapshot.LastIncludedIndex != s.log.boundary.Index || snapshot.LastIncludedTerm != s.log.boundary.Term {
		return InstallSnapshotArgs{}, false
	}
	return InstallSnapshotArgs{
		Term:              s.persistent.CurrentTerm,
		LeaderID:          s.config.ID,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              append([]byte(nil), snapshot.StateMachineData...),
	}, true
}

func (s *runtimeState) sendSnapshot(target NodeID) {
	request, ok := s.snapshotRequest()
	if !ok {
		return
	}
	transfer, ok := s.beginReplication(target, replicationSnapshot, request.Term, request.LastIncludedIndex)
	if !ok {
		return
	}
	go s.requestInstallSnapshot(target, transfer, request)
}

func (s *runtimeState) requestInstallSnapshot(target NodeID, transfer replicationRequest, request InstallSnapshotArgs) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReplicationTimeout)
	defer cancel()
	reply, err := s.config.Transport.InstallSnapshot(ctx, target, request)
	s.node.enqueueCompletion(installSnapshotReplyEvent{target: target, request: transfer, reply: reply, err: err})
}

func (s *runtimeState) requestAppendEntries(target NodeID, transfer replicationRequest, request AppendEntriesArgs) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReplicationTimeout)
	defer cancel()
	reply, err := s.config.Transport.AppendEntries(ctx, target, request)
	s.node.enqueueCompletion(appendEntriesReplyEvent{target: target, request: transfer, entries: request, reply: reply, err: err})
}

func (s *runtimeState) handleInstallSnapshot(request InstallSnapshotArgs) (InstallSnapshotReply, bool) {
	reply := InstallSnapshotReply{Term: s.persistent.CurrentTerm}
	if request.Term < s.persistent.CurrentTerm {
		return reply, false
	}
	if request.Term > s.persistent.CurrentTerm {
		if err := s.updateTerm(request.Term); err != nil {
			return reply, false
		}
	}
	if request.LastIncludedIndex <= s.log.boundary.Index {
		return reply, false
	}
	if request.LastIncludedTerm == 0 || request.LastIncludedIndex == 0 {
		return reply, false
	}
	if s.config.SnapshotStorage == nil {
		return reply, false
	}
	snapshot := Snapshot{Version: SnapshotVersion, LastIncludedIndex: request.LastIncludedIndex, LastIncludedTerm: request.LastIncludedTerm, StateMachineData: append([]byte(nil), request.Data...)}
	if err := snapshot.Validate(); err != nil {
		return reply, false
	}
	candidateLog := &raftLog{boundary: LogBoundary{Index: request.LastIncludedIndex, Term: request.LastIncludedTerm}}
	for index := request.LastIncludedIndex + 1; index <= s.log.lastIndex(); index++ {
		if entry, ok := s.log.entry(index); ok {
			_ = candidateLog.append(entry)
		}
	}
	candidate := clonePersistentState(s.persistent)
	candidate.Log = candidateLog.snapshot()
	candidate.CommitIndex = maxIndex(s.commitIndex, request.LastIncludedIndex)
	candidate.SnapshotBoundary = candidateLog.boundary
	if err := s.config.SnapshotStorage.SaveSnapshot(context.Background(), snapshot); err != nil {
		return reply, false
	}
	if err := s.config.Storage.Save(context.Background(), candidate); err != nil {
		return reply, false
	}
	if err := s.config.StateMachine.Restore(context.Background(), snapshot.StateMachineData); err != nil {
		return reply, true
	}
	s.log, s.persistent = candidateLog, candidate
	s.commitIndex = candidate.CommitIndex
	s.lastApplied = request.LastIncludedIndex
	s.leaderID = request.LeaderID
	s.resetElectionTimer = true
	reply.Term = s.persistent.CurrentTerm
	return reply, false
}

func maxIndex(a, b LogIndex) LogIndex {
	if a > b {
		return a
	}
	return b
}

func (s *runtimeState) handleAppendEntries(request AppendEntriesArgs) AppendEntriesReply {
	reply := AppendEntriesReply{Term: s.persistent.CurrentTerm}
	if request.Term < s.persistent.CurrentTerm {
		return reply
	}
	if request.Term > s.persistent.CurrentTerm {
		if err := s.updateTerm(request.Term); err != nil {
			return reply
		}
	}
	if request.Term == s.persistent.CurrentTerm {
		if s.role != Follower {
			s.role = Follower
			s.clearActiveReplication()
			s.invalidateLostProposals()
			s.electionTerm = 0
			s.votes = nil
			s.votedReplies = nil
		}
	}
	reply.Term = s.persistent.CurrentTerm
	if !s.log.matches(request.PrevLogIndex, request.PrevLogTerm) {
		return reply
	}
	s.leaderID = request.LeaderID
	s.resetElectionTimer = true

	candidateLog := &raftLog{boundary: s.log.boundary, entries: s.log.snapshot()}
	logChanged := false
	for i, incoming := range request.Entries {
		index := incoming.Index
		existing, ok := candidateLog.entry(index)
		if ok {
			if existing.Term != incoming.Term {
				if err := candidateLog.replaceFrom(index, request.Entries[i:]); err != nil {
					return reply
				}
				logChanged = true
				break
			}
			continue
		}
		if err := candidateLog.append(request.Entries[i:]...); err != nil {
			return reply
		}
		logChanged = true
		break
	}
	if logChanged {
		state := clonePersistentState(s.persistent)
		state.Log = candidateLog.snapshot()
		if err := s.config.Storage.Save(context.Background(), state); err != nil {
			return reply
		}
		s.log = candidateLog
		s.persistent = state
		s.invalidateLostProposals()
	}
	if request.LeaderCommit > s.commitIndex {
		s.publishCommit(minIndex(request.LeaderCommit, s.log.lastIndex()))
	}
	reply.Success = true
	return reply
}

func (s *runtimeState) handleAppendEntriesReply(event appendEntriesReplyEvent) {
	if !s.finishReplication(event.target, event.request) {
		return
	}
	if event.err != nil {
		// The attempt never produced a reply, so this peer learned nothing about
		// its log. Release the slot (done above) and back nextIndex off by one so
		// the next heartbeat probes from a shorter prefix. Without this the peer
		// would be retried with the identical request forever and could never
		// converge once the transport recovered. The retry is deliberately left
		// to the heartbeat rather than dispatched here, so a persistently failing
		// peer cannot spin the event loop.
		if s.role == Leader && event.request.term == s.persistent.CurrentTerm {
			s.backOffNextIndex(event.target)
		}
		return
	}
	if event.reply.Term > s.persistent.CurrentTerm {
		_ = s.updateTerm(event.reply.Term)
		return
	}
	if s.role != Leader || event.request.term != s.persistent.CurrentTerm || event.reply.Term != event.request.term {
		return
	}
	if event.reply.Success {
		lastSent := event.entries.PrevLogIndex + LogIndex(len(event.entries.Entries))
		if lastSent > s.matchIndex[event.target] {
			s.matchIndex[event.target] = lastSent
		}
		if next := s.matchIndex[event.target] + 1; next > s.nextIndex[event.target] {
			s.nextIndex[event.target] = next
		}
		s.advanceCommitIndex()
		return
	}
	s.backOffNextIndex(event.target)
	s.sendAppendEntriesTo(event.target)
}

func (s *runtimeState) backOffNextIndex(peer NodeID) {
	if s.nextIndex[peer] > 1 {
		s.nextIndex[peer]--
	} else {
		s.nextIndex[peer] = 1
	}
}

func (s *runtimeState) sendAppendEntriesTo(peer NodeID) {
	if s.role != Leader {
		return
	}
	next := s.nextIndex[peer]
	if next <= s.log.boundary.Index {
		s.sendSnapshot(peer)
		return
	}
	prevIndex := LogIndex(0)
	prevTerm := Term(0)
	if next > 0 {
		prevIndex = next - 1
		if termAt, ok := s.log.term(prevIndex); ok {
			prevTerm = termAt
		}
	}
	entries := make([]LogEntry, 0)
	for index := next; index <= s.log.lastIndex(); index++ {
		entry, ok := s.log.entry(index)
		if ok {
			entries = append(entries, entry)
		}
	}
	request := AppendEntriesArgs{Term: s.persistent.CurrentTerm, LeaderID: s.config.ID, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: s.commitIndex}
	transfer, ok := s.beginReplication(peer, replicationAppend, request.Term, prevIndex+LogIndex(len(entries)))
	if !ok {
		return
	}
	go s.requestAppendEntries(peer, transfer, request)
}

func (s *runtimeState) advanceCommitIndex() {
	for index := s.log.lastIndex(); index > s.commitIndex; index-- {
		term, ok := s.log.term(index)
		if !ok || term != s.persistent.CurrentTerm {
			continue
		}
		count := 1
		for _, peer := range s.config.Peers {
			if peer != s.config.ID && s.matchIndex[peer] >= index {
				count++
			}
		}
		if count*2 > len(s.config.Peers)+1 {
			s.publishCommit(index)
			return
		}
	}
}

type installSnapshotReplyEvent struct {
	target  NodeID
	request replicationRequest
	reply   InstallSnapshotReply
	err     error
}

func (e installSnapshotReplyEvent) handle(s *runtimeState) bool {
	if !s.finishReplication(e.target, e.request) {
		return false
	}
	if e.err != nil {
		return false
	}
	if e.reply.Term > s.persistent.CurrentTerm {
		_ = s.updateTerm(e.reply.Term)
		return false
	}
	if s.role != Leader || e.request.kind != replicationSnapshot || e.request.term != s.persistent.CurrentTerm || e.reply.Term != e.request.term {
		return false
	}
	if e.request.index < s.nextIndex[e.target] {
		return false
	}
	s.matchIndex[e.target] = e.request.index
	s.nextIndex[e.target] = e.request.index + 1
	s.sendAppendEntriesTo(e.target)
	return false
}

func minIndex(a, b LogIndex) LogIndex {
	if a < b {
		return a
	}
	return b
}
