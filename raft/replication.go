package raft

import "context"

func (s *runtimeState) sendAppendEntries() {
	if s.role != Leader {
		return
	}
	term := s.persistent.CurrentTerm
	for _, peer := range s.config.Peers {
		if peer == s.config.ID {
			continue
		}
		next := s.nextIndex[peer]
		if next <= s.log.boundary.Index {
			if send, ok := s.snapshotRequest(); ok {
				go s.requestInstallSnapshot(peer, s.persistent.CurrentTerm, send)
			}
			continue
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
		request := AppendEntriesArgs{
			Term:         term,
			LeaderID:     s.config.ID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}
		go s.requestAppendEntries(peer, term, request)
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

func (s *runtimeState) requestInstallSnapshot(target NodeID, term Term, request InstallSnapshotArgs) {
	reply, err := s.config.Transport.InstallSnapshot(context.Background(), target, request)
	if err != nil {
		return
	}
	s.node.enqueueBackground(installSnapshotReplyEvent{target: target, leaderTerm: term, request: request, reply: reply})
}

func (s *runtimeState) requestAppendEntries(target NodeID, term Term, request AppendEntriesArgs) {
	reply, err := s.config.Transport.AppendEntries(context.Background(), target, request)
	if err != nil {
		return
	}
	s.node.enqueueBackground(appendEntriesReplyEvent{target: target, leaderTerm: term, request: request, reply: reply})
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
	if event.reply.Term > s.persistent.CurrentTerm {
		_ = s.updateTerm(event.reply.Term)
		return
	}
	if s.role != Leader || event.leaderTerm != s.persistent.CurrentTerm || event.reply.Term != event.leaderTerm {
		return
	}
	if event.reply.Success {
		lastSent := event.request.PrevLogIndex + LogIndex(len(event.request.Entries))
		if lastSent > s.matchIndex[event.target] {
			s.matchIndex[event.target] = lastSent
		}
		if next := s.matchIndex[event.target] + 1; next > s.nextIndex[event.target] {
			s.nextIndex[event.target] = next
		}
		s.advanceCommitIndex()
		return
	}
	if s.nextIndex[event.target] > 1 {
		s.nextIndex[event.target]--
	} else {
		s.nextIndex[event.target] = 1
	}
	s.sendAppendEntriesTo(event.target)
}

func (s *runtimeState) sendAppendEntriesTo(peer NodeID) {
	if s.role != Leader {
		return
	}
	next := s.nextIndex[peer]
	if next <= s.log.boundary.Index {
		if request, ok := s.snapshotRequest(); ok {
			go s.requestInstallSnapshot(peer, s.persistent.CurrentTerm, request)
		}
		return
	}
	prevIndex := next - 1
	prevTerm, _ := s.log.term(prevIndex)
	entries := make([]LogEntry, 0)
	for index := next; index <= s.log.lastIndex(); index++ {
		entry, ok := s.log.entry(index)
		if ok {
			entries = append(entries, entry)
		}
	}
	request := AppendEntriesArgs{Term: s.persistent.CurrentTerm, LeaderID: s.config.ID, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: s.commitIndex}
	go s.requestAppendEntries(peer, s.persistent.CurrentTerm, request)
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
	target     NodeID
	leaderTerm Term
	request    InstallSnapshotArgs
	reply      InstallSnapshotReply
}

func (e installSnapshotReplyEvent) handle(s *runtimeState) bool {
	if e.reply.Term > s.persistent.CurrentTerm {
		_ = s.updateTerm(e.reply.Term)
		return false
	}
	if s.role != Leader || e.leaderTerm != s.persistent.CurrentTerm || e.reply.Term != e.leaderTerm {
		return false
	}
	if e.request.LastIncludedIndex >= s.nextIndex[e.target] {
		s.matchIndex[e.target] = e.request.LastIncludedIndex
		s.nextIndex[e.target] = e.request.LastIncludedIndex + 1
		s.sendAppendEntriesTo(e.target)
	}
	return false
}

func minIndex(a, b LogIndex) LogIndex {
	if a < b {
		return a
	}
	return b
}
