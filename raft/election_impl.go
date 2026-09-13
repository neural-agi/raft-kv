package raft

import (
	"context"
	"math/rand"
	"time"
)

type runtimeState struct {
	node       *Node
	config     Config
	persistent PersistentState
	log        *raftLog
	role       Role
	leaderID   NodeID

	commitIndex LogIndex
	lastApplied LogIndex

	nextIndex  map[NodeID]LogIndex
	matchIndex map[NodeID]LogIndex

	electionTerm       Term
	votes              map[NodeID]struct{}
	votedReplies       map[NodeID]struct{}
	resetElectionTimer bool
	random             *rand.Rand

	proposals    map[LogIndex]*proposalWaiter
	applyResults map[LogIndex][]byte
}

func newRuntimeState(node *Node, config Config, persistent PersistentState) (*runtimeState, error) {
	rng := config.Random
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	log, err := newRaftLog(persistent.Log)
	if err != nil {
		return nil, err
	}
	return &runtimeState{
		node:         node,
		config:       config,
		persistent:   clonePersistentState(persistent),
		log:          log,
		role:         Follower,
		commitIndex:  persistent.CommitIndex,
		lastApplied:  persistent.CommitIndex,
		random:       rng,
		proposals:    make(map[LogIndex]*proposalWaiter),
		applyResults: make(map[LogIndex][]byte),
	}, nil
}

func (s *runtimeState) nextElectionTimeout() time.Duration {
	span := s.config.ElectionTimeoutMax - s.config.ElectionTimeoutMin
	return s.config.ElectionTimeoutMin + time.Duration(s.random.Int63n(int64(span)))
}

func (s *runtimeState) nextHeartbeatTimeout() time.Duration {
	return s.config.HeartbeatInterval
}

func (s *runtimeState) lastLogIndexTerm() (LogIndex, Term) {
	return s.log.lastIndex(), s.log.lastTerm()
}

func (s *runtimeState) onElectionTimeout() {
	if s.role == Leader {
		return
	}
	term := s.persistent.CurrentTerm + 1
	candidate := clonePersistentState(s.persistent)
	candidate.CurrentTerm = term
	candidate.VotedFor = s.config.ID
	if err := s.config.Storage.Save(context.Background(), candidate); err != nil {
		return
	}
	s.persistent = candidate
	s.role = Candidate
	s.leaderID = ""
	s.electionTerm = term
	s.votes = map[NodeID]struct{}{s.config.ID: {}}
	s.votedReplies = map[NodeID]struct{}{s.config.ID: {}}
	s.resetElectionTimer = true
	if s.hasMajority() {
		s.becomeLeader()
		return
	}
	lastIndex, lastTerm := s.lastLogIndexTerm()
	request := RequestVoteArgs{Term: term, CandidateID: s.config.ID, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	for _, peer := range s.config.Peers {
		if peer != s.config.ID {
			go s.requestVote(peer, term, request)
		}
	}
}

func (s *runtimeState) requestVote(target NodeID, term Term, request RequestVoteArgs) {
	reply, err := s.config.Transport.RequestVote(context.Background(), target, request)
	if err != nil {
		return
	}
	s.node.enqueueBackground(voteReplyEvent{target: target, electionTerm: term, reply: reply})
}

func (s *runtimeState) hasMajority() bool {
	return len(s.votes)*2 > len(s.config.Peers)+1
}

func (s *runtimeState) becomeLeader() {
	s.role = Leader
	s.leaderID = s.config.ID
	s.nextIndex = make(map[NodeID]LogIndex, len(s.config.Peers))
	s.matchIndex = make(map[NodeID]LogIndex, len(s.config.Peers))
	lastIndex := s.log.lastIndex()
	for _, peer := range s.config.Peers {
		s.nextIndex[peer] = lastIndex + 1
		s.matchIndex[peer] = 0
	}
	s.matchIndex[s.config.ID] = lastIndex
	s.sendAppendEntries()
}

func (s *runtimeState) handleVoteRequest(request RequestVoteArgs) RequestVoteReply {
	reply := RequestVoteReply{Term: s.persistent.CurrentTerm}
	if request.Term < s.persistent.CurrentTerm {
		return reply
	}
	if request.Term > s.persistent.CurrentTerm {
		if err := s.updateTerm(request.Term); err != nil {
			return reply
		}
	}
	reply.Term = s.persistent.CurrentTerm
	lastIndex, lastTerm := s.lastLogIndexTerm()
	upToDate := request.LastLogTerm > lastTerm || (request.LastLogTerm == lastTerm && request.LastLogIndex >= lastIndex)
	canVote := s.persistent.VotedFor == "" || s.persistent.VotedFor == request.CandidateID
	if upToDate && canVote {
		candidate := clonePersistentState(s.persistent)
		candidate.VotedFor = request.CandidateID
		if err := s.config.Storage.Save(context.Background(), candidate); err != nil {
			return reply
		}
		s.persistent = candidate
		s.role = Follower
		s.leaderID = ""
		s.invalidateLostProposals()
		s.resetElectionTimer = true
		reply.VoteGranted = true
	}
	return reply
}

func (s *runtimeState) updateTerm(term Term) error {
	if term <= s.persistent.CurrentTerm {
		return nil
	}
	candidate := clonePersistentState(s.persistent)
	candidate.CurrentTerm = term
	candidate.VotedFor = ""
	if err := s.config.Storage.Save(context.Background(), candidate); err != nil {
		return err
	}
	s.persistent = candidate
	s.role = Follower
	s.leaderID = ""
	s.invalidateLostProposals()
	s.electionTerm = 0
	s.votes = nil
	s.votedReplies = nil
	s.resetElectionTimer = true
	return nil
}

func (s *runtimeState) handleVoteReply(target NodeID, electionTerm Term, reply RequestVoteReply) {
	if reply.Term > s.persistent.CurrentTerm {
		_ = s.updateTerm(reply.Term)
		return
	}
	if s.role != Candidate || electionTerm != s.electionTerm || electionTerm != s.persistent.CurrentTerm || reply.Term != electionTerm {
		return
	}
	if _, seen := s.votedReplies[target]; seen {
		return
	}
	s.votedReplies[target] = struct{}{}
	if reply.VoteGranted {
		s.votes[target] = struct{}{}
		if s.hasMajority() {
			s.becomeLeader()
		}
	}
}
