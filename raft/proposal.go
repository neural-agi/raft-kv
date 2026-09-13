package raft

import (
	"context"
	"errors"
)

var (
	ErrProposalStopped = errors.New("proposal terminated because raft node stopped")
	ErrProposalLost    = errors.New("proposal lost before commitment")
)

type proposalWaiter struct {
	index  LogIndex
	term   Term
	result chan proposalCompletion
	done   bool
}

type proposalCompletion struct {
	result ProposalResult
	err    error
}

type proposalRequestEvent struct {
	ctx     context.Context
	command []byte
	reply   chan proposalCompletion
}

func (e proposalRequestEvent) handle(s *runtimeState) bool {
	if err := e.ctx.Err(); err != nil {
		e.reply <- proposalCompletion{err: err}
		return false
	}
	if s.role != Leader {
		e.reply <- proposalCompletion{err: &Error{Code: ErrCodeNotLeader, LeaderID: s.leaderID}}
		return false
	}
	entry := LogEntry{Term: s.persistent.CurrentTerm, Index: s.log.lastIndex() + 1, Command: append([]byte(nil), e.command...)}
	candidate := &raftLog{entries: s.log.snapshot()}
	if err := candidate.append(entry); err != nil {
		e.reply <- proposalCompletion{err: err}
		return false
	}
	state := clonePersistentState(s.persistent)
	state.Log = candidate.snapshot()
	if err := s.config.Storage.Save(context.Background(), state); err != nil {
		e.reply <- proposalCompletion{err: err}
		return false
	}
	s.log = candidate
	s.persistent = state
	s.invalidateLostProposals()
	waiter := &proposalWaiter{index: entry.Index, term: entry.Term, result: e.reply}
	s.proposals[entry.Index] = waiter
	s.matchIndex[s.config.ID] = s.log.lastIndex()
	s.sendAppendEntries()
	s.advanceCommitIndex()
	s.tryCompleteProposals()
	return false
}

func (n *Node) Propose(ctx context.Context, command []byte) (ProposalResult, error) {
	if err := ctx.Err(); err != nil {
		return ProposalResult{}, err
	}
	reply := make(chan proposalCompletion, 1)
	if err := n.enqueue(ctx, proposalRequestEvent{ctx: ctx, command: append([]byte(nil), command...), reply: reply}); err != nil {
		return ProposalResult{}, err
	}
	select {
	case completion := <-reply:
		return completion.result, completion.err
	case <-ctx.Done():
		return ProposalResult{}, ctx.Err()
	}
}
