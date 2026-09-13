package raft

import "context"

// applyCommitted applies the committed prefix in strict log-index order. The
// event loop is the only caller, so both lastApplied and applyResults remain
// single-owner state.
func (s *runtimeState) applyCommitted() {
	for s.lastApplied < s.commitIndex {
		index := s.lastApplied + 1
		entry, ok := s.log.entry(index)
		if !ok {
			return
		}
		result, err := s.config.StateMachine.Apply(context.Background(), append([]byte(nil), entry.Command...))
		if err != nil {
			return
		}
		s.applyResults[index] = append([]byte(nil), result...)
		s.lastApplied = index
	}
	s.tryCompleteProposals()
}

func (s *runtimeState) tryCompleteProposals() {
	for index, waiter := range s.proposals {
		entry, ok := s.log.entry(index)
		if !ok || entry.Term != waiter.term {
			s.completeProposal(index, proposalCompletion{err: ErrProposalLost})
			continue
		}
		if index > s.commitIndex || index > s.lastApplied {
			continue
		}
		result, ok := s.applyResults[index]
		if !ok {
			continue
		}
		s.completeProposal(index, proposalCompletion{result: ProposalResult{
			Index:  index,
			Term:   waiter.term,
			Result: append([]byte(nil), result...),
		}})
	}
}

func (s *runtimeState) completeAllProposals(err error) {
	for index := range s.proposals {
		s.completeProposal(index, proposalCompletion{err: err})
	}
}

func (s *runtimeState) completeProposal(index LogIndex, completion proposalCompletion) {
	waiter, ok := s.proposals[index]
	if !ok || waiter.done {
		return
	}
	waiter.done = true
	delete(s.proposals, index)
	delete(s.applyResults, index)
	waiter.result <- completion
}

func (s *runtimeState) invalidateLostProposals() {
	for index, waiter := range s.proposals {
		entry, ok := s.log.entry(index)
		if !ok || entry.Term != waiter.term {
			s.completeProposal(index, proposalCompletion{err: ErrProposalLost})
		}
	}
}
