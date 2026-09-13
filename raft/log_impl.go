package raft

import "errors"

// raftLog is the event-loop-owned in-memory log. Index zero is the synthetic
// position before the first entry and is never stored in entries.
type raftLog struct {
	entries []LogEntry
}

func newRaftLog(entries []LogEntry) (*raftLog, error) {
	log := &raftLog{entries: append([]LogEntry(nil), entries...)}
	for i := range log.entries {
		if err := log.entries[i].Validate(); err != nil {
			return nil, err
		}
		log.entries[i].Command = append([]byte(nil), log.entries[i].Command...)
	}
	return log, nil
}

func (l *raftLog) lastIndex() LogIndex {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *raftLog) lastTerm() Term {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *raftLog) entry(index LogIndex) (LogEntry, bool) {
	if index == 0 {
		return LogEntry{}, false
	}
	for _, entry := range l.entries {
		if entry.Index == index {
			entry.Command = append([]byte(nil), entry.Command...)
			return entry, true
		}
	}
	return LogEntry{}, false
}

func (l *raftLog) term(index LogIndex) (Term, bool) {
	if index == 0 {
		return 0, true
	}
	entry, ok := l.entry(index)
	if !ok {
		return 0, false
	}
	return entry.Term, true
}

func (l *raftLog) append(entries ...LogEntry) error {
	for _, entry := range entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if len(l.entries) > 0 && entry.Index != l.lastIndex()+1 {
			return errors.New("log entry index is not contiguous")
		}
		if len(l.entries) == 0 && entry.Index != 1 {
			return errors.New("first log entry index must be one")
		}
		entry.Command = append([]byte(nil), entry.Command...)
		l.entries = append(l.entries, entry)
	}
	return nil
}

func (l *raftLog) truncateFrom(index LogIndex) {
	if index == 0 {
		l.entries = nil
		return
	}
	for i, entry := range l.entries {
		if entry.Index >= index {
			l.entries = l.entries[:i]
			return
		}
	}
}

func (l *raftLog) replaceFrom(index LogIndex, entries []LogEntry) error {
	if index == 0 {
		return errors.New("replacement index must be greater than zero")
	}
	for i, entry := range entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if entry.Index != index+LogIndex(i) {
			return errors.New("replacement entries are not contiguous")
		}
	}
	candidate := &raftLog{entries: append([]LogEntry(nil), l.entries...)}
	candidate.truncateFrom(index)
	if err := candidate.append(entries...); err != nil {
		return err
	}
	l.entries = candidate.entries
	return nil
}

func (l *raftLog) matches(index LogIndex, term Term) bool {
	actual, ok := l.term(index)
	return ok && actual == term
}

func (l *raftLog) snapshot() []LogEntry {
	entries := append([]LogEntry(nil), l.entries...)
	for i := range entries {
		entries[i].Command = append([]byte(nil), entries[i].Command...)
	}
	return entries
}
