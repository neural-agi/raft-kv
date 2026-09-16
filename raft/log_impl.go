package raft

import "errors"

// raftLog is the event-loop-owned in-memory log. Index zero is the synthetic
// position before the first entry and is never stored in entries.
type raftLog struct {
	boundary LogBoundary
	entries  []LogEntry
}

func newRaftLog(boundary LogBoundary, entries []LogEntry) (*raftLog, error) {
	if boundary.Index > 0 && boundary.Term == 0 {
		return nil, errors.New("snapshot boundary term must be greater than zero")
	}
	log := &raftLog{boundary: boundary, entries: append([]LogEntry(nil), entries...)}
	for i := range log.entries {
		if err := log.entries[i].Validate(); err != nil {
			return nil, err
		}
		if i == 0 && log.entries[i].Index != log.firstIndex() {
			return nil, errors.New("retained log does not follow snapshot boundary")
		}
		if i > 0 && log.entries[i].Index != log.entries[i-1].Index+1 {
			return nil, errors.New("log entries are not contiguous")
		}
		log.entries[i].Command = append([]byte(nil), log.entries[i].Command...)
	}
	return log, nil
}

func (l *raftLog) firstIndex() LogIndex { return l.boundary.Index + 1 }

func (l *raftLog) lastIndex() LogIndex {
	if len(l.entries) == 0 {
		return l.boundary.Index
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *raftLog) lastTerm() Term {
	if len(l.entries) == 0 {
		return l.boundary.Term
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *raftLog) entry(index LogIndex) (LogEntry, bool) {
	if index == l.boundary.Index && index > 0 {
		return LogEntry{Index: l.boundary.Index, Term: l.boundary.Term}, true
	}
	if index < l.firstIndex() || index > l.lastIndex() {
		return LogEntry{}, false
	}
	entry := l.entries[index-l.firstIndex()]
	entry.Command = append([]byte(nil), entry.Command...)
	return entry, true
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
		if entry.Index != l.lastIndex()+1 {
			return errors.New("log entry index is not contiguous")
		}

		entry.Command = append([]byte(nil), entry.Command...)
		l.entries = append(l.entries, entry)
	}
	return nil
}

func (l *raftLog) truncateFrom(index LogIndex) {
	if index <= l.boundary.Index {
		l.entries = nil
		return
	}
	if index <= l.lastIndex() {
		l.entries = l.entries[:index-l.firstIndex()]
	}
}

func (l *raftLog) replaceFrom(index LogIndex, entries []LogEntry) error {
	if index <= l.boundary.Index {
		return errors.New("cannot replace compacted log prefix")
	}
	for i, entry := range entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if entry.Index != index+LogIndex(i) {
			return errors.New("replacement entries are not contiguous")
		}
	}
	candidate := &raftLog{boundary: l.boundary, entries: append([]LogEntry(nil), l.entries...)}
	candidate.truncateFrom(index)
	if err := candidate.append(entries...); err != nil {
		return err
	}
	l.entries = candidate.entries
	return nil
}

func (l *raftLog) compact(index LogIndex, term Term) error {
	if index < l.boundary.Index || index > l.lastIndex() {
		return errors.New("invalid compaction index")
	}
	actual, ok := l.term(index)
	if !ok || actual != term {
		return errors.New("compaction term mismatch")
	}
	if index == l.lastIndex() {
		l.entries = nil
	} else {
		l.entries = append([]LogEntry(nil), l.entries[index-l.firstIndex()+1:]...)
	}
	l.boundary = LogBoundary{Index: index, Term: term}
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
