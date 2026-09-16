package raft

import "testing"

func TestRaftLogCompactionBoundary(t *testing.T) {
	log, err := newRaftLog(LogBoundary{}, []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 2, Index: 3}, {Term: 2, Index: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if err := log.compact(2, 1); err != nil {
		t.Fatal(err)
	}
	if log.firstIndex() != 3 || log.lastIndex() != 4 {
		t.Fatalf("indexes = first %d last %d", log.firstIndex(), log.lastIndex())
	}
	if !log.matches(2, 1) || log.matches(2, 2) {
		t.Fatal("boundary term lookup is incorrect")
	}
	if _, ok := log.entry(1); ok {
		t.Fatal("compacted entry remained addressable")
	}
	if !log.matches(3, 2) || !log.matches(0, 0) {
		t.Fatal("retained/index-zero matching is incorrect")
	}
	if err := log.append(LogEntry{Term: 2, Index: 5}); err != nil {
		t.Fatal(err)
	}
	if err := log.replaceFrom(3, []LogEntry{{Term: 3, Index: 3}}); err != nil {
		t.Fatal(err)
	}
	if log.lastIndex() != 3 || log.entries[0].Index != 3 {
		t.Fatalf("replacement after compaction = %#v", log.entries)
	}
	if err := log.replaceFrom(2, []LogEntry{{Term: 4, Index: 2}}); err == nil {
		t.Fatal("replacement crossed compacted boundary")
	}
}
