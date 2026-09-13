package raft

import "testing"

func TestRaftLogMechanics(t *testing.T) {
	log, err := newRaftLog(nil)
	if err != nil {
		t.Fatal(err)
	}
	entries := []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}, {Term: 1, Index: 2, Command: []byte("b")}}
	if err := log.append(entries...); err != nil {
		t.Fatal(err)
	}
	if log.lastIndex() != 2 || log.lastTerm() != 1 {
		t.Fatalf("last = %d/%d", log.lastIndex(), log.lastTerm())
	}
	if !log.matches(1, 1) || log.matches(2, 2) {
		t.Fatal("previous-log matching incorrect")
	}
	entry, ok := log.entry(2)
	if !ok || string(entry.Command) != "b" {
		t.Fatalf("lookup = %#v, %v", entry, ok)
	}
	log.truncateFrom(2)
	if log.lastIndex() != 1 {
		t.Fatalf("truncate last index = %d", log.lastIndex())
	}
	if err := log.replaceFrom(2, []LogEntry{{Term: 2, Index: 2, Command: []byte("new")}, {Term: 2, Index: 3, Command: []byte("c")}}); err != nil {
		t.Fatal(err)
	}
	if log.lastIndex() != 3 || log.lastTerm() != 2 {
		t.Fatalf("replacement last = %d/%d", log.lastIndex(), log.lastTerm())
	}
}
