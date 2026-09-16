package kv

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryStoreSnapshotIsDeterministicAndRestores(t *testing.T) {
	ctx := context.Background()
	first := NewMemoryStore()
	second := NewMemoryStore()
	commands := []Command{
		{Type: Put, Key: []byte{0, 1}, Value: []byte{2, 0, 255}},
		{Type: Put, Key: []byte("other"), Value: []byte{0}},
	}
	for _, command := range commands {
		encoded, err := command.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := first.Apply(ctx, encoded); err != nil {
			t.Fatal(err)
		}
	}
	for i := len(commands) - 1; i >= 0; i-- {
		encoded, err := commands[i].Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := second.Apply(ctx, encoded); err != nil {
			t.Fatal(err)
		}
	}
	left, err := first.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	right, err := second.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(left, right) {
		t.Fatalf("equivalent stores have different snapshots: %x != %x", left, right)
	}
	target := NewMemoryStore()
	if _, err := target.Apply(ctx, mustEncode(t, Command{Type: Put, Key: []byte("stale"), Value: []byte("value")})); err != nil {
		t.Fatal(err)
	}
	if err := target.Restore(ctx, left); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := target.Get(ctx, []byte("stale")); ok {
		t.Fatal("restore did not replace prior state")
	}
	value, ok, err := target.Get(ctx, []byte{0, 1})
	if err != nil || !ok || !bytes.Equal(value, []byte{2, 0, 255}) {
		t.Fatalf("restored binary value = %x, %v, %v", value, ok, err)
	}
}

func TestMemoryStoreRestoreFailureDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if _, err := store.Apply(ctx, mustEncode(t, Command{Type: Put, Key: []byte("keep"), Value: []byte("yes")})); err != nil {
		t.Fatal(err)
	}
	if err := store.Restore(ctx, []byte{0, 0, 0, 1}); err == nil {
		t.Fatal("invalid snapshot accepted")
	}
	if value, ok, _ := store.Get(ctx, []byte("keep")); !ok || string(value) != "yes" {
		t.Fatal("failed restore mutated state")
	}
}
