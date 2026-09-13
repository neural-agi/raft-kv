package kv

import (
	"context"
	"testing"
)

func TestMemoryStoreSemantics(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if result, err := store.Apply(ctx, mustEncode(t, Command{Type: Put, Key: []byte("k"), Value: []byte("one")})); err != nil || string(result) != "one" {
		t.Fatalf("put = %q, %v", result, err)
	}
	if result, err := store.Apply(ctx, mustEncode(t, Command{Type: Put, Key: []byte("k"), Value: []byte("two")})); err != nil || string(result) != "two" {
		t.Fatalf("overwrite = %q, %v", result, err)
	}
	value, ok, err := store.Get(ctx, []byte("k"))
	if err != nil || !ok || string(value) != "two" {
		t.Fatalf("get = %q, %v, %v", value, ok, err)
	}
	value[0] = 'x'
	value, _, _ = store.Get(ctx, []byte("k"))
	if string(value) != "two" {
		t.Fatal("GET returned aliased value")
	}
	if result, err := store.Apply(ctx, mustEncode(t, Command{Type: Delete, Key: []byte("k")})); err != nil || string(result) != "deleted" {
		t.Fatalf("delete = %q, %v", result, err)
	}
	if result, err := store.Apply(ctx, mustEncode(t, Command{Type: Delete, Key: []byte("missing")})); err != nil || string(result) != "missing" {
		t.Fatalf("missing delete = %q, %v", result, err)
	}
	if _, ok, _ := store.Get(ctx, []byte("k")); ok {
		t.Fatal("deleted key still present")
	}
}

func TestMemoryStoreApplyFailureDoesNotMutate(t *testing.T) {
	store := NewMemoryStore()
	if _, err := store.Apply(context.Background(), []byte{1, byte(Put), 0, 0, 0, 1, 0, 0, 0, 1, 'k'}); err == nil {
		t.Fatal("malformed command applied")
	}
	if _, ok, _ := store.Get(context.Background(), []byte("k")); ok {
		t.Fatal("failed apply mutated store")
	}
}

func mustEncode(t *testing.T, command Command) []byte {
	t.Helper()
	data, err := command.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
