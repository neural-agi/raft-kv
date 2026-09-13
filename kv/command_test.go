package kv

import "testing"

func TestCommandRoundTrips(t *testing.T) {
	cases := []Command{
		{Type: Put, Key: []byte{0, 1, 2}, Value: []byte{255, 0, 4}},
		{Type: Delete, Key: []byte("name")},
	}
	for _, want := range cases {
		encoded, err := want.Encode()
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Key) != string(want.Key) || string(got.Value) != string(want.Value) || got.Type != want.Type {
			t.Fatalf("got %#v want %#v", got, want)
		}
	}
}

func TestDecodeCopiesInput(t *testing.T) {
	encoded, err := (Command{Type: Put, Key: []byte("k"), Value: []byte("v")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] = 'x'
	if string(decoded.Value) != "v" {
		t.Fatal("decoded value aliases input")
	}
}

func TestInvalidCommandsRejected(t *testing.T) {
	valid, _ := (Command{Type: Put, Key: []byte("k"), Value: []byte("v")}).Encode()
	cases := [][]byte{
		{},
		{1, byte(Put), 0, 0, 0, 1, 0, 0, 0, 5, 'k'},
		{2, byte(Put), 0, 0, 0, 1, 0, 0, 0, 1, 'k', 'v'},
		{1, 9, 0, 0, 0, 1, 0, 0, 0, 0, 'k'},
		valid[:len(valid)-1],
	}
	for _, data := range cases {
		if _, err := Decode(data); err == nil {
			t.Fatalf("invalid command accepted: %x", data)
		}
	}
	if _, err := (Command{Type: Put, Key: []byte("k")}).Encode(); err == nil {
		t.Fatal("PUT without value accepted")
	}
	if _, err := (Command{Type: Delete, Key: []byte("k"), Value: []byte("v")}).Encode(); err == nil {
		t.Fatal("DELETE with value accepted")
	}
}
