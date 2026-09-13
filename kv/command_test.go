package kv

import "testing"

func TestCommandRoundTrip(t *testing.T) {
	want := Command{Op: OpPut, Key: "name", Value: []byte("raft")}
	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := UnmarshalCommand(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Op != want.Op || got.Key != want.Key || string(got.Value) != string(want.Value) {
		t.Fatalf("round trip mismatch: got %#v, want %#v", got, want)
	}
}

func TestInvalidCommandsAreRejected(t *testing.T) {
	for _, data := range []string{
		`{"op":"get","key":"name"}`,
		`{"op":"put","key":"" ,"value":"cmFmdA=="}`,
		`{"op":"put","key":"name"}`,
		`{"op":"unknown","key":"name"}`,
	} {
		if _, err := UnmarshalCommand([]byte(data)); err == nil {
			t.Fatalf("invalid command accepted: %s", data)
		}
	}
}

func TestGetIsNotACommand(t *testing.T) {
	if _, err := UnmarshalCommand([]byte(`{"op":"get","key":"name"}`)); err == nil {
		t.Fatal("GET unexpectedly accepted as a replicated command")
	}
}
