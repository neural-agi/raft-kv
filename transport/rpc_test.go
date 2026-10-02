package transport

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/neural-agi/raft-kv/raft"
)

func roundTripPayload(t *testing.T, value raft.RequestVoteArgs) {
	t.Helper()
	payload, err := EncodeVoteRequest(value)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeVoteRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, value) {
		t.Fatalf("vote request decoded = %#v, want %#v", got, value)
	}
}

func TestRequestVoteRoundTrip(t *testing.T) {
	cases := []raft.RequestVoteArgs{
		{Term: 1, CandidateID: "a", LastLogIndex: 0, LastLogTerm: 0},
		{Term: 3, CandidateID: "node-b-long", LastLogIndex: 42, LastLogTerm: 2},
		{Term: 1 << 40, CandidateID: "candidate", LastLogIndex: 1 << 60, LastLogTerm: 1 << 40},
	}
	for _, value := range cases {
		roundTripPayload(t, value)
	}
}

func TestRequestVoteReplyRoundTrip(t *testing.T) {
	for _, value := range []raft.RequestVoteReply{
		{Term: 1, VoteGranted: false},
		{Term: 9, VoteGranted: true},
	} {
		payload, err := EncodeVoteResponse(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeVoteResponse(payload)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, value) {
			t.Fatalf("vote response decoded = %#v, want %#v", got, value)
		}
	}
}

func TestAppendEntriesRoundTrip(t *testing.T) {
	commandA := []byte{0x00, 0x01, 0xFE, 0xFF}
	commandB := []byte("put foo bar")
	commandC := []byte{0x00, 0x00, 0x00, 0x00}
	cases := []raft.AppendEntriesArgs{
		{Term: 1, LeaderID: "a", PrevLogIndex: 0, PrevLogTerm: 0, Entries: nil, LeaderCommit: 0},
		{Term: 2, LeaderID: "b", PrevLogIndex: 5, PrevLogTerm: 1, LeaderCommit: 4},
		{
			Term: 3, LeaderID: "c", PrevLogIndex: 10, PrevLogTerm: 2,
			Entries: []raft.LogEntry{
				{Term: 3, Index: 11, Command: commandA},
				{Term: 3, Index: 12, Command: commandB},
				{Term: 3, Index: 13, Command: commandC},
				{Term: 3, Index: 14, Command: nil},
			},
			LeaderCommit: 12,
		},
	}
	for _, value := range cases {
		payload, err := EncodeAppendRequest(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeAppendRequest(payload)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, value) {
			t.Fatalf("append request decoded = %#v, want %#v", got, value)
		}
	}
}

func TestAppendEntriesReplyRoundTrip(t *testing.T) {
	for _, value := range []raft.AppendEntriesReply{
		{Term: 1, Success: true},
		{Term: 4, Success: false},
	} {
		payload, err := EncodeAppendResponse(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeAppendResponse(payload)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, value) {
			t.Fatalf("append response decoded = %#v, want %#v", got, value)
		}
	}
}

func TestInstallSnapshotRoundTrip(t *testing.T) {
	small := raft.InstallSnapshotArgs{Term: 1, LeaderID: "a", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte{0x00, 0xFF, 0x01, 0xFE}}
	large := raft.InstallSnapshotArgs{
		Term:              5,
		LeaderID:          "snapshot-leader",
		LastIncludedIndex: 1000,
		LastIncludedTerm:  4,
		Data:              make([]byte, 1<<20),
	}
	for i := range large.Data {
		large.Data[i] = byte(i * 31)
	}
	for _, value := range []raft.InstallSnapshotArgs{small, large, {Term: 0, LeaderID: "a", LastIncludedIndex: 0, LastIncludedTerm: 0, Data: nil}} {
		payload, err := EncodeSnapshotRequest(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeSnapshotRequest(payload)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, value) {
			t.Fatalf("snapshot request decoded = %#v, want %#v", got, value)
		}
	}
}

func TestInstallSnapshotReplyRoundTrip(t *testing.T) {
	for _, value := range []raft.InstallSnapshotReply{{Term: 1}, {Term: 55}} {
		payload, err := EncodeSnapshotResponse(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeSnapshotResponse(payload)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, value) {
			t.Fatalf("snapshot response decoded = %#v, want %#v", got, value)
		}
	}
}

func TestEmptyFieldsWhereLegal(t *testing.T) {
	// Empty candidate ID is structurally valid on the wire; Raft validates at
	// the node boundary, not here.
	payload, err := EncodeVoteRequest(raft.RequestVoteArgs{Term: 1, CandidateID: "", LastLogIndex: 0, LastLogTerm: 0})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeVoteRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.CandidateID != "" || got.Term != 1 {
		t.Fatalf("decoded = %#v, want empty candidate id", got)
	}

	// Empty append request (heartbeat) and empty entry command.
	heartbeat := raft.AppendEntriesArgs{Term: 2, LeaderID: "", PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 0}
	payload, err = EncodeAppendRequest(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	gotAppend, err := DecodeAppendRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if gotAppend.LeaderID != "" || len(gotAppend.Entries) != 0 {
		t.Fatalf("heartbeat decoded = %#v", gotAppend)
	}

	// Empty snapshot data.
	empty := raft.InstallSnapshotArgs{Term: 1, LeaderID: "a", LastIncludedIndex: 1, LastIncludedTerm: 1, Data: nil}
	payload, err = EncodeSnapshotRequest(empty)
	if err != nil {
		t.Fatal(err)
	}
	gotSnap, err := DecodeSnapshotRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotSnap.Data, nil) || gotSnap.LastIncludedIndex != 1 {
		t.Fatalf("empty-snapshot decoded = %#v", gotSnap)
	}
}

func TestDecodeRejectsTrailingBytes(t *testing.T) {
	payload, err := EncodeVoteResponse(raft.RequestVoteReply{Term: 1, VoteGranted: true})
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, 0xAA)
	if _, err := DecodeVoteResponse(payload); err != ErrTrailingBytes {
		t.Fatalf("err = %v, want ErrTrailingBytes", err)
	}
}

func TestDecodeRejectsTruncatedPayloads(t *testing.T) {
	payloads := []([]byte){
		[]byte{},
		[]byte{0x00},
		[]byte{0x00, 0x00, 0x00, 0x00},
	}
	for _, payload := range payloads {
		if _, err := DecodeVoteRequest(payload); err != ErrMalformedPayload {
			t.Fatalf("vote request %v err = %v, want ErrMalformedPayload", payload, err)
		}
	}
	for _, payload := range payloads {
		if _, err := DecodeVoteResponse(payload); err != ErrMalformedPayload {
			t.Fatalf("vote response %v err = %v, want ErrMalformedPayload", payload, err)
		}
	}
	if _, err := DecodeAppendRequest([]byte{1, 2, 3}); err != ErrMalformedPayload {
		t.Fatalf("append request err = %v, want ErrMalformedPayload", err)
	}
	if _, err := DecodeAppendResponse([]byte{1}); err != ErrMalformedPayload {
		t.Fatalf("append response err = %v, want ErrMalformedPayload", err)
	}
	if _, err := DecodeSnapshotRequest([]byte{1, 2, 3}); err != ErrMalformedPayload {
		t.Fatalf("snapshot request err = %v, want ErrMalformedPayload", err)
	}
	if _, err := DecodeSnapshotResponse([]byte{}); err != ErrMalformedPayload {
		t.Fatalf("snapshot response err = %v, want ErrMalformedPayload", err)
	}
}

func TestDecodeRejectsTruncatedInMiddle(t *testing.T) {
	valid, err := EncodeVoteRequest(raft.RequestVoteArgs{Term: 7, CandidateID: "zebra", LastLogIndex: 9, LastLogTerm: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVoteRequest(valid[:len(valid)-1]); err != ErrMalformedPayload {
		t.Fatalf("truncated vote request err = %v, want ErrMalformedPayload", err)
	}
	valid, err = EncodeAppendRequest(raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []raft.LogEntry{{Term: 1, Index: 1, Command: []byte("cmd")}},
		LeaderCommit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAppendRequest(valid[:len(valid)-1]); err != ErrMalformedPayload {
		t.Fatalf("truncated append request err = %v, want ErrMalformedPayload", err)
	}
}

func TestDecodeRejectsInvalidBoolean(t *testing.T) {
	payload, err := EncodeVoteResponse(raft.RequestVoteReply{Term: 1, VoteGranted: true})
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-1] = 2
	if _, err := DecodeVoteResponse(payload); err != ErrMalformedPayload {
		t.Fatalf("vote response err = %v, want ErrMalformedPayload", err)
	}
	payload, err = EncodeAppendResponse(raft.AppendEntriesReply{Term: 1, Success: true})
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-1] = 77
	if _, err := DecodeAppendResponse(payload); err != ErrMalformedPayload {
		t.Fatalf("append response err = %v, want ErrMalformedPayload", err)
	}
}

func TestDecodeRejectsImpossibleEntryCount(t *testing.T) {
	payload, err := EncodeAppendRequest(raft.AppendEntriesArgs{Term: 1, LeaderID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	countField := len(payload) - 8 - 4 // count field sits just before leaderCommit(8)
	bad := append([]byte{}, payload[:countField]...)
	bad = append(bad, 0xFF, 0xFF, 0xFF, 0xFF)
	if _, err := DecodeAppendRequest(bad); err != ErrMalformedPayload {
		t.Fatalf("impossible entry count err = %v, want ErrMalformedPayload", err)
	}
}

func TestDecodeDispatcher(t *testing.T) {
	voteArgs := raft.RequestVoteArgs{Term: 1, CandidateID: "a", LastLogIndex: 0, LastLogTerm: 0}
	votePayload, err := EncodeVoteRequest(voteArgs)
	if err != nil {
		t.Fatal(err)
	}
	message := Message{Version: ProtocolVersion, Type: MessageTypeVoteRequest, RequestID: 1, Payload: votePayload}
	decoded, err := Decode(message)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.(raft.RequestVoteArgs); !ok {
		t.Fatalf("Decode returned %T, want raft.RequestVoteArgs", decoded)
	}
	if !reflect.DeepEqual(decoded, voteArgs) {
		t.Fatalf("Decode vote request = %#v, want %#v", decoded, voteArgs)
	}

	snapshotArgs := raft.InstallSnapshotArgs{Term: 1, LeaderID: "a", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte{1, 2, 3}}
	snapshotPayload, err := EncodeSnapshotRequest(snapshotArgs)
	if err != nil {
		t.Fatal(err)
	}
	message = Message{Version: ProtocolVersion, Type: MessageTypeSnapshotRequest, RequestID: 1, Payload: snapshotPayload}
	decoded, err = Decode(message)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.(raft.InstallSnapshotArgs); !ok {
		t.Fatalf("Decode returned %T, want raft.InstallSnapshotArgs", decoded)
	}
	if !reflect.DeepEqual(decoded, snapshotArgs) {
		t.Fatalf("Decode snapshot request = %#v, want %#v", decoded, snapshotArgs)
	}

	if _, err := Decode(Message{Version: ProtocolVersion, Type: 99, RequestID: 1}); err != ErrUnknownMessageType {
		t.Fatalf("unknown type err = %v, want ErrUnknownMessageType", err)
	}
	if _, err := Decode(Message{Version: ProtocolVersion, Type: MessageTypeVoteRequest}); err != ErrInvalidRequestID {
		t.Fatalf("zero id err = %v, want ErrInvalidRequestID", err)
	}
}

func TestDecodeEveryDispatcherBranch(t *testing.T) {
	request := func(payload []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	branches := []struct {
		messageType MessageType
		payload     func() []byte
		wantType    any
	}{
		{MessageTypeVoteRequest, func() []byte { return request(EncodeVoteRequest(raft.RequestVoteArgs{Term: 1})) }, raft.RequestVoteArgs{}},
		{MessageTypeVoteResponse, func() []byte { return request(EncodeVoteResponse(raft.RequestVoteReply{Term: 1})) }, raft.RequestVoteReply{}},
		{MessageTypeAppendRequest, func() []byte { return request(EncodeAppendRequest(raft.AppendEntriesArgs{Term: 1})) }, raft.AppendEntriesArgs{}},
		{MessageTypeAppendResponse, func() []byte { return request(EncodeAppendResponse(raft.AppendEntriesReply{Term: 1})) }, raft.AppendEntriesReply{}},
		{MessageTypeSnapshotRequest, func() []byte { return request(EncodeSnapshotRequest(raft.InstallSnapshotArgs{Term: 1})) }, raft.InstallSnapshotArgs{}},
		{MessageTypeSnapshotResponse, func() []byte { return request(EncodeSnapshotResponse(raft.InstallSnapshotReply{Term: 1})) }, raft.InstallSnapshotReply{}},
	}
	for _, branch := range branches {
		message := Message{Version: ProtocolVersion, Type: branch.messageType, RequestID: 1, Payload: branch.payload()}
		decoded, err := Decode(message)
		if err != nil {
			t.Fatalf("Decode %v: %v", branch.messageType, err)
		}
		if reflect.TypeOf(decoded) != reflect.TypeOf(branch.wantType) {
			t.Fatalf("Decode %v returned %T, want %T", branch.messageType, decoded, branch.wantType)
		}
	}
}
