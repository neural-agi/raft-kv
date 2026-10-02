package transport

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func mustEncode(t *testing.T, message Message) []byte {
	t.Helper()
	frame, err := Encode(message)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return frame
}

func readMessage(t *testing.T, data []byte) Message {
	t.Helper()
	message, err := ReadMessage(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	return message
}

func sampleMessage(messageType MessageType, requestID uint64) Message {
	return Message{Version: ProtocolVersion, Type: messageType, RequestID: requestID, Payload: []byte("payload")}
}

func TestEncodeReadRoundTripAllTypes(t *testing.T) {
	types := []MessageType{
		MessageTypeVoteRequest,
		MessageTypeVoteResponse,
		MessageTypeAppendRequest,
		MessageTypeAppendResponse,
		MessageTypeSnapshotRequest,
		MessageTypeSnapshotResponse,
	}
	for _, messageType := range types {
		want := sampleMessage(messageType, 1234)
		got := readMessage(t, mustEncode(t, want))
		if got.Version != want.Version || got.Type != want.Type || got.RequestID != want.RequestID {
			t.Fatalf("round trip %v = {%d %v %d}, want {%d %v %d}",
				messageType, got.Version, got.Type, got.RequestID, want.Version, want.Type, want.RequestID)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("round trip %v payload = %q, want %q", messageType, got.Payload, want.Payload)
		}
	}
}

func TestEmptyPayloadFrame(t *testing.T) {
	got := readMessage(t, mustEncode(t, Message{Version: ProtocolVersion, Type: MessageTypeVoteResponse, RequestID: 1}))
	if len(got.Payload) != 0 {
		t.Fatalf("payload = %q, want empty", got.Payload)
	}
}

func TestEmptyStreamReturnsEOF(t *testing.T) {
	if _, err := ReadMessage(bytes.NewReader(nil)); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestTruncatedLengthField(t *testing.T) {
	if _, err := ReadMessage(bytes.NewReader([]byte{0, 1, 2})); err != ErrTruncatedFrame {
		t.Fatalf("err = %v, want ErrTruncatedFrame", err)
	}
}

func TestTruncatedBody(t *testing.T) {
	frame := mustEncode(t, sampleMessage(MessageTypeVoteRequest, 7))
	short := frame[:len(frame)-3]
	if _, err := ReadMessage(bytes.NewReader(short)); err != ErrTruncatedFrame {
		t.Fatalf("err = %v, want ErrTruncatedFrame", err)
	}
}

func TestBodyShorterThanHeader(t *testing.T) {
	frame := make([]byte, 4+5)
	binary.BigEndian.PutUint32(frame[0:4], 5)
	copy(frame[4:], "abcde")
	if _, err := ReadMessage(bytes.NewReader(frame)); err != ErrMalformedPayload {
		t.Fatalf("err = %v, want ErrMalformedPayload", err)
	}
}

func TestDeclaredLengthAboveMaximum(t *testing.T) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, MaxFrameSize+1)
	if _, err := ReadMessage(bytes.NewReader(header)); err != ErrFrameTooLarge {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestEncodePayloadTooLarge(t *testing.T) {
	payload := make([]byte, MaxFrameSize)
	message := Message{Version: ProtocolVersion, Type: MessageTypeSnapshotRequest, RequestID: 1, Payload: payload}
	if _, err := Encode(message); err != ErrFrameTooLarge {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	frame := mustEncode(t, sampleMessage(MessageTypeVoteRequest, 1))
	frame[4] = 2
	if _, err := ReadMessage(bytes.NewReader(frame)); err != ErrUnsupportedVersion {
		t.Fatalf("read err = %v, want ErrUnsupportedVersion", err)
	}
	for _, version := range []uint16{0, ProtocolVersion + 1} {
		message := Message{Version: version, Type: MessageTypeVoteRequest, RequestID: 1}
		if _, err := Encode(message); err != ErrUnsupportedVersion {
			t.Fatalf("encode version %d err = %v, want ErrUnsupportedVersion", version, err)
		}
	}
}

func TestUnknownMessageType(t *testing.T) {
	frame := mustEncode(t, sampleMessage(MessageTypeVoteRequest, 1))
	frame[6] = 99
	if _, err := ReadMessage(bytes.NewReader(frame)); err != ErrUnknownMessageType {
		t.Fatalf("read err = %v, want ErrUnknownMessageType", err)
	}
	for _, messageType := range []MessageType{0, 99, 7} {
		message := Message{Version: ProtocolVersion, Type: messageType, RequestID: 1}
		if _, err := Encode(message); err != ErrUnknownMessageType {
			t.Fatalf("encode type %d err = %v, want ErrUnknownMessageType", messageType, err)
		}
	}
}

func TestZeroRequestIDRejected(t *testing.T) {
	for _, messageType := range []MessageType{MessageTypeVoteRequest, MessageTypeVoteResponse, MessageTypeAppendRequest, MessageTypeAppendResponse, MessageTypeSnapshotRequest, MessageTypeSnapshotResponse} {
		message := Message{Version: ProtocolVersion, Type: messageType}
		if _, err := Encode(message); err != ErrInvalidRequestID {
			t.Fatalf("encode %v err = %v, want ErrInvalidRequestID", messageType, err)
		}
	}
	frame := mustEncode(t, sampleMessage(MessageTypeVoteResponse, 42))
	binary.BigEndian.PutUint64(frame[7:15], 0)
	if _, err := ReadMessage(bytes.NewReader(frame)); err != ErrInvalidRequestID {
		t.Fatalf("read err = %v, want ErrInvalidRequestID", err)
	}
}

func TestSequentialFramesOnOneStream(t *testing.T) {
	const count = 3
	stream := make([]byte, 0)
	for i := uint64(1); i <= count; i++ {
		stream = append(stream, mustEncode(t, sampleMessage(MessageTypeVoteRequest, i))...)
	}
	reader := bytes.NewReader(stream)
	for i := uint64(1); i <= count; i++ {
		got, err := ReadMessage(reader)
		if err != nil {
			t.Fatalf("ReadMessage frame %d: %v", i, err)
		}
		if got.RequestID != i {
			t.Fatalf("frame %d request id = %d, want %d", i, got.RequestID, i)
		}
	}
	if _, err := ReadMessage(reader); err != io.EOF {
		t.Fatalf("trailing stream err = %v, want io.EOF", err)
	}
}

func TestTrailingBytesBelongToNextFrame(t *testing.T) {
	first := mustEncode(t, sampleMessage(MessageTypeAppendRequest, 10))
	second := mustEncode(t, sampleMessage(MessageTypeAppendResponse, 11))
	stream := append(append([]byte{}, first...), second...)
	got, err := ReadMessage(bytes.NewReader(stream))
	if err != nil {
		t.Fatalf("ReadMessage first: %v", err)
	}
	if got.RequestID != 10 || got.Type != MessageTypeAppendRequest {
		t.Fatalf("first = {%v %d}, want AppendRequest 10", got.Type, got.RequestID)
	}
	got, err = ReadMessage(bytes.NewReader(stream[len(first):]))
	if err != nil {
		t.Fatalf("ReadMessage second: %v", err)
	}
	if got.RequestID != 11 || got.Type != MessageTypeAppendResponse {
		t.Fatalf("second = {%v %d}, want AppendResponse 11", got.Type, got.RequestID)
	}
}

func TestClientDoesNotMutatePayload(t *testing.T) {
	payload := []byte("immutable")
	message := Message{Version: ProtocolVersion, Type: MessageTypeAppendRequest, RequestID: 3, Payload: payload}
	frame, err := Encode(message)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	got := readMessage(t, frame)
	if string(got.Payload) != "immutable" {
		t.Fatal("Encode retained a reference to the caller payload")
	}
}

func TestMessageTypeHelpers(t *testing.T) {
	requests := map[MessageType]MessageType{
		MessageTypeVoteRequest:     MessageTypeVoteResponse,
		MessageTypeAppendRequest:   MessageTypeAppendResponse,
		MessageTypeSnapshotRequest: MessageTypeSnapshotResponse,
	}
	for request, response := range requests {
		if !request.IsRequest() {
			t.Fatalf("%v should report IsRequest", request)
		}
		got, ok := request.ResponseType()
		if !ok || got != response {
			t.Fatalf("%v ResponseType = %v,%v want %v,true", request, got, ok, response)
		}
	}
	for _, response := range []MessageType{MessageTypeVoteResponse, MessageTypeAppendResponse, MessageTypeSnapshotResponse} {
		if response.IsRequest() {
			t.Fatalf("%v should not report IsRequest", response)
		}
		if got, ok := response.ResponseType(); ok {
			t.Fatalf("response %v has unexpected response type %v", response, got)
		}
	}
}
