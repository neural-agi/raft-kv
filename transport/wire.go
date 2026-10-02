package transport

import (
	"errors"
	"fmt"
)

// ProtocolVersion is the current wire protocol version. Every frame carries it
// in its fixed header; a frame with any other version is rejected with
// ErrUnsupportedVersion. Bump this constant only when the frame layout or a
// payload encoding changes incompatibly; V9.3+ transports must refuse to speak
// versions they do not implement.
const ProtocolVersion uint16 = 1

// MaxFrameSize bounds a single frame's total body length, including the fixed
// 11-byte header (version 2 | message type 1 | request ID 8). The limit is
// chosen so a frame can carry realistic InstallSnapshot payloads (a full
// compacted KV state machine) while capping how much memory one malformed or
// hostile frame can force a receiver to allocate. The on-disk snapshot encoding
// uses a uint32 payload length, so a snapshot can in principle grow to 4 GiB;
// the transport deliberately keeps in-memory copies much smaller. ReadMessage
// validates a frame's declared length against this limit before allocating any
// payload buffer; Encoders reject payloads that would not fit.
const MaxFrameSize = 64 << 20 // 64 MiB

// headerSize is the fixed part of a frame body that precedes the payload:
// protocol version (2) | message type (1) | request ID (8).
const headerSize = 2 + 1 + 8

var (
	// ErrUnsupportedVersion is returned when a frame or message declares a
	// protocol version other than ProtocolVersion.
	ErrUnsupportedVersion = errors.New("transport: unsupported protocol version")
	// ErrUnknownMessageType is returned when a message type is outside the
	// defined request/response set.
	ErrUnknownMessageType = errors.New("transport: unknown message type")
	// ErrInvalidRequestID is returned when a request ID is zero. Zero is
	// reserved so no message can be mistaken for a correlatable RPC.
	ErrInvalidRequestID = errors.New("transport: request id is zero")
	// ErrFrameTooLarge is returned when a frame body would exceed MaxFrameSize.
	ErrFrameTooLarge = errors.New("transport: frame exceeds maximum size")
	// ErrTruncatedFrame is returned when a stream ends in the middle of a
	// frame's declared body.
	ErrTruncatedFrame = errors.New("transport: truncated frame")
	// ErrMalformedPayload is returned when payload bytes fail to decode as the
	// message type they claim to be.
	ErrMalformedPayload = errors.New("transport: malformed payload")
	// ErrTrailingBytes is returned when payload bytes remain after a complete
	// typed message has been decoded.
	ErrTrailingBytes = errors.New("transport: trailing bytes after payload")
)

// MessageType identifies the six wire kinds: the three Raft RPC requests and
// their replies. The receiver always distinguishes request from response
// without inspecting the payload.
type MessageType byte

const (
	MessageTypeVoteRequest MessageType = iota + 1
	MessageTypeVoteResponse
	MessageTypeAppendRequest
	MessageTypeAppendResponse
	MessageTypeSnapshotRequest
	MessageTypeSnapshotResponse
)

func (t MessageType) String() string {
	switch t {
	case MessageTypeVoteRequest:
		return "RequestVote"
	case MessageTypeVoteResponse:
		return "RequestVoteReply"
	case MessageTypeAppendRequest:
		return "AppendEntries"
	case MessageTypeAppendResponse:
		return "AppendEntriesReply"
	case MessageTypeSnapshotRequest:
		return "InstallSnapshot"
	case MessageTypeSnapshotResponse:
		return "InstallSnapshotReply"
	default:
		return fmt.Sprintf("MessageType(%d)", t)
	}
}

// IsRequest reports whether t is one of the three outgoing request types.
func (t MessageType) IsRequest() bool {
	return t == MessageTypeVoteRequest || t == MessageTypeAppendRequest || t == MessageTypeSnapshotRequest
}

// ResponseType returns the reply type paired with request type t, and whether
// t is a request type at all.
func (t MessageType) ResponseType() (MessageType, bool) {
	switch t {
	case MessageTypeVoteRequest:
		return MessageTypeVoteResponse, true
	case MessageTypeAppendRequest:
		return MessageTypeAppendResponse, true
	case MessageTypeSnapshotRequest:
		return MessageTypeSnapshotResponse, true
	default:
		return 0, false
	}
}

// Message is the wire envelope common to every frame. It is independent of any
// specific Raft message structure; the payload is the serialized RPC argument
// or reply for the message type. Messages are immutable by convention: Encoders
// copy the payload and never retain or mutate caller-owned slices.
type Message struct {
	Version   uint16
	Type      MessageType
	RequestID uint64
	Payload   []byte
}
