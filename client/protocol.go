package client

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/neural-agi/raft-kv/raft"
)

const (
	// frameHeaderSize is the big-endian payload length prefix on the wire.
	frameHeaderSize = 4
	// maxFrameSize bounds a single request/response frame to keep the frame
	// length prefix meaningful.
	maxFrameSize = 1 << 20
)

// Op identifies a client operation.
type Op string

const (
	OpStatus Op = "status"
	OpPut    Op = "put"
	OpGet    Op = "get"
	OpDelete Op = "delete"
)

// ErrUnknownOp is returned for an operation the server does not implement.
var ErrUnknownOp = errors.New("unknown operation")

// Request is one client-to-server operation. Key/Value serialized as JSON bytes
// auto-decode from base64. The protocol is deliberately independent of the Raft
// wire frame protocol, which keeps a frozen message-type range.
type Request struct {
	Op    Op     `json:"op"`
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

// Response is the server's reply. OK=false carries an Error explaining the
// failure; a not-leader rejection carries the Leader the client should try.
type Response struct {
	OK     bool                `json:"ok"`
	Error  string              `json:"error,omitempty"`
	Leader raft.NodeID         `json:"leader,omitempty"`
	Role   raft.Role           `json:"role,omitempty"`
	Result raft.ProposalResult `json:"result,omitempty"`
	Found  bool                `json:"found,omitempty"`
	Value  []byte              `json:"value,omitempty"`
}

// errNotLeader marks a Response carrying a redirect in the OK=false case.
var errNotLeader = errors.New("not leader")

// notLeaderMessage is the wire Error value a server uses to tell a client the
// proposal was rejected by a non-leader and to follow the Leader field.
const notLeaderMessage = "not leader"

// writeFrame encodes v as JSON and writes it length-prefixed in one call so a
// concurrent client never interleaves bytes. It returns the wire error.
func writeFrame(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	if len(payload) > maxFrameSize {
		return fmt.Errorf("frame too large: %d bytes", len(payload))
	}
	header := [frameHeaderSize]byte{}
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	var frame []byte
	frame = append(frame, header[:]...)
	frame = append(frame, payload...)
	_, err = w.Write(frame)
	if err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

// readFrame reads one length-prefixed frame and decodes it into v.
func readFrame(r io.Reader, v any) error {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("read frame header: %w", err)
	}
	length := binary.BigEndian.Uint32(header)
	if length > maxFrameSize {
		return fmt.Errorf("frame too large: %d bytes", length)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return fmt.Errorf("read frame payload: %w", err)
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("decode frame: %w", err)
	}
	return nil
}

// newFrameReader returns a fresh buffered reader. One reader per connection: the
// server and client each create exactly one for their owned side of the socket.
func newFrameReader(r io.Reader) *bufio.Reader {
	return bufio.NewReaderSize(r, frameHeaderSize+maxFrameSize)
}
