package transport

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/neural-agi/raft-kv/raft"
)

// Wire payload encodings. All multi-byte integers are unsigned big-endian;
// every length field is uint32. The frame budget (see MaxFrameSize) bounds a
// payload, and every decoder validates length fields against the bytes that
// actually remain, so a malformed or hostile payload can never index out of
// range, allocate beyond the frame, or panic.
//
//	vote request:     term(8) candidateIDLen(4) candidateID lastLogIndex(8) lastLogTerm(8)
//	vote response:    term(8) granted(1)
//	append request:   term(8) leaderIDLen(4) leaderID prevLogIndex(8) prevLogTerm(8)
//	                  count(4) { term(8) index(8) commandLen(4) command }*count leaderCommit(8)
//	append response:  term(8) success(1)
//	snapshot request: term(8) leaderIDLen(4) leaderID lastIncludedIndex(8)
//	                  lastIncludedTerm(8) dataLen(4) data
//	snapshot response: term(8)
//
// Decoded byte fields (candidate IDs, entry commands, snapshot data) are copied
// so the returned Raft values own their memory. A zero-length byte field
// decodes to nil.

func payloadTooLarge(size int) error {
	if size > MaxFrameSize-headerSize {
		return ErrFrameTooLarge
	}
	return nil
}

func lengthTooLong(size int) error {
	if uint64(size) > uint64(^uint32(0)) {
		return errors.New("transport: field exceeds uint32 length limit")
	}
	return nil
}

func appendU64(dst []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(dst, v) }
func appendU32(dst []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(dst, v) }

func appendID(dst []byte, id raft.NodeID) []byte {
	dst = appendU32(dst, uint32(len(id)))
	return append(dst, id...)
}

func appendBytes(dst []byte, data []byte) []byte {
	dst = appendU32(dst, uint32(len(data)))
	return append(dst, data...)
}

// EncodeVoteRequest serializes a RequestVote RPC request.
func EncodeVoteRequest(args raft.RequestVoteArgs) ([]byte, error) {
	if err := lengthTooLong(len(args.CandidateID)); err != nil {
		return nil, err
	}
	payload := appendU64(nil, uint64(args.Term))
	payload = appendID(payload, args.CandidateID)
	payload = appendU64(payload, uint64(args.LastLogIndex))
	payload = appendU64(payload, uint64(args.LastLogTerm))
	if err := payloadTooLarge(len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

// EncodeAppendRequest serializes an AppendEntries RPC request. Empty entries
// represent a heartbeat; heartbeats are a log-level concept and share this wire
// message.
func EncodeAppendRequest(args raft.AppendEntriesArgs) ([]byte, error) {
	if err := lengthTooLong(len(args.LeaderID)); err != nil {
		return nil, err
	}
	if err := lengthTooLong(len(args.Entries)); err != nil {
		return nil, err
	}
	payload := appendU64(nil, uint64(args.Term))
	payload = appendID(payload, args.LeaderID)
	payload = appendU64(payload, uint64(args.PrevLogIndex))
	payload = appendU64(payload, uint64(args.PrevLogTerm))
	payload = appendU32(payload, uint32(len(args.Entries)))
	for _, entry := range args.Entries {
		if err := lengthTooLong(len(entry.Command)); err != nil {
			return nil, err
		}
		payload = appendU64(payload, uint64(entry.Term))
		payload = appendU64(payload, uint64(entry.Index))
		payload = appendBytes(payload, entry.Command)
	}
	payload = appendU64(payload, uint64(args.LeaderCommit))
	if err := payloadTooLarge(len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

// EncodeSnapshotRequest serializes an InstallSnapshot RPC request, preserving
// every field the Raft implementation requires: the snapshot boundary
// (LastIncludedIndex/LastIncludedTerm) and the opaque state-machine data.
func EncodeSnapshotRequest(args raft.InstallSnapshotArgs) ([]byte, error) {
	if err := lengthTooLong(len(args.LeaderID)); err != nil {
		return nil, err
	}
	if err := lengthTooLong(len(args.Data)); err != nil {
		return nil, err
	}
	payload := appendU64(nil, uint64(args.Term))
	payload = appendID(payload, args.LeaderID)
	payload = appendU64(payload, uint64(args.LastIncludedIndex))
	payload = appendU64(payload, uint64(args.LastIncludedTerm))
	payload = appendBytes(payload, args.Data)
	if err := payloadTooLarge(len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

// EncodeVoteResponse serializes a RequestVote RPC reply.
func EncodeVoteResponse(reply raft.RequestVoteReply) ([]byte, error) {
	var granted byte
	if reply.VoteGranted {
		granted = 1
	}
	payload := appendU64(nil, uint64(reply.Term))
	payload = append(payload, granted)
	if err := payloadTooLarge(len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

// EncodeAppendResponse serializes an AppendEntries RPC reply.
func EncodeAppendResponse(reply raft.AppendEntriesReply) ([]byte, error) {
	var success byte
	if reply.Success {
		success = 1
	}
	payload := appendU64(nil, uint64(reply.Term))
	payload = append(payload, success)
	if err := payloadTooLarge(len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

// EncodeSnapshotResponse serializes an InstallSnapshot RPC reply.
func EncodeSnapshotResponse(reply raft.InstallSnapshotReply) ([]byte, error) {
	payload := appendU64(nil, uint64(reply.Term))
	if err := payloadTooLarge(len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

// byteReader is a bounded cursor over a payload slice. Every read validates the
// required byte count against the remaining bytes first, so malformed input can
// never cause an out-of-range index or an allocation driven by payload data.
type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) remaining() int { return len(r.data) - r.pos }

func (r *byteReader) u8() (byte, error) {
	if r.remaining() < 1 {
		return 0, ErrMalformedPayload
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *byteReader) u32() (uint32, error) {
	if r.remaining() < 4 {
		return 0, ErrMalformedPayload
	}
	v := binary.BigEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *byteReader) u64() (uint64, error) {
	if r.remaining() < 8 {
		return 0, ErrMalformedPayload
	}
	v := binary.BigEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v, nil
}

// bytes reads a uint32-length-prefixed field and returns a copy of its bytes.
func (r *byteReader) bytes() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if uint64(n) > uint64(r.remaining()) {
		return nil, ErrMalformedPayload
	}
	value := append([]byte(nil), r.data[r.pos:r.pos+int(n)]...)
	r.pos += int(n)
	return value, nil
}

func (r *byteReader) boolByte() (bool, error) {
	b, err := r.u8()
	if err != nil {
		return false, err
	}
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, ErrMalformedPayload
	}
}

func (r *byteReader) done() error {
	if r.remaining() != 0 {
		return ErrTrailingBytes
	}
	return nil
}

func decodeID(r *byteReader) (raft.NodeID, error) {
	id, err := r.bytes()
	if err != nil {
		return "", err
	}
	return raft.NodeID(id), nil
}

func decodeTerm(r *byteReader) (raft.Term, error) {
	v, err := r.u64()
	if err != nil {
		return 0, err
	}
	return raft.Term(v), nil
}

func decodeIndex(r *byteReader) (raft.LogIndex, error) {
	v, err := r.u64()
	if err != nil {
		return 0, err
	}
	return raft.LogIndex(v), nil
}

// DecodeVoteRequest parses a RequestVote request payload.
func DecodeVoteRequest(payload []byte) (raft.RequestVoteArgs, error) {
	r := &byteReader{data: payload}
	args := raft.RequestVoteArgs{}
	var err error
	if args.Term, err = decodeTerm(r); err != nil {
		return raft.RequestVoteArgs{}, err
	}
	if args.CandidateID, err = decodeID(r); err != nil {
		return raft.RequestVoteArgs{}, err
	}
	if args.LastLogIndex, err = decodeIndex(r); err != nil {
		return raft.RequestVoteArgs{}, err
	}
	if args.LastLogTerm, err = decodeTerm(r); err != nil {
		return raft.RequestVoteArgs{}, err
	}
	if err := r.done(); err != nil {
		return raft.RequestVoteArgs{}, err
	}
	return args, nil
}

// DecodeAppendRequest parses an AppendEntries request payload.
func DecodeAppendRequest(payload []byte) (raft.AppendEntriesArgs, error) {
	r := &byteReader{data: payload}
	args := raft.AppendEntriesArgs{}
	var err error
	if args.Term, err = decodeTerm(r); err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	if args.LeaderID, err = decodeID(r); err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	if args.PrevLogIndex, err = decodeIndex(r); err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	if args.PrevLogTerm, err = decodeTerm(r); err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	count, err := r.u32()
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	// Every entry occupies at least 20 bytes on the wire, so a count beyond the
	// remaining payload can only be a lie; reject it before allocating.
	if uint64(count) > uint64(r.remaining()/20) {
		return raft.AppendEntriesArgs{}, ErrMalformedPayload
	}
	if count > 0 {
		args.Entries = make([]raft.LogEntry, 0, count)
		for i := uint32(0); i < count; i++ {
			var entry raft.LogEntry
			if entry.Term, err = decodeTerm(r); err != nil {
				return raft.AppendEntriesArgs{}, err
			}
			if entry.Index, err = decodeIndex(r); err != nil {
				return raft.AppendEntriesArgs{}, err
			}
			if entry.Command, err = r.bytes(); err != nil {
				return raft.AppendEntriesArgs{}, err
			}
			args.Entries = append(args.Entries, entry)
		}
	}
	if args.LeaderCommit, err = decodeIndex(r); err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	if err := r.done(); err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	return args, nil
}

// DecodeSnapshotRequest parses an InstallSnapshot request payload.
func DecodeSnapshotRequest(payload []byte) (raft.InstallSnapshotArgs, error) {
	r := &byteReader{data: payload}
	args := raft.InstallSnapshotArgs{}
	var err error
	if args.Term, err = decodeTerm(r); err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if args.LeaderID, err = decodeID(r); err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if args.LastIncludedIndex, err = decodeIndex(r); err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if args.LastIncludedTerm, err = decodeTerm(r); err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if args.Data, err = r.bytes(); err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if err := r.done(); err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	return args, nil
}

// DecodeVoteResponse parses a RequestVote reply payload.
func DecodeVoteResponse(payload []byte) (raft.RequestVoteReply, error) {
	r := &byteReader{data: payload}
	reply := raft.RequestVoteReply{}
	var err error
	if reply.Term, err = decodeTerm(r); err != nil {
		return raft.RequestVoteReply{}, err
	}
	if reply.VoteGranted, err = r.boolByte(); err != nil {
		return raft.RequestVoteReply{}, err
	}
	if err := r.done(); err != nil {
		return raft.RequestVoteReply{}, err
	}
	return reply, nil
}

// DecodeAppendResponse parses an AppendEntries reply payload.
func DecodeAppendResponse(payload []byte) (raft.AppendEntriesReply, error) {
	r := &byteReader{data: payload}
	reply := raft.AppendEntriesReply{}
	var err error
	if reply.Term, err = decodeTerm(r); err != nil {
		return raft.AppendEntriesReply{}, err
	}
	if reply.Success, err = r.boolByte(); err != nil {
		return raft.AppendEntriesReply{}, err
	}
	if err := r.done(); err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return reply, nil
}

// DecodeSnapshotResponse parses an InstallSnapshot reply payload.
func DecodeSnapshotResponse(payload []byte) (raft.InstallSnapshotReply, error) {
	r := &byteReader{data: payload}
	reply := raft.InstallSnapshotReply{}
	var err error
	if reply.Term, err = decodeTerm(r); err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	if err := r.done(); err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	return reply, nil
}

// Decode parses a message's payload into the concrete Raft RPC type for its
// message type. It validates the header first and returns typed values without
// the caller needing to know payload layout:
//
//	MessageTypeVoteRequest     -> raft.RequestVoteArgs
//	MessageTypeVoteResponse    -> raft.RequestVoteReply
//	MessageTypeAppendRequest   -> raft.AppendEntriesArgs
//	MessageTypeAppendResponse  -> raft.AppendEntriesReply
//	MessageTypeSnapshotRequest -> raft.InstallSnapshotArgs
//	MessageTypeSnapshotResponse -> raft.InstallSnapshotReply
func Decode(message Message) (any, error) {
	if err := validateHeader(message.Version, message.Type, message.RequestID); err != nil {
		return nil, err
	}
	switch message.Type {
	case MessageTypeVoteRequest:
		return DecodeVoteRequest(message.Payload)
	case MessageTypeVoteResponse:
		return DecodeVoteResponse(message.Payload)
	case MessageTypeAppendRequest:
		return DecodeAppendRequest(message.Payload)
	case MessageTypeAppendResponse:
		return DecodeAppendResponse(message.Payload)
	case MessageTypeSnapshotRequest:
		return DecodeSnapshotRequest(message.Payload)
	case MessageTypeSnapshotResponse:
		return DecodeSnapshotResponse(message.Payload)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownMessageType, message.Type)
	}
}
