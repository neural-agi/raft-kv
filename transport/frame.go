package transport

import (
	"encoding/binary"
	"io"
)

// validateHeader checks the fixed frame header invariants: a supported protocol
// version, a known message type, and a non-zero request ID. Zero is reserved so
// a request is always correlatable and a response can never be mistaken for a
// message without a correlation identity.
func validateHeader(version uint16, messageType MessageType, requestID uint64) error {
	if version != ProtocolVersion {
		return ErrUnsupportedVersion
	}
	if messageType < MessageTypeVoteRequest || messageType > MessageTypeSnapshotResponse {
		return ErrUnknownMessageType
	}
	if requestID == 0 {
		return ErrInvalidRequestID
	}
	return nil
}

// Encode serializes message into one complete length-prefixed frame:
//
//	body length (4) | version (2) | message type (1) | request ID (8) | payload
//
// The length field is the number of bytes that follow it, so one Write of the
// returned slice carries exactly one frame and a receiver never guesses at TCP
// stream boundaries. Encode validates the header and the frame budget, never
// mutates message, and copies the payload into the frame.
func Encode(message Message) ([]byte, error) {
	if err := validateHeader(message.Version, message.Type, message.RequestID); err != nil {
		return nil, err
	}
	if len(message.Payload) > MaxFrameSize-headerSize {
		return nil, ErrFrameTooLarge
	}
	frame := make([]byte, 4+headerSize+len(message.Payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(headerSize+len(message.Payload)))
	binary.BigEndian.PutUint16(frame[4:6], message.Version)
	frame[6] = byte(message.Type)
	binary.BigEndian.PutUint64(frame[7:15], message.RequestID)
	copy(frame[15:], message.Payload)
	return frame, nil
}

// ReadMessage reads exactly one frame from r and returns its envelope. The
// payload is a freshly allocated slice owned by the returned Message.
//
// A clean end of stream returns io.EOF. A stream that ends inside the 4-byte
// length field or inside the declared body returns ErrTruncatedFrame. A
// declared body length above MaxFrameSize is rejected before any payload is
// allocated, so malformed input can never cause an unbounded allocation.
func ReadMessage(r io.Reader) (Message, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		if err == io.EOF {
			return Message{}, io.EOF
		}
		return Message{}, ErrTruncatedFrame
	}
	bodyLength := binary.BigEndian.Uint32(lengthBuf[:])
	if bodyLength > MaxFrameSize {
		return Message{}, ErrFrameTooLarge
	}
	if bodyLength < headerSize {
		return Message{}, ErrMalformedPayload
	}
	body := make([]byte, bodyLength)
	if _, err := io.ReadFull(r, body); err != nil {
		return Message{}, ErrTruncatedFrame
	}
	version := binary.BigEndian.Uint16(body[0:2])
	messageType := MessageType(body[2])
	requestID := binary.BigEndian.Uint64(body[3:11])
	if err := validateHeader(version, messageType, requestID); err != nil {
		return Message{}, err
	}
	return Message{
		Version:   version,
		Type:      messageType,
		RequestID: requestID,
		Payload:   body[11:],
	}, nil
}
