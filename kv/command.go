package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const encodingVersion byte = 1

// CommandType identifies a deterministic replicated KV operation.
type CommandType byte

const (
	Put    CommandType = 1
	Delete CommandType = 2
)

// Command is the opaque command decoded by the KV state machine. Keys and values
// are arbitrary bytes. DELETE ignores Value and requires it to be empty.
type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte
}

func (c Command) Validate() error {
	if len(c.Key) == 0 {
		return errors.New("key must not be empty")
	}
	switch c.Type {
	case Put:
		if c.Value == nil {
			return errors.New("put value must not be nil")
		}
	case Delete:
		if len(c.Value) != 0 {
			return errors.New("delete value must be empty")
		}
	default:
		return fmt.Errorf("unknown command type %d", c.Type)
	}
	return nil
}

// Encode returns versioned binary data:
// version(1) | type(1) | keyLen(4) | valueLen(4) | key | value.
// Lengths are big-endian uint32 values and all bytes are copied.
func (c Command) Encode() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if uint64(len(c.Key)) > uint64(^uint32(0)) || uint64(len(c.Value)) > uint64(^uint32(0)) {
		return nil, errors.New("command field is too large")
	}
	data := make([]byte, 10+len(c.Key)+len(c.Value))
	data[0] = encodingVersion
	data[1] = byte(c.Type)
	binary.BigEndian.PutUint32(data[2:6], uint32(len(c.Key)))
	binary.BigEndian.PutUint32(data[6:10], uint32(len(c.Value)))
	copy(data[10:], c.Key)
	copy(data[10+len(c.Key):], c.Value)
	return data, nil
}

// Decode validates the complete input and copies key/value bytes out of it.
func Decode(data []byte) (Command, error) {
	if len(data) < 10 {
		return Command{}, errors.New("command is shorter than header")
	}
	if data[0] != encodingVersion {
		return Command{}, fmt.Errorf("unsupported command version %d", data[0])
	}
	keyLen := uint64(binary.BigEndian.Uint32(data[2:6]))
	valueLen := uint64(binary.BigEndian.Uint32(data[6:10]))
	if keyLen+valueLen != uint64(len(data)-10) {
		return Command{}, errors.New("command length does not match header")
	}
	keyEnd := 10 + int(keyLen)
	command := Command{Type: CommandType(data[1]), Key: append([]byte(nil), data[10:keyEnd]...), Value: append([]byte(nil), data[keyEnd:]...)}
	if err := command.Validate(); err != nil {
		return Command{}, err
	}
	return command, nil
}
