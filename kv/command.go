package kv

import (
	"encoding/json"
	"errors"
)

// Op is the complete command set for the initial key-value state machine.
type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
)

// Command is the deterministic state-machine input replicated through Raft.
// GET is deliberately absent: reads do not mutate state and are served locally by
// the application boundary. A future linearizable-read protocol may add a read
// confirmation without turning GET into a log command.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func (c Command) Validate() error {
	if c.Key == "" {
		return errors.New("key must not be empty")
	}
	switch c.Op {
	case OpPut:
		if c.Value == nil {
			return errors.New("put value must not be nil")
		}
	case OpDelete:
	default:
		return errors.New("unsupported command operation")
	}
	return nil
}

func (c Command) Marshal() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(c)
}

func UnmarshalCommand(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, err
	}
	if err := c.Validate(); err != nil {
		return Command{}, err
	}
	return c, nil
}
