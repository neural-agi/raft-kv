package kv

import "context"

// Store is the application-facing read boundary. GET is local and returns a
// snapshot of the node's applied state; this contract does not claim linearizability.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
}

// StateMachine combines local reads with committed-command application.
type StateMachine interface {
	Store
	Apply(ctx context.Context, command []byte) ([]byte, error)
}
