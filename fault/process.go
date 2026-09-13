package fault

import (
	"context"

	"github.com/neural-agi/raft-kv/raft"
)

// Stop stops a running node and records the lifecycle boundary in the trace.
func Stop(ctx context.Context, trace *Trace, id string, node *raft.Node) error {
	if trace != nil {
		trace.Add(Event{Name: "node.stop", From: id})
	}
	return node.Stop(ctx)
}

// Restart constructs, initializes, and starts a fresh node using the supplied
// configuration. The caller owns storage and state-machine instances.
func Restart(ctx context.Context, trace *Trace, id string, config raft.Config) (*raft.Node, error) {
	if trace != nil {
		trace.Add(Event{Name: "node.restart", From: id})
	}
	node, err := raft.NewNode(config)
	if err != nil {
		return nil, err
	}
	if err := node.Initialize(ctx); err != nil {
		return nil, err
	}
	if err := node.Start(ctx); err != nil {
		return nil, err
	}
	return node, nil
}
