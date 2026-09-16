package raft

import "context"

func triggerElectionTimeoutForTest(n *Node, ctx context.Context) error {
	return n.enqueue(ctx, electionTimeoutEvent{})
}

func triggerBecomeLeaderForTest(n *Node, ctx context.Context) error {
	return n.enqueue(ctx, becomeLeaderEvent{})
}

func compactForTest(n *Node, ctx context.Context, index LogIndex) error {
	reply := make(chan error, 1)
	if err := n.enqueue(ctx, compactEvent{index: index, reply: reply}); err != nil {
		return err
	}
	return <-reply
}

func appendTestEntryForTest(n *Node, ctx context.Context, entry LogEntry) error {
	reply := make(chan error, 1)
	if err := n.enqueue(ctx, appendTestEntryEvent{entry: entry, reply: reply}); err != nil {
		return err
	}
	return <-reply
}
