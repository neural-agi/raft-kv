package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

// Defaults for operations that carry no explicit deadline in their context.
const (
	defaultDialTimeout = 3 * time.Second
	// defaultAttemptTimeout bounds one request to one member. A member that
	// accepts a connection and never answers must not consume the caller's whole
	// budget, otherwise one unresponsive node makes a redirect-based client
	// useless exactly when a cluster needs it most.
	defaultAttemptTimeout = 5 * time.Second
	defaultOpTimeout      = 20 * time.Second
)

// Client talks to a cluster of server processes over the client protocol. It
// knows the static member IDs and addresses from the same config the servers
// use, so it can redirect a not-leader reply by ID.
type Client struct {
	addrs map[raft.NodeID]string
	ids   []raft.NodeID

	mu             sync.Mutex
	rotateIdx      int
	dialTimeout    time.Duration
	attemptTimeout time.Duration
	opTimeout      time.Duration
}

// NewClient builds a client for the given address map. The map must include
// every member the client may talk to, keyed by node ID.
func NewClient(addrs map[raft.NodeID]string) *Client {
	ids := make([]raft.NodeID, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	// Sort for a deterministic member order: rotation then spreads load, but the
	// starting point stays predictable.
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return &Client{
		addrs:          addrs,
		ids:            ids,
		dialTimeout:    defaultDialTimeout,
		attemptTimeout: defaultAttemptTimeout,
		opTimeout:      defaultOpTimeout,
	}
}

// SetTimeouts overrides the dial timeout, the per-attempt timeout, and the
// overall operation timeout. Each must be positive to take effect.
func (c *Client) SetTimeouts(dial, attempt, op time.Duration) {
	if dial > 0 {
		c.dialTimeout = dial
	}
	if attempt > 0 {
		c.attemptTimeout = attempt
	}
	if op > 0 {
		c.opTimeout = op
	}
}

// Status reports the role of the leader of the cluster by asking the given
// member; it returns the member's own role and the leader it currently knows.
func (c *Client) Status(ctx context.Context, id raft.NodeID) (raft.Role, raft.NodeID, error) {
	resp, err := c.roundTrip(ctx, id, Request{Op: OpStatus})
	if err != nil {
		return raft.Role(0), "", err
	}
	if !resp.OK {
		return raft.Role(0), "", errors.New(resp.Error)
	}
	return resp.Role, resp.Leader, nil
}

// Put commits key/value and returns once the cluster has applied it.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.leaderWrite(ctx, Request{Op: OpPut, Key: key, Value: value})
}

// Delete commits removal of key and returns once applied.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.leaderWrite(ctx, Request{Op: OpDelete, Key: key})
}

// Get reads key from a local store of one member. The read is not linearizable:
// it may observe a slightly stale value after a leadership change.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	for _, id := range c.rotateOrder() {
		resp, err := c.roundTrip(ctx, id, Request{Op: OpGet, Key: key})
		if err != nil {
			if ctx.Err() != nil {
				return nil, false, ctx.Err()
			}
			continue
		}
		if !resp.OK {
			continue
		}
		return resp.Value, resp.Found, nil
	}
	return nil, false, errors.New("no reachable member")
}

// GetFrom reads key from one specific member's local store. It is how a caller
// checks that a particular member has caught up. The read is not linearizable.
func (c *Client) GetFrom(ctx context.Context, id raft.NodeID, key []byte) ([]byte, bool, error) {
	resp, err := c.roundTrip(ctx, id, Request{Op: OpGet, Key: key})
	if err != nil {
		return nil, false, err
	}
	if !resp.OK {
		return nil, false, errors.New(resp.Error)
	}
	return resp.Value, resp.Found, nil
}

// leaderWrite sends a mutating command toward the leader, following not-leader
// redirects. It neither retries indefinitely nor loops around the ring more than
// once; a failure to reach a leader for a given attempt surfaces to the caller.
func (c *Client) leaderWrite(ctx context.Context, req Request) error {
	order := c.rotateOrder()
	attempted := make(map[raft.NodeID]bool, len(order))
	lastErr := errors.New("no reachable member")
	hint := raft.NodeID("")

	for len(attempted) < len(order) {
		next := hint
		if next != "" && attempted[next] {
			hint = ""
			continue
		}
		if next == "" {
			for _, id := range order {
				if !attempted[id] {
					next = id
					break
				}
			}
		}
		if next == "" {
			break
		}
		attempted[next] = true

		resp, err := c.roundTrip(ctx, next, req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
			hint = ""
			continue
		}
		if resp.OK {
			return nil
		}
		if resp.Leader != "" && (resp.Error == notLeaderMessage || resp.Error == "not the leader") {
			hint = resp.Leader
			lastErr = errNotLeader
			continue
		}
		// If the member is a follower but does not know the leader, try another
		// member; the error may be transient while an election is in progress.
		if resp.Leader == "" && resp.Error == notLeaderMessage {
			lastErr = errNotLeader
			hint = ""
			continue
		}
		return errors.New(resp.Error)
	}
	return lastErr
}

// roundTrip sends one request to a specific member and waits for its response.
// The wait is bounded by the per-attempt timeout and by the caller's deadline,
// whichever is sooner.
func (c *Client) roundTrip(ctx context.Context, id raft.NodeID, req Request) (Response, error) {
	addr, ok := c.addrs[id]
	if !ok {
		return Response{}, fmt.Errorf("unknown member %q", id)
	}
	dialer := net.Dialer{Timeout: c.dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		return Response{}, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(c.attemptTimeout)
	if opDeadline := time.Now().Add(c.opTimeout); opDeadline.Before(deadline) {
		deadline = opDeadline
	}
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return Response{}, fmt.Errorf("set deadline: %w", err)
	}

	if err := writeFrame(conn, req); err != nil {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		return Response{}, err
	}
	var resp Response
	if err := readFrame(newFrameReader(conn), &resp); err != nil {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		return Response{}, err
	}
	return resp, nil
}

// rotateOrder returns the member IDs with a rotating starting point so repeated
// reads are spread across members instead of always hammering the first one.
func (c *Client) rotateOrder() []raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ids) == 0 {
		return nil
	}
	rot := c.rotateIdx % len(c.ids)
	c.rotateIdx++
	order := make([]raft.NodeID, 0, len(c.ids))
	order = append(order, c.ids[rot:]...)
	order = append(order, c.ids[:rot]...)
	return order
}
