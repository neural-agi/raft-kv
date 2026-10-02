package transport

import (
	"errors"
	"sync"
)

var (
	// ErrRequestIDsExhausted is returned by Allocate when the 64-bit request
	// ID space has been exhausted for this correlator instance.
	ErrRequestIDsExhausted = errors.New("transport: request ids exhausted")
	// ErrDuplicateRequestID is returned by Register when the request ID is
	// already pending.
	ErrDuplicateRequestID = errors.New("transport: request id already pending")
	// ErrCorrelatorClosed is delivered to every pending waiter by Close, and is
	// returned by Register and Allocate after the correlator is closed.
	ErrCorrelatorClosed = errors.New("transport: correlator closed")
	// ErrCanceled is delivered to a waiter when its request is canceled.
	ErrCanceled = errors.New("transport: request canceled")
)

// Completion is the terminal outcome of a pending request: either the matching
// reply (Err is nil) or the error that ended the request (canceled or the
// correlator closing). A waiter receives exactly one Completion per allocated
// request.
type Completion struct {
	Reply Message
	Err   error
}

// Correlator maps request IDs to pending waiters so that a response always
// resolves back to the exact request that produced it, even when many requests
// are in flight at once or replies arrive out of order. It is concurrency-safe
// and owns no Raft or networking state: it is the reusable request/response
// correlation primitive a V9.3+ socket transport can drive without knowing how
// requests are generated or how replies are matched to peers.
//
// Guarantees:
//   - every registered request is completed exactly once (with a reply, ErrCanceled,
//     or ErrCorrelatorClosed) and never leaks a waiter;
//   - Complete on an unknown, already-completed, or late request is a no-op that
//     returns false and can never corrupt another waiter;
//   - Close unblocks all pending waiters and rejects new registrations;
//   - request IDs are unique and monotonic within a correlator instance, are
//     generated under lock, and never include the reserved zero value.
type Correlator struct {
	mu      sync.Mutex
	next    uint64
	pending map[uint64]chan Completion
	closed  bool
}

// NewCorrelator returns an empty correlator. The first allocated request ID is 1.
func NewCorrelator() *Correlator {
	return &Correlator{pending: make(map[uint64]chan Completion)}
}

// Register registers an explicit non-zero request ID and returns its completion
// channel. It fails with ErrInvalidRequestID for zero, ErrDuplicateRequestID
// for an ID already pending, and ErrCorrelatorClosed after Close.
func (c *Correlator) Register(requestID uint64) (chan Completion, error) {
	if requestID == 0 {
		return nil, ErrInvalidRequestID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrCorrelatorClosed
	}
	if _, ok := c.pending[requestID]; ok {
		return nil, ErrDuplicateRequestID
	}
	ch := make(chan Completion, 1)
	c.pending[requestID] = ch
	return ch, nil
}

// Allocate generates the next unique request ID, registers it, and returns the
// ID together with its completion channel. The waiter receives exactly one
// Completion on the channel: the reply from Complete, ErrCanceled from Cancel,
// or ErrCorrelatorClosed from Close. ID generation and registration happen
// under one lock, so concurrent callers can never observe duplicate IDs even as
// Close runs.
func (c *Correlator) Allocate() (uint64, chan Completion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, nil, ErrCorrelatorClosed
	}
	if c.next == ^uint64(0) {
		return 0, nil, ErrRequestIDsExhausted
	}
	c.next++
	id := c.next
	ch := make(chan Completion, 1)
	c.pending[id] = ch
	return id, ch, nil
}

// Complete delivers reply to the waiter registered for requestID and removes
// the pending entry. It returns false for an ID that is not pending (unknown,
// stale, late, or already resolved), leaving that waiter untouched. Duplicate
// and late completions are therefore harmless.
func (c *Correlator) Complete(requestID uint64, reply Message) bool {
	c.mu.Lock()
	ch, ok := c.pending[requestID]
	if !ok {
		c.mu.Unlock()
		return false
	}
	delete(c.pending, requestID)
	c.mu.Unlock()
	select {
	case ch <- Completion{Reply: reply}:
	default:
	}
	return true
}

// Cancel removes the pending entry for requestID and delivers ErrCanceled to
// its waiter. A subsequent Complete for the same ID is a no-op. Cancel is
// idempotent for unknown or already-resolved IDs.
func (c *Correlator) Cancel(requestID uint64) {
	c.mu.Lock()
	ch, ok := c.pending[requestID]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.pending, requestID)
	c.mu.Unlock()
	select {
	case ch <- Completion{Err: ErrCanceled}:
	default:
	}
}

// Close unblocks every pending waiter with ErrCorrelatorClosed and rejects all
// subsequent Register and Allocate calls. Late responses are ignored safely. It
// is idempotent and safe to call concurrently with Complete and Cancel.
func (c *Correlator) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	pending := c.pending
	c.pending = make(map[uint64]chan Completion)
	c.mu.Unlock()
	for _, ch := range pending {
		select {
		case ch <- Completion{Err: ErrCorrelatorClosed}:
		default:
		}
	}
}
