package transport

import (
	"sync"
	"testing"
)

func replyFor(requestID uint64) Message {
	return Message{Version: ProtocolVersion, Type: MessageTypeVoteResponse, RequestID: requestID, Payload: []byte("ok")}
}

func waitCompletion(t *testing.T, ch <-chan Completion) Completion {
	t.Helper()
	completion, ok := <-ch
	if !ok {
		t.Fatal("completion channel closed")
	}
	return completion
}

func expectReply(t *testing.T, completion Completion, wantID uint64) {
	t.Helper()
	if completion.Err != nil {
		t.Fatalf("completion err = %v, want reply", completion.Err)
	}
	if completion.Reply.RequestID != wantID {
		t.Fatalf("reply request id = %d, want %d", completion.Reply.RequestID, wantID)
	}
}

func TestAllocateStartsAtOneAndIsMonotonic(t *testing.T) {
	correlator := NewCorrelator()
	const count = 1024
	for i := uint64(1); i <= count; i++ {
		id, ch, err := correlator.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		if id != i {
			t.Fatalf("allocate %d, want %d", id, i)
		}
		if ch == nil {
			t.Fatalf("allocate %d returned nil channel", id)
		}
		correlator.Cancel(id)
	}
}

func TestRegisterExplicitID(t *testing.T) {
	correlator := NewCorrelator()
	if _, err := correlator.Register(0); err != ErrInvalidRequestID {
		t.Fatalf("register zero err = %v, want ErrInvalidRequestID", err)
	}
	ch, err := correlator.Register(7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := correlator.Register(7); err != ErrDuplicateRequestID {
		t.Fatalf("duplicate register err = %v, want ErrDuplicateRequestID", err)
	}
	if !correlator.Complete(7, replyFor(7)) {
		t.Fatal("Complete(7) returned false for registered id")
	}
	expectReply(t, waitCompletion(t, ch), 7)
	if correlator.Complete(7, replyFor(7)) {
		t.Fatal("second Complete(7) returned true after resolution")
	}
}

func TestCompleteDeliversExactReply(t *testing.T) {
	correlator := NewCorrelator()
	id, ch, err := correlator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	if !correlator.Complete(id, replyFor(id)) {
		t.Fatal("Complete returned false for pending id")
	}
	expectReply(t, waitCompletion(t, ch), id)
}

func TestOutOfOrderCompletion(t *testing.T) {
	correlator := NewCorrelator()
	type waiter struct {
		id uint64
		ch <-chan Completion
	}
	var waiters []waiter
	for i := 0; i < 3; i++ {
		id, ch, err := correlator.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		waiters = append(waiters, waiter{id: id, ch: ch})
	}
	// Complete in reverse order; each waiter must resolve to its own reply.
	for i := len(waiters) - 1; i >= 0; i-- {
		if !correlator.Complete(waiters[i].id, replyFor(waiters[i].id)) {
			t.Fatalf("Complete(%d) returned false", waiters[i].id)
		}
	}
	for _, w := range waiters {
		expectReply(t, waitCompletion(t, w.ch), w.id)
	}
}

func TestDuplicateCompletionIsHarmless(t *testing.T) {
	correlator := NewCorrelator()
	id, ch, err := correlator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	if !correlator.Complete(id, replyFor(id)) {
		t.Fatal("first Complete returned false")
	}
	if correlator.Complete(id, replyFor(id)) {
		t.Fatal("duplicate Complete returned true")
	}
	expectReply(t, waitCompletion(t, ch), id)
}

func TestStaleAndUnknownCompletionAreIgnored(t *testing.T) {
	correlator := NewCorrelator()
	if correlator.Complete(999, replyFor(999)) {
		t.Fatal("Complete returned true for never-registered id")
	}
	id, _, err := correlator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	correlator.Cancel(id)
	if correlator.Complete(id, replyFor(id)) {
		t.Fatal("Complete returned true for canceled id")
	}
}

func TestCancelDeliversCanceled(t *testing.T) {
	correlator := NewCorrelator()
	id, ch, err := correlator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	correlator.Cancel(id)
	completion := waitCompletion(t, ch)
	if completion.Err != ErrCanceled {
		t.Fatalf("completion err = %v, want ErrCanceled", completion.Err)
	}
	// Cancel is idempotent and never panics.
	correlator.Cancel(id)
}

func TestCancelThenCompleteOrder(t *testing.T) {
	correlator := NewCorrelator()
	id, ch, err := correlator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	correlator.Cancel(id)
	if correlator.Complete(id, replyFor(id)) {
		t.Fatal("Complete returned true after Cancel")
	}
	completion := waitCompletion(t, ch)
	if completion.Err != ErrCanceled {
		t.Fatalf("completion err = %v, want ErrCanceled (complete raced first)", completion.Err)
	}
}

func TestCompleteThenCancelOrder(t *testing.T) {
	correlator := NewCorrelator()
	id, ch, err := correlator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	if !correlator.Complete(id, replyFor(id)) {
		t.Fatal("Complete returned false")
	}
	correlator.Cancel(id)
	completion := waitCompletion(t, ch)
	if completion.Err != nil {
		t.Fatalf("completion err = %v, want reply", completion.Err)
	}
	expectReply(t, completion, id)
}

func TestCancellationRacingCompletion(t *testing.T) {
	for i := 0; i < 200; i++ {
		correlator := NewCorrelator()
		id, ch, err := correlator.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			correlator.Complete(id, replyFor(id))
		}()
		go func() {
			defer wg.Done()
			<-start
			correlator.Cancel(id)
		}()
		close(start)
		completion := waitCompletion(t, ch)
		if completion.Err != nil && completion.Err != ErrCanceled {
			t.Fatalf("completion err = %v, want reply or ErrCanceled", completion.Err)
		}
		var received int
		select {
		case <-ch:
			received++
		default:
		}
		if received != 0 {
			t.Fatal("waiter received more than one completion")
		}
		wg.Wait()
	}
}

func TestCloseUnblocksPendingWaiters(t *testing.T) {
	correlator := NewCorrelator()
	const count = 5
	channels := make([]<-chan Completion, 0, count)
	for i := 0; i < count; i++ {
		_, ch, err := correlator.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		channels = append(channels, ch)
	}
	correlator.Close()
	for _, ch := range channels {
		completion := waitCompletion(t, ch)
		if completion.Err != ErrCorrelatorClosed {
			t.Fatalf("completion err = %v, want ErrCorrelatorClosed", completion.Err)
		}
	}
}

func TestCloseIsIdempotentAndRejectsNewWork(t *testing.T) {
	correlator := NewCorrelator()
	correlator.Close()
	correlator.Close()
	if _, _, err := correlator.Allocate(); err != ErrCorrelatorClosed {
		t.Fatalf("Allocate after close err = %v, want ErrCorrelatorClosed", err)
	}
	if _, err := correlator.Register(5); err != ErrCorrelatorClosed {
		t.Fatalf("Register after close err = %v, want ErrCorrelatorClosed", err)
	}
	if correlator.Complete(5, replyFor(5)) {
		t.Fatal("Complete returned true after close")
	}
}

func TestConcurrentRegisterAndComplete(t *testing.T) {
	const actors = 64
	correlator := NewCorrelator()
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[uint64]bool)
	wg.Add(actors)
	for i := 0; i < actors; i++ {
		go func() {
			defer wg.Done()
			id, ch, err := correlator.Allocate()
			if err != nil {
				t.Errorf("Allocate: %v", err)
				return
			}
			mu.Lock()
			seen[id] = true
			mu.Unlock()
			if !correlator.Complete(id, replyFor(id)) {
				t.Errorf("Complete(%d) returned false", id)
				return
			}
			completion, ok := <-ch
			if !ok {
				t.Errorf("wait for %d: channel closed", id)
				return
			}
			if completion.Err != nil {
				t.Errorf("wait for %d: err = %v", id, completion.Err)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	for id := range seen {
		if id == 0 {
			t.Error("allocated request id zero")
		}
	}
}

func TestConcurrentShutdownDeliversExactlyOnce(t *testing.T) {
	correlator := NewCorrelator()
	const pendingCount = 40

	// Reserve a deterministic set of pending requests, then race completer,
	// canceler, allocator, and closer goroutines against each other. Every
	// reserved id receives exactly one termination event regardless of order,
	// and the closer guarantees no waiter is left blocked.
	type waiter struct {
		id uint64
		ch <-chan Completion
	}
	waiters := make([]waiter, pendingCount)
	for i := range waiters {
		id, ch, err := correlator.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		waiters[i] = waiter{id: id, ch: ch}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	close(start)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < pendingCount/2; i++ {
			correlator.Complete(waiters[i].id, replyFor(waiters[i].id))
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := pendingCount / 2; i < pendingCount; i++ {
			correlator.Cancel(waiters[i].id)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, ch, err := correlator.Allocate()
			if err != nil {
				return
			}
			<-ch
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		correlator.Close()
	}()
	wg.Wait()

	// Every reserved waiter must have received exactly one completion: its
	// reply, ErrCanceled, or ErrCorrelatorClosed.
	for _, w := range waiters {
		select {
		case completion := <-w.ch:
			if completion.Err != nil && completion.Err != ErrCanceled && completion.Err != ErrCorrelatorClosed {
				t.Fatalf("waiter %d err = %v, want reply/canceled/closed", w.id, completion.Err)
			}
		default:
			t.Fatalf("waiter %d received no completion", w.id)
		}
		select {
		case _, more := <-w.ch:
			if more {
				t.Fatalf("waiter %d received multiple completions", w.id)
			}
		default:
		}
	}
}

func TestAllocateAfterCloseShouldNotPanic(t *testing.T) {
	correlator := NewCorrelator()
	correlator.Close()
	for i := 0; i < 100; i++ {
		if _, _, err := correlator.Allocate(); err != ErrCorrelatorClosed {
			t.Fatalf("Allocate err = %v, want ErrCorrelatorClosed", err)
		}
	}
}
