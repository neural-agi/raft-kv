// Package fault provides deterministic, test-oriented fault injection helpers.
// It deliberately contains no production Raft protocol behavior.
package fault

import (
	"errors"
	"sync"
)

var ErrInjected = errors.New("injected fault")

type Kind string

const (
	RequestVote     Kind = "RequestVote"
	AppendEntries   Kind = "AppendEntries"
	InstallSnapshot Kind = "InstallSnapshot"
)

type edge struct {
	from string
	to   string
	kind Kind
}

type Rule struct {
	Drop  bool
	Error bool
	Delay bool
}

// Controller stores deterministic one-shot and persistent transport faults.
type Controller struct {
	mu      sync.Mutex
	rules   map[edge]Rule
	oneShot map[edge]Rule
	blocked map[string]map[string]bool
	trace   *Trace
}

func NewController(trace *Trace) *Controller {
	return &Controller{rules: make(map[edge]Rule), oneShot: make(map[edge]Rule), blocked: make(map[string]map[string]bool), trace: trace}
}

func (c *Controller) Set(from, to string, kind Kind, rule Rule) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rules[edge{from: from, to: to, kind: kind}] = rule
	c.record("fault.set", from, to, kind, rule)
}

func (c *Controller) Clear(from, to string, kind Kind) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.rules, edge{from: from, to: to, kind: kind})
	c.record("fault.clear", from, to, kind, Rule{})
}

func (c *Controller) Next(from, to string, kind Kind, rule Rule) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.oneShot[edge{from: from, to: to, kind: kind}] = rule
	c.record("fault.next", from, to, kind, rule)
}

func (c *Controller) Partition(a, b string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blocked[a] == nil {
		c.blocked[a] = make(map[string]bool)
	}
	if c.blocked[b] == nil {
		c.blocked[b] = make(map[string]bool)
	}
	c.blocked[a][b], c.blocked[b][a] = true, true
	c.record("partition", a, b, "", Rule{})
}

func (c *Controller) Heal(a, b string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blocked[a] != nil {
		delete(c.blocked[a], b)
	}
	if c.blocked[b] != nil {
		delete(c.blocked[b], a)
	}
	c.record("heal", a, b, "", Rule{})
}

func (c *Controller) rule(from, to string, kind Kind) (Rule, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blocked[from] != nil && c.blocked[from][to] {
		return Rule{Error: true}, true
	}
	e := edge{from: from, to: to, kind: kind}
	if r, ok := c.oneShot[e]; ok {
		delete(c.oneShot, e)
		return r, true
	}
	r, ok := c.rules[e]
	return r, ok
}

func (c *Controller) record(name, from, to string, kind Kind, rule Rule) {
	if c.trace != nil {
		c.trace.Add(Event{Name: name, From: from, To: to, Kind: string(kind), Detail: rule})
	}
}

// Trace is a concurrency-safe structured test trace.
type Trace struct {
	mu     sync.Mutex
	events []Event
}

type Event struct {
	Name   string
	From   string
	To     string
	Kind   string
	Detail any
}

func (t *Trace) Add(event Event) { t.mu.Lock(); t.events = append(t.events, event); t.mu.Unlock() }
func (t *Trace) Events() []Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]Event(nil), t.events...)
}
