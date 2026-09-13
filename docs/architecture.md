# Architecture

## Current milestone

The implementation covers lifecycle, leader election, RequestVote, AppendEntries, heartbeats, opaque log replication, conflict repair, replication bookkeeping, commit-index calculation, deterministic in-memory KV state-machine application, the in-process client write path through NodeAPI.Propose, and V5.1 filesystem persistence of complete Raft state. Restart/recovery is implemented in V5.2 through persisted commit metadata and committed-prefix replay.

## Package boundaries

```text
cmd/server       process wiring (later)
cmd/client       process wiring (later)
cluster          static member identity and configuration
raft             lifecycle, event loop, elections, log, replication, commitment, apply ordering
transport        production delivery adapter (not implemented)
storage          filesystem-backed complete PersistentState storage (V5.1); WAL not implemented
kv               binary commands and in-memory state machine
fault            deterministic test-only fault controls
integration      future end-to-end tests
docs             contracts and correctness argument
```

Raft sees only opaque command bytes in `LogEntry.Command`. The KV package owns command meaning and state mutation. Raft never decodes PUT or DELETE.

## Event-loop ownership

One Raft event loop owns all mutable protocol state:

- role, current term, voted-for state;
- the in-memory log;
- leader ID;
- `commitIndex` and `lastApplied`;
- election and heartbeat timer state;
- leader `nextIndex` and `matchIndex`;
- election vote tracking, replication decisions, application ordering, and proposal waiter completion.

RPC handlers enqueue immutable request events. Transport calls run outside the loop and enqueue immutable response events. The event loop alone advances `lastApplied` and invokes `StateMachine.Apply`. The KV map is not concurrently mutated by multiple apply calls.

## Lifecycle

A node moves through:

```text
Created -> Initialized -> Running -> Stopped
```

Initialization loads and validates the complete Raft persistent state through the Storage interface. V5.1 provides an atomic filesystem implementation for that state. The state-machine object is supplied during construction; KV contents remain in memory and are not part of Raft persistent storage.

## Replication and application flow

```text
leader election
      |
      v
AppendEntries replication -> majority commitment
      |
      v
commitIndex advances
      |
      v
apply entries from lastApplied+1 through commitIndex
      |
      v
kv.StateMachine.Apply(encoded command)
      |
      v
lastApplied advances only after success
```

Committed entries are applied strictly in increasing log-index order. If Apply fails, the failed entry remains at `lastApplied+1`; later entries are not skipped and unrelated events do not reapply successful entries. A single event-loop-owned retry timer retries the failed prefix without requiring a new Raft protocol event.

## KV command boundary

The KV command format is binary and versioned:

```text
version(1) | type(1) | keyLen(4) | valueLen(4) | key bytes | value bytes
```

Lengths are big-endian uint32 values. PUT requires a non-empty key and a value, including an explicitly encoded empty value. DELETE requires a non-empty key and no value. The decoder validates exact input length, known version/type, and operation semantics, then copies decoded bytes.

## KV semantics

- PUT creates or replaces a value.
- DELETE is idempotent. It returns `deleted` when a key existed and `missing` otherwise.
- GET is a local `MemoryStore.Get` operation only. It is not a Raft command and is not linearizable.
- Returned values are copied so callers cannot mutate internal store state.

## Commit versus apply

`commitIndex` means the Raft log prefix is known committed. `lastApplied` means the prefix successfully applied to the configured state machine. V3 maintains `lastApplied <= commitIndex`; commitment does not itself imply application success.

State-machine application is not included in Raft persistent state. On restart, V5.2 rebuilds the in-memory state machine by replaying exactly the persisted committed log prefix before normal operation. A replay failure rejects initialization.

## Proposal flow

`NodeAPI.Propose` accepts copied opaque command bytes only on a leader. The event loop appends and persists the entry, reuses AppendEntries replication, and completes an event-loop-owned waiter only after the exact `(index, term)` entry is committed and successfully applied. Apply results are retained by log index, so concurrent proposals cannot exchange results.

Follower and candidate proposals return `ErrCodeNotLeader` with the best-known leader when available. Caller cancellation detaches the caller but does not remove an accepted log entry. Pending waiters terminate with `ErrProposalStopped` on node shutdown or `ErrProposalLost` if conflicting history removes their exact entry. There are no exactly-once client semantics.

## Persistent storage boundary

`Storage.Load` returns an independent complete `PersistentState`, or an error for missing/corrupt data other than a fresh zero state when the file does not yet exist. `Storage.Save` encodes the complete state as a versioned binary file, writes and syncs a temporary sibling, atomically renames it, and syncs the parent directory before reporting success. A failed replacement leaves the previous live file intact.

V5.2 persists `currentTerm`, `votedFor`, the complete Raft log, and `commitIndex`. `commitIndex` is durable recovery metadata because it identifies exactly which log prefix may be replayed. `lastApplied`, role, leader identity, replication maps, timers, and proposal waiters remain volatile. KV contents remain in memory and are reconstructed by replaying only the persisted committed prefix. A committed-prefix replay failure rejects initialization; uncommitted suffix entries are not replayed.

## Deferred behavior

Not implemented in V5.1:

- automatic proposal forwarding or a networked client protocol;
- real TCP/HTTP/gRPC networking;
- filesystem WAL;
- snapshots and compaction;
- ReadIndex or linearizable reads;
- transactions, deduplication, or production fault orchestration.
