# Raft Protocol and Replication

The current implementation includes node lifecycle, leader election, RequestVote, AppendEntries, heartbeats, opaque log replication, conflict repair, replication bookkeeping, and commit-index calculation. It intentionally does not apply committed entries to a state machine yet.

## Log

`raft.LogEntry` contains an explicit term, index, and opaque command bytes. The event-loop-owned in-memory log supports:

- last index and last term;
- entry and term lookup;
- append of contiguous entries;
- previous-log matching, including synthetic index zero with term zero;
- truncation from a conflicting index;
- replacement suffix append.

No snapshot base index or compaction exists yet.

## AppendEntries follower flow

For every request:

1. A request from an older term is rejected without changing the log or resetting the election timer.
2. A newer term is durably persisted, then the node becomes follower and clears obsolete election state.
3. A current/new leader is recorded and valid leader contact resets the election timer.
4. `PrevLogIndex` and `PrevLogTerm` must match the local log. A mismatch returns `Success=false` without appending.
5. The first existing entry with a different term causes the local suffix to be truncated and the incoming suffix appended. Missing entries are appended directly.
6. Log changes are persisted before `Success=true` is returned.
7. `commitIndex` advances to `min(LeaderCommit, lastLogIndex)` and never decreases.

An empty `Entries` slice is a heartbeat. A valid empty AppendEntries still performs leader recognition and timer reset.

## Leader heartbeat and replication flow

When a candidate becomes leader:

- `nextIndex[follower]` is initialized to `leaderLastLogIndex + 1`;
- `matchIndex[follower]` starts at zero;
- the leader's own `matchIndex` is its last log index;
- an immediate AppendEntries round is sent.

The heartbeat timer sends AppendEntries periodically. If a follower is behind, the same message carries the suffix beginning at its `nextIndex`; otherwise it is empty.

A successful reply advances `matchIndex` to the highest index included in that request and advances `nextIndex` to `matchIndex+1`. A failed reply decrements `nextIndex` down to one and retries with the corrected previous-log position. Replies are tagged by the leader term and ignored if stale or if the node is no longer leader.

Transport calls execute outside the event loop. Their immutable replies are re-enqueued as events before replication state changes occur.

## Commitment

The leader examines candidate indexes from newest to oldest. An index can advance `commitIndex` only when:

- a majority has `matchIndex >= index`; and
- the entry at that index has the leader's current term.

Once a current-term entry is committed, preceding entries become committed as part of the same prefix. An older-term entry replicated to a majority by itself does not advance `commitIndex`.

The follower applies the same monotonic bound when processing `LeaderCommit`: its commit index cannot exceed its local last log index.

## Committed versus applied

This milestone deliberately keeps `commitIndex` and `lastApplied` separate. Committed entries are not passed to `StateMachine.Apply`, and `lastApplied` remains unchanged. KV command decoding and state-machine application belong to the next milestone.

## Terms and persistence

Every higher observed term is persisted before the node adopts it. Higher terms force follower state and clear obsolete election/vote bookkeeping. Vote changes and log changes are persisted before successful protocol responses are returned.

A higher-term AppendEntries reply steps a leader down. Stale AppendEntries replies cannot modify `nextIndex`, `matchIndex`, or `commitIndex`.

## Event-loop ownership

The event loop exclusively owns role, term, vote state, log, commit index, last-applied index, leader ID, timers, `nextIndex`, and `matchIndex`.

RPC handlers enqueue request events. Timer callbacks enqueue timer work. Transport goroutines enqueue immutable response events. No external goroutine mutates protocol state directly.

## Deferred behavior

Still out of scope:

- KV state-machine application;
- PUT, DELETE, GET, and client proposals;
- real TCP/HTTP/gRPC networking;
- filesystem WAL;
- snapshots and compaction;
- read-index or linearizable reads;
- general fault injection;
- optimized conflict-term/index responses.
