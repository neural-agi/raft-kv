# Raft Protocol and KV Application

The current implementation includes lifecycle, leader election, RequestVote, AppendEntries, InstallSnapshot, heartbeats, opaque log replication, conflict repair, replication bookkeeping, snapshot compaction, commit-index calculation, ordered state-machine application, and the in-process NodeAPI.Propose write path.

## Raft/KV boundary

Raft stores and replicates opaque `LogEntry.Command` bytes. It does not know KV operations. The KV package encodes and decodes commands and implements `raft.StateMachine`.

```text
kv.Command.Encode()
        |
        v
opaque LogEntry.Command
        |
        v
Raft replication and commitment
        |
        v
StateMachine.Apply(command bytes)
        |
        v
lastApplied advances
        |
        v
ProposalResult is returned to the waiting caller
```

## Command encoding

V3 uses a deterministic binary format:

```text
version(1) | type(1) | keyLen(4) | valueLen(4) | key | value
```

All integer lengths are big-endian uint32 values. Version is currently `1`. Type `1` is PUT and type `2` is DELETE. The decoder requires the input to be exactly the declared length, rejects unknown versions/types and invalid operation-specific lengths, and copies key/value data out of the input buffer.

Keys are arbitrary non-empty byte strings. PUT values may contain arbitrary bytes, including zero bytes and empty values. DELETE has no value field. Empty keys are rejected.

## KV behavior

`MemoryStore.Apply` atomically decodes and applies one command:

- PUT creates or replaces the key and returns a copy of the stored value;
- DELETE removes the key and returns `deleted` if present or `missing` for an idempotent no-op;
- malformed commands fail before map mutation.

`MemoryStore.Get` is local-only. It returns a copied value and presence flag. GET is not a log command and is not a linearizable Raft read.

## Applying committed entries

The Raft event loop checks whether `lastApplied < commitIndex` after event processing. It repeatedly selects exactly `lastApplied+1`, passes that entry's immutable command bytes to `StateMachine.Apply`, and advances `lastApplied` only after Apply succeeds.

If Apply fails:

- `lastApplied` remains before the failed entry;
- later committed entries are not skipped;
- an event-loop-owned retry timer schedules another application pass;
- successful earlier entries are not applied again;
- the next successful pass retries the failed entry and continues the committed prefix.

Application remains event-loop-owned; no external goroutine mutates `lastApplied` or the state machine.

## Proposal behavior

`NodeAPI.Propose` is leader-only. A leader copies the opaque command into a new current-term log entry, persists the complete updated Raft state, and starts normal AppendEntries replication. The proposal is successful only after the exact entry is committed and applied; local append or commitment alone is insufficient. The result is correlated by log index and term and contains the state-machine result.

Follower and candidate calls return `ErrCodeNotLeader`. Cancellation affects only the waiting caller; accepted commands remain eligible to commit and apply. If conflicting history removes the exact entry, the waiter receives `ErrProposalLost`; stopping the node completes unresolved waiters with `ErrProposalStopped`. Concurrent proposals have independent event-loop-owned waiters. This API does not provide exactly-once client semantics.

## Persistent Raft state

The Storage contract persists the complete `PersistentState`: current term, voted-for identity, compacted boundary metadata, retained log, and commit index. `storage.FileStorage` uses a versioned binary representation:

```text
magic(2) | version(1) | reserved(1) |
currentTerm(8) | votedForLength(4) | votedFor |
logCount(4) | commitIndex(8) | lastIncludedIndex(8) | lastIncludedTerm(8) |
  repeated: term(8) | index(8) | commandLength(4) | command bytes
```

All integers are big-endian. Version 2 includes the durable commit index; version 1 is intentionally rejected rather than silently reinterpreted. The decoder requires exact lengths, validates version, log entry structure, contiguous indexes, commitIndex <= log end, and rejects trailing/corrupt data. A missing state file represents fresh zero state. Save writes and syncs a same-directory temporary file, renames it over the live file, then syncs the parent directory. The returned-success boundary is after these operations; platform/filesystem crash guarantees may vary.

This is complete-state replacement storage, not a WAL. SnapshotStorage separately stores the versioned opaque state-machine image. `lastApplied` is not persisted. On restart, the node restores the snapshot first, then replays only committed retained entries after the snapshot boundary. Entries beyond commitIndex remain in the retained log but are not applied.

## Snapshot and compaction

A snapshot may cover only an applied committed index. The state machine creates opaque bytes, SnapshotStorage durably replaces the snapshot, and Storage then durably records the matching compacted boundary before the live log is replaced. The retained log begins at `lastIncludedIndex+1`; term lookup at the boundary remains available for AppendEntries matching.

When a leader's `nextIndex` falls at or before its compacted boundary, it sends InstallSnapshot. The follower validates the term and metadata, durably saves the snapshot and compacted Raft state, restores the state machine, and then publishes the new event-loop state. Successful installation sets replication progress to the boundary and normal AppendEntries resumes at boundary+1.

## Existing replication behavior

AppendEntries validates terms and previous-log matching, repairs conflicting suffixes atomically with respect to persistence, and advances follower commit index with a local-log bound. Leaders track `nextIndex`/`matchIndex` and commit only current-term entries replicated to a majority. These committed entries are now eligible for ordered state-machine application but are not submitted by a client proposal yet.

## Deferred behavior

Still out of scope:

- WAL replay or segmented WAL;
- durable KV files;
- automatic proposal forwarding;
- networked client protocol and CLI KV commands;
- real TCP/HTTP/gRPC networking;
- snapshots and compaction;
- ReadIndex and linearizable reads;
- transactions, deduplication, and production fault orchestration.
