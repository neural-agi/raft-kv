# Architecture

## Current milestone

The implementation now covers lifecycle, leader election, RequestVote, AppendEntries, heartbeats, opaque log replication, conflict repair, replication bookkeeping, and commit-index calculation for a fixed three-node cluster. It does not apply committed entries to the KV state machine yet.

## Package boundaries

```text
cmd/server       process wiring (later)
cmd/client       client wiring (later)
cluster          static member identity and configuration
raft             lifecycle, event loop, elections, log, replication, commitment
transport        production delivery adapter (not implemented)
storage          production durable adapter (not implemented)
kv               deterministic command/state-machine contracts; not applied yet
fault            future test-only fault controls
integration      future end-to-end tests
docs             contracts and correctness argument
```

Raft sees only opaque command bytes in `LogEntry.Command`. The KV package owns command meaning and is not called by the current milestone.

## Event-loop ownership

One Raft event loop owns all mutable protocol state:

- role, current term, voted-for state;
- the in-memory log;
- leader ID;
- `commitIndex` and `lastApplied`;
- election and heartbeat timer state;
- leader `nextIndex` and `matchIndex`;
- election vote tracking and replication decisions.

RPC handlers enqueue immutable request events. Timer callbacks cause work to be processed by the event loop. Transport calls run outside the loop and enqueue immutable response events. No external goroutine directly reads or mutates protocol state.

Blocking storage operations are invoked by the event-loop transition before acknowledging a durable state change. This keeps persistence ordering explicit. A future implementation may move storage calls outside the loop only if it preserves event ordering and stale-completion guards.

## Lifecycle

A node moves through:

```text
Created -> Initialized -> Running -> Stopped
```

Initialization loads and validates the complete persistent state. Start creates the event loop and election timer. Running nodes accept RequestVote and AppendEntries. Stop enqueues a shutdown event and waits for the loop to exit.

## Log and replication flow

```text
leader election
      |
      v
leader initializes nextIndex/matchIndex
      |
      v
heartbeat timer -> AppendEntries request
      |
      v
follower prev-log check and conflict repair
      |
      v
AppendEntries reply -> leader bookkeeping
      |
      v
majority + current-term rule -> commitIndex
      |
      v
lastApplied remains unchanged in this milestone
```

The leader sends an empty AppendEntries when a follower is caught up and a suffix when it is behind. Failed replies decrement `nextIndex`; successful replies advance `matchIndex` and `nextIndex`. Stale replies are ignored.

## Commit versus apply

`commitIndex` means the Raft log prefix is known committed. `lastApplied` is the separate application position and remains unchanged in this milestone. No committed entry is passed to `StateMachine.Apply` yet.

## Persistence

`Storage` persists exactly the current term, voted-for node, and complete log. Term, vote, and log changes are saved before successful protocol responses. Commit indexes, timers, and replication maps remain volatile.

## Deferred behavior

Not implemented in this milestone:

- KV state-machine application;
- PUT, DELETE, GET, and client proposals;
- real TCP/HTTP/gRPC networking;
- filesystem WAL;
- snapshots and compaction;
- read-index or linearizable reads;
- general fault injection.
