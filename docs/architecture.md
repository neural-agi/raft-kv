# Architecture

## Current milestone

The implementation covers lifecycle, leader election, RequestVote, AppendEntries, InstallSnapshot, heartbeats, opaque log replication, conflict repair, replication bookkeeping, commit-index calculation, deterministic in-memory KV state-machine application, snapshots and log compaction, the in-process client write path through NodeAPI.Propose, and filesystem persistence of complete Raft state plus separate snapshots. Restart/recovery restores a snapshot and replays only the committed retained suffix. The `transport` package defines the deterministic wire protocol (versioned framing, battle-tested payload codecs, request IDs), the request/response correlation primitive, and a real TCP client transport and inbound server that speak it: `TCPTransport` implements `raft.Transport` over per-peer sockets and `Server` dispatches decoded requests to the `raft.Node` RPC handlers, so two real nodes replicate over TCP (see `transport/tcp_raft_e2e_test.go`).

V9.6 adds the process layer. `cmd/server` assembles one node per OS process from a JSON config: filesystem Raft state and snapshot storage, the KV state machine, `TCPTransport` pointed at the configured peers, the inbound Raft `Server`, and a second listener for the client protocol in the `client` package. Three such processes form a cluster in which every inter-node message crosses a socket. `cmd/client` performs one operation (`put`, `get`, `delete`, `status`) per invocation. V9.7 exercises the same binaries under real process failures: kills, graceful stops, restarts on retained directories, and a leader frozen with `SIGSTOP` while the majority elects a replacement (see `integration/process_cluster_v96_test.go` and `integration/process_cluster_v97_test.go`).

## Package boundaries

```text
cmd/server       one cluster process: storage, transport, Raft node, client listener, signals
cmd/client       single-operation client CLI
cluster          static member identity, in-process config, and process (JSON) config
client           client protocol codec, TCP client with leader redirect, and client server
raft             lifecycle, event loop, elections, log, replication, commitment, apply ordering
transport        production delivery adapters; wire protocol, correlation, TCP client transport, and inbound RPC server
storage          filesystem-backed PersistentState and SnapshotStorage; WAL not implemented
kv               binary commands and in-memory state machine
fault            deterministic test-only fault controls
integration      deterministic in-process scenarios and real multi-process scenarios
docs             contracts and correctness argument
```

Raft sees only opaque command bytes in `LogEntry.Command`. The KV package owns command meaning and state mutation. Raft never decodes PUT or DELETE.

## Event-loop ownership

One Raft event loop owns all mutable protocol state:

- role, current term, voted-for state;
- the in-memory log;
- leader ID;
- `commitIndex` and `lastApplied`;
- the compacted log boundary;
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

State-machine application is not included in Raft persistent state. A committed applied prefix can be represented by a separate opaque snapshot. On restart, the snapshot is restored first and only committed retained entries are replayed. A restore or replay failure rejects initialization.

## Proposal flow

`NodeAPI.Propose` accepts copied opaque command bytes only on a leader. The event loop appends and persists the entry, reuses AppendEntries replication, and completes an event-loop-owned waiter only after the exact `(index, term)` entry is committed and successfully applied. Apply results are retained by log index, so concurrent proposals cannot exchange results.

Follower and candidate proposals return `ErrCodeNotLeader` with the best-known leader when available. Caller cancellation detaches the caller but does not remove an accepted log entry. Pending waiters terminate with `ErrProposalStopped` on node shutdown or `ErrProposalLost` if conflicting history removes their exact entry. There are no exactly-once client semantics.

## Persistent storage boundary

`Storage.Load` returns an independent complete `PersistentState`, or an error for unreadable/invalid persisted data other than a fresh zero state when the file does not yet exist. V7 adds a versioned compacted-boundary index/term to this state. `SnapshotStorage` separately persists opaque state-machine snapshots using the same temporary-file, sync, rename, and parent-directory-sync discipline. Compaction persists the snapshot before the compacted Raft metadata and publishes neither live boundary until both saves succeed.

V5.2 persists `currentTerm`, `votedFor`, the complete Raft log, and `commitIndex`. `commitIndex` is durable recovery metadata because it identifies exactly which log prefix may be replayed. `lastApplied`, role, leader identity, replication maps, timers, and proposal waiters remain volatile. KV contents remain in memory and are reconstructed by replaying only the persisted committed prefix. A committed-prefix replay failure rejects initialization; uncommitted suffix entries are not replayed.

## Process and client boundary

A process is configured by one JSON file (`cluster.ProcessConfig`, examples in `configs/`). It names the local node ID, the Raft listener, the client listener, the state and snapshot directories, every peer's Raft and client addresses, and optional timing overrides. `raft.Config.Peers` excludes self, so the majority formula stays `len(Peers)+1`; `TCPTransport` receives the same peers mapped to their Raft addresses.

Startup order is bind-then-serve: both listeners bind before the node starts, so a port conflict fails the process immediately instead of leaving a half-running node. Shutdown reverses it. A `SIGINT`/`SIGTERM` cancels the serving context, waits for in-flight handlers, stops the node, and closes the transport, then exits zero.

The client protocol is deliberately not the Raft wire protocol. The Raft message-type range is frozen, and a client operation is not a Raft message, so `client` defines its own length-prefixed JSON frame with `status`, `put`, `get`, and `delete` operations. A `put` or `delete` is encoded as a `kv.Command` and proposed through `NodeAPI.Propose`; a `get` is served from the local `MemoryStore` and is not linearizable. A proposal rejected by a non-leader returns the known leader, and the client redirects to it. Each request to one member is bounded by its own attempt timeout, so an unresponsive member cannot consume the caller's whole operation budget.

## Deferred behavior

Not implemented:

- proposal forwarding inside the server (the client redirects, the server does not);
- transport encryption or authentication;
- production networking beyond the TCP transport (HTTP/gRPC wire clients);
- filesystem WAL;
- snapshot transfer over the network in the process tests (the code path exists; its end-to-end coverage is not yet written);
- ReadIndex or linearizable reads;
- transactions, deduplication, or production fault orchestration.
