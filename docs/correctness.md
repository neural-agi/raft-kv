# Correctness Audit and V6 Failure Evidence

This document classifies the current election, replication, commitment, and KV state-machine behavior. Status labels are evidence-based and are not mathematical proofs.

## Status summary

| Invariant | Status | Evidence |
|---|---|---|
| Election Safety | Both | Event-loop vote accounting, durable votes, duplicate/stale-response tests, repeated three-node elections |
| Term monotonicity | Both | Higher-term adoption paths and adversarial term tests |
| Leader Append-Only | Enforced by implementation | Replication never truncates leader state; client append remains deferred |
| Log Matching | Both | Previous-log checks, atomic conflict replacement, adversarial suffix tests |
| Leader Completeness | Not yet established end-to-end | Log freshness and current-term commitment are implemented; full failover-history test remains future work |
| Commit monotonicity | Both | Leader/follower paths only increase commitIndex and bounded tests |
| Last-applied monotonicity | Both | Apply loop advances only after success; ordered/retry tests |
| Apply ordering | Both | Event-loop apply loop selects `lastApplied+1`; multi-entry tests |
| State Machine Safety | Not yet established end-to-end | Local application ordering is tested, but no multi-node applied-state/failover proof exists yet |
| Persistence ordering | Both | Term/vote/log saves precede successful acknowledgments; injected failures |
| Timer ownership | Both | Event-loop timers and heartbeat-loss tests |
| Shutdown safety | Both | Stop waits for loop exit and completes buffered proposal waiters; late completion tests |
| KV command determinism | Both | Binary encoding round trips, malformed-input rejection, store tests |
| Proposal completion safety | Both | Event-loop-owned exact index/term waiters; leader/follower, concurrent, cancellation, and shutdown tests |
| Proposal result correlation | Both | Apply results retained by log index and concurrent proposal tests |
| Proposal cancellation semantics | Both | Cancellation detaches caller without deleting accepted log entry; cancellation tests |
| Proposal failover/replacement semantics | Enforced but not fully tested | Exact `(index, term)` identity is checked and replacement terminates the waiter; full failover proposal scenario remains future work |
| Proposal index uniqueness | Both | Event-loop serialization assigns each accepted append from the current last index; concurrent proposal tests |
| Proposal completion requires commitment | Both | Completion checks `index <= commitIndex`; local append alone is insufficient |
| Proposal completion requires application | Both | Completion checks `index <= lastApplied` and an exact apply result; Apply-failure tests |

| Applied bound | Both | Apply loop advances only while `lastApplied < commitIndex`; V3/V4 tests |
| Failover proposal safety | Not established | No complete deterministic failover proposal suite yet |
| Restart recovery | Enforced and tested locally | V5.2 restores durable state and replays exactly the persisted committed prefix; broader failover/restart safety remains incomplete |
| Vote Persistence | Both | Vote changes publish only after Storage.Save succeeds; failure-injection tests |
| Log Persistence | Both | Proposal/AppendEntries log changes publish only after Storage.Save succeeds; filesystem and in-memory tests |
| Save Atomicity | Both | Temporary-file replacement and failed-rename preservation tests |
| Load Validation | Both | Version, length, truncation, trailing-data, entry, and contiguous-index validation tests |
| Persistent State Isolation | Both | FileStorage copies encoded/decoded buffers; aliasing tests |
| Binary Command Preservation | Both | Length-prefixed binary command encoding and filesystem round-trip tests |
| Crash Durability Boundary | Enforced by implementation | File sync, atomic rename, and parent-directory sync precede successful Save; crash recovery remains untested |
| V5.1 filesystem storage | Both | Fresh, round-trip, corruption, failure, and atomicity tests |
| Persistent CommitIndex Safety | Both | CommitIndex is validated against the compacted boundary/log end and persisted before publication |
| Commit Persistence Ordering | Both | Leader/follower commit advances use candidate Save before live publication; injected failure test |
| Recovery State Reconstruction | Both | Fresh follower state, snapshot restore, volatile-role reset, and post-snapshot replay tests |
| LastApplied Recovery | Both | Successful recovery initializes `lastApplied == commitIndex` |
| Committed State-Machine Replay | Both | Replay uses opaque log commands in index order |
| Uncommitted Entry Non-Replay | Both | Durable suffix remains in the log but is excluded from recovery replay |
| Recovery Failure Safety | Both | Snapshot corruption/restore failure and Apply failure during committed replay reject initialization |
| Restart Leadership Safety | Both | Restart always reconstructs as follower |
| Restart Proposal Safety | Enforced by implementation | Proposal waiters are runtime-only and are not persisted |
| Restart Log Safety | Both | Complete log survives process-independent reopen |
| Failover + Restart Safety | Tested under selected deterministic failures | V6 exercises leader/follower stop, partitions, healing, and restart; this is not a proof of all schedules |
| Fault-controller determinism | Enforced + tested | Explicit drop/error/delay/release/partition/heal controls and trace tests |
| Failure observability | Enforced + tested | Structured fault trace and diagnostic invariant assertions |
| Partition Safety | Tested under selected deterministic failures | Majority/minority and leader-isolation scenarios; arbitrary combinations remain unestablished |
| Recovery under injected failure | Tested under selected deterministic failures | Restart, snapshot storage, transport, snapshot restore, and Apply-failure boundaries are exercised selectively |

## Raft application invariant

The event loop maintains:

```text
`snapshotBoundary <= lastApplied <= commitIndex <= local lastLogIndex`
```

Only committed entries are eligible for application. Entries are selected strictly by increasing index. A successful Apply advances `lastApplied` by one entry. A failed Apply leaves the failed entry at `lastApplied+1` and prevents later entries from being applied. An event-loop-owned retry timer eventually schedules another attempt without requiring a new Raft protocol event.

The V3 tests validate:

- uncommitted entries are not applied;
- a committed prefix is applied in order;
- prior successes are not replayed on unrelated events;
- a later entry cannot bypass an earlier failure;
- an Apply failure does not advance `lastApplied` past the failed entry;
- command bytes passed to the state machine equal the corresponding log entry bytes.

## KV atomicity and semantics

Command decoding and validation happen before the store map is changed. PUT replaces a copied value. DELETE is an idempotent success, returning `deleted` or `missing`. GET returns copied data. Malformed commands do not mutate the store.

The in-memory store is expected to be called by the Raft event loop for replication application. Its read mutex also prevents concurrent test/internal reads from racing with application.

## Committed versus applied

`commitIndex` records consensus commitment. `lastApplied` records successful local application. They are intentionally distinct. A committed entry may remain unapplied when Apply fails. V7 additionally permits an applied committed prefix to be represented by an opaque durable snapshot; entries at or below its boundary are not replayed after restart.

## Existing Raft invariants

- **Election Safety — both:** one vote per node/term, majority required, stale and duplicate responses ignored.
- **Term monotonicity — both:** terms only increase and durable transitions precede successful protocol responses.
- **Leader Append-Only — implementation enforced:** replication paths do not truncate the leader log.
- **Log Matching — both:** previous-log checks and conflict replacement are tested.
- **Commit monotonicity — both:** commit indexes never decrease or exceed local log bounds.
- **Replication bookkeeping — both:** stale responses are ignored and successful/failure paths preserve safe index movement.
- **Timer ownership — both:** timer work is processed by the event loop.
- **Persistence ordering — both:** injected failures do not produce successful durable transitions.

Full Leader Completeness and State Machine Safety across failover/restart remain not yet established end-to-end. Proposal success is local to the exact committed/applied entry and does not establish linearizability or exactly-once client semantics. GET also remains a local non-linearizable read.

## V4 proposal audit status

The proposal path is event-loop-owned from request acceptance through waiter completion. A successful result requires the exact entry to be committed, applied, and associated with a retained result by log index. A caller cancellation does not undo an accepted command. A conflicting replacement produces `ErrProposalLost`; node shutdown produces `ErrProposalStopped`. Higher-term and same-term leader step-down paths invalidate only waiters whose exact entries are no longer present with the original term.

The following remain deliberately unestablished: complete proposal behavior across a leadership failover, restart/replay recovery, and full multi-node State Machine Safety. The current tests validate local ordering, exact result correlation, malformed-command failure, cancellation, shutdown, and the three-node happy path, but these are not mathematical proofs.

## V6 failure-testing evidence

V6 adds deterministic, test-only fault injection. The controller can drop, error, or delay a selected RequestVote or AppendEntries edge, explicitly release delayed requests, partition/heal node pairs, and record injected faults. The transport remains an implementation of the unchanged `raft.Transport` interface; source identity is bound by the test network when constructing a sender transport.

The failure tests establish selected evidence for leader/follower failure, partition healing, message delay/drop, storage errors, Apply retry, and restart during activity. A passing deterministic scenario is not a mathematical proof of the global Raft invariant under every schedule. Crash durability remains an implementation boundary, not an empirically proven power-loss result.

## Scope exclusions

V6 does not implement or claim:

- networked client protocol or automatic proposal forwarding;
- client protocol or CLI KV commands;
- real networking;
- WAL replay or segmented WAL;
- durable KV persistence;
- durable KV files;
- ReadIndex;
- linearizable reads;
- transactions or exactly-once semantics.
