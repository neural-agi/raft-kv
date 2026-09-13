# Correctness Audit and V3 Invariants

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
| Shutdown safety | Both | Stop waits for loop exit; late completion test |
| KV command determinism | Both | Binary encoding round trips, malformed-input rejection, store tests |

## Raft application invariant

The event loop maintains:

```text
lastApplied <= commitIndex <= local lastLogIndex
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

`commitIndex` records consensus commitment. `lastApplied` records successful local application. They are intentionally distinct. A committed entry may remain unapplied when Apply fails. V3 does not persist KV state-machine contents or implement restart replay; that recovery design remains future work.

## Existing Raft invariants

- **Election Safety — both:** one vote per node/term, majority required, stale and duplicate responses ignored.
- **Term monotonicity — both:** terms only increase and durable transitions precede successful protocol responses.
- **Leader Append-Only — implementation enforced:** replication paths do not truncate the leader log.
- **Log Matching — both:** previous-log checks and conflict replacement are tested.
- **Commit monotonicity — both:** commit indexes never decrease or exceed local log bounds.
- **Replication bookkeeping — both:** stale responses are ignored and successful/failure paths preserve safe index movement.
- **Timer ownership — both:** timer work is processed by the event loop.
- **Persistence ordering — both:** injected failures do not produce successful durable transitions.

Full Leader Completeness and State Machine Safety across failover/restart remain not yet established end-to-end. GET also remains a local non-linearizable read.

## Scope exclusions

V3 does not implement or claim:

- client proposals;
- client protocol or CLI KV commands;
- real networking;
- WAL or filesystem KV persistence;
- snapshots;
- ReadIndex;
- linearizable reads;
- transactions or exactly-once semantics.
