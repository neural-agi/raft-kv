# V6 deterministic failure scenarios

V6 adds a test-oriented fault controller around the existing in-process Raft transport. Faults are explicit and reproducible; they are not random network simulation and do not change production Raft protocol behavior.

## Fault model

A test creates a `fault.Controller`, a `fault.Network`, and one sender-bound transport per node. The production `raft.Transport` interface is unchanged. The controller supports persistent or one-shot rules for `RequestVote` and `AppendEntries`: drop/error, delay until explicit release, bidirectional partition, and healing. Delayed requests are released by their deterministic pending ID. `fault.Trace` records injected faults and lifecycle events.

The transport interface does not carry a source ID. V6 therefore binds source identity when a test asks the fault network for a sender transport; this enables edge-specific controls without leaking test-only fields into Raft.

## Scenarios

| Scenario | Setup / injected fault | Expected behavior | Invariant / limitation |
|---|---|---|---|
| Leader crash | Stop the elected leader | Remaining majority elects and continues | Election safety and committed-prefix preservation; full global Leader Completeness remains broader than one test |
| Follower crash | Stop one follower, continue proposals, restart it | Majority commits; restarted follower catches up | Commit monotonicity and restart reconstruction |
| Minority partition | Partition one node from the other two | Majority commits; isolated node cannot falsely commit | No majority, no false proposal success |
| Leader partition | Isolate current leader | Old leader cannot commit; majority elects a higher-term leader; healing repairs stale suffix | Higher-term step-down and Log Matching |
| Delayed messages | Delay a selected edge and release explicitly after newer traffic | Stale completion is handled by existing term/index checks | Timing is deterministic; broader stale-reply coverage is scenario-specific |
| Dropped messages | Drop vote or append delivery | Retry/election behavior remains safe; no false success | Tests do not prove arbitrary scheduler fairness |
| Storage failure | Fail the relevant Save in a test Storage | Candidate state is not published before successful Save | Storage adapter failure surface is test-specific |
| State-machine failure | Fail Apply, then clear the failure | Failed index remains eligible; later entries remain ordered | Apply retry is local, not an exactly-once guarantee |
| Restart recovery | Stop, construct a new Node and state machine using the same storage | Durable term/vote/log/commitIndex reload; committed prefix replays; volatile state resets | KV is reconstructed in memory, not independently durable |
| Snapshot catch-up | Compact leader, partition follower, heal after follower falls behind | InstallSnapshot restores the compacted boundary, then AppendEntries repairs the retained suffix | Selected deterministic scenario; not a proof of every schedule |
| Snapshot persistence failure | Fail SnapshotStorage during compaction/install | Live boundary/log state does not advance or acknowledge success | Atomicity is tested at the adapter boundary |
| Snapshot restore failure | State machine rejects Restore | Installation/recovery fails without publishing the dependent live state | Node-instance recovery failure is terminal |
| Delayed/stale snapshot | Delay or release an older InstallSnapshot | Stale snapshot is ignored by term/boundary checks | Timing is explicit; arbitrary schedules remain out of scope |
| Combined failure | Combine partition/delay/storage/apply/snapshot faults | Safety boundaries hold while progress resumes after healing/retry | Selected deterministic combinations are evidence, not a proof of all combinations |

## Durability limitation

These tests exercise the documented filesystem protocol (write, file sync, atomic rename, directory sync) and process-independent reopening. On Windows, directory synchronization may return `Access is denied`; this is a platform limitation of the existing durability boundary. The tests do not prove power-loss behavior empirically or implement WAL or real networking.
