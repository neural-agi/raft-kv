# Failure scenarios

V6 adds a test-oriented fault controller around the existing in-process Raft transport. Faults are explicit and reproducible; they are not random network simulation and do not change production Raft protocol behavior.

V9.7 adds a second, process-level layer on top of it: real OS processes running `cmd/server`, which reach each other only through TCP sockets. The two layers answer different questions, and neither replaces the other.

| Layer | Question it answers | Mechanism |
|---|---|---|
| Deterministic (`fault`) | Does Raft stay correct under an exact, reproducible message schedule? | `fault.Network` drops, delays, errors, and partitions individual messages, with explicit release points |
| Process (`integration`) | Does the shipped binary survive real process death, a real lost connection, and a real restart? | `SIGKILL`, `SIGTERM`, `SIGSTOP`/`SIGCONT`, and separate OS processes over TCP |

The deterministic layer can express message schedules the process layer cannot: a single delayed vote, a stale snapshot released after newer traffic, a dropped append with no follower restart. The process layer can express things the deterministic layer cannot: a process that dies with no chance to flush, a file that is reopened by a fresh process, a port that is already bound, a leader frozen so that its sockets stay open while it answers nothing.

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

## Process-level scenarios (V9.7)

Each scenario starts three real processes from `cmd/server`, waits by polling observable conditions (never a fixed sleep), and asserts through the client protocol and the on-disk Raft state.

| Scenario | Setup | Expected behavior | Invariant / limitation |
|---|---|---|---|
| Canonical failover | Put, kill the leader, put again, restart the failed process | A different leader is elected, writes continue, the restarted process rejoins and converges | Leadership may move; committed data and availability may not be lost |
| Follower crash | Kill one follower, keep writing, restart it | The majority keeps committing; the follower catches up on everything it missed | A follower loss must not stop the cluster |
| Connection failure and lost majority | Kill both peers of the leader | The surviving leader keeps its role but cannot commit: the write never returns success and nothing is applied | A role is not a commit. The pending entry's fate after the majority returns is unspecified, so the test asserts member agreement, not a value |
| Leader partition | `SIGSTOP` the leader, then `SIGCONT` it | The majority elects a higher-term leader and commits; the frozen node cannot serve reads or commits; on resume it steps down and converges | A frozen process keeps its sockets open, so peers see silence rather than a reset. `SIGSTOP`/`SIGCONT` is a process-level stand-in for a partition, not a network-layer one |
| Restart and persistence | Kill a process after a commit, write more, restart it | The durable log on disk contains the committed entry; the restarted process replays it and catches up | Durability is asserted by reading the persisted state directly before the kill, so recovery cannot be confused with re-replication |
| Graceful shutdown | `SIGTERM` a process, keep writing, restart it | The process exits zero, the majority is unaffected, the restart rejoins | The signal path is the operator/supervisor path, distinct from crash recovery |
| Repeated restarts | Alternate crash and graceful restarts of one member | Every earlier write is still present after each restart | Catch-up plus replay, repeatedly |

Delayed and stale completions (the previous "delayed messages" row) are intentionally **not** reproduced with OS processes. Timing that is precise enough to release one stale reply after newer traffic cannot be forced against a separate process without test hooks in production code, and the property is already covered at the two layers that can control it: the deterministic `fault` layer (`raft/replication_lifecycle_test.go`, `raft/adversarial_test.go`, `integration/distributed_snapshot_test.go`) and the correlation layer over real sockets (`transport/tcp_test.go`). The process layer covers death, disconnection, and restart, which is what it can express honestly.

## Durability limitation

These tests exercise the documented filesystem protocol (write, file sync, atomic rename, directory sync) and process-independent reopening. On Windows, directory synchronization may return `Access is denied`; this is a platform limitation of the existing durability boundary. The tests do not prove power-loss behavior empirically and do not implement a WAL.

The process-level scenarios run the same filesystem implementation, but they do reopen it in a new process, so they additionally cover process death between a commit and its effect being observed. They do not prove power-loss behavior: a `SIGKILL` does not drop the page cache, only the process.
