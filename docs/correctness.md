# Correctness Audit and Invariants

This audit covers the current lifecycle, election, AppendEntries, log replication, and commitment core. It does not implement or validate KV state-machine application. Status labels describe evidence from the implementation and deterministic tests; they are not mathematical proofs.

## Status summary

| Invariant | Status | Evidence |
|---|---|---|
| Election Safety | Both | Event-loop vote accounting, durable votes, duplicate/stale-response tests, and three-node election tests |
| Term monotonicity | Both | All term-adoption paths use higher-term checks and persistence; higher-term RequestVote/AppendEntries/reply tests |
| Leader Append-Only | Enforced by implementation | Replication never truncates the leader log; direct leader proposal append remains deferred to the next milestone |
| Log Matching | Both | Previous-log checks, atomic conflict replacement, exact suffix tests, and log unit tests |
| Leader Completeness | Not yet established end-to-end | Log freshness and current-term commitment rules are implemented, but a full failover history test remains future work |
| State Machine Safety | Not yet established | Entries are intentionally not applied; committed/applied separation is tested, but no state-machine implementation exists |
| Commit monotonicity | Both | Leader/follower paths only increase commitIndex and tests cover bounded/non-decreasing behavior |
| Applied bound | Enforced by implementation | No application path exists; `lastApplied` remains zero and separate from commitIndex |
| Replication bookkeeping | Both | Event-loop ownership, monotonic success updates, bounded retries, and stale-reply tests |
| Persistence ordering | Both | Term/vote/log saves precede successful acknowledgments; injected storage-failure tests |
| Timer ownership | Both | Timers are owned by the event loop; valid/stale contact and heartbeat-loss tests |
| Shutdown safety | Both | Stop waits for loop exit and late completion test verifies no blocking/resurrection |

## Term transitions

Terms are changed only by event-loop-owned transitions:

- election timeout increments the term and self-vote is persisted before candidacy;
- newer RequestVote terms are persisted before adoption;
- newer AppendEntries terms are persisted before adoption;
- newer AppendEntries replies are handled through the event loop and force follower state.

Stale messages return without term mutation. Higher terms clear obsolete election state, including election term and vote-response tracking. Persistence failures leave the in-memory term/vote state unchanged.

## Election Safety

A node grants at most one vote per term because `VotedFor` is checked and persisted before a successful response. Candidates count a target only once and only for the current election term. A candidate needs a strict majority. Higher-term replies force follower state.

Validated by deterministic tests covering duplicate votes, stale responses, log freshness ordering, higher-term transitions, and repeated three-node elections.

## RequestVote log freshness

Candidate logs are compared by last term first and last index second:

- a higher last term wins even with a shorter index;
- equal last term uses the higher index;
- a lower last term loses even with a larger index.

These cases are covered by `TestVoteLogFreshnessOrdering` and the existing RequestVote tests.

## AppendEntries safety

Follower handling follows this order:

1. stale term rejection without leader recording or timer reset;
2. durable adoption of a higher term;
3. role/election-state transition to follower;
4. previous-log validation;
5. leader recording and election timer reset only after the previous-log check succeeds;
6. candidate-log conflict calculation on a copy;
7. durable replacement/append;
8. monotonic bounded commit advancement.

The candidate-log copy is important: if storage fails, the event-loop log remains unchanged and the reply is unsuccessful.

Validated with empty logs, matching prefixes, mismatching terms, exact matching suffixes, shorter incoming suffixes, multi-entry conflict replacement, and injected persistence failures.

## Leader replication

On leadership, followers start with `nextIndex = lastLogIndex + 1`, `matchIndex = 0`, and the leader has its own match index at the local last index.

Success replies advance replication positions only for the matching leader term and only to indexes included in the acknowledged request. Failure replies decrement `nextIndex` no lower than one and retry. Stale replies cannot roll back progress.

The current deterministic transport supports independent vote and AppendEntries dropping. Tests cover stale failures, successful progress, conflict repair, and lagging follower recovery paths. Fully delayed/reordered transport queues remain future test infrastructure; current stale-response tests inject immutable response events directly through the event loop.

## Current-term commitment rule

The leader scans newest to oldest and advances `commitIndex` only for an index whose entry has the leader's current term and whose `matchIndex` count is a majority. Once that current-term index commits, all preceding entries in the prefix are committed as well.

Validated scenarios include:

- old-term entry alone does not commit;
- current-term entry commits with a majority;
- a current-term entry commits a preceding old-term prefix;
- minority replication does not commit;
- commitIndex does not decrease or exceed the local last index.

## Heartbeats and timers

Only a valid current/new-term AppendEntries whose previous-log prefix matches resets the follower timer. Stale requests and mismatching-prefix failures do not reset it. Heartbeat scheduling and election timeout processing occur in the event loop.

The deterministic tests confirm leader recognition, valid/stale heartbeat behavior, stopped heartbeat election behavior, and stale completion handling. Timer-driven tests retain bounded real-time polling only for observing an initial heartbeat; the loss-to-election transition itself is explicitly triggered through a test-only event.

## Event-loop concurrency

Mutable protocol state is never accessed directly by transport goroutines. Transport calls run outside the loop and enqueue immutable replies. The event channel is buffered, and completion enqueue waits for either delivery, node shutdown, or a bounded shutdown fallback; late completions cannot resurrect a stopped node.

Log entries and command byte slices are copied when crossing storage, transport, and debug boundaries. Maps are copied for debug snapshots. The race detector passes across the full repository and repeated Raft tests.

## Persistence ordering

The following transitions require successful `Storage.Save` before success is exposed:

- current-term adoption;
- vote changes;
- follower log append/replacement;
- leader test-only log append used by replication tests.

Injected storage failures verify that failed term and log transitions do not change the event-loop state or report success.

## Log implementation

The in-memory log enforces:

- indexes beginning at one;
- contiguous appends;
- synthetic index-zero term zero matching;
- defensive entry/command copies;
- out-of-range lookup failure;
- truncation from zero or a middle index;
- atomic replacement validation.

## Committed versus applied

`commitIndex` and `lastApplied` are distinct. The current core never calls `StateMachine.Apply`; committed entries remain unapplied and `lastApplied` remains unchanged. Full State Machine Safety begins with the next KV milestone.

## Remaining concerns

- Full failover Leader Completeness is not yet established by an end-to-end history-preservation test.
- The test transport does not yet provide a general delayed/reordered delivery queue; stale reply behavior is tested with direct event injection.
- Storage calls remain synchronous inside the event loop, which is correct but may require a carefully ordered asynchronous design later for latency.
