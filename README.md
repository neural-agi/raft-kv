# raft-kv

A small, portfolio-grade Raft-based distributed key-value store in Go.

The project is being built in contract-first stages. The current implementation includes the single-owner Raft event loop, election and replication, the in-memory KV state machine, an in-process leader-only `NodeAPI.Propose` write path, and V5.2 restart recovery from filesystem-persisted Raft state. Networked clients, durable KV files, WALs, snapshots, and compaction remain future work.

See:

- [`docs/architecture.md`](docs/architecture.md) for package boundaries, event-loop ownership, lifecycle, and data flow;
- [`docs/raft.md`](docs/raft.md) for protocol message, persistence, and proposal contracts;
- [`docs/correctness.md`](docs/correctness.md) for implementation-testable invariants.

V4 writes accept opaque encoded PUT/DELETE commands through `NodeAPI.Propose`; success means the exact entry was committed and applied. V5.1 stores currentTerm, votedFor, and the complete log through an atomic versioned filesystem file; V5.2 adds persisted commitIndex and committed-prefix replay into a fresh in-memory KV state machine. GET remains a local, non-linearizable read, and Propose has no exactly-once client semantics.

Runtime dependencies are intentionally limited to the Go standard library.
