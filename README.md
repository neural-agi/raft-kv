# raft-kv

A small, portfolio-grade Raft-based distributed key-value store in Go.

The project is being built in contract-first stages. The current stage freezes the Raft protocol, single-owner concurrency model, lifecycle, persistence boundary, proposal semantics, and KV command boundary without implementing election, replication, networking, persistence, or failure handling.

See:

- [`docs/architecture.md`](docs/architecture.md) for package boundaries, event-loop ownership, lifecycle, and data flow;
- [`docs/raft.md`](docs/raft.md) for protocol message, persistence, and proposal contracts;
- [`docs/correctness.md`](docs/correctness.md) for implementation-testable invariants.

Runtime dependencies are intentionally limited to the Go standard library.
