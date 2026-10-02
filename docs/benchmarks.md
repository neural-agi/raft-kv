# Benchmarks

## What is measured

`BenchmarkThreeNodePUT` in [`integration/benchmark_test.go`](../integration/benchmark_test.go)
measures the real, in-process three-node Raft **proposal path** for a single
key `PUT`:

1. the elected leader encodes a `kv.Put` command;
2. the command goes through the real `Node.Propose` RPC path;
3. the entry is appended, replicated to a majority of the other nodes,
   committed, and applied to the KV state machine;
4. `Propose` returns only after commit and apply finish.

The benchmark reuses the existing integration harness: all three nodes use the
in-memory transport and in-memory storage, so this measures the Raft consensus
code itself. It is **not** a measurement of a production network, disk
durability, or end-to-end clients.

Cluster construction and first-leader election happen before
`b.ResetTimer()` and are excluded from the measurement. One warm-up proposal
runs before the timer starts, and each iteration issues one `PUT` for the same
key.

### Environment-dependent numbers

The reported throughput (`ns/op`), allocated bytes (`B/op`), and allocations
(`allocs/op`) depend on the machine, Go version, and load. Treat any absolute
number from this benchmark as environment-specific evidence, not as a
performance target or durability claim.

## How to run

```bash
make benchmark
```

or directly:

```bash
go test -run '^$' -bench '^BenchmarkThreeNodePUT$' -benchmem -benchtime=100ms ./integration
```

`-benchtime=100ms` keeps a single run fast; increase it (for example
`-benchtime=5s`) when you want more stable per-iteration timings. Repeat the
run a few times before comparing runs across machines.