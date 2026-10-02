# Transport wire protocol, correlation, and TCP transport

This document defines the deterministic wire format (V9.1), the request/response
correlation primitive (V9.2), and the real TCP transport plus server (V9.3/V9.4)
that speak it. `RAFT` — meaning `raft`, `kv`, and `storage` — never sees the
wire: the client side (`TCPTransport`) implements `raft.Transport`, and the
server side (`Server`) dispatches decoded requests to the same `raft.Node`
handler methods that `fault.Network` calls directly. A full two-node
replication scenario runs in `transport/tcp_raft_e2e_test.go`.

The framing, payload codecs, malformed-input contract, and request ID semantics
below are unchanged by the socket additions; `tcp.go` and `server.go` are
qualified in the final sections. The client operation protocol used by
`cmd/client` and the client listener in `cmd/server` is a separate, deliberately
small protocol described at the end: it is not a Raft message, so it does not
extend the frozen message-type range above.

## Versioning and limits

- `ProtocolVersion = 1`. Every frame carries it in a fixed header; any other
  version is rejected with `ErrUnsupportedVersion` before the payload is read.
  Future incompatible layout changes must bump this constant, and transports
  must refuse to speak versions they do not implement.
- `MaxFrameSize = 64 MiB` bounds a single frame body, including the 11-byte
  header. The on-disk snapshot encoding uses a `uint32` payload length, so a
  compacted snapshot can in principle grow to 4 GiB; the transport deliberately
  caps how much memory a single malformed or hostile frame can force a receiver
  to allocate. `ReadMessage` validates the declared body length against this
  limit before allocating any payload buffer, and the encoders reject payloads
  that would not fit.

## Frame layout

A frame is sent over a byte stream as:

```text
body length (4 BE) | version (2 BE) | message type (1) | request ID (8 BE) | payload
|------------------|------------------------------------------------|-------|
   length-prefixed |               fixed 11-byte header             |  var  |
```

- `body length` is the byte count of everything after it: header plus payload.
  It is `uint32`, so a single frame can only be as large as `MaxFrameSize`,
  which is well below the field's range.
- `version` must equal `ProtocolVersion`.
- `message type` selects one of the six kinds below.
- `request ID` is an application-chosen non-zero `uint64` that lets the requester
  correlate a reply with its original request. Zero is reserved and rejected on
  both encode and decode.

`Encode` produces a self-contained frame and copies the message payload, so
messages are immutable by convention. `ReadMessage` reads exactly one frame
from an `io.Reader`, returning `io.EOF` only on a clean stream end between
frames and `ErrTruncatedFrame` when the stream ends mid-frame.

## Message types

| Value | Kind | Wire meaning | Payload struct |
| --- | --- | --- | --- |
| 1 | request | RequestVote | `raft.RequestVoteArgs` |
| 2 | response | RequestVoteReply | `raft.RequestVoteReply` |
| 3 | request | AppendEntries | `raft.AppendEntriesArgs` |
| 4 | response | AppendEntriesReply | `raft.AppendEntriesReply` |
| 5 | request | InstallSnapshot | `raft.InstallSnapshotArgs` |
| 6 | response | InstallSnapshotReply | `raft.InstallSnapshotReply` |

The type alone tells the receiver whether the message is a request or a reply,
without touching the payload. `IsRequest` and `ResponseType` expose the pairing;
`Decode` dispatches on the type. Request and response types always come in
pairs, and the RPC payloads are exactly the existing `raft` argument/reply
structs (V8 dispatcher targets), so no Raft semantic change is needed.

## Payload encoding

All multi-byte integers are unsigned big-endian; every length field is `uint32`.

```text
vote request:      term(8) candidateIDLen(4) candidateID lastLogIndex(8) lastLogTerm(8)
vote response:     term(8) granted(1)
append request:    term(8) leaderIDLen(4) leaderID prevLogIndex(8) prevLogTerm(8)
                   count(4) { term(8) index(8) commandLen(4) command }*count leaderCommit(8)
append response:   term(8) success(1)
snapshot request:  term(8) leaderIDLen(4) leaderID lastIncludedIndex(8)
                   lastIncludedTerm(8) dataLen(4) data
snapshot response: term(8)
```

Decoded byte fields (candidate IDs, entry commands, snapshot data) are copied so
the returned Raft values own their memory; a zero-length byte field decodes to
`nil`.

## Malformed-input contract

The decoders are the receiver's last line of defense before any handler runs.
A malformed or hostile payload must never panic, index out of range, or force a
large allocation. Every decoder validates each length field against the bytes
that actually remain (`byteReader`), returns `ErrMalformedPayload` when a length
or count is impossible (including an entry count larger than the remaining
bytes can hold), returns `ErrTrailingBytes` when payload bytes remain after a
complete message, and never reads beyond the frame's declared payload.

## Request ID semantics

The correlator's `Allocate` maps request ID to a waiter. Guarantees:

1. IDs are unique and monotonic within a correlator instance, never include the
   reserved zero value, and are generated under lock so concurrent callers (and
   a racing `Close`) cannot observe duplicates.
2. Every registered request is completed exactly once, with a reply, `ErrCanceled`,
   or `ErrCorrelatorClosed`, and never leaks a waiter.
3. `Complete` on an unknown, already-completed, or late request returns `false`
   and is otherwise a no-op; duplicate and stale completions are harmless.
4. `Close` unblocks every pending waiter and rejects new registrations, and is
   idempotent and safe to run concurrently with `Complete` and `Cancel`.

A waiter selects on its completion channel; anything else (timeouts, peer
failure) is a transport decision layered on top of the correlator in V9.3+.

## Compatibility rules

Any binary running this protocol must agree on endianness, the fixed header,
the message-type numbering, and the payload layouts above. A binary that bumps
`ProtocolVersion` must reject frames from older binaries rather than guess;
a binary that admits new message kinds while speaking version 1 must be able to
explain them through `Decode` without breaking the six existing kinds.

## TCP transport (V9.3)

`TCPTransport` is the `raft.Transport` implementation (compile-time asserted via
`var _ raft.Transport = (*TCPTransport)(nil)`). It uses only the standard
library: `net` for sockets, `context` for caller cancellation, and the wire
primitives above.

- **Peer addressing.** `Config.Peers` maps `raft.NodeID` to a fixed `host:port`
  from cluster configuration. A call whose target is not in the map fails with
  `ErrUnknownPeer` before any socket work.
- **Connection reuse.** The transport keeps one connection per peer
  (`peerConn`). Concurrent RPCs to the same peer multiplex over that single
  connection: each request carries its own request ID, `writeMu` serializes
  frame writes, and the per-connection reader loop routes replies to the waiter
  that registered the matching ID (so replies may arrive out of order).
- **Cancellation.** Every RPC takes a `context.Context`. An RPC that observes
  `ctx.Done()` during dialing, write, or the wait cancels its correlator entry
  and returns the context error. No waiter leaks: the entry is delivered a reply,
  `ErrCanceled`, or `ErrCorrelatorClosed` exactly once.
- **Failure and reconnection.** A peer connection failure closes the connection
  and fails every in-flight request on it. The transport forgets the broken
  connection; the next RPC to that peer dials a fresh connection (the server may
  have restarted on the same address). `acquire` resolves concurrent dials so a
  racing pair yields one winner and one discarded duplicate connection.
- **Shutdown.** `Close` fails all in-flight requests (`ErrTransportClosed`),
  closes every connection, and rejects new calls without deadlocking.

## Server (V9.4)

`Server` is the inbound side. A `Server` is constructed with an `RPC` handler;
the concrete `*raft.Node` satisfies `RPC` via its exported `RequestVote`,
`AppendEntries`, and `InstallSnapshot` methods (`var _ RPC = (*raft.Node)(nil)`),
so the server dispatches to real Raft with no glue.

- **Lifecycle.** `Listen(addr)` binds the listener; `Serve(ctx)` runs the accept
  loop, dispatching each connection and returning `nil` once the context is
  cancelled and in-flight handlers finish. `Close` unblocks `Serve` by closing
  the listener and every connection.
- **Concurrency.** Each request frame is handled on its own goroutine, so a slow
  handler (for example a gated RPC during a test) never blocks unrelated
  requests on the same connection. Replies are written in request order per
  connection but may leave out of order; each echoes the request's ID, which is
  all the client needs to route it.
- **Reply discipline.** The server replies to request frames only; response-type
  frames are ignored. A request the handler cannot return (handler error, or a
  malformed frame) causes the connection to be closed rather than sending a
  fabricated reply, so the client's correlator fails the in-flight request and
  a later RPC reconnects.
- **Write timeout.** `SetWriteTimeout` bounds how long a reply may block on a
  peer that has stopped reading; the connection is dropped when it fires.

## Client protocol (V9.6)

`client` defines the operation protocol that `cmd/client` speaks and that
`cmd/server` serves. It is intentionally separate from the Raft wire protocol:

- **Why separate.** The Raft frame carries a `MessageType` from a frozen range
  (`RequestVote` through `SnapshotResponse`) and the header validator rejects
  anything outside it. A client operation is not a Raft message, has no
  `raft.NodeID` peer semantics, and must not be able to reach the Raft
  handlers, so it gets its own framing.
- **Framing.** One request or response per frame: a 4-byte big-endian payload
  length followed by a JSON body, capped at 1 MiB. `[]byte` fields are base64 in
  JSON, so binary KV keys and values round-trip without a second codec.
- **Operations.** `status` reports the member's role and best-known leader;
  `put` and `delete` encode a `kv.Command` and propose it through
  `NodeAPI.Propose`; `get` reads the local `MemoryStore` and is not
  linearizable.
- **Redirects.** A proposal rejected by a non-leader is reported as
  `ok=false` with the leader's `NodeID` (when the node knows one). The client,
  not the server, follows the redirect: it maps the ID to an address from its own
  config and retries there. A member never forwards another member's proposal.
- **Unresponsive members.** Each request to one member is bounded by its own
  attempt timeout, independent of the overall operation budget. A member that
  accepts a connection and never answers therefore costs one attempt rather than
  the whole operation, which is what keeps a client usable while one member of a
  cluster is frozen or gone.
- **Concurrency.** The server handles each request on its own goroutine and
  serializes replies per connection with a mutex, so frames never interleave.
  `Serve` returns once its context is cancelled and in-flight handlers finish,
  after closing the listener and every live connection.
