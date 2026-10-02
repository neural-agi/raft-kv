// Package transport contains delivery adapters for the raft.Transport
// interface.
//
// The package is layered:
//
//   - wire.go, frame.go, rpc.go (V9.1) define the deterministic wire protocol:
//     versioned, length-prefixed frames for the three Raft RPC requests and
//     their replies, with battle-tested payload codecs.
//   - correlate.go (V9.2) provides the request/response correlation primitive
//     that maps a reply to the request that waited for it.
//   - tcp.go (V9.3) builds a real client-side TCP transport on top of those
//     primitives, implementing raft.Transport with one reuseable connection
//     per peer, per-connection correlation, and reconnection after failure.
//   - server.go (V9.4) provides the inbound side: a TCP listener that decodes
//     request frames and dispatches them to the raft.Node RPC handlers, then
//     sends the replies back over the same connection.
//
// The transport is the second raft.Transport implementation (the fault package
// remains first). Raft is untouched: TCPTransport and Server speak exactly the
// existing RequestVote, AppendEntries, and InstallSnapshot semantics from
// raft/interfaces.go.
package transport
