// Command server runs one node of a multi-process cluster: a Raft node that
// reaches its peers over real TCP sockets, plus a client listener that accepts
// the small client protocol. Everything it needs comes from a JSON config file
// (see configs/three-node.json).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/neural-agi/raft-kv/client"
	"github.com/neural-agi/raft-kv/cluster"
	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
	"github.com/neural-agi/raft-kv/storage"
	"github.com/neural-agi/raft-kv/transport"
)

const (
	raftStateFile   = "raft-state"
	snapshotFile    = "snapshot"
	shutdownGrace   = 10 * time.Second
	fatalExitCode   = 1
	successExitCode = 0
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr *os.File) int {
	flags := flag.NewFlagSet("raft-kv-server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the node's JSON cluster config")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "usage: raft-kv-server -config <path>")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return fatalExitCode
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "raft-kv-server: -config is required")
		flags.Usage()
		return fatalExitCode
	}

	cfg, err := cluster.LoadProcessConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "raft-kv-server: %v\n", err)
		return fatalExitCode
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	if err := runNode(ctx, cfg, stdout); err != nil {
		fmt.Fprintf(stderr, "raft-kv-server: %v\n", err)
		return fatalExitCode
	}
	return successExitCode
}

// runNode owns the whole process lifecycle: it binds both listeners before the
// Raft node starts so a port conflict fails fast, then shuts down in the reverse
// order (servers first, then the node, then the transport).
func runNode(ctx context.Context, cfg cluster.ProcessConfig, stdout *os.File) error {
	stateStorage, err := storage.NewFileStorage(filepath.Join(cfg.Node.StorageDir, raftStateFile))
	if err != nil {
		return fmt.Errorf("open state storage: %w", err)
	}
	snapshotStorage, err := storage.NewFileSnapshotStorage(filepath.Join(cfg.Node.SnapshotDir, snapshotFile))
	if err != nil {
		return fmt.Errorf("open snapshot storage: %w", err)
	}
	store := kv.NewMemoryStore()

	// The transport is the only path to peers. Peers map excludes self: a node
	// never dials itself.
	nodeTransport := transport.NewTCPTransport(transport.Config{
		Peers:        cfg.PeerTransportAddrs(),
		DialTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	node, err := raft.NewNode(raft.Config{
		ID:                 cfg.Node.ID,
		Peers:              cfg.PeerIDs(),
		ElectionTimeoutMin: cfg.ElectionTimeoutMin.D(),
		ElectionTimeoutMax: cfg.ElectionTimeoutMax.D(),
		HeartbeatInterval:  cfg.HeartbeatInterval.D(),
		ApplyRetryInterval: cfg.HeartbeatInterval.D(),
		ReplicationTimeout: cfg.ReplicationTimeout.D(),
		Transport:          nodeTransport,
		Storage:            stateStorage,
		SnapshotStorage:    snapshotStorage,
		StateMachine:       store,
	})
	if err != nil {
		return fmt.Errorf("create node: %w", err)
	}

	// Recovery (replay of the committed log and snapshot restore) happens before
	// the node is allowed to serve.
	loadCtx, cancelLoad := context.WithTimeout(ctx, shutdownGrace)
	defer cancelLoad()
	if err := node.Initialize(loadCtx); err != nil {
		return fmt.Errorf("initialize node: %w", err)
	}

	raftServer := transport.NewServer(node)
	if err := raftServer.Listen(cfg.Node.Listen); err != nil {
		return fmt.Errorf("listen raft %s: %w", cfg.Node.Listen, err)
	}
	defer raftServer.Close()

	clientServer := client.NewServer(node, store)
	if err := clientServer.Listen(cfg.Node.ClientListen); err != nil {
		return fmt.Errorf("listen client %s: %w", cfg.Node.ClientListen, err)
	}
	defer clientServer.Close()

	serveCtx, stopServing := context.WithCancel(ctx)
	defer stopServing()
	raftDone := make(chan error, 1)
	go func() { raftDone <- raftServer.Serve(serveCtx) }()
	clientDone := make(chan error, 1)
	go func() { clientDone <- clientServer.Serve(serveCtx) }()

	startCtx, cancelStart := context.WithTimeout(ctx, shutdownGrace)
	defer cancelStart()
	if err := node.Start(startCtx); err != nil {
		stopServing()
		return fmt.Errorf("start node: %w", err)
	}

	// The manifest is a single line on stdout: the harness uses it to confirm
	// which process is up and that peers are reached over real TCP sockets.
	fmt.Fprintf(stdout, "raft-kv node=%s transport=%T raft_listen=%s client_listen=%s ready\n",
		cfg.Node.ID, nodeTransport, raftServer.Addr(), clientServer.Addr())
	_ = stdout.Sync()

	var runErr error
	select {
	case <-ctx.Done():
		fmt.Fprintf(stdout, "raft-kv node=%s shutting down\n", cfg.Node.ID)
	case err := <-raftDone:
		if err != nil {
			runErr = fmt.Errorf("raft server: %w", err)
		}
	case err := <-clientDone:
		if err != nil {
			runErr = fmt.Errorf("client server: %w", err)
		}
	}

	// Shutdown order: stop accepting client and Raft traffic, wait for in-flight
	// handlers, then stop the node, then close the transport.
	stopServing()
	waitFor(raftDone, shutdownGrace)
	waitFor(clientDone, shutdownGrace)

	stopCtx, cancelStop := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelStop()
	if err := node.Stop(stopCtx); err != nil && runErr == nil {
		runErr = fmt.Errorf("stop node: %w", err)
	}
	if err := nodeTransport.Close(); err != nil && runErr == nil {
		runErr = fmt.Errorf("close transport: %w", err)
	}
	if err := clientServer.Close(); err != nil && runErr == nil && !errors.Is(err, net.ErrClosed) {
		runErr = fmt.Errorf("close client server: %w", err)
	}
	if err := raftServer.Close(); err != nil && runErr == nil && !errors.Is(err, net.ErrClosed) {
		runErr = fmt.Errorf("close raft server: %w", err)
	}
	return runErr
}

// waitFor drains a Serve result without blocking shutdown past the grace period.
func waitFor(done <-chan error, grace time.Duration) {
	select {
	case <-done:
	case <-time.After(grace):
	}
}
