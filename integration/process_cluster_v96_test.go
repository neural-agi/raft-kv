package integration

// process_cluster_v96_test.go covers the V9.6 canonical scenario against real OS
// processes: three independent servers, real TCP between them, one leader,
// replicated writes, a crash, an election, more writes, and a restart that
// rejoins with the same directories.

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/client"
	"github.com/neural-agi/raft-kv/raft"
)

func TestProcessClusterCanonicalFailoverScenario(t *testing.T) {
	c := newProcessCluster(t)

	// Step 1: every member is a separate process reached over TCP sockets, and
	// the startup manifest names the TCP transport. If any member were wired to
	// an in-process transport this would fail, which is the point.
	requireTCPTransport(t, c)

	// Step 2: the three independent processes converge on exactly one leader.
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatalf("elect leader: %v", err)
	}
	t.Logf("initial leader: %s", leader.id)

	// Step 3: a write goes through the client protocol to the leader.
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := c.put(ctx, "foo", "bar"); err != nil {
		t.Fatalf("put foo: %v", err)
	}

	// Step 4: the value replicates to every process.
	if err := c.waitForValue(convergenceTimeout, "foo", "bar", c.members...); err != nil {
		t.Fatalf("replicate foo: %v", err)
	}

	// Step 5: deletion replicates too, so a full write lifecycle is exercised
	// over the network and not just a single put.
	if err := c.put(ctx, "temporary", "discard-me"); err != nil {
		t.Fatalf("put temporary: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "temporary", "discard-me", c.members...); err != nil {
		t.Fatalf("replicate temporary: %v", err)
	}
	if err := c.client.Delete(ctx, []byte("temporary")); err != nil {
		t.Fatalf("delete temporary: %v", err)
	}
	if err := c.waitForMissing(convergenceTimeout, "temporary", c.members...); err != nil {
		t.Fatalf("replicate delete: %v", err)
	}

	// Step 6: the leader process is killed outright. No graceful shutdown, no
	// chance to hand over leadership.
	c.kill(leader)
	if leader.isRunning() {
		t.Fatalf("member %s still running after kill", leader.id)
	}

	// Step 7: the two survivors elect a replacement.
	newLeader, err := c.waitForNewLeader(convergenceTimeout, leader.id)
	if err != nil {
		t.Fatalf("replacement leader: %v", err)
	}
	t.Logf("replacement leader: %s (previous %s)", newLeader.id, leader.id)

	// Step 8: the cluster keeps accepting writes through the new leader.
	ctx2, cancel2 := context.WithTimeout(context.Background(), opTimeout)
	defer cancel2()
	if err := c.put(ctx2, "baz", "qux"); err != nil {
		t.Fatalf("put baz after failover: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "baz", "qux", c.others(leader)...); err != nil {
		t.Fatalf("replicate baz after failover: %v", err)
	}

	// Step 9: the failed process comes back with the same config and the same
	// directories.
	if err := c.restart(leader); err != nil {
		t.Fatalf("restart %s: %v", leader.id, err)
	}

	// Step 10: it rejoins as a follower and catches up on everything it missed,
	// including the value written before the crash.
	if err := c.waitForValue(convergenceTimeout, "foo", "bar", c.members...); err != nil {
		t.Fatalf("restarted member lost pre-crash value: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "baz", "qux", c.members...); err != nil {
		t.Fatalf("restarted member did not catch up: %v", err)
	}
	if err := c.waitForMissing(convergenceTimeout, "temporary", c.members...); err != nil {
		t.Fatalf("restarted member resurrected a deleted key: %v", err)
	}

	// Exactly one leader again, and it is one of the three.
	final, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatalf("final leader: %v", err)
	}
	if final.id == leader.id && !leader.isRunning() {
		t.Fatalf("leader reported as %s but that process is down", final.id)
	}
}

// requireTCPTransport asserts that every member's startup manifest names the TCP
// transport. This is the guard against a process that quietly falls back to an
// in-process transport, which would make every other test in this file
// meaningless.
func requireTCPTransport(t *testing.T, c *processCluster) {
	t.Helper()
	const want = "transport=*transport.TCPTransport"
	for _, m := range c.members {
		if !m.isRunning() {
			t.Fatalf("member %s is not running:\n%s", m.id, c.diagnostics())
		}
		out := m.outputTail()
		if !strings.Contains(out, want) {
			t.Fatalf("member %s did not start with %s:\n%s", m.id, want, out)
		}
		if !strings.Contains(out, "ready") {
			t.Fatalf("member %s never reported ready:\n%s", m.id, out)
		}
	}
}

// TestProcessClusterFollowerRefusesWrite checks the client-visible contract on a
// healthy cluster: every member answers status and agrees on one leader, and a
// client that can only reach a follower cannot commit anything.
func TestProcessClusterFollowerRefusesWrite(t *testing.T) {
	c := newProcessCluster(t)
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatal(err)
	}
	follower := c.others(leader)[0]

	// Every member answers status and agrees on the leader.
	for _, m := range c.members {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		role, knownLeader, err := c.client.Status(ctx, m.id)
		cancel()
		if err != nil {
			t.Fatalf("status %s: %v", m.id, err)
		}
		if knownLeader != leader.id {
			t.Fatalf("member %s reports leader %q, want %q", m.id, knownLeader, leader.id)
		}
		if m.id == leader.id && role != raft.Leader {
			t.Fatalf("leader %s reports role %v", m.id, role)
		}
		if m.id != leader.id && role == raft.Leader {
			t.Fatalf("follower %s reports itself leader", m.id)
		}
	}

	// A client whose only reachable member is a follower cannot commit: the
	// follower rejects the proposal and names the leader it knows.
	followerOnly := client.NewClient(map[raft.NodeID]string{follower.id: follower.clientAddr})
	followerOnly.SetTimeouts(2*time.Second, 2*time.Second, opTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := followerOnly.Put(ctx, []byte("direct"), []byte("value")); err == nil {
		t.Fatal("follower committed a write for a client that cannot reach the leader")
	}
	if err := c.waitForMissing(2*time.Second, "direct", c.members...); err != nil {
		t.Fatalf("rejected write became visible: %v", err)
	}
}

// TestProcessClusterCLI drives the same cluster through the cmd/client binary so
// the shipped entry point is covered, not just the Go client.
func TestProcessClusterCLI(t *testing.T) {
	c := newProcessCluster(t)
	cliPath := clientBinary(t)
	cfg := c.cfgPath

	if out, code := runCLI(t, cliPath, cfg, "put", "clikey", "clivalue"); code != 0 {
		t.Fatalf("cli put failed (%d): %s", code, out)
	}
	if err := c.waitForValue(convergenceTimeout, "clikey", "clivalue", c.members...); err != nil {
		t.Fatalf("cli put did not replicate: %v", err)
	}
	if out, code := runCLI(t, cliPath, cfg, "get", "clikey"); code != 0 || !strings.Contains(out, "clivalue") {
		t.Fatalf("cli get = %q (code %d), want clivalue", out, code)
	}
	if out, code := runCLI(t, cliPath, cfg, "status"); code != 0 || !strings.Contains(out, "leader=") {
		t.Fatalf("cli status = %q (code %d)", out, code)
	}
	if out, code := runCLI(t, cliPath, cfg, "delete", "clikey"); code != 0 {
		t.Fatalf("cli delete failed (%d): %s", code, out)
	}
	if err := c.waitForMissing(convergenceTimeout, "clikey", c.members...); err != nil {
		t.Fatalf("cli delete did not replicate: %v", err)
	}
	if out, code := runCLI(t, cliPath, cfg, "get", "clikey"); code != 0 || !strings.Contains(out, "missing") {
		t.Fatalf("cli get after delete = %q (code %d), want missing", out, code)
	}
	if out, code := runCLI(t, cliPath, cfg, "get", "clikey", "extra"); code == 0 {
		t.Fatalf("cli accepted a malformed get: %q", out)
	}
}

// runCLI runs the client binary once and returns its combined output and exit
// code. A non-zero exit is an expected outcome for some assertions, so it is
// returned rather than raised.
func runCLI(t *testing.T, path, config string, args ...string) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, append([]string{"-config", config}, args...)...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(out), exitErr.ExitCode()
	}
	t.Fatalf("run cli %v: %v", args, err)
	return string(out), -1
}
