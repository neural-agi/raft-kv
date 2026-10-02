package integration

// process_cluster_v97_test.go covers the V9.7 network failure scenarios against
// real OS processes: follower crash, a leader that cannot commit without a
// majority, a leader that is isolated, and restart-driven recovery from
// persisted state. The partition is produced with SIGSTOP/SIGCONT rather than a
// fault injector, so the process is genuinely unable to answer while the OS
// still holds its sockets open: peers see an unreachable leader, which is what a
// network partition looks like from the Raft side.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

// TestProcessFollowerCrashAndRecovery covers scenario 8.2: losing a follower must
// not cost availability, and the follower must catch up when it returns.
func TestProcessFollowerCrashAndRecovery(t *testing.T) {
	c := newProcessCluster(t)
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatal(err)
	}
	follower := c.others(leader)[0]

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := c.put(ctx, "before", "crash"); err != nil {
		t.Fatalf("put before: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "before", "crash", c.members...); err != nil {
		t.Fatalf("replicate before: %v", err)
	}

	c.kill(follower)
	if follower.isRunning() {
		t.Fatalf("follower %s still running after kill", follower.id)
	}

	// The majority survives, so writes keep committing. Leadership is allowed to
	// move here (a brief timeout on the remaining follower is legal Raft
	// behavior); what must not happen is loss of availability or data.
	if err := c.put(ctx, "during", "outage"); err != nil {
		t.Fatalf("put during follower outage: %v", err)
	}
	live := c.liveMembers()
	if len(live) != 2 {
		t.Fatalf("expected 2 live members, got %d", len(live))
	}
	if err := c.waitForValue(convergenceTimeout, "during", "outage", live...); err != nil {
		t.Fatalf("replicate during outage: %v", err)
	}
	if _, err := c.waitForLeader(startTimeout); err != nil {
		t.Fatalf("cluster lost its leader during a follower outage: %v", err)
	}

	// The follower returns with the same directories and catches up on both the
	// pre-crash and during-outage values.
	if err := c.restart(follower); err != nil {
		t.Fatalf("restart follower: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "before", "crash", c.members...); err != nil {
		t.Fatalf("restarted follower lost pre-crash value: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "during", "outage", c.members...); err != nil {
		t.Fatalf("restarted follower did not catch up: %v", err)
	}
	if _, err := c.waitForLeader(startTimeout); err != nil {
		t.Fatalf("cluster has no leader after follower recovery: %v", err)
	}
}

// TestProcessLeaderCannotCommitWithoutMajority covers scenarios 8.3 and 9 from the
// commit side: with the other two processes dead, the surviving leader keeps its
// role but cannot commit, so a client write fails and nothing is applied. When a
// majority returns, the cluster commits again and every live member agrees.
func TestProcessLeaderCannotCommitWithoutMajority(t *testing.T) {
	c := newProcessCluster(t)
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatal(err)
	}
	survivors := c.others(leader)

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := c.put(ctx, "committed", "value"); err != nil {
		t.Fatalf("put committed: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "committed", "value", c.members...); err != nil {
		t.Fatalf("replicate committed: %v", err)
	}

	// Real connection failures: both peers are killed, so the leader's outbound
	// RPCs are refused and time out.
	for _, m := range survivors {
		c.kill(m)
	}
	if len(c.liveMembers()) != 1 {
		t.Fatalf("expected a single live member, got %d", len(c.liveMembers()))
	}

	// The lone leader still believes it leads. A follower cannot detect its own
	// isolation in this implementation, so the write must fail on the commit,
	// not on a role check. That distinction is the point of the scenario.
	role, reachable := c.role(leader)
	if !reachable {
		t.Fatalf("leader %s is not answering status: %s", leader.id, c.diagnostics())
	}
	if role != raft.Leader {
		t.Fatalf("leader %s reports role %v, want leader", leader.id, role)
	}

	// Propose blocks until the entry commits or the caller's deadline expires,
	// so the caller's deadline is what ends this write. The meaningful
	// assertions are that it never reports success and that nothing is applied.
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), opTimeout)
	err = c.put(writeCtx, "blocked", "uncommitted")
	cancelWrite()
	if err == nil {
		t.Fatal("leader committed a write with no majority")
	}
	t.Logf("write without majority did not complete: %v", err)

	// Nothing was applied anywhere: the entry is not committed, so no member
	// serves it.
	if err := c.waitForMissing(2*time.Second, "blocked", leader); err != nil {
		t.Fatalf("uncommitted write became visible on the leader: %v", err)
	}

	// Bring one peer back: the leader has a majority again and the cluster
	// becomes writable. The pending entry is not required to appear or vanish,
	// so the assertion is that every live member agrees about it.
	restored := survivors[0]
	if err := c.restart(restored); err != nil {
		t.Fatalf("restart %s: %v", restored.id, err)
	}
	if err := c.waitForAgreement(convergenceTimeout, "blocked", c.liveMembers()...); err != nil {
		t.Fatalf("members disagree after the majority returned: %v", err)
	}

	recoverCtx, cancelRecover := context.WithTimeout(context.Background(), opTimeout)
	defer cancelRecover()
	if err := c.put(recoverCtx, "after", "recovery"); err != nil {
		t.Fatalf("put after majority returned: %v", err)
	}
	live := c.liveMembers()
	if err := c.waitForValue(convergenceTimeout, "after", "recovery", live...); err != nil {
		t.Fatalf("replicate after recovery: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "committed", "value", live...); err != nil {
		t.Fatalf("committed value lost after recovery: %v", err)
	}

	// The last member returns too, and the whole cluster converges.
	last := survivors[1]
	if err := c.restart(last); err != nil {
		t.Fatalf("restart %s: %v", last.id, err)
	}
	if err := c.waitForValue(convergenceTimeout, "after", "recovery", c.members...); err != nil {
		t.Fatalf("full cluster did not converge: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "committed", "value", c.members...); err != nil {
		t.Fatalf("full cluster lost the committed value: %v", err)
	}
	if _, err := c.waitForLeader(startTimeout); err != nil {
		t.Fatalf("no leader after full recovery: %v", err)
	}
}

// TestProcessLeaderPartitionElectsReplacementAndRejoins covers scenario 9: the
// leader is frozen in place, so the remaining majority elects a replacement and
// commits new entries while the isolated process cannot participate. When it
// thaws it must learn of the higher term, step down, and converge.
func TestProcessLeaderPartitionElectsReplacementAndRejoins(t *testing.T) {
	c := newProcessCluster(t)
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatal(err)
	}
	survivors := c.others(leader)

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := c.put(ctx, "pre", "partition"); err != nil {
		t.Fatalf("put pre: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "pre", "partition", c.members...); err != nil {
		t.Fatalf("replicate pre: %v", err)
	}

	// Freeze the leader. Its sockets stay open, so peers see no connection
	// reset, only silence.
	if err := c.suspend(leader); err != nil {
		t.Fatalf("suspend %s: %v", leader.id, err)
	}
	defer func() {
		if err := c.resume(leader); err != nil {
			t.Logf("resume %s during cleanup: %v", leader.id, err)
		}
	}()

	// The isolated leader cannot even serve a read: it is cut off, not just
	// deposed.
	readCtx, cancelRead := context.WithTimeout(context.Background(), opTimeout)
	_, _, readErr := c.client.Status(readCtx, leader.id)
	cancelRead()
	if readErr == nil {
		t.Fatal("frozen leader still answered a request")
	}

	// The majority elects a replacement and commits through it.
	newLeader, err := c.waitForNewLeader(convergenceTimeout, leader.id)
	if err != nil {
		t.Fatalf("majority did not replace the partitioned leader: %v", err)
	}
	t.Logf("replacement leader while %s was frozen: %s", leader.id, newLeader.id)

	// The write has to skip the frozen member before it can reach the new
	// leader, so it gets a budget that covers more than one attempt.
	partCtx, cancelPart := context.WithTimeout(context.Background(), redirectBudget)
	defer cancelPart()
	if err := c.put(partCtx, "during", "partition"); err != nil {
		t.Fatalf("put during partition: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "during", "partition", survivors...); err != nil {
		t.Fatalf("majority did not commit during partition: %v", err)
	}

	// The frozen leader had no way to commit anything: it is still isolated, so
	// it cannot serve the new value either.
	readCtx2, cancelRead2 := context.WithTimeout(context.Background(), opTimeout)
	_, _, readErr = c.client.Status(readCtx2, leader.id)
	cancelRead2()
	if readErr == nil {
		t.Fatal("frozen leader recovered on its own")
	}

	// Thaw it. It must discover a higher term, give up leadership, and catch up.
	if err := c.resume(leader); err != nil {
		t.Fatalf("resume %s: %v", leader.id, err)
	}
	if err := c.waitForRole(leader, raft.Follower, true, convergenceTimeout); err != nil {
		t.Fatalf("rejoining leader did not step down: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "during", "partition", c.members...); err != nil {
		t.Fatalf("rejoining leader did not catch up: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "pre", "partition", c.members...); err != nil {
		t.Fatalf("rejoining leader lost the pre-partition value: %v", err)
	}

	// The full cluster is writable and consistent again.
	finalCtx, cancelFinal := context.WithTimeout(context.Background(), redirectBudget)
	defer cancelFinal()
	if err := c.put(finalCtx, "post", "rejoined"); err != nil {
		t.Fatalf("put after rejoin: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "post", "rejoined", c.members...); err != nil {
		t.Fatalf("replicate after rejoin: %v", err)
	}
}

// TestProcessRestartRecoversPersistedState covers scenario 10: the durable Raft
// state on disk is what carries committed data across a crash, and the restarted
// process rebuilds its state machine from it before it rejoins.
func TestProcessRestartRecoversPersistedState(t *testing.T) {
	c := newProcessCluster(t)
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatal(err)
	}
	crashed := c.others(leader)[0]

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := c.put(ctx, "durable", "survives"); err != nil {
		t.Fatalf("put durable: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "durable", "survives", c.members...); err != nil {
		t.Fatalf("replicate durable: %v", err)
	}

	// Read the durable state directly: the committed entry must be on disk
	// before the process is killed, which is what makes the later recovery a
	// persistence result rather than a replication result.
	state := loadPersistedState(t, crashed)
	if state.CommitIndex == 0 {
		t.Fatalf("%s has no durable commit index", crashed.id)
	}
	index := committedPutIndex(t, state, "durable")
	if index == 0 {
		t.Fatalf("%s has no durable committed entry for key durable (log=%d entries, commit=%d)",
			crashed.id, len(state.Log), state.CommitIndex)
	}
	t.Logf("%s persisted commit index %d with durable at index %d", crashed.id, state.CommitIndex, index)

	c.kill(crashed)

	// More writes happen while it is down, so its restart has real catch-up work
	// to do in addition to replaying its own log.
	if err := c.put(ctx, "later", "written-while-down"); err != nil {
		t.Fatalf("put while down: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "later", "written-while-down", c.others(crashed)...); err != nil {
		t.Fatalf("replicate while down: %v", err)
	}

	if err := c.restart(crashed); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "durable", "survives", c.members...); err != nil {
		t.Fatalf("restarted process lost its persisted value: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "later", "written-while-down", c.members...); err != nil {
		t.Fatalf("restarted process did not catch up: %v", err)
	}
	if _, err := c.waitForLeader(startTimeout); err != nil {
		t.Fatalf("no leader after restart: %v", err)
	}

	// The recovered process is a normal member: it follows the leader and
	// replicates further writes.
	finalCtx, cancelFinal := context.WithTimeout(context.Background(), opTimeout)
	defer cancelFinal()
	if err := c.put(finalCtx, "post-restart", "ok"); err != nil {
		t.Fatalf("put after restart: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "post-restart", "ok", c.members...); err != nil {
		t.Fatalf("recovered process did not follow the leader: %v", err)
	}
}

// TestProcessGracefulShutdownExitsCleanly covers the lifecycle half of scenario
// 10: SIGTERM is the path an operator or supervisor uses, the process must exit
// zero, and a restart on the same directories must be safe.
func TestProcessGracefulShutdownExitsCleanly(t *testing.T) {
	c := newProcessCluster(t)
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatal(err)
	}
	victim := c.others(leader)[0]

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := c.put(ctx, "graceful", "stop"); err != nil {
		t.Fatalf("put graceful: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "graceful", "stop", c.members...); err != nil {
		t.Fatalf("replicate graceful: %v", err)
	}

	if err := c.stopGracefully(victim); err != nil {
		t.Fatalf("graceful stop: %v", err)
	}
	if victim.isRunning() {
		t.Fatalf("member %s still running after SIGTERM", victim.id)
	}

	// The majority is intact throughout.
	if err := c.put(ctx, "after-stop", "still-up"); err != nil {
		t.Fatalf("put after graceful stop: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "after-stop", "still-up", c.others(victim)...); err != nil {
		t.Fatalf("replicate after graceful stop: %v", err)
	}

	if err := c.restart(victim); err != nil {
		t.Fatalf("restart after graceful stop: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "graceful", "stop", c.members...); err != nil {
		t.Fatalf("restarted process lost a committed value: %v", err)
	}
	if err := c.waitForValue(convergenceTimeout, "after-stop", "still-up", c.members...); err != nil {
		t.Fatalf("restarted process did not catch up: %v", err)
	}
}

// TestProcessRepeatedRestartsStayConsistent hammers the restart path to catch
// state that only breaks after the second or third restart.
func TestProcessRepeatedRestartsStayConsistent(t *testing.T) {
	c := newProcessCluster(t)
	leader, err := c.waitForLeader(startTimeout)
	if err != nil {
		t.Fatal(err)
	}
	victim := c.others(leader)[0]

	for round := 0; round < 3; round++ {
		key := fmt.Sprintf("round-%d", round)
		value := fmt.Sprintf("value-%d", round)
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		err := c.put(ctx, key, value)
		cancel()
		if err != nil {
			t.Fatalf("round %d put: %v", round, err)
		}
		if err := c.waitForValue(convergenceTimeout, key, value, c.members...); err != nil {
			t.Fatalf("round %d replicate: %v", round, err)
		}
		// Alternate the two restart paths so both are exercised repeatedly: an
		// abrupt crash and a clean operator-initiated stop.
		if round%2 == 0 {
			c.kill(victim)
		} else if err := c.stopGracefully(victim); err != nil {
			t.Fatalf("round %d graceful stop: %v", round, err)
		}
		if err := c.restart(victim); err != nil {
			t.Fatalf("round %d restart: %v", round, err)
		}
		// Every earlier round must still be there after the restart.
		for earlier := 0; earlier <= round; earlier++ {
			earlierKey := fmt.Sprintf("round-%d", earlier)
			earlierValue := fmt.Sprintf("value-%d", earlier)
			if err := c.waitForValue(convergenceTimeout, earlierKey, earlierValue, victim); err != nil {
				t.Fatalf("round %d lost %s after restart: %v", round, earlierKey, err)
			}
		}
	}
	if _, err := c.waitForLeader(startTimeout); err != nil {
		t.Fatalf("no leader after repeated restarts: %v", err)
	}
}
