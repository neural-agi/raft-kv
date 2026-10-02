package integration

// processcluster_test.go builds a real multi-process cluster: each member is a
// separate OS process running cmd/server, and members reach each other over
// real TCP sockets. Nothing here uses fault.Network, so every Raft message that
// crosses a member boundary crosses a socket, a listener, and the wire codec.
//
// The harness never sleeps for a fixed duration to "let things settle". Every
// wait is a poll against an observable condition (socket reachable, status
// answered, a value present) bounded by a deadline, and a failure prints the
// per-process output so the reason is visible.

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/client"
	"github.com/neural-agi/raft-kv/cluster"
	"github.com/neural-agi/raft-kv/kv"
	"github.com/neural-agi/raft-kv/raft"
	"github.com/neural-agi/raft-kv/storage"
)

const (
	// pollInterval is the retry spacing for every condition wait below.
	pollInterval = 20 * time.Millisecond
	// startTimeout covers process start plus first leader election.
	startTimeout = 20 * time.Second
	// opTimeout bounds a single client operation. It stays well below
	// raft's replication timeout so a lost majority surfaces as a client error
	// instead of a hung test.
	opTimeout = 2 * time.Second
	// convergenceTimeout bounds catch-up after a membership change.
	convergenceTimeout = 20 * time.Second
	// shutdownTimeout bounds a graceful (SIGTERM) exit.
	shutdownTimeout = 15 * time.Second
	// statusProbeTimeout bounds one member's status request during a poll. It is
	// per member: a frozen process accepts the connection and never replies.
	statusProbeTimeout = 1500 * time.Millisecond
	// redirectBudget bounds a write that must succeed while a member is
	// unreachable. The client spends one attempt timeout on that member before
	// trying the next one, so the budget has to cover more than a single attempt.
	redirectBudget = 20 * time.Second
	// outputTailBytes is how much of each process's output is kept for
	// diagnostics.
	outputTailBytes = 8 << 10
)

var (
	binaryMu  sync.Mutex
	binaryDir string
	binaries  = make(map[string]string) // command name -> built path
)

// serverBinary builds cmd/server once per test binary run and returns its path.
func serverBinary(t *testing.T) string {
	t.Helper()
	return buildBinary(t, "github.com/neural-agi/raft-kv/cmd/server", "raft-kv-server")
}

// clientBinary builds cmd/client once per test binary run and returns its path.
func clientBinary(t *testing.T) string {
	t.Helper()
	return buildBinary(t, "github.com/neural-agi/raft-kv/cmd/client", "raft-kv-client")
}

// buildBinary compiles a command once per test binary run and caches it, so a
// suite of process tests pays for one build instead of one per test.
func buildBinary(t *testing.T, pkg, name string) string {
	t.Helper()
	binaryMu.Lock()
	defer binaryMu.Unlock()
	if path, ok := binaries[name]; ok {
		return path
	}
	if binaryDir == "" {
		dir, err := os.MkdirTemp("", "raft-kv-bins")
		if err != nil {
			t.Fatalf("temp dir for binaries: %v", err)
		}
		binaryDir = dir
	}
	path := filepath.Join(binaryDir, name)
	cmd := exec.Command("go", "build", "-o", path, pkg)
	cmd.Dir = repoRoot(t)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, output)
	}
	binaries[name] = path
	return path
}

// repoRoot locates the module root from this test's working directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// This test file lives in <root>/integration.
	return filepath.Dir(wd)
}

// member is one cluster process plus everything needed to restart it exactly
// as it was.
type member struct {
	id         raft.NodeID
	raftAddr   string
	clientAddr string
	storageDir string
	snapDir    string
	configPath string

	mu      sync.Mutex
	cmd     *exec.Cmd
	running bool
	exitErr error
	output  []string
	stopped chan struct{}
}

// processCluster is a running (or partially running) multi-process cluster.
type processCluster struct {
	t       *testing.T
	root    string
	members []*member
	byID    map[raft.NodeID]*member
	client  *client.Client
	cfgPath string // config of an arbitrary member, for the CLI
	ports   []int
}

// memberCount is fixed at three: the smallest cluster where a majority is
// meaningfully different from "everything".
const memberCount = 3

// newProcessCluster writes configs for three members, builds the server binary,
// and starts the whole cluster. It retries with a fresh port plan if a port
// turns out to be taken between allocation and bind.
func newProcessCluster(t *testing.T) *processCluster {
	t.Helper()
	// Building first means a compile error fails fast, before any port is taken.
	serverBinary(t)

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		c := newProcessClusterUnstarted(t, reservePorts(t, 2*memberCount))
		if err := c.startAll(); err != nil {
			lastErr = err
			c.killAll()
			t.Logf("cluster start attempt %d failed, retrying with new ports: %v\n%s", attempt+1, err, c.diagnostics())
			continue
		}
		if _, err := c.waitForLeader(startTimeout); err != nil {
			lastErr = err
			c.killAll()
			t.Logf("cluster attempt %d elected no leader, retrying: %v", attempt+1, err)
			continue
		}
		return c
	}
	t.Fatalf("could not start a cluster after retries: %v", lastErr)
	return nil
}

func newProcessClusterUnstarted(t *testing.T, ports []int) *processCluster {
	t.Helper()
	root := t.TempDir()
	ids := make([]raft.NodeID, memberCount)
	raftAddrs := make([]string, memberCount)
	clientAddrs := make([]string, memberCount)
	for i := 0; i < memberCount; i++ {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i+1))
		raftAddrs[i] = fmt.Sprintf("127.0.0.1:%d", ports[2*i])
		clientAddrs[i] = fmt.Sprintf("127.0.0.1:%d", ports[2*i+1])
	}

	c := &processCluster{
		t:     t,
		root:  root,
		byID:  make(map[raft.NodeID]*member, memberCount),
		ports: ports,
	}
	for i := 0; i < memberCount; i++ {
		m := &member{
			id:         ids[i],
			raftAddr:   raftAddrs[i],
			clientAddr: clientAddrs[i],
			storageDir: filepath.Join(root, "data", string(ids[i]), "state"),
			snapDir:    filepath.Join(root, "data", string(ids[i]), "snapshot"),
		}
		c.members = append(c.members, m)
		c.byID[ids[i]] = m
	}
	for i, m := range c.members {
		peers := make([]clusterProcessPeer, 0, memberCount-1)
		for j, other := range c.members {
			if i == j {
				continue
			}
			peers = append(peers, clusterProcessPeer{
				ID:            other.id,
				Address:       other.raftAddr,
				ClientAddress: other.clientAddr,
			})
		}
		m.configPath = filepath.Join(root, fmt.Sprintf("%s.json", m.id))
		writeProcessConfig(t, m.configPath, processConfigJSON(m, peers))
	}
	c.cfgPath = c.members[0].configPath

	addrs := make(map[raft.NodeID]string, memberCount)
	for _, m := range c.members {
		addrs[m.id] = m.clientAddr
	}
	c.client = client.NewClient(addrs)
	// attemptTimeout is deliberately short: a frozen or dead member must be skipped
	// quickly so the remaining budget can reach the live majority.
	c.client.SetTimeouts(2*time.Second, 2*time.Second, 20*time.Second)

	t.Cleanup(func() {
		c.killAll()
		_ = os.RemoveAll(root)
	})
	return c
}

type clusterProcessPeer struct {
	ID            raft.NodeID
	Address       string
	ClientAddress string
}

// writeProcessConfig writes a member config through the production loader's
// schema, so a broken schema fails here instead of inside a child process.
func writeProcessConfig(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config %s: %v", path, err)
	}
	if _, err := cluster.LoadProcessConfig(path); err != nil {
		t.Fatalf("generated config %s is invalid: %v\n%s", path, err, content)
	}
}

func processConfigJSON(m *member, peers []clusterProcessPeer) string {
	var peerLines strings.Builder
	for i, p := range peers {
		if i > 0 {
			peerLines.WriteString(",\n")
		}
		fmt.Fprintf(&peerLines, `    {"id": %q, "address": %q, "client_address": %q}`, p.ID, p.Address, p.ClientAddress)
	}
	return fmt.Sprintf(`{
  "node": {"id": %q, "listen": %q, "client_listen": %q, "storage_dir": %q, "snapshot_dir": %q},
  "peers": [
%s
  ],
  "election_timeout_min": "200ms",
  "election_timeout_max": "400ms",
  "heartbeat_interval": "40ms",
  "replication_timeout": "2s"
}
`, m.id, m.raftAddr, m.clientAddr, m.storageDir, m.snapDir, peerLines.String())
}

// reservePorts asks the kernel for free ports. There is an unavoidable race
// between closing these sockets and the child binding them, which is why
// newProcessCluster retries with a fresh plan.
func reservePorts(t *testing.T, count int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
			t.Fatalf("reserve port: %v", err)
		}
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports
}

// startAll starts every member and waits for each process to answer a status
// request. A process that cannot bind exits non-zero, which surfaces as an
// error here and triggers a retry with new ports.
func (c *processCluster) startAll() error {
	for _, m := range c.members {
		if err := c.startMember(m); err != nil {
			return err
		}
	}
	for _, m := range c.members {
		if err := m.waitReachable(c.t, startTimeout); err != nil {
			return fmt.Errorf("member %s: %w\n%s", m.id, err, m.outputTail())
		}
	}
	return nil
}

// startMember launches one process and waits until it is serving.
func (c *processCluster) startMember(m *member) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return fmt.Errorf("member %s is already running", m.id)
	}
	cmd := exec.Command(serverBinary(c.t), "-config", m.configPath)
	cmd.Dir = c.root
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		m.mu.Unlock()
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if err := cmd.Start(); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("start %s: %w", m.id, err)
	}
	stopped := make(chan struct{})
	m.cmd = cmd
	m.running = true
	m.exitErr = nil
	m.stopped = stopped
	m.mu.Unlock()

	go m.consume(stdout)
	go m.consume(stderr)
	go func() {
		waitErr := cmd.Wait()
		m.mu.Lock()
		m.running = false
		m.exitErr = waitErr
		m.mu.Unlock()
		close(stopped)
	}()
	return nil
}

// consume appends the process's output lines, keeping only the tail.
func (m *member) consume(r interface{ Read([]byte) (int, error) }) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		m.mu.Lock()
		m.output = append(m.output, scanner.Text())
		if len(m.output) > 200 {
			m.output = m.output[len(m.output)-200:]
		}
		m.mu.Unlock()
	}
}

func (m *member) outputTail() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return strings.Join(m.output, "\n")
}

func (m *member) isRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// waitReachable polls until the client listener answers a status request.
func (m *member) waitReachable(t *testing.T, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if !m.isRunning() {
			m.mu.Lock()
			exitErr := m.exitErr
			m.mu.Unlock()
			return fmt.Errorf("process exited early (%v)", exitErr)
		}
		conn, err := net.DialTimeout("tcp", m.clientAddr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(pollInterval)
	}
	return fmt.Errorf("not reachable within %s: %v", timeout, lastErr)
}

// kill terminates the process abruptly (SIGKILL), simulating a crash: no
// graceful shutdown, no chance to flush anything not already durable.
func (c *processCluster) kill(m *member) {
	m.mu.Lock()
	cmd := m.cmd
	stopped := m.stopped
	m.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGKILL)
	if stopped != nil {
		<-stopped
	}
	m.mu.Lock()
	m.cmd = nil
	m.stopped = nil
	m.running = false
	m.mu.Unlock()
}

// killAll crashes every member, ignoring errors; used for cleanup and retries.
func (c *processCluster) killAll() {
	for _, m := range c.members {
		c.kill(m)
	}
}

// stopGracefully asks the process to exit the way an operator or a supervisor
// would (SIGTERM) and waits for a clean exit.
func (c *processCluster) stopGracefully(m *member) error {
	m.mu.Lock()
	cmd := m.cmd
	stopped := m.stopped
	m.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("member %s is not running", m.id)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal %s: %w", m.id, err)
	}
	select {
	case <-stopped:
	case <-time.After(shutdownTimeout):
		// Escalate so a hung process cannot wedge the test binary.
		_ = cmd.Process.Signal(syscall.SIGKILL)
		<-stopped
		return fmt.Errorf("member %s did not exit within %s", m.id, shutdownTimeout)
	}
	m.mu.Lock()
	exitErr := m.exitErr
	m.cmd = nil
	m.stopped = nil
	m.mu.Unlock()
	if exitErr != nil {
		return fmt.Errorf("member %s exited with %v", m.id, exitErr)
	}
	return nil
}

// restart brings a previously killed or stopped member back with the same
// config and the same directories, which is what makes persistence observable.
func (c *processCluster) restart(m *member) error {
	if err := c.startMember(m); err != nil {
		return err
	}
	if err := m.waitReachable(c.t, startTimeout); err != nil {
		return fmt.Errorf("restart %s: %w\n%s", m.id, err, m.outputTail())
	}
	return nil
}

// suspend freezes a process without letting it exit or flush anything. The OS
// keeps its TCP sockets open, so the effect is a leader that stops answering:
// the peers see a partition, and the frozen node cannot heartbeat or replicate.
func (c *processCluster) suspend(m *member) error {
	m.mu.Lock()
	cmd := m.cmd
	m.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("member %s is not running", m.id)
	}
	return cmd.Process.Signal(syscall.SIGSTOP)
}

// resume thaws a suspended process so it can observe the new state.
func (c *processCluster) resume(m *member) error {
	m.mu.Lock()
	cmd := m.cmd
	m.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("member %s is not running", m.id)
	}
	return cmd.Process.Signal(syscall.SIGCONT)
}

// waitForLeader polls until exactly one live member reports itself leader.
func (c *processCluster) waitForLeader(timeout time.Duration) (*member, error) {
	deadline := time.Now().Add(timeout)
	var lastSeen string
	for time.Now().Before(deadline) {
		leaders := c.currentLeaders()
		switch len(leaders) {
		case 0:
			lastSeen = "no leader yet"
		case 1:
			return leaders[0], nil
		default:
			lastSeen = fmt.Sprintf("conflicting leaders: %s", idsOf(leaders))
		}
		time.Sleep(pollInterval)
	}
	return nil, fmt.Errorf("no single leader within %s (%s)\n%s", timeout, lastSeen, c.diagnostics())
}

// waitForNewLeader waits until some leader exists and it is not the one given.
func (c *processCluster) waitForNewLeader(timeout time.Duration, exclude raft.NodeID) (*member, error) {
	deadline := time.Now().Add(timeout)
	var lastSeen string
	for time.Now().Before(deadline) {
		leaders := c.currentLeaders()
		var fresh []*member
		for _, l := range leaders {
			if l.id != exclude {
				fresh = append(fresh, l)
			}
		}
		if len(fresh) == 1 {
			return fresh[0], nil
		}
		if len(fresh) > 1 {
			lastSeen = fmt.Sprintf("multiple new leaders: %s", idsOf(fresh))
		} else if len(leaders) == 1 {
			lastSeen = fmt.Sprintf("old leader %s still leading", leaders[0].id)
		} else {
			lastSeen = "no leader yet"
		}
		time.Sleep(pollInterval)
	}
	return nil, fmt.Errorf("no new leader within %s (%s)\n%s\n%s",
		timeout, lastSeen, c.probeStates(), c.diagnostics())
}

// probeStates reports, per member, whether the process is alive, whether its
// client port accepts a connection, and what role it reports. A failure message
// that separates "the process died", "the socket is refused", and "the node
// answers but is not a leader" is the difference between a ten-minute and a
// one-minute investigation.
func (c *processCluster) probeStates() string {
	var b strings.Builder
	for _, m := range c.members {
		fmt.Fprintf(&b, "  %s: process=%s", m.id, map[bool]string{true: "up", false: "down"}[m.isRunning()])
		conn, err := net.DialTimeout("tcp", m.clientAddr, 500*time.Millisecond)
		if err != nil {
			fmt.Fprintf(&b, " dial=%v", err)
		} else {
			_ = conn.Close()
			b.WriteString(" dial=ok")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		role, leader, statusErr := c.client.Status(ctx, m.id)
		cancel()
		if statusErr != nil {
			fmt.Fprintf(&b, " status=%v", statusErr)
		} else {
			fmt.Fprintf(&b, " role=%s leader=%s", role, leader)
		}
		b.WriteString("\n")
	}
	return b.String()
}

// currentLeaders returns the live members that report role leader. Each member
// gets its own deadline: a frozen member accepts connections but never answers,
// so a context shared across the loop would be spent entirely on it and the real
// leader would never be polled.
func (c *processCluster) currentLeaders() []*member {
	var leaders []*member
	for _, m := range c.members {
		if !m.isRunning() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), statusProbeTimeout)
		role, leader, err := c.client.Status(ctx, m.id)
		cancel()
		if err != nil {
			continue
		}
		if role == raft.Leader && leader == m.id {
			leaders = append(leaders, m)
		}
	}
	return leaders
}

// role reports a member's current role, and whether it answered at all.
func (c *processCluster) role(m *member) (raft.Role, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	role, _, err := c.client.Status(ctx, m.id)
	if err != nil {
		return raft.Role(0), false
	}
	return role, true
}

// waitForValue polls each named member until it reports the expected value.
// Convergence is the point of the test, so it is asserted per member rather
// than through the rotating client, which hides which node is behind.
func (c *processCluster) waitForValue(timeout time.Duration, key, want string, members ...*member) error {
	deadline := time.Now().Add(timeout)
	var pending []string
	for {
		pending = pending[:0]
		for _, m := range members {
			if !m.isRunning() {
				pending = append(pending, string(m.id)+":down")
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			value, found, err := c.client.GetFrom(ctx, m.id, []byte(key))
			cancel()
			if err != nil {
				pending = append(pending, string(m.id)+":"+err.Error())
				continue
			}
			if !found {
				pending = append(pending, string(m.id)+":missing")
				continue
			}
			if string(value) != want {
				pending = append(pending, fmt.Sprintf("%s:%q", m.id, value))
			}
		}
		if len(pending) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("key %q not %q on [%s] within %s\n%s",
				key, want, strings.Join(idsOf(members), ","), timeout, strings.Join(pending, " "))
		}
		time.Sleep(pollInterval)
	}
}

// waitForMissing polls until none of the members report the key.
func (c *processCluster) waitForMissing(timeout time.Duration, key string, members ...*member) error {
	deadline := time.Now().Add(timeout)
	for {
		var present []string
		for _, m := range members {
			if !m.isRunning() {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			_, found, err := c.client.GetFrom(ctx, m.id, []byte(key))
			cancel()
			if err == nil && found {
				present = append(present, string(m.id))
			}
		}
		if len(present) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("key %q still present on [%s] after %s", key, strings.Join(present, ","), timeout)
		}
		time.Sleep(pollInterval)
	}
}

// waitForAgreement polls until every named member reports the same state for
// key: either all hold the same value, or none has it. It is the right
// assertion for an entry whose fate after a failure is genuinely unspecified,
// such as a proposal that was pending when a leader lost its majority: the
// invariant is agreement, not a particular value.
func (c *processCluster) waitForAgreement(timeout time.Duration, key string, members ...*member) error {
	deadline := time.Now().Add(timeout)
	var last string
	for {
		values := make(map[string]int)
		for _, m := range members {
			if !m.isRunning() {
				values["down"]++
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			value, found, err := c.client.GetFrom(ctx, m.id, []byte(key))
			cancel()
			switch {
			case err != nil:
				values["unreachable"]++
			case !found:
				values["missing"]++
			default:
				values[fmt.Sprintf("%q", value)]++
			}
		}
		if len(values) == 1 {
			return nil
		}
		last = fmt.Sprintf("%v", values)
		if time.Now().After(deadline) {
			return fmt.Errorf("members disagree about key %q after %s: %s", key, timeout, last)
		}
		time.Sleep(pollInterval)
	}
}

// waitForRole polls until a member reports the wanted role, or is unreachable
// when reachable is false.
func (c *processCluster) waitForRole(m *member, want raft.Role, reachable bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		role, ok := c.role(m)
		if !ok {
			last = "unreachable"
		} else {
			last = role.String()
			if role == want {
				return nil
			}
		}
		if !reachable && last == "unreachable" {
			return nil
		}
		time.Sleep(pollInterval)
	}
	return fmt.Errorf("member %s role never became %s (last %s) within %s", m.id, want, last, timeout)
}

// waitForCommit polls the leader's client status until a proposal is accepted,
// or fails when the leader has lost its majority. It distinguishes "committed"
// from "the leader believes it is still leader", which is the whole point of the
// majority test.
func (c *processCluster) put(ctx context.Context, key, value string) error {
	return c.client.Put(ctx, []byte(key), []byte(value))
}

func idsOf(members []*member) []string {
	ids := make([]string, 0, len(members))
	for _, m := range members {
		ids = append(ids, string(m.id))
	}
	return ids
}

// others returns every member except the given one.
func (c *processCluster) others(m *member) []*member {
	var out []*member
	for _, other := range c.members {
		if other != m {
			out = append(out, other)
		}
	}
	return out
}

// liveMembers returns the members whose process is running.
func (c *processCluster) liveMembers() []*member {
	var out []*member
	for _, m := range c.members {
		if m.isRunning() {
			out = append(out, m)
		}
	}
	return out
}

// diagnostics renders a failure report: per-process state plus recent output.
func (c *processCluster) diagnostics() string {
	var b strings.Builder
	for _, m := range c.members {
		m.mu.Lock()
		running, exitErr := m.running, m.exitErr
		m.mu.Unlock()
		state := "down"
		if running {
			state = "running"
		}
		fmt.Fprintf(&b, "  %s (%s raft=%s client=%s) %s", m.id, state, m.raftAddr, m.clientAddr, state)
		if exitErr != nil {
			fmt.Fprintf(&b, " exit=%v", exitErr)
		}
		b.WriteString("\n")
		if tail := m.outputTail(); tail != "" {
			for _, line := range strings.Split(tail, "\n") {
				fmt.Fprintf(&b, "      | %s\n", line)
			}
		}
	}
	return b.String()
}

// loadPersistedState reads a member's durable Raft state from disk, which is how
// a test can prove a value was persisted before a process was killed.
func loadPersistedState(t *testing.T, m *member) raft.PersistentState {
	t.Helper()
	store, err := storage.NewFileStorage(filepath.Join(m.storageDir, "raft-state"))
	if err != nil {
		t.Fatalf("open %s state: %v", m.id, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load %s state: %v", m.id, err)
	}
	return state
}

// committedPutIndex returns the index of the committed entry that put key, or 0
// if no committed entry in the persisted state does. Reading it straight from
// the durable log is what makes "the value survived the crash" a direct
// observation rather than an inference.
func committedPutIndex(t *testing.T, state raft.PersistentState, key string) raft.LogIndex {
	t.Helper()
	for _, entry := range state.Log {
		if entry.Index > state.CommitIndex {
			break
		}
		command, err := kv.Decode(entry.Command)
		if err != nil {
			continue
		}
		if command.Type == kv.Put && string(command.Key) == key {
			return entry.Index
		}
	}
	return 0
}
