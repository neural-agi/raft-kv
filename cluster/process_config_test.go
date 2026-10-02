package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

func writeConfig(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadProcessConfigFull(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, `{
  "node": {"id": "n1", "listen": "127.0.0.1:7001", "client_listen": "127.0.0.1:7101", "storage_dir": "data/n1/state", "snapshot_dir": "data/n1/snap"},
  "peers": [
    {"id": "n2", "address": "127.0.0.1:7002", "client_address": "127.0.0.1:7102"},
    {"id": "n3", "address": "127.0.0.1:7003", "client_address": "127.0.0.1:7103"}
  ],
  "election_timeout_min": "150ms",
  "election_timeout_max": "300ms",
  "heartbeat_interval": "25ms",
  "replication_timeout": "2s"
}`)

	cfg, err := LoadProcessConfig(path)
	if err != nil {
		t.Fatalf("LoadProcessConfig: %v", err)
	}

	if cfg.Node.ID != "n1" {
		t.Fatalf("node id = %q, want n1", cfg.Node.ID)
	}
	wantIDs := []raft.NodeID{"n1", "n2", "n3"}
	for i, id := range cfg.NodeIDs() {
		if id != wantIDs[i] {
			t.Fatalf("NodeIDs[%d] = %q, want %q", i, id, wantIDs[i])
		}
	}
	if got := cfg.PeerIDs(); len(got) != 2 || got[0] != "n2" || got[1] != "n3" {
		t.Fatalf("PeerIDs = %v, want [n2 n3]", got)
	}
	wantTCP := map[raft.NodeID]string{"n2": "127.0.0.1:7002", "n3": "127.0.0.1:7003"}
	if !equalAddrs(cfg.PeerTransportAddrs(), wantTCP) {
		t.Fatalf("PeerTransportAddrs = %v, want %v", cfg.PeerTransportAddrs(), wantTCP)
	}
	wantClient := map[raft.NodeID]string{"n1": "127.0.0.1:7101", "n2": "127.0.0.1:7102", "n3": "127.0.0.1:7103"}
	if !equalAddrs(cfg.ClientAddresses(), wantClient) {
		t.Fatalf("ClientAddresses = %v, want %v", cfg.ClientAddresses(), wantClient)
	}
	if cfg.ElectionTimeoutMin.D() != 150*time.Millisecond || cfg.ElectionTimeoutMax.D() != 300*time.Millisecond {
		t.Fatalf("election timeouts = %v,%v", cfg.ElectionTimeoutMin.D(), cfg.ElectionTimeoutMax.D())
	}
	if cfg.HeartbeatInterval.D() != 25*time.Millisecond {
		t.Fatalf("heartbeat = %v", cfg.HeartbeatInterval.D())
	}
	if cfg.ReplicationTimeout.D() != 2*time.Second {
		t.Fatalf("replication timeout = %v", cfg.ReplicationTimeout.D())
	}
}

func TestLoadProcessConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, `{
  "node": {"id": "n1", "listen": "127.0.0.1:7001", "client_listen": "127.0.0.1:7101", "storage_dir": "d1", "snapshot_dir": "d2"},
  "peers": [{"id": "n2", "address": "127.0.0.1:7002", "client_address": "127.0.0.1:7102"}]
}`)

	cfg, err := LoadProcessConfig(path)
	if err != nil {
		t.Fatalf("LoadProcessConfig: %v", err)
	}
	if cfg.ElectionTimeoutMin.D() != DefaultElectionTimeoutMin {
		t.Fatalf("default election min = %v, want %v", cfg.ElectionTimeoutMin.D(), DefaultElectionTimeoutMin)
	}
	if cfg.ElectionTimeoutMax.D() != DefaultElectionTimeoutMax {
		t.Fatalf("default election max = %v, want %v", cfg.ElectionTimeoutMax.D(), DefaultElectionTimeoutMax)
	}
	if cfg.HeartbeatInterval.D() != DefaultHeartbeatInterval {
		t.Fatalf("default heartbeat = %v, want %v", cfg.HeartbeatInterval.D(), DefaultHeartbeatInterval)
	}
	if cfg.ReplicationTimeout.D() != 0 {
		t.Fatalf("replication timeout should default to zero (raft keeps its own default)")
	}
}

func TestLoadProcessConfigErrors(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name    string
		content string
	}{
		{"self-in-peers", `{"node": {"id": "n1", "listen": "a:1", "client_listen": "a:2", "storage_dir": "d1", "snapshot_dir": "d2"}, "peers": [{"id": "n1", "address": "a:3", "client_address": "a:4"}]}`},
		{"duplicate-peer", `{"node": {"id": "n1", "listen": "a:1", "client_listen": "a:2", "storage_dir": "d1", "snapshot_dir": "d2"}, "peers": [{"id": "n2", "address": "a:3", "client_address": "a:4"}, {"id": "n2", "address": "a:5", "client_address": "a:6"}]}`},
		{"empty-node-id", `{"node": {"listen": "a:1", "client_listen": "a:2", "storage_dir": "d1", "snapshot_dir": "d2"}, "peers": []}`},
		{"bad-duration", `{"node": {"id": "n1", "listen": "a:1", "client_listen": "a:2", "storage_dir": "d1", "snapshot_dir": "d2"}, "peers": [], "election_timeout_min": "soon"}`},
		{"one-sided-timeout", `{"node": {"id": "n1", "listen": "a:1", "client_listen": "a:2", "storage_dir": "d1", "snapshot_dir": "d2"}, "peers": [], "election_timeout_min": "700ms"}`},
		{"inverted-timeout", `{"node": {"id": "n1", "listen": "a:1", "client_listen": "a:2", "storage_dir": "d1", "snapshot_dir": "d2"}, "peers": [], "election_timeout_min": "600ms", "election_timeout_max": "300ms"}`},
		{"negative-replication", `{"node": {"id": "n1", "listen": "a:1", "client_listen": "a:2", "storage_dir": "d1", "snapshot_dir": "d2"}, "peers": [], "replication_timeout": "-1s"}`},
		{"not-json", `garbage`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, dir, tc.content)
			if _, err := LoadProcessConfig(path); err == nil {
				t.Fatalf("expected error for %q, got none", tc.name)
			}
		})
	}
}

func TestLoadProcessConfigMissingFile(t *testing.T) {
	if _, err := LoadProcessConfig(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func equalAddrs(a, b map[raft.NodeID]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestMembersMatchesNodeIDs(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, `
{"node": {"id": "n1", "listen": "127.0.0.1:7001", "client_listen": "127.0.0.1:7101", "storage_dir": "d1", "snapshot_dir": "d2"},
"peers": [{"id": "n2", "address": "127.0.0.1:7002", "client_address": "127.0.0.1:7102"}]}`)
	cfg, err := LoadProcessConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Members()) != len(cfg.NodeIDs()) {
		t.Fatalf("members %d != node ids %d", len(cfg.Members()), len(cfg.NodeIDs()))
	}
	for _, id := range cfg.NodeIDs() {
		if !containsID(cfg.Members(), id) {
			t.Fatalf("missing member %q", id)
		}
	}
}

func containsID(members []Member, id raft.NodeID) bool {
	for _, m := range members {
		if m.ID == id {
			return true
		}
	}
	return false
}
