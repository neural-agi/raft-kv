package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/neural-agi/raft-kv/raft"
)

// Standard timing defaults applied when a process config omits a field. They
// favor a responsive local demo; a slower deployment can override any of them.
// Raft's own default ReplicationTimeout (10s) is used when a config leaves
// replication_timeout unset.
const (
	DefaultElectionTimeoutMin = 300 * time.Millisecond
	DefaultElectionTimeoutMax = 600 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
)

// Duration makes time.Duration JSON-friendly: a config writes Go duration
// strings such as "300ms" or "2s".
type Duration time.Duration

// UnmarshalJSON accepts only a duration string, so a typo is a load-time error,
// not a silent zero timeout.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	value, err := time.ParseDuration(text)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", text, err)
	}
	*d = Duration(value)
	return nil
}

// D returns the duration as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// NodeSpec configures the local process.
type NodeSpec struct {
	ID           raft.NodeID `json:"id"`
	Listen       string      `json:"listen"`        // raft RPC listener for peers
	ClientListen string      `json:"client_listen"` // client operations listener
	StorageDir   string      `json:"storage_dir"`   // persistent Raft state directory
	SnapshotDir  string      `json:"snapshot_dir"`  // persistent snapshot directory
}

// PeerSpec addresses one static cluster peer.
type PeerSpec struct {
	ID            raft.NodeID `json:"id"`
	Address       string      `json:"address"`        // raft RPC address the peer listens on
	ClientAddress string      `json:"client_address"` // client operations address
}

// ProcessConfig is the static configuration of one server process plus its view
// of the cluster. The repository is intentionally stdlib-only, so configuration
// uses JSON (encoding/json) rather than a YAML dependency; the fields mirror the
// shape of the plan's example YAML.
type ProcessConfig struct {
	Node NodeSpec `json:"node"`

	Peers []PeerSpec `json:"peers"`

	// Optional timing overrides; zeros select the defaults above.
	ElectionTimeoutMin Duration `json:"election_timeout_min,omitempty"`
	ElectionTimeoutMax Duration `json:"election_timeout_max,omitempty"`
	HeartbeatInterval  Duration `json:"heartbeat_interval,omitempty"`
	ReplicationTimeout Duration `json:"replication_timeout,omitempty"`
}

// LoadProcessConfig reads, validates, and default-fills a process config file.
func LoadProcessConfig(path string) (ProcessConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return ProcessConfig{}, fmt.Errorf("open config %s: %w", path, err)
	}
	defer file.Close()
	var config ProcessConfig
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return ProcessConfig{}, fmt.Errorf("decode config %s: %w", path, err)
	}
	if err := config.validate(); err != nil {
		return ProcessConfig{}, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return config, nil
}

func (c *ProcessConfig) validate() error {
	if c.Node.ID == "" {
		return errors.New("node.id must not be empty")
	}
	if c.Node.Listen == "" {
		return errors.New("node.listen must not be empty")
	}
	if c.Node.ClientListen == "" {
		return errors.New("node.client_listen must not be empty")
	}
	if c.Node.StorageDir == "" {
		return errors.New("node.storage_dir must not be empty")
	}
	if c.Node.SnapshotDir == "" {
		return errors.New("node.snapshot_dir must not be empty")
	}
	seen := make(map[raft.NodeID]bool, len(c.Peers)+1)
	seen[c.Node.ID] = true
	for _, peer := range c.Peers {
		if peer.ID == "" {
			return errors.New("peer id must not be empty")
		}
		if seen[peer.ID] {
			return fmt.Errorf("duplicate or self-referential member id %q", peer.ID)
		}
		if peer.Address == "" {
			return fmt.Errorf("peer %q address must not be empty", peer.ID)
		}
		if peer.ClientAddress == "" {
			return fmt.Errorf("peer %q client_address must not be empty", peer.ID)
		}
		seen[peer.ID] = true
	}
	c.fillTimingDefaults()
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return errors.New("election_timeout_min/max must be a positive range")
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return errors.New("heartbeat_interval must be positive and below election_timeout_min")
	}
	if c.ReplicationTimeout < 0 {
		return errors.New("replication_timeout must not be negative")
	}
	return nil
}

func (c *ProcessConfig) fillTimingDefaults() {
	if c.ElectionTimeoutMin == 0 && c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMin = Duration(DefaultElectionTimeoutMin)
		c.ElectionTimeoutMax = Duration(DefaultElectionTimeoutMax)
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = Duration(DefaultHeartbeatInterval)
	}
}

// NodeIDs returns every cluster member in configuration order: the local node
// first, then the peers.
func (c ProcessConfig) NodeIDs() []raft.NodeID {
	ids := make([]raft.NodeID, 0, 1+len(c.Peers))
	ids = append(ids, c.Node.ID)
	for _, peer := range c.Peers {
		ids = append(ids, peer.ID)
	}
	return ids
}

// PeerIDs returns the Raft peers as configured, in configuration order. Raft's
// Config.Peers excludes self (the majority formula is len(Peers)+1).
func (c ProcessConfig) PeerIDs() []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(c.Peers))
	for _, peer := range c.Peers {
		ids = append(ids, peer.ID)
	}
	return ids
}

// PeerTransportAddrs maps every peer ID to the raft RPC address the transport
// dials for that peer. Self is intentionally absent: a node never dials itself.
func (c ProcessConfig) PeerTransportAddrs() map[raft.NodeID]string {
	addrs := make(map[raft.NodeID]string, len(c.Peers))
	for _, peer := range c.Peers {
		addrs[peer.ID] = peer.Address
	}
	return addrs
}

// ClientAddresses maps every cluster member (including self) to the address of
// its client operations listener.
func (c ProcessConfig) ClientAddresses() map[raft.NodeID]string {
	addrs := make(map[raft.NodeID]string, 1+len(c.Peers))
	addrs[c.Node.ID] = c.Node.ClientListen
	for _, peer := range c.Peers {
		addrs[peer.ID] = peer.ClientAddress
	}
	return addrs
}

// Static membership view for the process layer.
func (c ProcessConfig) Members() []Member {
	members := make([]Member, 0, 1+len(c.Peers))
	members = append(members, Member{ID: c.Node.ID, Address: c.Node.Listen})
	for _, peer := range c.Peers {
		members = append(members, Member{ID: peer.ID, Address: peer.Address})
	}
	return members
}
