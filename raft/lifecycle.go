package raft

// Lifecycle is the state machine for a Raft node's process lifetime.
type Lifecycle uint8

const (
	Created Lifecycle = iota
	Initialized
	Running
	Stopped
)

func (l Lifecycle) String() string {
	switch l {
	case Created:
		return "created"
	case Initialized:
		return "initialized"
	case Running:
		return "running"
	case Stopped:
		return "stopped"
	default:
		return "unknown"
	}
}
