package raft 

import "time"

type Config struct {
	NodeID NodeID
	Peers []NodeID

	PeerAddress map[NodeID]string

	HeartbeatTimeout time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

func DefaultConfig(id NodeID, peers []NodeID, addresses map[NodeID]string) Config {
	return Config{
		NodeID: id,
		Peers: peers,
		PeerAddress: addresses,
		HeartbeatTimeout: 50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
	}
}
