package node

import "github.com/aym-n/cosmo/types"

// Config holds parameters to start a Cosmo node.
type Config struct {
	NodeID   types.NodeID
	Port     string
	DataDir  string
	PeerAddrs map[types.NodeID]string
}
