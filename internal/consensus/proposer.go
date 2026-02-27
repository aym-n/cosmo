package consensus

import "github.com/aym-n/cosmo/types"

// ProposeResult is the result of proposing a command to the replicated log.
type ProposeResult struct {
	Index    types.LogIndex
	Term     types.Term
	IsLeader bool
}

// Proposer replicates commands and returns once they are accepted (not necessarily committed).
// Implemented by the Raft leader; callers use this to submit state machine commands.
type Proposer interface {
	Propose(cmd []byte) ProposeResult
}
