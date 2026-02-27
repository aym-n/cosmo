package consensus

import "github.com/aym-n/cosmo/types"

// Persister persists Raft metadata and log (e.g. WAL). Pass nil for no persistence.
type Persister interface {
	SaveMetadata(term types.Term, votedFor types.NodeID) error
	LoadMetadata() (term types.Term, votedFor types.NodeID, err error)
	AppendLogEntry(entry types.LogEntry) error
	LoadLog() ([]types.LogEntry, error)
	TruncateLog(fromIndex types.LogIndex) error

	SaveSnapshot(snapshotData []byte, lastIncludedIndex types.LogIndex, lastIncludedTerm types.Term) error
	LoadSnapshot() (snapshotData []byte, lastIncludedIndex types.LogIndex, lastIncludedTerm types.Term, err error)
	CompactLog(lastIncludedIndex types.LogIndex) error
}
