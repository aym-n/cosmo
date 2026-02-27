package rpc

import (
	"context"

	"github.com/aym-n/cosmo/raft"
	pb "github.com/aym-n/cosmo/rpc/proto"
)

// RequestVote handles Raft RequestVote RPC (consensus layer).
func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	resp := s.node.HandleRequestVote(
		raft.Term(req.Term),
		raft.NodeID(req.CandidateId),
		raft.LogIndex(req.LastLogIndex),
		raft.Term(req.LastLogTerm),
	)
	return &pb.RequestVoteResponse{
		Term:        uint64(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

// AppendEntries handles Raft AppendEntries RPC (consensus layer).
func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	var entries []raft.LogEntry
	for _, entry := range req.Entries {
		entries = append(entries, raft.LogEntry{
			Index:   raft.LogIndex(entry.Index),
			Term:    raft.Term(entry.Term),
			Command: entry.Command,
		})
	}
	raftReq := raft.AppendEntriesRequest{
		Term:         raft.Term(req.Term),
		LeaderID:     raft.NodeID(req.LeaderId),
		PrevLogIndex: raft.LogIndex(req.PrevLogIndex),
		PrevLogTerm:  raft.Term(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: raft.LogIndex(req.LeaderCommit),
	}
	resp := s.node.HandleAppendEntries(raftReq)
	return &pb.AppendEntriesResponse{
		Term:          uint64(resp.Term),
		Success:       resp.Success,
		ConflictIndex: uint64(resp.ConflictIndex),
		ConflictTerm:  uint64(resp.ConflictTerm),
	}, nil
}
