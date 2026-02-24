package rpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/aym-n/cosmo/raft"
	pb "github.com/aym-n/cosmo/rpc/proto"
	"github.com/aym-n/cosmo/statemachine"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedRaftServiceServer
	node *raft.Node
	kv   *statemachine.KVStore
}

func NewServer(node *raft.Node, kv *statemachine.KVStore) *Server {
	return &Server{node: node, kv: kv}
}

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

func StartGRPCServer(node *raft.Node, kv *statemachine.KVStore, listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, NewServer(node, kv))

	log.Printf("gRPC server listening on %s", listenAddr)
	return grpcServer.Serve(lis)
}

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

func (s *Server) SubmitCommand(ctx context.Context, req *pb.SubmitCommandRequest) (*pb.SubmitCommandResponse, error) {
	result := s.node.Propose(req.Command)
	if !result.IsLeader {
		leaderID, _ := s.node.GetLeader()
		return &pb.SubmitCommandResponse{
			Success:  false,
			LeaderId: string(leaderID),
		}, nil
	}
	return &pb.SubmitCommandResponse{
		Success: true,
		Index:   uint64(result.Index),
		Term:    uint64(result.Term),
	}, nil
}

func (s *Server) GetKey(ctx context.Context, req *pb.GetKeyRequest) (*pb.GetKeyResponse, error) {
	if s.kv == nil {
		return &pb.GetKeyResponse{Found: false}, nil
	}
	value, found := s.kv.Get(req.Key)
	return &pb.GetKeyResponse{Value: value, Found: found}, nil
}
