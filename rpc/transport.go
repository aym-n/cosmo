package rpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/aym-n/cosmo/raft"
	pb "github.com/aym-n/cosmo/rpc/proto"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedRaftServiceServer
	node *raft.Node
}

func NewServer(node *raft.Node) *Server {
	return &Server{node: node}
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

func StartGRPCServer(node *raft.Node, listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, NewServer(node))

	log.Printf("gRPC server listening on %s", listenAddr)
	return grpcServer.Serve(lis)
}
