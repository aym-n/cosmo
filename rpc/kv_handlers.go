package rpc

import (
	"context"

	pb "github.com/aym-n/cosmo/rpc/proto"
)

// SubmitCommand handles KV write command submission (application layer).
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

// GetKey handles KV read (application layer).
func (s *Server) GetKey(ctx context.Context, req *pb.GetKeyRequest) (*pb.GetKeyResponse, error) {
	if s.kv == nil {
		return &pb.GetKeyResponse{Found: false}, nil
	}
	value, found := s.kv.Get(req.Key)
	return &pb.GetKeyResponse{Value: value, Found: found}, nil
}
