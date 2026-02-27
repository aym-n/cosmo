package rpc

import (
	"fmt"
	"log"
	"net"

	"github.com/aym-n/cosmo/raft"
	pb "github.com/aym-n/cosmo/rpc/proto"
	"github.com/aym-n/cosmo/statemachine"
	"google.golang.org/grpc"
)

// Server implements the combined gRPC service (Raft + KV). Raft handlers live in
// raft_handlers.go; KV handlers in kv_handlers.go.
type Server struct {
	pb.UnimplementedRaftServiceServer
	node *raft.Node
	kv   *statemachine.KVStore
}

// NewServer builds a gRPC server that handles both Raft and KV RPCs.
func NewServer(node *raft.Node, kv *statemachine.KVStore) *Server {
	return &Server{node: node, kv: kv}
}

// StartGRPCServer starts the gRPC server on listenAddr, serving Raft and KV RPCs.
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
