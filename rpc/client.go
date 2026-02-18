package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/aym-n/cosmo/rpc/proto"
	"github.com/aym-n/cosmo/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	mu             sync.Mutex
	connectionPool map[types.NodeID]*grpc.ClientConn
	PeerAddresses  map[types.NodeID]string
}

func NewClient(peerAddresses map[types.NodeID]string) *Client {
	return &Client{
		connectionPool: make(map[types.NodeID]*grpc.ClientConn),
		PeerAddresses:  peerAddresses,
	}
}

func (c *Client) getConn(peerID types.NodeID, addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.connectionPool[peerID]; ok {
		return conn, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	c.connectionPool[peerID] = conn
	return conn, nil
}

func (c *Client) RequestVote(ID types.NodeID, addr string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	conn, err := c.getConn(ID, addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	return client.RequestVote(ctx, req)
}

// SendRequestVote implements rpc.Transport for use by raft.Node.
func (c *Client) SendRequestVote(peerID types.NodeID, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	addr, ok := c.PeerAddresses[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer %s", peerID)
	}
	return c.RequestVote(peerID, addr, req)
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.connectionPool {
		conn.Close()
	}
	c.connectionPool = make(map[types.NodeID]*grpc.ClientConn)
}
