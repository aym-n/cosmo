package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aym-n/cosmo/internal/node"
	"github.com/aym-n/cosmo/types"
)

func main() {
	nodeID := flag.String("id", "node1", "Node ID")
	port := flag.String("port", "50051", "gRPC port")
	dataDir := flag.String("data", "data", "Data directory for WAL")
	flag.Parse()

	peerAddrs := map[types.NodeID]string{
		"node1": "localhost:50051",
		"node2": "localhost:50052",
		"node3": "localhost:50053",
	}

	config := node.Config{
		NodeID:    types.NodeID(*nodeID),
		Port:      *port,
		DataDir:   *dataDir,
		PeerAddrs: peerAddrs,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	if err := node.Run(ctx, config); err != nil {
		log.Fatalf("Node failed: %v", err)
	}
}
