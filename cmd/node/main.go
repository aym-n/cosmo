package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aym-n/cosmo/raft"
	"github.com/aym-n/cosmo/rpc"
)

func main() {
	nodeID := flag.String("id", "node1", "Node ID")
	port := flag.String("port", "50051", "gRPC port")
	flag.Parse()

	// Hardcode a 3-node cluster for now
	peerAddrs := map[raft.NodeID]string{
		"node1": "localhost:50051",
		"node2": "localhost:50052",
		"node3": "localhost:50053",
	}

	var peers []raft.NodeID
	for id := range peerAddrs {
		if id != raft.NodeID(*nodeID) {
			peers = append(peers, id)
		}
	}

	config := raft.DefaultConfig(raft.NodeID(*nodeID), peers, peerAddrs)
	applyCh := make(chan raft.LogEntry, 100)

	transport := rpc.NewClient(peerAddrs)

	node := raft.NewNode(config, applyCh, transport)
	node.Start()

	// Consume applied entries
	go func() {
		for entry := range applyCh {
			log.Printf("[%s] STATE MACHINE: Applied index=%d, term=%d, command=%s",
				*nodeID, entry.Index, entry.Term, string(entry.Command))
		}
	}()

	// Start gRPC server
	listenAddr := ":" + *port
	go func() {
		if err := rpc.StartGRPCServer(node, listenAddr); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	log.Printf("Node %s started on port %s", *nodeID, *port)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	node.Stop()
}
