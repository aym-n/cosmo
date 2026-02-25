package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/aym-n/cosmo/raft"
	"github.com/aym-n/cosmo/rpc"
	"github.com/aym-n/cosmo/statemachine"
	"github.com/aym-n/cosmo/storage"
)

func main() {
	nodeID := flag.String("id", "node1", "Node ID")
	port := flag.String("port", "50051", "gRPC port")
	dataDir := flag.String("data", "data", "Data directory for WAL")
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

	// WAL storage per node
	store, err := storage.NewStorage(filepath.Join(*dataDir, *nodeID))
	if err != nil {
		log.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	node, err := raft.NewNode(config, applyCh, transport, store)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	node.Start()

	kv := statemachine.NewKVStore(node)

	// Apply committed entries to KV store
	go func() {
		for entry := range applyCh {
			if err := kv.Apply(entry); err != nil {
				log.Printf("[%s] KV apply error index=%d: %v", *nodeID, entry.Index, err)
			} else {
				log.Printf("[%s] KV applied index=%d, term=%d", *nodeID, entry.Index, entry.Term)
			}
		}
	}()

	// Start gRPC server
	listenAddr := ":" + *port
	go func() {
		if err := rpc.StartGRPCServer(node, kv, listenAddr); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	log.Printf("Node %s started on port %s", *nodeID, *port)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	node.Stop()
	store.Close()
}
