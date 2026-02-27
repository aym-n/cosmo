package node

import (
	"context"
	"log"
	"path/filepath"

	"github.com/aym-n/cosmo/raft"
	"github.com/aym-n/cosmo/rpc"
	"github.com/aym-n/cosmo/statemachine"
	"github.com/aym-n/cosmo/storage"
	"github.com/aym-n/cosmo/types"
)

// Run starts the node and blocks until ctx is cancelled. Returns nil when shutdown cleanly.
func Run(ctx context.Context, config Config) error {
	store, err := storage.NewStorage(filepath.Join(config.DataDir, string(config.NodeID)))
	if err != nil {
		return err
	}
	defer store.Close()

	var peers []raft.NodeID
	for id := range config.PeerAddrs {
		if id != config.NodeID {
			peers = append(peers, id)
		}
	}
	raftConfig := raft.DefaultConfig(config.NodeID, peers, config.PeerAddrs)
	applyCh := make(chan raft.LogEntry, 100)
	transport := rpc.NewClient(config.PeerAddrs)

	raftNode, err := raft.NewNode(raftConfig, applyCh, transport, store)
	if err != nil {
		return err
	}

	kv := statemachine.NewKVStore(raftNode)
	if snapData, _, _, err := store.LoadSnapshot(); err == nil && len(snapData) > 0 {
		if err := kv.RestoreSnapshot(snapData); err != nil {
			log.Printf("[%s] RestoreSnapshot warning: %v", config.NodeID, err)
		} else {
			log.Printf("[%s] Restored KV from snapshot", config.NodeID)
		}
	}
	raftNode.SetSnapshotFunc(func(lastApplied raft.LogIndex, _ raft.Term) ([]byte, error) {
		return kv.SnapshotBytes()
	})

	raftNode.Start()
	defer raftNode.Stop()

	go applyLoop(config.NodeID, applyCh, kv)

	listenAddr := ":" + config.Port
	go func() {
		if err := rpc.StartGRPCServer(raftNode, kv, listenAddr); err != nil {
			log.Printf("[%s] gRPC server error: %v", config.NodeID, err)
		}
	}()

	log.Printf("Node %s started on port %s", config.NodeID, config.Port)
	<-ctx.Done()
	log.Println("Shutting down...")
	return nil
}

func applyLoop(nodeID types.NodeID, applyCh <-chan raft.LogEntry, kv *statemachine.KVStore) {
	for entry := range applyCh {
		if err := kv.Apply(entry); err != nil {
			log.Printf("[%s] KV apply error index=%d: %v", nodeID, entry.Index, err)
		} else {
			log.Printf("[%s] KV applied index=%d, term=%d", nodeID, entry.Index, entry.Term)
		}
	}
}
