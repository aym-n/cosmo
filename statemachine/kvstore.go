package statemachine

import (
	"fmt"
	"sync"

	"github.com/aym-n/cosmo/raft"
)

type KVStore struct {
	mu    sync.RWMutex
	data  map[string]string 
	raft  *raft.Node   
}

func NewKVStore(raftNode *raft.Node) *KVStore {
	return &KVStore{
		data: make(map[string]string),
		raft: raftNode,
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	
	value, exists := kv.data[key]
	return value, exists
}

func (kv *KVStore) Put(key, value string) (raft.LogIndex, error) {
	cmd := NewPutCommand(key, value)
	encoded, err := cmd.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode command: %w", err)
	}

	result := kv.raft.Propose(encoded)
	if !result.IsLeader {
		return 0, fmt.Errorf("not the leader")
	}

	return result.Index, nil
}

func (kv *KVStore) Delete(key string) (raft.LogIndex, error) {
	cmd := NewDeleteCommand(key)
	encoded, err := cmd.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode command: %w", err)
	}

	result := kv.raft.Propose(encoded)
	if !result.IsLeader {
		return 0, fmt.Errorf("not the leader")
	}

	return result.Index, nil
}

func (kv *KVStore) Apply(entry raft.LogEntry) error {
	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		return fmt.Errorf("failed to decode command: %w", err)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Type {
	case CommandPut:
		kv.data[cmd.Key] = cmd.Value
		
	case CommandDelete:
		delete(kv.data, cmd.Key)
		
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	return nil
}

func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	snapshot := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		snapshot[k] = v
	}
	return snapshot
}

func (kv *KVStore) Size() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}