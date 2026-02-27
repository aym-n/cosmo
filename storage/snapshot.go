package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aym-n/cosmo/types"
)

type Snapshot struct {
	LastSaveIndex types.LogIndex   `json:"last_save_index"`
	LastSaveTerm  types.Term       `json:"last_save_term"`
	Data          map[string]string `json:"data"`
}

func (s *Storage) SaveSnapshot(snapshotData []byte, lastIncludedIndex types.LogIndex, lastIncludedTerm types.Term) error {
	var data map[string]string
	if err := json.Unmarshal(snapshotData, &data); err != nil {
		return fmt.Errorf("snapshot decode: %w", err)
	}
	snap := &Snapshot{LastSaveIndex: lastIncludedIndex, LastSaveTerm: lastIncludedTerm, Data: data}
	return s.saveSnapshot(snap)
}

func (s *Storage) LoadSnapshot() ([]byte, types.LogIndex, types.Term, error) {
	snap, err := s.loadSnapshot()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}
	data, err := json.Marshal(snap.Data)
	if err != nil {
		return nil, 0, 0, err
	}
	return data, snap.LastSaveIndex, snap.LastSaveTerm, nil
}

func (s *Storage) CompactLog(lastIncludedIndex types.LogIndex) error {
	return s.compactLog(lastIncludedIndex)
}

func (s *Storage) saveSnapshot(snap *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshotPath := filepath.Join(s.directory, "snapshot.json")
	tempPath := snapshotPath + ".tmp"
	f, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp snapshot: %w", err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snap); err != nil {
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot: %w", err)
	}
	f.Close()

	if err := os.Rename(tempPath, snapshotPath); err != nil {
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}
	return nil
}

func (s *Storage) loadSnapshot() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshotPath := filepath.Join(s.directory, "snapshot.json")
	f, err := os.Open(snapshotPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var snap Snapshot
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&snap); err != nil && err != io.EOF {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return &snap, nil
}

func (s *Storage) compactLog(lastSaveIndex types.LogIndex) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.logFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}

	var entries []types.LogEntry
	decoder := json.NewDecoder(s.logFile)
	for {
		var entry types.LogEntry
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("decode log entry: %w", err)
		}
		if entry.Index > lastSaveIndex {
			entries = append(entries, entry)
		}
	}

	if err := s.logFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	if _, err := s.logFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}
	encoder := json.NewEncoder(s.logFile)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("encode log: %w", err)
		}
	}
	return s.logFile.Sync()
}