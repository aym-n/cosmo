package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aym-n/cosmo/types"
)

type Storage struct {
	mu         sync.Mutex
	directory  string
	metadataFile *os.File
	logFile      *os.File
}

type metadata struct {
	CurrentTerm types.Term   `json:"current_term"`
	VotedFor    types.NodeID `json:"voted_for"`
}

func NewStorage(directory string) (*Storage, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	s := &Storage{directory: directory}

	metadataPath := filepath.Join(directory, "metadata.json")
	metaFile, err := os.OpenFile(metadataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}
	s.metadataFile = metaFile

	logPath := filepath.Join(directory, "log.bin")
	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		_ = metaFile.Close()
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	s.logFile = logFile

	return s, nil
}

func (s *Storage) SaveMetadata(term types.Term, votedFor types.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.metadataFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}
	if err := s.metadataFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	encoder := json.NewEncoder(s.metadataFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata{CurrentTerm: term, VotedFor: votedFor}); err != nil {
		return fmt.Errorf("encode failed: %w", err)
	}
	if err := s.metadataFile.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}
	return nil
}

func (s *Storage) LoadMetadata() (types.Term, types.NodeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.metadataFile.Seek(0, 0); err != nil {
		return 0, "", fmt.Errorf("seek failed: %w", err)
	}
	var meta metadata
	decoder := json.NewDecoder(s.metadataFile)
	err := decoder.Decode(&meta)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("decode failed: %w", err)
	}
	return meta.CurrentTerm, meta.VotedFor, nil
}

func (s *Storage) AppendLogEntry(entry types.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	encoder := json.NewEncoder(s.logFile)
	if err := encoder.Encode(entry); err != nil {
		return fmt.Errorf("encode failed: %w", err)
	}

	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}

	return nil
}

func (s *Storage) LoadLog() ([]types.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.logFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seek failed: %w", err)
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
			return nil, fmt.Errorf("decode failed at entry %d: %w", len(entries), err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *Storage) TruncateLog(fromIndex types.LogIndex) error {
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
			return fmt.Errorf("decode failed: %w", err)
		}
		if entry.Index < fromIndex {
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
			return fmt.Errorf("encode failed: %w", err)
		}
	}

	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}

	return nil
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	if err := s.metadataFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.logFile.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}