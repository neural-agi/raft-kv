package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/neural-agi/raft-kv/raft"
)

// FileSnapshotStorage atomically replaces one durable snapshot file. Directory
// synchronization follows FileStorage's documented durability boundary.
type FileSnapshotStorage struct {
	path string
	ops  fileOps
}

func NewFileSnapshotStorage(path string) (*FileSnapshotStorage, error) {
	if path == "" {
		return nil, errors.New("snapshot storage path must not be empty")
	}
	return &FileSnapshotStorage{path: filepath.Clean(path), ops: defaultFileOps()}, nil
}

func (s *FileSnapshotStorage) LoadSnapshot(ctx context.Context) (raft.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return raft.Snapshot{}, err
	}
	file, err := s.ops.openFile(s.path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return raft.Snapshot{}, nil
	}
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("open snapshot: %w", err)
	}
	data, err := io.ReadAll(file)
	closeErr := file.Close()
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("read snapshot: %w", err)
	}
	if closeErr != nil {
		return raft.Snapshot{}, fmt.Errorf("close snapshot: %w", closeErr)
	}
	snapshot, err := raft.DecodeSnapshot(data)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *FileSnapshotStorage) SaveSnapshot(ctx context.Context, snapshot raft.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := snapshot.Encode()
	if err != nil {
		return err
	}
	if err := s.ops.mkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	tmpName := s.path + ".tmp"
	tmp, err := s.ops.openFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open temporary snapshot: %w", err)
	}
	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = s.ops.remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary snapshot: %w", err)
	}
	if err := s.ops.rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replace snapshot: %w", err)
	}
	cleanup = false
	dir, err := s.ops.openDir(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close snapshot directory: %w", err)
	}
	return nil
}
