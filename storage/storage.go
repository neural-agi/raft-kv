// Package storage contains durable implementations of the raft.Storage interface.
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

const (
	formatVersion byte = 2
	headerSize         = 4 // magic(2), version(1), reserved(1)
	maxFieldSize       = uint64(^uint32(0))
)

var magic = [2]byte{'R', 'S'}

var (
	errInvalidFormat      = errors.New("invalid raft state format")
	errUnsupportedVersion = errors.New("unsupported raft state format version")
)

type fileOps struct {
	mkdirAll func(string, os.FileMode) error
	openFile func(string, int, os.FileMode) (*os.File, error)
	rename   func(string, string) error
	remove   func(string) error
	openDir  func(string) (*os.File, error)
}

func defaultFileOps() fileOps {
	return fileOps{
		mkdirAll: os.MkdirAll,
		openFile: os.OpenFile,
		rename:   os.Rename,
		remove:   os.Remove,
		openDir:  func(path string) (*os.File, error) { return os.Open(path) },
	}
}

// FileStorage atomically persists the complete raft.PersistentState in one file.
// Save writes and syncs a temporary sibling file, renames it over the live file,
// then syncs the parent directory. It does not implement a WAL or snapshots.
type FileStorage struct {
	path string
	ops  fileOps
}

// NewFileStorage creates a filesystem-backed Storage rooted at path. The file is
// created on the first successful Save; a missing file loads as an empty state.
func NewFileStorage(path string) (*FileStorage, error) {
	if path == "" {
		return nil, errors.New("storage path must not be empty")
	}
	return &FileStorage{path: filepath.Clean(path), ops: defaultFileOps()}, nil
}

func (s *FileStorage) Load(ctx context.Context) (raft.PersistentState, error) {
	if err := ctx.Err(); err != nil {
		return raft.PersistentState{}, err
	}
	file, err := s.ops.openFile(s.path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return raft.PersistentState{}, nil
	}
	if err != nil {
		return raft.PersistentState{}, fmt.Errorf("open raft state: %w", err)
	}
	data, err := io.ReadAll(file)
	closeErr := file.Close()
	if err != nil {
		return raft.PersistentState{}, fmt.Errorf("read raft state: %w", err)
	}
	if closeErr != nil {
		return raft.PersistentState{}, fmt.Errorf("close raft state: %w", closeErr)
	}
	state, err := decodeState(data)
	if err != nil {
		return raft.PersistentState{}, fmt.Errorf("decode raft state: %w", err)
	}
	return state, nil
}

func (s *FileStorage) Save(ctx context.Context, state raft.PersistentState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := encodeState(state)
	if err != nil {
		return err
	}
	if err := s.ops.mkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create storage directory: %w", err)
	}
	tmp, err := s.ops.openFile(s.path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open temporary raft state: %w", err)
	}
	tmpName := s.path + ".tmp"
	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = s.ops.remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary raft state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary raft state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary raft state: %w", err)
	}
	if err := s.ops.rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replace raft state: %w", err)
	}
	cleanup = false
	dir, err := s.ops.openDir(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("open storage directory: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync storage directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close storage directory: %w", err)
	}
	return nil
}

func encodeState(state raft.PersistentState) ([]byte, error) {
	size := uint64(headerSize + 8 + 4 + 4 + 8 + len(state.VotedFor))
	if uint64(len(state.VotedFor)) > maxFieldSize || uint64(len(state.Log)) > maxFieldSize {
		return nil, errors.New("raft state field is too large")
	}
	for _, entry := range state.Log {
		if err := entry.Validate(); err != nil {
			return nil, err
		}
		if uint64(len(entry.Command)) > maxFieldSize {
			return nil, errors.New("raft command is too large")
		}
		size += 8 + 8 + 4 + uint64(len(entry.Command))
	}
	if size > uint64(^uint(0)>>1) {
		return nil, errors.New("raft state is too large")
	}
	data := make([]byte, int(size))
	copy(data[:2], magic[:])
	data[2] = formatVersion
	putU64(data[4:12], uint64(state.CurrentTerm))
	putU32(data[12:16], uint32(len(state.VotedFor)))
	copy(data[16:16+len(state.VotedFor)], state.VotedFor)
	pos := 16 + len(state.VotedFor)
	putU32(data[pos:pos+4], uint32(len(state.Log)))
	pos += 4
	putU64(data[pos:pos+8], uint64(state.CommitIndex))
	pos += 8
	for _, entry := range state.Log {
		putU64(data[pos:pos+8], uint64(entry.Term))
		putU64(data[pos+8:pos+16], uint64(entry.Index))
		putU32(data[pos+16:pos+20], uint32(len(entry.Command)))
		pos += 20
		copy(data[pos:pos+len(entry.Command)], entry.Command)
		pos += len(entry.Command)
	}
	return data, nil
}

func decodeState(data []byte) (raft.PersistentState, error) {
	if len(data) < 20 || data[0] != magic[0] || data[1] != magic[1] {
		return raft.PersistentState{}, errInvalidFormat
	}
	if data[2] != formatVersion || data[3] != 0 {
		return raft.PersistentState{}, errUnsupportedVersion
	}
	pos := headerSize
	term, ok := readU64(data, &pos)
	if !ok {
		return raft.PersistentState{}, io.ErrUnexpectedEOF
	}
	votedLen, ok := readU32(data, &pos)
	if !ok || uint64(votedLen) > uint64(len(data)-pos) {
		return raft.PersistentState{}, errInvalidFormat
	}
	votedFor := string(append([]byte(nil), data[pos:pos+int(votedLen)]...))
	pos += int(votedLen)
	count, ok := readU32(data, &pos)
	if !ok {
		return raft.PersistentState{}, io.ErrUnexpectedEOF
	}
	commitIndex, ok := readU64(data, &pos)
	if !ok {
		return raft.PersistentState{}, io.ErrUnexpectedEOF
	}
	if uint64(count) > uint64(len(data)-pos)/20 {
		return raft.PersistentState{}, errInvalidFormat
	}
	log := make([]raft.LogEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		entryTerm, ok := readU64(data, &pos)
		if !ok {
			return raft.PersistentState{}, io.ErrUnexpectedEOF
		}
		index, ok := readU64(data, &pos)
		if !ok {
			return raft.PersistentState{}, io.ErrUnexpectedEOF
		}
		commandLen, ok := readU32(data, &pos)
		if !ok || uint64(commandLen) > uint64(len(data)-pos) {
			return raft.PersistentState{}, errInvalidFormat
		}
		command := append([]byte(nil), data[pos:pos+int(commandLen)]...)
		pos += int(commandLen)
		entry := raft.LogEntry{Term: raft.Term(entryTerm), Index: raft.LogIndex(index), Command: command}
		if err := entry.Validate(); err != nil {
			return raft.PersistentState{}, err
		}
		if len(log) > 0 && entry.Index != log[len(log)-1].Index+1 {
			return raft.PersistentState{}, errors.New("persisted log is not contiguous")
		}
		log = append(log, entry)
	}
	if pos != len(data) {
		return raft.PersistentState{}, errInvalidFormat
	}
	if commitIndex > uint64(len(log)) {
		return raft.PersistentState{}, errors.New("persisted commit index exceeds log end")
	}
	return raft.PersistentState{CurrentTerm: raft.Term(term), VotedFor: raft.NodeID(votedFor), Log: log, CommitIndex: raft.LogIndex(commitIndex)}, nil
}

func putU32(dst []byte, value uint32) {
	dst[0] = byte(value >> 24)
	dst[1] = byte(value >> 16)
	dst[2] = byte(value >> 8)
	dst[3] = byte(value)
}
func putU64(dst []byte, value uint64) {
	for i := 7; i >= 0; i-- {
		dst[7-i] = byte(value >> (uint(i) * 8))
	}
}
func readU32(data []byte, pos *int) (uint32, bool) {
	if len(data)-*pos < 4 {
		return 0, false
	}
	value := uint32(data[*pos])<<24 | uint32(data[*pos+1])<<16 | uint32(data[*pos+2])<<8 | uint32(data[*pos+3])
	*pos += 4
	return value, true
}
func readU64(data []byte, pos *int) (uint64, bool) {
	if len(data)-*pos < 8 {
		return 0, false
	}
	var value uint64
	for i := 0; i < 8; i++ {
		value = value<<8 | uint64(data[*pos+i])
	}
	*pos += 8
	return value, true
}
