package filestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-json"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/lock"
)

// FileStore is a disk-based implementation of the daramjwee.Store.
type FileStore struct {
	baseDir            string
	logger             log.Logger
	locker             daramjwee.Locker
	useCopyAndTruncate bool // useCopyAndTruncate, if true, uses a copy-and-truncate strategy for writing files.
	capacity           int64
	currentSize        int64
	policy             daramjwee.EvictionPolicy
}

// Option configures the FileStore.
type Option func(*FileStore)

// WithCopyAndTruncate sets the store to use a copy-and-truncate strategy
// instead of an atomic rename. This can be necessary for compatibility with
// some network filesystems like NFS.
func WithCopyAndTruncate() Option {
	return func(fs *FileStore) {
		fs.useCopyAndTruncate = true
	}
}

// WithLocker sets the locker for the filestore.
func WithLocker(locker daramjwee.Locker) Option {
	return func(fs *FileStore) {
		fs.locker = locker
	}
}

// New creates a new FileStore.
// It ensures the base directory exists and initializes the file locking mechanism.
func New(dir string, logger log.Logger, capacity int64, policy daramjwee.EvictionPolicy, opts ...Option) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory %s: %w", dir, err)
	}
	if policy == nil {
		policy = daramjwee.NewNullEvictionPolicy()
	}
	fs := &FileStore{
		baseDir:  dir,
		logger:   logger,
		locker:   lock.NewMutexLock(),
		capacity: capacity,
		policy:   policy,
	}
	for _, opt := range opts {
		opt(fs)
	}
	return fs, nil
}

// GetStream reads an object from the store.
// It returns an io.ReadCloser, the object's metadata, and an error if any.
func (fs *FileStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.locker.RLock(path)

	file, err := os.Open(path)
	if err != nil {
		fs.locker.RUnlock(path)
		if os.IsNotExist(err) {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	meta, dataOffset, err := readMetadata(file)
	if err != nil {
		file.Close()
		fs.locker.RUnlock(path)
		return nil, nil, err
	}

	if _, err := file.Seek(dataOffset, io.SeekStart); err != nil {
		file.Close()
		fs.locker.RUnlock(path)
		return nil, nil, fmt.Errorf("failed to seek to data section: %w", err)
	}

	fs.policy.Touch(key)

	return newLockedReadCloser(file, func() { fs.locker.RUnlock(path) }), meta, nil
}

// SetWithWriter returns a writer that streams data to the store.
// The data is written to a temporary file and then atomically moved to the final location
// upon closing the writer, or copied if WithCopyAndTruncate option is used.
func (fs *FileStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	path := fs.toDataPath(key)
	fs.locker.Lock(path)

	tmpFile, err := os.CreateTemp(fs.baseDir, "daramjwee-tmp-*.data")
	if err != nil {
		fs.locker.Unlock(path)
		return nil, err
	}

	// Write metadata to the temporary file first.
	if err := writeMetadata(tmpFile, metadata); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		fs.locker.Unlock(path)
		return nil, err
	}

	onClose := func() (err error) {
		defer fs.locker.Unlock(path)

		defer func() {
			if errRemove := os.Remove(tmpFile.Name()); errRemove != nil && !os.IsNotExist(errRemove) {
				level.Warn(fs.logger).Log("msg", "failed to remove temporary file", "file", tmpFile.Name(), "err", errRemove)
				if err == nil {
					err = errRemove
				}
			}
		}()

		if fs.useCopyAndTruncate {
			// Non-atomic copy strategy for NFS compatibility.
			if err := copyFile(tmpFile.Name(), path); err != nil {
				return fmt.Errorf("failed to copy temp file to final path: %w", err)
			}
		} else {
			// Atomic rename strategy.
			if err := os.Rename(tmpFile.Name(), path); err != nil {
				return fmt.Errorf("failed to rename temporary file: %w", err)
			}
		}

		fileInfo, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("failed to get file info for key '%s': %w", key, err)
		}
		newItemSize := fileInfo.Size()

		fs.currentSize += newItemSize
		fs.policy.Add(key, newItemSize)

		if fs.capacity > 0 {
			for fs.currentSize > fs.capacity {
				keysToEvict := fs.policy.Evict()
				if len(keysToEvict) == 0 {
					break
				}

				var actuallyEvicted bool
				for _, keyToEvict := range keysToEvict {
					evictedPath := fs.toDataPath(keyToEvict)
					fileInfo, err := os.Stat(evictedPath)
					if err != nil {
						continue
					}
					evictedSize := fileInfo.Size()

					if err := os.Remove(evictedPath); err == nil {
						fs.currentSize -= evictedSize
						actuallyEvicted = true
					}
				}

				if !actuallyEvicted {
					break
				}
			}
		}

		return nil
	}

	return newLockedWriteCloser(tmpFile, onClose), nil
}

// Delete removes an object from the store.
func (fs *FileStore) Delete(ctx context.Context, key string) error {
	path := fs.toDataPath(key)
	fs.locker.Lock(path)
	defer fs.locker.Unlock(path)

	fileInfo, err := os.Stat(path)
	if err == nil {
		fs.currentSize -= fileInfo.Size()
		fs.policy.Remove(key)
	}

	err = os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Stat reads the metadata of an object without reading the data.
func (fs *FileStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.locker.RLock(path)
	defer fs.locker.RUnlock(path)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	defer file.Close()

	meta, _, err := readMetadata(file)
	fs.policy.Touch(key)
	return meta, err
}

// toDataPath converts a key into a safe file path within the base directory.
// It prevents path traversal by stripping ".." and leading path separators.
func (fs *FileStore) toDataPath(key string) string {
	safeKey := strings.ReplaceAll(key, "..", "")
	safeKey = strings.TrimPrefix(safeKey, string(os.PathSeparator))
	return filepath.Join(fs.baseDir, safeKey)
}

// writeMetadata serializes metadata, prefixes it with its length, and writes it to the provided writer.
func writeMetadata(w io.Writer, meta *daramjwee.Metadata) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(metaBytes)))

	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to write metadata length: %w", err)
	}
	if _, err := w.Write(metaBytes); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}
	return nil
}

// readMetadata reads metadata from a reader.
// It expects the metadata length as a uint32 prefix, followed by the JSON-encoded metadata.
// It returns the metadata, the offset where the data begins, and an error if any.
func readMetadata(r io.Reader) (*daramjwee.Metadata, int64, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, 0, daramjwee.ErrNotFound // Treat empty/short files as not found
		}
		return nil, 0, fmt.Errorf("failed to read metadata length: %w", err)
	}

	metaLen := binary.BigEndian.Uint32(lenBuf)
	// Add a sanity check for metaLen to avoid allocating huge buffers
	if metaLen > 10*1024*1024 { // 10MB limit for metadata
		return nil, 0, fmt.Errorf("metadata size is too large: %d bytes", metaLen)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(r, metaBytes); err != nil {
		return nil, 0, fmt.Errorf("failed to read metadata bytes: %w", err)
	}

	var meta daramjwee.Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	dataOffset := int64(4 + metaLen)
	return &meta, dataOffset, nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := in.Close(); err == nil {
			err = closeErr
		}
	}()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
	}()

	_, err = io.Copy(out, in)
	return err
}

// lockedReadCloser wraps an os.File and executes an unlock function on Close.
type lockedReadCloser struct {
	*os.File
	unlockFunc func()
}

// newLockedReadCloser creates a new lockedReadCloser.
func newLockedReadCloser(f *os.File, unlockFunc func()) io.ReadCloser {
	return &lockedReadCloser{File: f, unlockFunc: unlockFunc}
}

// Close closes the underlying file and executes the unlock function.
func (lrc *lockedReadCloser) Close() error {
	defer lrc.unlockFunc()
	return lrc.File.Close()
}

// lockedWriteCloser wraps an os.File and executes an onClose function on Close.
type lockedWriteCloser struct {
	*os.File
	onClose func() error
}

// newLockedWriteCloser creates a new lockedWriteCloser.
func newLockedWriteCloser(f *os.File, onClose func() error) io.WriteCloser {
	return &lockedWriteCloser{File: f, onClose: onClose}
}

// Close closes the underlying file and then executes the onClose callback.
// It prioritizes the error from the onClose callback.
func (lwc *lockedWriteCloser) Close() error {
	closeErr := lwc.File.Close()

	onCloseErr := lwc.onClose()

	if onCloseErr != nil {
		return onCloseErr
	}
	return closeErr
}
