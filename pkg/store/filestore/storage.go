package filestore

import (
	"context"
	"crypto/sha256" // Import for hashing
	"encoding/binary"
	"encoding/hex" // Import for hex encoding hash
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/lock"
)

// FileStore is a disk-based implementation of the daramjwee.Store.
type FileStore struct {
	mu                 sync.Mutex // Mutex to protect currentSize
	baseDir            string
	logger             log.Logger
	locker             daramjwee.Locker
	useCopyAndTruncate bool // useCopyAndTruncate, if true, uses a copy-and-truncate strategy for writing files.
	capacity           int64
	currentSize        int64
	policy             daramjwee.EvictionPolicy
	hashKey            bool // New field to enable/disable key hashing
	dirDepth           int  // New field for directory depth (e.g., 2 for /xx/yy/file)
	dirPrefixLength    int  // New field for length of each directory prefix (e.g., 2 for /xx/yy/file)
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

// WithHashedKeys enables hashing of cache keys to generate file paths.
// dirDepth specifies how many levels of subdirectories to create based on the hash.
// dirPrefixLength specifies the length of each directory prefix (e.g., 2 for "ab/cd").
func WithHashedKeys(dirDepth, dirPrefixLength int) Option {
	return func(fs *FileStore) {
		fs.hashKey = true
		fs.dirDepth = dirDepth
		fs.dirPrefixLength = dirPrefixLength
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
		baseDir:         dir,
		logger:          logger,
		locker:          lock.NewMutexLock(),
		capacity:        capacity,
		policy:          policy,
		hashKey:         false, // Default to no hashing
		dirDepth:        0,
		dirPrefixLength: 0,
	}
	for _, opt := range opts {
		opt(fs)
	}

	// Basic validation for hashed keys options
	if fs.hashKey {
		if fs.dirDepth < 0 {
			return nil, fmt.Errorf("invalid dirDepth for hashed keys: %d", fs.dirDepth)
		}
		if fs.dirPrefixLength <= 0 {
			return nil, fmt.Errorf("invalid dirPrefixLength for hashed keys: %d", fs.dirPrefixLength)
		}
		// SHA256 produces 64 hex characters (32 bytes * 2). Make sure dirPrefixLength * dirDepth doesn't exceed this.
		if fs.dirPrefixLength*fs.dirDepth > sha256.Size*2 {
			return nil, fmt.Errorf("combined directory prefix length (%d) exceeds hash size (%d)", fs.dirPrefixLength*fs.dirDepth, sha256.Size*2)
		}
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
	finalPath := fs.toDataPath(key) // Use finalPath to distinguish from temporary path
	fs.locker.Lock(finalPath)       // Lock on the final path

	// Create a temporary file in the baseDir, not a subdirectory of the finalPath
	tmpFile, err := os.CreateTemp(fs.baseDir, "daramjwee-tmp-*.data")
	if err != nil {
		fs.locker.Unlock(finalPath)
		return nil, err
	}

	// Write metadata to the temporary file first.
	if err := writeMetadata(tmpFile, metadata); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		fs.locker.Unlock(finalPath)
		return nil, err
	}

	onClose := func() (err error) {
		level.Debug(fs.logger).Log("msg", "onClose function called", "key", key)
		defer fs.locker.Unlock(finalPath) // Unlock the final path

		defer func() {
			if errRemove := os.Remove(tmpFile.Name()); errRemove != nil && !os.IsNotExist(errRemove) {
				level.Warn(fs.logger).Log("msg", "failed to remove temporary file", "file", tmpFile.Name(), "err", errRemove)
				if err == nil {
					err = errRemove
				}
			}
		}()

		// Ensure the directory for the final path exists before renaming/copying
		dir := filepath.Dir(finalPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create target directory %s: %w", dir, err)
		}

		// Check if the file already exists to subtract its old size
		oldSize := int64(0)
		existingFileInfo, statErr := os.Stat(finalPath)
		if statErr == nil {
			oldSize = existingFileInfo.Size()
			level.Debug(fs.logger).Log("msg", "existing file found", "finalPath", finalPath, "oldSize", oldSize)
		} else if os.IsNotExist(statErr) {
			level.Debug(fs.logger).Log("msg", "no existing file found", "finalPath", finalPath)
		} else {
			level.Warn(fs.logger).Log("msg", "failed to stat existing file", "finalPath", finalPath, "err", statErr)
		}

		if fs.useCopyAndTruncate {
			// Non-atomic copy strategy for NFS compatibility.
			if err := copyFile(tmpFile.Name(), finalPath); err != nil {
				return fmt.Errorf("failed to copy temp file to final path: %w", err)
			}
		} else {
			// Atomic rename strategy.
			if err := os.Rename(tmpFile.Name(), finalPath); err != nil {
				return fmt.Errorf("failed to rename temporary file: %w", err)
			}
		}

		fileInfo, err := os.Stat(finalPath)
		if err != nil {
			return fmt.Errorf("failed to get file info for key '%s': %w", key, err)
		}
		newItemSize := fileInfo.Size()

		level.Debug(fs.logger).Log(
			"msg", "SetWithWriter onClose: before currentSize update",
			"key", key,
			"oldSize", oldSize,
			"newItemSize", newItemSize,
			"currentSizeBeforeLock", fs.currentSize,
		)

		fs.mu.Lock() // Protect currentSize
		fs.currentSize -= oldSize // Subtract old size if it existed
		fs.currentSize += newItemSize // Add new size
		fs.policy.Add(key, newItemSize)

		level.Debug(fs.logger).Log(
			"msg", "SetWithWriter onClose: after currentSize update",
			"key", key,
			"currentSizeAfterLock", fs.currentSize,
		)

		if fs.capacity > 0 {
			for fs.currentSize > fs.capacity {
				keysToEvict := fs.policy.Evict()
				if len(keysToEvict) == 0 {
					break // No more keys to evict, or policy is stuck
				}

				var actuallyEvicted bool
				for _, keyToEvict := range keysToEvict {
					evictedPath := fs.toDataPath(keyToEvict) // Use toDataPath for eviction as well
					fileInfo, err := os.Stat(evictedPath)
					if err != nil {
						// File might have been deleted by another process or never existed
						level.Debug(fs.logger).Log("msg", "file for key to evict not found", "key", keyToEvict, "path", evictedPath, "err", err)
						fs.policy.Remove(keyToEvict) // Remove from policy if file doesn't exist
						continue
					}
					evictedSize := fileInfo.Size()

					level.Debug(fs.logger).Log("msg", "evicting file", "key", keyToEvict, "evictedSize", evictedSize, "currentSizeBeforeEvict", fs.currentSize)

					if err := os.Remove(evictedPath); err == nil {
						fs.currentSize -= evictedSize // Protected by fs.mu
						fs.policy.Remove(keyToEvict) // Remove from policy after successful eviction
						actuallyEvicted = true
					} else {
						level.Warn(fs.logger).Log("msg", "failed to remove evicted file", "file", evictedPath, "key", keyToEvict, "err", err)
					}
					level.Debug(fs.logger).Log("msg", "file evicted", "key", keyToEvict, "currentSizeAfterEvict", fs.currentSize)
				}

				if !actuallyEvicted {
					// No files were actually evicted in this loop, likely capacity cannot be met.
					// This prevents infinite loops if policy.Evict keeps returning the same un-evictable keys.
					break
				}
			}
		}
		fs.mu.Unlock() // Release currentSize protection

		return nil
	}

	return newLockedWriteCloser(tmpFile, onClose), nil
}

// Delete removes an object from the store.
func (fs *FileStore) Delete(ctx context.Context, key string) error {
	path := fs.toDataPath(key)
	fs.locker.Lock(path)
	defer fs.locker.Unlock(path)

	// Stat the file *before* removal to get its size for currentSize adjustment.
	// This also correctly handles cases where the file doesn't exist,
	// avoiding attempting to subtract size from currentSize unnecessarily.
	fileInfo, err := os.Stat(path)
	if err == nil { // Only if file exists
		deletedSize := fileInfo.Size()
		level.Debug(fs.logger).Log(
			"msg", "Delete: before currentSize update",
			"key", key,
			"deletedSize", deletedSize,
			"currentSizeBeforeLock", fs.currentSize,
		)
		fs.mu.Lock() // Protect currentSize
		fs.currentSize -= deletedSize
		fs.policy.Remove(key) // Move inside the mutex
		level.Debug(fs.logger).Log(
			"msg", "Delete: after currentSize update",
			"key", key,
			"currentSizeAfterLock", fs.currentSize,
		)
		fs.mu.Unlock() // Release currentSize protection
	} else if os.IsNotExist(err) {
		level.Debug(fs.logger).Log("msg", "Delete: file not found, no size update", "key", key, "path", path)
	} else {
		level.Warn(fs.logger).Log("msg", "Delete: failed to stat file before deletion", "key", key, "path", path, "err", err)
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
// It applies hashing and directory partitioning if fs.hashKey is true.
func (fs *FileStore) toDataPath(key string) string {
	if !fs.hashKey {
		// Original implementation for non-hashed keys
		// Prevents path traversal by stripping ".." and leading path separators.
		safeKey := strings.ReplaceAll(key, "..", "")
		safeKey = strings.TrimPrefix(safeKey, string(os.PathSeparator))
		return filepath.Join(fs.baseDir, safeKey)
	}

	// Hashing logic
	hasher := sha256.New()
	hasher.Write([]byte(key))
	hashedKey := hex.EncodeToString(hasher.Sum(nil)) // Get hex string of hash

	// Construct path with directory partitioning
	pathParts := make([]string, 0, fs.dirDepth+2) // baseDir + dirDepth subdirs + hashedKey filename
	pathParts = append(pathParts, fs.baseDir)

	for i := 0; i < fs.dirDepth; i++ {
		start := i * fs.dirPrefixLength
		end := start + fs.dirPrefixLength
		if end > len(hashedKey) {
			// This case should be prevented by validation in New, but as a safeguard.
			// Use the remaining part of the hash, or fewer parts than dirDepth if hash is too short.
			if start >= len(hashedKey) { // No more hash characters left
				break
			}
			end = len(hashedKey) // Take whatever's left
		}
		pathParts = append(pathParts, hashedKey[start:end])
	}
	pathParts = append(pathParts, hashedKey) // Final filename is the full hash

	return filepath.Join(pathParts...)
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
