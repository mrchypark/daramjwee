package filestore

import (
	"context"
	"encoding/binary"
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
)

// FileStore is a disk-based implementation of the daramjwee.Store.
type FileStore struct {
	baseDir            string
	logger             log.Logger
	lockManager        *FileLockManager
	useCopyAndTruncate bool // useCopyAndTruncate, if true, uses a copy-and-truncate strategy for writing files.

	// Policy-related fields
	mu          sync.RWMutex
	capacity    int64 // Capacity in bytes (0 means unlimited)
	currentSize int64 // Current total size of stored files in bytes
	policy      daramjwee.EvictionPolicy
	fileSizes   map[string]int64 // Track file sizes for eviction
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

// WithCapacity sets the maximum capacity of the store in bytes.
// When capacity is exceeded, the eviction policy will be used to remove files.
// If capacity is 0 or less, the store has no limit.
func WithCapacity(capacity int64) Option {
	return func(fs *FileStore) {
		fs.capacity = capacity
	}
}

// WithEvictionPolicy sets the eviction policy for the store.
// If policy is nil, a no-op policy is used (no eviction).
func WithEvictionPolicy(policy daramjwee.EvictionPolicy) Option {
	return func(fs *FileStore) {
		fs.policy = policy
	}
}

// New creates a new FileStore.
// It ensures the base directory exists and initializes the file locking mechanism.
func New(dir string, logger log.Logger, opts ...Option) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory %s: %w", dir, err)
	}
	fs := &FileStore{
		baseDir:     dir,
		logger:      logger,
		lockManager: NewFileLockManager(2048),
		fileSizes:   make(map[string]int64),
	}

	// Apply options
	for _, opt := range opts {
		opt(fs)
	}

	// Set default policy if none provided
	if fs.policy == nil {
		fs.policy = daramjwee.NewNullEvictionPolicy()
	}

	// Initialize current size by scanning existing files
	if err := fs.initializeCurrentSize(); err != nil {
		level.Warn(logger).Log("msg", "failed to initialize current size", "err", err)
	}

	return fs, nil
}

// GetStream reads an object from the store.
// It returns an io.ReadCloser, the object's metadata, and an error if any.
func (fs *FileStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.lockManager.RLock(path)

	file, err := os.Open(path)
	if err != nil {
		fs.lockManager.RUnlock(path)
		if os.IsNotExist(err) {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	meta, dataOffset, err := readMetadata(file)
	if err != nil {
		file.Close()
		fs.lockManager.RUnlock(path)
		return nil, nil, err
	}

	if _, err := file.Seek(dataOffset, io.SeekStart); err != nil {
		file.Close()
		fs.lockManager.RUnlock(path)
		return nil, nil, fmt.Errorf("failed to seek to data section: %w", err)
	}

	// Notify the policy that this key was accessed
	fs.mu.Lock()
	fs.policy.Touch(key)
	fs.mu.Unlock()

	return newLockedReadCloser(file, func() { fs.lockManager.RUnlock(path) }), meta, nil
}

// SetWithWriter returns a writer that streams data to the store.
// The data is written to a temporary file and then atomically moved to the final location
// upon closing the writer, or copied if WithCopyAndTruncate option is used.
func (fs *FileStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	path := fs.toDataPath(key)
	fs.lockManager.Lock(path)

	// Ensure the directory exists for the target path
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		fs.lockManager.Unlock(path)
		return nil, fmt.Errorf("failed to create directory for key %s: %w", key, err)
	}

	tmpFile, err := os.CreateTemp(fs.baseDir, "daramjwee-tmp-*.data")
	if err != nil {
		fs.lockManager.Unlock(path)
		return nil, err
	}

	// Write metadata to the temporary file first.
	if err := writeMetadata(tmpFile, metadata); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		fs.lockManager.Unlock(path)
		return nil, err
	}

	onClose := func() (err error) {
		defer fs.lockManager.Unlock(path)

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

		// Update policy and size tracking after successful write
		if err := fs.updateAfterSet(key, path); err != nil {
			level.Warn(fs.logger).Log("msg", "failed to update policy after set", "key", key, "err", err)
		}

		return nil
	}

	return newLockedWriteCloser(tmpFile, onClose), nil
}

// Delete removes an object from the store.
func (fs *FileStore) Delete(ctx context.Context, key string) error {
	path := fs.toDataPath(key)
	fs.lockManager.Lock(path)
	defer fs.lockManager.Unlock(path)

	// Get file size before deletion for policy update
	fs.mu.Lock()
	fileSize, exists := fs.fileSizes[key]
	if exists {
		fs.currentSize -= fileSize
		delete(fs.fileSizes, key)
		fs.policy.Remove(key)
	}
	fs.mu.Unlock()

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		// If deletion failed but we already updated the policy, revert the changes
		if exists {
			fs.mu.Lock()
			fs.fileSizes[key] = fileSize
			fs.currentSize += fileSize
			fs.policy.Add(key, fileSize)
			fs.mu.Unlock()
		}
		return err
	}
	return nil
}

// Stat reads the metadata of an object without reading the data.
func (fs *FileStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.lockManager.RLock(path)
	defer fs.lockManager.RUnlock(path)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	defer file.Close()

	meta, _, err := readMetadata(file)
	if err != nil {
		return nil, err
	}

	// Access via Stat should also be considered a "touch"
	fs.mu.Lock()
	fs.policy.Touch(key)
	fs.mu.Unlock()

	return meta, nil
}

// toDataPath converts a key into a safe file path within the base directory.
// It prevents path traversal by cleaning the path and ensuring it stays within baseDir.
func (fs *FileStore) toDataPath(key string) string {
	safeFallback := func(key string) string {
		safeKey := strings.ReplaceAll(key, "..", "")
		safeKey = strings.ReplaceAll(safeKey, string(os.PathSeparator), "_")
		safeKey = strings.ReplaceAll(safeKey, "/", "_")
		if safeKey == "" {
			safeKey = "safe_fallback"
		}
		return filepath.Join(fs.baseDir, safeKey)
	}

	// Handle empty key
	if key == "" {
		return filepath.Join(fs.baseDir, "empty_key")
	}

	// Sanitize the key to prevent path traversal while preserving directory structure.
	// By cleaning the key relative to a root, we resolve all ".." segments safely.
	// We use "/" as it's the canonical separator for this operation.
	slashedKey := filepath.ToSlash(key)
	cleanKey := filepath.Clean("/" + slashedKey)
	cleanKey = strings.TrimPrefix(cleanKey, "/")

	// If cleaning results in an empty or dot path, use a safe default.
	if cleanKey == "" || cleanKey == "." {
		cleanKey = "root_file"
	}

	// Join with base directory. Join will handle OS-specific separators.
	fullPath := filepath.Join(fs.baseDir, cleanKey)

	// Final safety check: ensure the resolved path is still within baseDir.
	absBase, err := filepath.Abs(fs.baseDir)
	if err != nil {
		return safeFallback(key)
	}

	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return safeFallback(key)
	}

	// Check if path is within base directory
	if !strings.HasPrefix(absPath+string(os.PathSeparator), absBase+string(os.PathSeparator)) && absPath != absBase {
		return safeFallback(key)
	}

	return fullPath
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

// initializeCurrentSize scans the base directory to calculate the current total size
// and populate the fileSizes map for existing files.
func (fs *FileStore) initializeCurrentSize() error {
	return filepath.Walk(fs.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Skip temporary files
		if strings.Contains(info.Name(), "daramjwee-tmp-") {
			return nil
		}

		// Convert file path back to key
		relPath, err := filepath.Rel(fs.baseDir, path)
		if err != nil {
			return err
		}

		key := filepath.ToSlash(relPath)
		size := info.Size()

		fs.fileSizes[key] = size
		fs.currentSize += size
		fs.policy.Add(key, size)

		return nil
	})
}

// updateAfterSet updates the policy and size tracking after a successful file write.
func (fs *FileStore) updateAfterSet(key, path string) error {
	// Get the file size
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file after write: %w", err)
	}

	newFileSize := fileInfo.Size()

	fs.mu.Lock()

	// If the file already existed, subtract its old size
	if oldSize, exists := fs.fileSizes[key]; exists {
		fs.currentSize -= oldSize
	}

	// Add the new file size
	fs.fileSizes[key] = newFileSize
	fs.currentSize += newFileSize
	fs.policy.Add(key, newFileSize)

	// Collect keys to evict while holding the lock
	var keysToEvict []string
	if fs.capacity > 0 {
		for fs.currentSize > fs.capacity {
			candidates := fs.policy.Evict()
			if len(candidates) == 0 {
				break
			}
			// Filter out the current key to avoid deadlock
			var filteredCandidates []string
			for _, candidate := range candidates {
				if candidate != key {
					filteredCandidates = append(filteredCandidates, candidate)
				}
			}

			if len(filteredCandidates) == 0 {
				// If only the current key was a candidate, we can't evict it now.
				// This means the cache might temporarily exceed capacity.
				level.Warn(fs.logger).Log("msg", "eviction failed, policy only suggested evicting the key being written", "key", key)
				break
			}

			keysToEvict = append(keysToEvict, filteredCandidates...)

			// Optimistically update size tracking
			for _, keyToEvict := range filteredCandidates {
				if size, exists := fs.fileSizes[keyToEvict]; exists {
					fs.currentSize -= size
					delete(fs.fileSizes, keyToEvict)
				}
			}
		}
	}

	fs.mu.Unlock()

	// Perform actual file deletions without holding the mutex
	for _, keyToEvict := range keysToEvict {
		if err := fs.deleteFileOnly(keyToEvict); err != nil {
			level.Warn(fs.logger).Log("msg", "failed to evict file", "key", keyToEvict, "err", err)
			// Revert the optimistic update for this key
			fs.mu.Lock()
			if fileInfo, statErr := os.Stat(fs.toDataPath(keyToEvict)); statErr == nil {
				fs.fileSizes[keyToEvict] = fileInfo.Size()
				fs.currentSize += fileInfo.Size()
				fs.policy.Add(keyToEvict, fileInfo.Size())
			}
			fs.mu.Unlock()
		} else {
			level.Debug(fs.logger).Log("msg", "file evicted", "key", keyToEvict)
		}
	}

	return nil
}

// deleteFileOnly removes a file from disk without updating internal tracking.
// This is used during eviction when tracking has already been updated optimistically.
func (fs *FileStore) deleteFileOnly(key string) error {
	path := fs.toDataPath(key)

	// Lock the file for deletion
	fs.lockManager.Lock(path)
	defer fs.lockManager.Unlock(path)

	// Remove the file
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file during eviction: %w", err)
	}

	return nil
}
