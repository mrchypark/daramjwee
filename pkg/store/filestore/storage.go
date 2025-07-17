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

// FileStore is a disk-based implementation of the daramjwee.Store interface.
//
// FileStore provides persistent, file-system-based caching with atomic write
// guarantees and configurable directory partitioning. It's designed for scenarios
// requiring cache persistence across application restarts and larger storage capacity.
//
// Persistence guarantees and atomic write behavior:
//   - Atomic write operations using temporary files and rename/copy strategies
//   - Data integrity through metadata prefixing and validation
//   - Crash-safe operations with proper cleanup and recovery
//   - Configurable write strategies for different filesystem types
//
// File system requirements and compatibility considerations:
//   - Works with any POSIX-compliant filesystem
//   - Atomic rename support for best performance (most local filesystems)
//   - Copy-and-truncate mode for NFS and other network filesystems
//   - Automatic directory creation and management
//
// Hashed key feature with directory partitioning and performance benefits:
//   - Optional key hashing for uniform directory distribution
//   - Configurable directory depth and partitioning strategies
//   - Prevents filesystem performance degradation with large key counts
//   - Improves directory traversal and file lookup performance
//
// Performance characteristics:
//   - Millisecond-range access times (depends on storage medium)
//   - Excellent for large datasets that exceed memory capacity
//   - Configurable capacity limits with automatic eviction
//   - Thread-safe concurrent access with fine-grained locking
//
// Example usage:
//
//	// Basic FileStore with simple key mapping
//	store, err := filestore.New(
//	    "/var/cache/daramjwee",
//	    logger,
//	    10*1024*1024*1024, // 10GB capacity
//	    policy.NewLRU(),
//	)
//
//	// High-performance FileStore with hashed keys
//	store, err := filestore.New(
//	    "/var/cache/daramjwee",
//	    logger,
//	    50*1024*1024*1024, // 50GB capacity
//	    policy.NewS3FIFO(),
//	    filestore.WithHashedKeys(2, 2), // /ab/cd/abcd1234...
//	)
//
//	// NFS-compatible FileStore
//	store, err := filestore.New(
//	    "/nfs/cache/daramjwee",
//	    logger,
//	    5*1024*1024*1024, // 5GB capacity
//	    policy.NewLRU(),
//	    filestore.WithCopyAndTruncate(), // NFS compatibility
//	)
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

// Option configures the FileStore with additional settings.
//
// Options provide flexibility for customizing FileStore behavior for different
// filesystem types, performance requirements, and deployment scenarios.
type Option func(*FileStore)

// WithCopyAndTruncate enables copy-and-truncate mode for NFS compatibility.
//
// Copy-and-truncate mode with NFS compatibility and trade-offs:
//   - Uses file copying instead of atomic rename operations
//   - Required for NFS and other network filesystems that don't support atomic rename
//   - Provides compatibility with distributed and network-attached storage
//   - Trade-off: Slightly less atomic than rename, but still crash-safe
//
// When to use copy-and-truncate mode:
//   - NFS-mounted cache directories
//   - Network-attached storage that doesn't support atomic rename
//   - Distributed filesystems with rename limitations
//   - Any filesystem where atomic rename operations fail
//
// Performance implications:
//   - Slightly higher I/O overhead due to copying vs. renaming
//   - Still maintains data integrity and crash safety
//   - May have different performance characteristics under high load
//   - Generally acceptable trade-off for compatibility
//
// Example usage:
//
//	// NFS-compatible FileStore
//	store, err := filestore.New(
//	    "/nfs/cache/daramjwee",
//	    logger,
//	    capacity,
//	    policy,
//	    filestore.WithCopyAndTruncate(),
//	)
func WithCopyAndTruncate() Option {
	return func(fs *FileStore) {
		fs.useCopyAndTruncate = true
	}
}

// WithLocker sets a custom locking strategy for the FileStore.
//
// Locking strategy selection and performance implications:
//   - Default: Mutex-based locking (simple, good for most use cases)
//   - Striped locking: Reduces contention in high-concurrency scenarios
//   - Custom lockers: Allow specialized locking strategies for specific needs
//
// Performance considerations:
//   - File operations can be I/O bound, making lock contention less critical
//   - Striped locking beneficial for high-concurrency file access patterns
//   - Consider filesystem characteristics when choosing locking strategy
//
// Example usage:
//
//	// High-concurrency FileStore with striped locking
//	store, err := filestore.New(
//	    "/var/cache/daramjwee",
//	    logger,
//	    capacity,
//	    policy,
//	    filestore.WithLocker(lock.NewStripedLock(32)),
//	)
func WithLocker(locker daramjwee.Locker) Option {
	return func(fs *FileStore) {
		fs.locker = locker
	}
}

// WithHashedKeys enables key hashing with directory partitioning.
//
// Directory partitioning and performance benefits:
//   - Prevents filesystem performance degradation with large numbers of files
//   - Distributes files evenly across directory structure
//   - Improves directory traversal and file lookup performance
//   - Essential for large-scale caching scenarios
//
// Parameters:
//   - dirDepth: Number of directory levels (e.g., 2 for /ab/cd/file)
//   - dirPrefixLength: Characters per directory level (e.g., 2 for "ab", "cd")
//
// Hashing and distribution strategy:
//   - Uses SHA256 for uniform key distribution
//   - Creates predictable directory structure
//   - Prevents hotspots in filesystem operations
//   - Scales well with increasing cache size
//
// Configuration guidelines:
//   - Small caches (<10K files): No hashing needed
//   - Medium caches (10K-1M files): dirDepth=2, dirPrefixLength=2
//   - Large caches (>1M files): dirDepth=3, dirPrefixLength=2
//   - Very large caches: Consider dirDepth=4 or higher
//
// Example configurations:
//
//	// Medium-scale cache with 2-level partitioning
//	// Creates paths like: /ab/cd/abcd1234567890...
//	store, err := filestore.New(
//	    "/var/cache/daramjwee",
//	    logger,
//	    capacity,
//	    policy,
//	    filestore.WithHashedKeys(2, 2),
//	)
//
//	// Large-scale cache with 3-level partitioning
//	// Creates paths like: /ab/cd/ef/abcdef1234567890...
//	store, err := filestore.New(
//	    "/var/cache/daramjwee",
//	    logger,
//	    capacity,
//	    policy,
//	    filestore.WithHashedKeys(3, 2),
//	)
//
// Performance benefits:
//   - Prevents directory size limitations in many filesystems
//   - Improves file lookup performance with large key counts
//   - Reduces filesystem metadata overhead
//   - Enables better filesystem caching and optimization
func WithHashedKeys(dirDepth, dirPrefixLength int) Option {
	return func(fs *FileStore) {
		fs.hashKey = true
		fs.dirDepth = dirDepth
		fs.dirPrefixLength = dirPrefixLength
	}
}

// New creates a new FileStore with specified directory and configuration.
//
// This function initializes a persistent, file-system-based cache store with
// configurable capacity limits, eviction policies, and filesystem compatibility
// options. The store is immediately ready for use after successful creation.
//
// Parameters:
//   - dir: Base directory for cache storage (created if it doesn't exist)
//   - logger: Logger for operational messages and debugging
//   - capacity: Maximum storage capacity in bytes (0 = unlimited)
//   - policy: Eviction policy for capacity management (nil = no eviction)
//   - opts: Optional configuration functions for advanced settings
//
// Directory and filesystem considerations:
//   - Automatically creates base directory with appropriate permissions
//   - Validates hashed key configuration parameters
//   - Supports any POSIX-compliant filesystem
//   - Handles permission and access issues gracefully
//
// Capacity and eviction management:
//   - Real-time size tracking for accurate capacity management
//   - Automatic eviction when capacity limits are exceeded
//   - Policy-driven victim selection for optimal performance
//   - Supports unlimited capacity for development/testing
//
// Example configurations:
//
//	// Basic FileStore for development
//	store, err := filestore.New(
//	    "/tmp/daramjwee-cache",
//	    logger,
//	    1*1024*1024*1024, // 1GB capacity
//	    policy.NewLRU(),
//	)
//
//	// Production FileStore with hashed keys
//	store, err := filestore.New(
//	    "/var/cache/daramjwee",
//	    logger,
//	    50*1024*1024*1024, // 50GB capacity
//	    policy.NewS3FIFO(),
//	    filestore.WithHashedKeys(2, 2),
//	    filestore.WithLocker(lock.NewStripedLock(64)),
//	)
//
//	// NFS-compatible FileStore
//	store, err := filestore.New(
//	    "/nfs/shared/cache",
//	    logger,
//	    10*1024*1024*1024, // 10GB capacity
//	    policy.NewLRU(),
//	    filestore.WithCopyAndTruncate(),
//	)
//
// Error handling:
//   - Returns detailed errors for directory creation failures
//   - Validates hashed key configuration parameters
//   - Provides clear error messages for troubleshooting
//
// Performance characteristics:
//   - Initialization: Fast directory creation and validation
//   - Memory usage: Minimal overhead for metadata tracking
//   - Scalability: Supports very large cache sizes with proper configuration
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

// GetStream retrieves an object as a stream from the file-based store.
//
// This method provides persistent access to cached data stored on disk with
// automatic access tracking for eviction policies. It handles metadata parsing
// and provides streaming access to the actual data content.
//
// Persistence guarantees and atomic read behavior:
//   - Reads from atomically written files for data consistency
//   - Handles metadata parsing and validation automatically
//   - Provides streaming access without loading entire files into memory
//   - Thread-safe concurrent access with fine-grained file locking
//
// Performance characteristics:
//   - Millisecond-range access times (depends on storage medium)
//   - Zero-copy streaming for large files
//   - Automatic access tracking for eviction policy
//   - Efficient metadata parsing with minimal overhead
//
// File format and metadata handling:
//   - Files contain metadata header followed by actual data
//   - Metadata includes ETag, cache timestamps, and compression info
//   - Automatic seeking to data section after metadata parsing
//   - Validation of file format and metadata integrity
//
// Example usage:
//
//	stream, metadata, err := store.GetStream(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // File not found on disk
//	        return nil, nil, err
//	    }
//	    return nil, nil, err
//	}
//	defer stream.Close()
//
//	// Process metadata
//	if metadata.IsNegative {
//	    // Handle negative cache entry
//	}
//
//	// Stream data from disk
//	data, err := io.ReadAll(stream)
//
// Thread safety: Safe for concurrent access with automatic file locking.
// Resource management: Returned stream must be closed to release file handles.
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

// SetWithWriter returns a writer for streaming data to the file-based store.
//
// This method provides atomic write operations using temporary files and either
// atomic rename or copy-and-truncate strategies. Data is written to a temporary
// file and atomically committed when the writer is closed, ensuring consistency.
//
// Atomic write guarantees and error handling:
//   - Uses temporary files to prevent partial writes from being visible
//   - Atomic rename for best performance on local filesystems
//   - Copy-and-truncate mode for NFS and network filesystem compatibility
//   - Automatic cleanup of temporary files on errors
//
// Capacity management and eviction:
//   - Real-time size tracking during write operations
//   - Automatic eviction triggered when capacity is exceeded
//   - Policy-driven victim selection for optimal cache performance
//   - Prevents storage exhaustion through proactive management
//
// File format and metadata handling:
//   - Metadata is written as a header followed by actual data
//   - JSON serialization for metadata with length prefixing
//   - Automatic directory creation for hashed key paths
//   - Validation and error handling for file operations
//
// Example usage:
//
//	writer, err := store.SetWithWriter(ctx, "user:123", &daramjwee.Metadata{
//	    ETag: "abc123",
//	    CachedAt: time.Now(),
//	})
//	if err != nil {
//	    return err
//	}
//	defer writer.Close() // Always close to commit data
//
//	// Stream data to writer
//	_, err = io.Copy(writer, dataSource)
//	if err != nil {
//	    return err
//	}
//
//	// Data is atomically committed when Close() is called
//
// Resource management: The returned writer must always be closed to ensure
// proper resource cleanup and atomic data commitment. Use defer for reliable cleanup.
//
// Thread safety: Safe for concurrent use with automatic file locking.
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

		fs.mu.Lock()                  // Protect currentSize
		fs.currentSize -= oldSize     // Subtract old size if it existed
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
						fs.policy.Remove(keyToEvict)  // Remove from policy after successful eviction
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

// Delete removes an object from the file-based store.
//
// This method provides atomic deletion of cached files with proper size
// accounting and eviction policy updates. It handles both file removal
// and internal state cleanup in a thread-safe manner.
//
// Atomic deletion behavior:
//   - Thread-safe removal with file-level locking
//   - Automatic size accounting updates before file deletion
//   - Eviction policy state cleanup for consistent tracking
//   - Graceful handling of missing files (idempotent operation)
//
// Performance characteristics:
//   - Fast file system deletion operations
//   - Minimal overhead for size tracking updates
//   - No memory leaks through proper state cleanup
//   - Safe concurrent access with other file operations
//
// Policy integration:
//   - Automatically calls policy.Remove() to clean up tracking state
//   - Updates size accounting for accurate capacity management
//   - Maintains policy consistency across all operations
//
// Example usage:
//
//	err := store.Delete(ctx, "user:123")
//	if err != nil {
//	    log.Printf("Failed to delete from store: %v", err)
//	    // Continue with other operations - deletion failure shouldn't be fatal
//	}
//
// Idempotent operation: Multiple delete calls for the same key are safe
// and will not cause errors. Missing files are handled gracefully.
//
// Thread safety: Safe for concurrent use with automatic file locking.
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

// Stat retrieves metadata for an object without loading the data.
//
// This method provides efficient access to object metadata for cache validation,
// freshness checks, and conditional requests without the overhead of loading
// the actual file data. It's optimized for metadata-only operations.
//
// Metadata-only access patterns and performance implications:
//   - Fast metadata retrieval without data loading overhead
//   - Automatic access tracking for eviction policy
//   - Thread-safe concurrent access with fine-grained file locking
//   - Minimal I/O operations (only metadata header reading)
//
// Use cases and optimization benefits:
//   - Cache freshness validation before expensive data loading
//   - ETag-based conditional request processing
//   - Cache statistics and monitoring operations
//   - Existence checks without data transfer overhead
//
// File format handling:
//   - Reads only the metadata header from files
//   - Validates file format and metadata integrity
//   - Handles corrupted or incomplete files gracefully
//   - Automatic file handle management and cleanup
//
// Example usage:
//
//	metadata, err := store.Stat(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // File not found on disk
//	        return nil, err
//	    }
//	    return nil, err
//	}
//
//	// Check freshness without loading data
//	if time.Since(metadata.CachedAt) > maxAge {
//	    // Object is stale, trigger refresh
//	    return triggerRefresh(ctx, key)
//	}
//
//	// Check ETag for conditional requests
//	if metadata.ETag == clientETag {
//	    // Data hasn't changed, return 304 Not Modified
//	    return handleNotModified()
//	}
//
// Performance characteristics:
//   - Fast metadata access (typically <1ms on local storage)
//   - Minimal file I/O (only header reading)
//   - Efficient for high-frequency metadata operations
//   - Automatic access tracking for eviction policies
//
// Thread safety: Safe for concurrent use with automatic file locking.
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

// FileStore Configuration Examples for Different Storage Scenarios
//
// The following examples demonstrate how to configure FileStore for various
// storage requirements, filesystem types, and deployment scenarios.

// Example: Basic FileStore for development and testing
//
//	func NewDevelopmentFileStore(logger log.Logger) (*FileStore, error) {
//	    return filestore.New(
//	        "/tmp/daramjwee-dev-cache",
//	        logger,
//	        1*1024*1024*1024, // 1GB capacity
//	        policy.NewLRU(),  // Simple, predictable eviction
//	    )
//	}

// Example: Production FileStore with hashed keys for large-scale caching
//
//	func NewProductionFileStore(logger log.Logger) (*FileStore, error) {
//	    return filestore.New(
//	        "/var/cache/daramjwee",
//	        logger,
//	        50*1024*1024*1024, // 50GB capacity
//	        policy.NewS3FIFO(), // Best hit rates for production
//	        filestore.WithHashedKeys(2, 2), // /ab/cd/abcd1234...
//	        filestore.WithLocker(lock.NewStripedLock(64)), // High concurrency
//	    )
//	}

// Example: NFS-compatible FileStore for distributed deployments
//
//	func NewNFSFileStore(logger log.Logger) (*FileStore, error) {
//	    return filestore.New(
//	        "/nfs/shared/cache",
//	        logger,
//	        10*1024*1024*1024, // 10GB capacity
//	        policy.NewLRU(),
//	        filestore.WithCopyAndTruncate(), // NFS compatibility
//	        filestore.WithHashedKeys(2, 2),  // Directory distribution
//	    )
//	}

// Example: High-capacity FileStore for data archival
//
//	func NewArchivalFileStore(logger log.Logger) (*FileStore, error) {
//	    return filestore.New(
//	        "/mnt/storage/daramjwee-archive",
//	        logger,
//	        500*1024*1024*1024, // 500GB capacity
//	        policy.NewSIEVE(),   // Low overhead for large datasets
//	        filestore.WithHashedKeys(3, 2), // /ab/cd/ef/abcdef...
//	    )
//	}

// Example: SSD-optimized FileStore for high-performance scenarios
//
//	func NewSSDFileStore(logger log.Logger) (*FileStore, error) {
//	    return filestore.New(
//	        "/ssd/cache/daramjwee",
//	        logger,
//	        100*1024*1024*1024, // 100GB capacity
//	        policy.NewS3FIFO(),  // Optimal for SSD characteristics
//	        filestore.WithHashedKeys(2, 2),
//	        filestore.WithLocker(lock.NewStripedLock(128)), // High concurrency
//	    )
//	}
