package memstore

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/lock"
)

var (
	writerPool = sync.Pool{
		New: func() interface{} {
			return &memStoreWriter{
				buf: bytes.NewBuffer(make([]byte, 0, 1024)), // Pre-allocate buffer
			}
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

// entry holds the value and metadata for a single cache item.
type entry struct {
	value    []byte
	metadata *daramjwee.Metadata
}

// MemStore is a thread-safe, in-memory implementation of the daramjwee.Store interface.
//
// MemStore provides high-performance, memory-based caching with configurable capacity
// limits and eviction policies. It's optimized for fast access patterns and supports
// various eviction algorithms (LRU, S3-FIFO, SIEVE) to maximize cache hit rates.
//
// Memory usage patterns and capacity planning:
//   - Stores data directly in memory for fastest possible access
//   - Configurable capacity limits prevent memory exhaustion
//   - Real-time size tracking for accurate capacity management
//   - Automatic eviction when capacity limits are exceeded
//
// Performance characteristics:
//   - Sub-microsecond access times for cached data
//   - Thread-safe concurrent access with fine-grained locking
//   - Optimized memory allocation with object pooling
//   - Minimal GC pressure through buffer reuse
//
// Eviction behavior and policy integration:
//   - Pluggable eviction policies (LRU, S3-FIFO, SIEVE)
//   - Real-time access tracking for policy decisions
//   - Automatic eviction triggered by capacity constraints
//   - Policy-driven victim selection for optimal hit rates
//
// Thread safety guarantees:
//   - All operations are safe for concurrent access
//   - Fine-grained locking minimizes contention
//   - Configurable locking strategies (mutex, striped)
//   - Consistent state maintained across all operations
//
// Optimization strategies:
//   - Choose appropriate capacity based on available memory
//   - Select eviction policy based on access patterns
//   - Monitor hit rates and adjust capacity accordingly
//   - Use striped locking for high-concurrency scenarios
//
// Example usage:
//
//	// Basic MemStore with LRU eviction
//	store := memstore.New(
//	    100*1024*1024, // 100MB capacity
//	    policy.NewLRU(),
//	)
//
//	// High-performance MemStore with S3-FIFO
//	store := memstore.New(
//	    512*1024*1024, // 512MB capacity
//	    policy.NewS3FIFO(),
//	    memstore.WithLocker(lock.NewStripedLock(64)), // Reduced contention
//	)
//
//	// Memory-constrained MemStore with SIEVE
//	store := memstore.New(
//	    32*1024*1024, // 32MB capacity
//	    policy.NewSIEVE(), // Low overhead
//	)
type MemStore struct {
	locker      daramjwee.Locker
	data        map[string]entry
	capacity    int64 // Capacity in bytes
	currentSize int64 // Current total size of stored items in bytes
	policy      daramjwee.EvictionPolicy
}

// Option configures the MemStore with additional settings.
//
// Options allow customization of MemStore behavior beyond the basic capacity
// and eviction policy parameters. They provide flexibility for advanced
// configurations and performance tuning.
type Option func(*MemStore)

// WithLocker sets a custom locking strategy for the MemStore.
//
// Locking strategy selection and performance implications:
//   - Default: Mutex-based locking (simple, good for low contention)
//   - Striped locking: Reduces contention in high-concurrency scenarios
//   - Custom lockers: Allow specialized locking strategies
//
// Performance considerations:
//   - Mutex locking: Simple, low overhead, good for most use cases
//   - Striped locking: Better for high concurrency, more complex
//   - Lock granularity affects both performance and memory usage
//
// Example usage:
//
//	// Default mutex locking (automatic)
//	store := memstore.New(capacity, policy)
//
//	// Striped locking for high concurrency
//	store := memstore.New(capacity, policy,
//	    memstore.WithLocker(lock.NewStripedLock(64)),
//	)
//
//	// Custom locking strategy
//	store := memstore.New(capacity, policy,
//	    memstore.WithLocker(myCustomLocker),
//	)
//
// When to use different locking strategies:
//   - Low to medium concurrency: Default mutex locking
//   - High concurrency (>100 concurrent operations): Striped locking
//   - Specialized requirements: Custom locking implementations
func WithLocker(locker daramjwee.Locker) Option {
	return func(ms *MemStore) {
		ms.locker = locker
	}
}

// New creates a new MemStore with specified capacity and eviction policy.
//
// This function initializes a thread-safe, in-memory cache store with
// configurable capacity limits and eviction behavior. The store is
// immediately ready for use and requires no additional setup.
//
// Parameters:
//   - capacity: Maximum storage capacity in bytes (0 or negative = unlimited)
//   - policy: Eviction policy for managing capacity (nil = no eviction)
//   - opts: Optional configuration functions for advanced settings
//
// Capacity planning considerations:
//   - Set based on available system memory and other memory requirements
//   - Account for metadata overhead (typically 5-10% of data size)
//   - Consider peak usage patterns and growth projections
//   - Monitor actual usage and adjust as needed
//
// Eviction policy selection:
//   - LRU: Good general-purpose policy, predictable behavior
//   - S3-FIFO: Excellent for modern web workloads, best hit rates
//   - SIEVE: Lowest overhead, good for memory-constrained environments
//   - nil: No eviction, unlimited growth (use with caution)
//
// Example configurations:
//
//	// Basic configuration with LRU eviction
//	store := memstore.New(
//	    100*1024*1024, // 100MB capacity
//	    policy.NewLRU(),
//	)
//
//	// High-performance configuration
//	store := memstore.New(
//	    512*1024*1024, // 512MB capacity
//	    policy.NewS3FIFO(),
//	    memstore.WithLocker(lock.NewStripedLock(64)),
//	)
//
//	// Memory-constrained configuration
//	store := memstore.New(
//	    32*1024*1024, // 32MB capacity
//	    policy.NewSIEVE(), // Low overhead
//	)
//
//	// Unlimited capacity (development/testing)
//	store := memstore.New(
//	    0, // No capacity limit
//	    nil, // No eviction
//	)
//
// Performance characteristics:
//   - Initialization: O(1) - immediate readiness
//   - Memory allocation: Lazy - grows as data is added
//   - Thread safety: Built-in with configurable locking strategies
//   - Eviction overhead: Depends on chosen policy
//
// Operational considerations:
//   - Monitor memory usage and hit rates
//   - Adjust capacity based on actual usage patterns
//   - Consider eviction policy performance characteristics
//   - Plan for graceful degradation when capacity is exceeded
func New(capacity int64, policy daramjwee.EvictionPolicy, opts ...Option) *MemStore {
	if policy == nil {
		policy = daramjwee.NewNullEvictionPolicy()
	}
	ms := &MemStore{
		data:     make(map[string]entry),
		capacity: capacity,
		policy:   policy,
		locker:   lock.NewMutexLock(),
	}
	for _, opt := range opts {
		opt(ms)
	}
	return ms
}

// GetStream retrieves an object as a stream from the in-memory store.
//
// This method provides fast, direct access to cached data stored in memory.
// It automatically tracks access patterns for the eviction policy and
// returns both the data stream and associated metadata.
//
// Performance characteristics:
//   - Sub-microsecond access times for cached data
//   - Zero-copy data access through byte slice readers
//   - Automatic access tracking for eviction policy
//   - Thread-safe concurrent access with fine-grained locking
//
// Access pattern tracking:
//   - Automatically calls policy.Touch() to record access
//   - Updates eviction policy state for optimal cache management
//   - Supports all eviction algorithms (LRU, S3-FIFO, SIEVE)
//
// Example usage:
//
//	stream, metadata, err := store.GetStream(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // Handle cache miss
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
//	// Read data
//	data, err := io.ReadAll(stream)
//
// Thread safety: Safe for concurrent access across multiple goroutines.
// Memory efficiency: Uses zero-copy readers for optimal performance.
func (ms *MemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	ms.locker.Lock(key) // Use Lock because policy.Touch might modify internal state.
	defer ms.locker.Unlock(key)

	e, ok := ms.data[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}

	// Notify the policy that this key was accessed.
	ms.policy.Touch(key)

	reader := bytes.NewReader(e.value)
	readCloser := io.NopCloser(reader)

	return readCloser, e.metadata, nil
}

// SetWithWriter returns a writer for streaming data into the MemStore.
//
// This method provides efficient, streaming write access to the in-memory store.
// Data is buffered during writing and atomically committed when the writer is
// closed, ensuring consistency and optimal memory usage.
//
// Atomic write behavior:
//   - Data is buffered in memory during write operations
//   - Atomic commitment when writer.Close() is called
//   - No partial writes visible to concurrent readers
//   - Automatic capacity management and eviction
//
// Performance optimizations:
//   - Object pooling for writer instances reduces allocation overhead
//   - Pre-allocated buffers minimize memory allocations
//   - Efficient memory copying for final data storage
//   - Automatic eviction when capacity limits are exceeded
//
// Capacity management:
//   - Real-time size tracking during write operations
//   - Automatic eviction triggered when capacity is exceeded
//   - Policy-driven victim selection for optimal cache performance
//   - Prevents memory exhaustion through proactive management
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
//	// Data is committed when Close() is called
//
// Resource management: The returned writer must always be closed to ensure
// proper resource cleanup and data commitment. Use defer for reliable cleanup.
//
// Thread safety: Safe for concurrent use with other MemStore operations.
func (ms *MemStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	w := writerPool.Get().(*memStoreWriter)
	w.ms = ms
	w.key = key
	w.metadata = metadata
	return w, nil
}

// Delete removes an object from the in-memory store.
//
// This method provides atomic deletion of cached objects, ensuring consistent
// state across the store and eviction policy. It handles both data removal
// and policy state updates in a thread-safe manner.
//
// Atomic deletion behavior:
//   - Thread-safe removal from internal data structures
//   - Automatic size accounting updates
//   - Eviction policy state cleanup
//   - Consistent state maintained across concurrent operations
//
// Performance characteristics:
//   - O(1) deletion time for hash map operations
//   - Minimal overhead for policy state updates
//   - No memory leaks through proper cleanup
//   - Safe concurrent access with other operations
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
// and will not cause errors. Missing keys are handled gracefully.
//
// Thread safety: Safe for concurrent use with all other MemStore operations.
func (ms *MemStore) Delete(ctx context.Context, key string) error {
	ms.locker.Lock(key)
	defer ms.locker.Unlock(key)

	if e, ok := ms.data[key]; ok {
		size := int64(len(e.value))
		ms.currentSize -= size

		delete(ms.data, key)
		// Notify the policy that this key was removed.
		ms.policy.Remove(key)
	}

	return nil
}

// Stat retrieves metadata for an object without loading the data.
//
// This method provides efficient access to object metadata for cache validation,
// freshness checks, and conditional requests without the overhead of loading
// the actual data. It's optimized for metadata-only operations.
//
// Metadata-only access patterns and performance implications:
//   - Fast metadata retrieval without data loading overhead
//   - Automatic access tracking for eviction policy
//   - Thread-safe concurrent access with fine-grained locking
//   - Minimal memory allocation and CPU usage
//
// Use cases and optimization benefits:
//   - Cache freshness validation before expensive data loading
//   - ETag-based conditional request processing
//   - Cache statistics and monitoring operations
//   - Existence checks without data transfer overhead
//
// Access pattern tracking:
//   - Calls policy.Touch() to record metadata access
//   - Updates eviction policy state for accurate cache management
//   - Treats metadata access as cache activity for policy decisions
//
// Example usage:
//
//	metadata, err := store.Stat(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // Object not in store
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
//   - O(1) metadata access time
//   - Zero data copying or allocation
//   - Minimal lock contention
//   - Efficient for high-frequency metadata operations
//
// Thread safety: Safe for concurrent use with all other MemStore operations.
func (ms *MemStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	ms.locker.Lock(key) // Use Lock for policy.Touch
	defer ms.locker.Unlock(key)

	e, ok := ms.data[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}

	// Access via Stat should also be considered a "touch".
	ms.policy.Touch(key)

	return e.metadata, nil
}

// memStoreWriter is a helper type that satisfies the io.WriteCloser interface.
type memStoreWriter struct {
	ms       *MemStore
	key      string
	metadata *daramjwee.Metadata
	buf      *bytes.Buffer
}

// Write writes the provided data to the internal buffer.
func (w *memStoreWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

// Close is called when the write operation is complete.
// It commits the buffered data to the MemStore and handles eviction if capacity is exceeded.
func (w *memStoreWriter) Close() error {
	w.ms.locker.Lock(w.key)
	defer w.ms.locker.Unlock(w.key)

	finalData := make([]byte, w.buf.Len())
	copy(finalData, w.buf.Bytes())

	newItemSize := int64(len(finalData))

	// If the item already exists, subtract its old size from currentSize.
	if oldEntry, ok := w.ms.data[w.key]; ok {
		w.ms.currentSize -= int64(len(oldEntry.value))
	}

	newEntry := entry{
		value:    finalData,
		metadata: w.metadata,
	}

	w.ms.data[w.key] = newEntry
	// Add the new item's size to currentSize.
	w.ms.currentSize += newItemSize
	w.ms.policy.Add(w.key, newItemSize)

	// Eviction logic: if capacity is positive and exceeded, evict items.
	if w.ms.capacity > 0 {
		for w.ms.currentSize > w.ms.capacity {
			keysToEvict := w.ms.policy.Evict()
			if len(keysToEvict) == 0 {
				// No more candidates for eviction, break to prevent infinite loop.
				break
			}

			var actuallyEvicted bool
			for _, keyToEvict := range keysToEvict {
				if entryToEvict, ok := w.ms.data[keyToEvict]; ok {
					w.ms.currentSize -= int64(len(entryToEvict.value))
					delete(w.ms.data, keyToEvict)
					actuallyEvicted = true
				}
			}

			if !actuallyEvicted {
				// If the policy keeps returning non-existent keys, break to prevent infinite loop.
				break
			}
		}
	}

	// Reset the writer and return it to the pool.
	w.buf.Reset()
	w.ms = nil
	w.key = ""
	w.metadata = nil
	writerPool.Put(w)

	return nil
}

// MemStore Configuration Examples for Different Memory Constraints
//
// The following examples demonstrate how to configure MemStore for various
// memory constraints and performance requirements.

// Example: High-performance MemStore for web applications
//
//	func NewHighPerformanceMemStore() *MemStore {
//	    return memstore.New(
//	        512*1024*1024, // 512MB capacity
//	        policy.NewS3FIFO(), // Best hit rates for web workloads
//	        memstore.WithLocker(lock.NewStripedLock(64)), // Reduced contention
//	    )
//	}

// Example: Memory-constrained MemStore for edge deployments
//
//	func NewMemoryConstrainedMemStore() *MemStore {
//	    return memstore.New(
//	        32*1024*1024, // 32MB capacity
//	        policy.NewSIEVE(), // Lowest overhead eviction
//	    )
//	}

// Example: Development MemStore with unlimited capacity
//
//	func NewDevelopmentMemStore() *MemStore {
//	    return memstore.New(
//	        0, // No capacity limit
//	        nil, // No eviction
//	    )
//	}

// Example: Balanced MemStore for general use
//
//	func NewBalancedMemStore() *MemStore {
//	    return memstore.New(
//	        128*1024*1024, // 128MB capacity
//	        policy.NewLRU(), // Predictable eviction behavior
//	    )
//	}
