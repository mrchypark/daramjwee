# Design Document

## Overview

This design focuses on creating comprehensive, high-quality documentation for all public APIs in the daramjwee library. The approach emphasizes practical usability, clear examples, and consistent documentation patterns that help developers understand not just what each component does, but how to use it effectively in real-world scenarios.

## Architecture

### Documentation Framework

The documentation strategy is organized into five main categories:

1. **Core Interface Documentation**: Cache, Store, Fetcher, EvictionPolicy, BufferPool
2. **Configuration Documentation**: Options, Config structs, tuning parameters
3. **Error Handling Documentation**: Error types, conditions, recovery strategies
4. **Storage Backend Documentation**: MemStore, FileStore, objstore adapter specifics
5. **Extension Documentation**: Custom implementation guidelines and best practices

### Documentation Standards

#### Godoc Comment Structure
```go
// ComponentName provides [brief description of primary purpose].
//
// [Detailed description explaining behavior, constraints, and important notes]
//
// Example usage:
//   [code example showing typical usage]
//
// Thread safety: [thread safety guarantees or requirements]
// Performance: [performance characteristics or considerations]
```

#### Method Documentation Template
```go
// MethodName [brief description of what the method does].
//
// Parameters:
//   - param1: [description of parameter and constraints]
//   - param2: [description of parameter and constraints]
//
// Returns:
//   - [description of return value and possible states]
//   - error: [description of error conditions]
//
// Example:
//   [code example showing method usage]
//
// Note: [any important behavioral notes or caveats]
```

## Components and Interfaces

### 1. Core Interface Documentation

#### Cache Interface Enhancement
```go
// Cache provides a high-performance, stream-based hybrid caching interface.
//
// Cache implements a multi-tier caching strategy with hot and cold storage tiers,
// supporting efficient data retrieval, background refresh, and stream-based operations
// to minimize memory overhead for large objects.
//
// All operations are thread-safe and support context-based cancellation and timeouts.
// The cache automatically handles promotion between tiers, background refresh of stale
// data, and negative caching to prevent repeated failed requests.
//
// Example usage:
//   cache, err := daramjwee.New(logger,
//       daramjwee.WithHotStore(memStore),
//       daramjwee.WithColdStore(fileStore),
//       daramjwee.WithCache(5*time.Minute),
//   )
//   if err != nil {
//       return err
//   }
//   defer cache.Close()
//
//   stream, err := cache.Get(ctx, "key", fetcher)
//   if err != nil {
//       return err
//   }
//   defer stream.Close()
//
// Thread safety: All methods are safe for concurrent use.
// Performance: Optimized for high-throughput scenarios with minimal memory allocation.
type Cache interface {
    // Get retrieves an object as a stream from the cache or origin.
    //
    // The method first checks the hot tier, then cold tier, and finally
    // fetches from the origin using the provided fetcher. Stale data is
    // served immediately while triggering background refresh.
    //
    // Parameters:
    //   - ctx: Context for cancellation and timeout control
    //   - key: Unique identifier for the cached object
    //   - fetcher: Interface for retrieving data from origin when not cached
    //
    // Returns:
    //   - io.ReadCloser: Stream of cached data (must be closed by caller)
    //   - error: ErrNotFound if object doesn't exist, ErrCacheClosed if cache is shut down
    //
    // Example:
    //   stream, err := cache.Get(ctx, "user:123", userFetcher)
    //   if err != nil {
    //       if errors.Is(err, daramjwee.ErrNotFound) {
    //           // Handle not found case
    //       }
    //       return err
    //   }
    //   defer stream.Close()
    //   data, err := io.ReadAll(stream)
    //
    // Note: The returned stream must always be closed to prevent resource leaks.
    Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error)
    
    // ... (similar detailed documentation for other methods)
}
```

#### Store Interface Enhancement
```go
// Store defines the interface for a single cache storage tier.
//
// Store implementations provide persistent or temporary storage for cached objects
// with associated metadata. Each store must handle concurrent access safely and
// provide streaming operations to minimize memory usage for large objects.
//
// Implementations should be optimized for their specific storage medium:
//   - MemStore: Fast access, limited by available memory
//   - FileStore: Persistent storage with atomic write guarantees
//   - Cloud stores: Network-based with retry and error handling
//
// Thread safety: All implementations must be safe for concurrent use.
// Error handling: Should return ErrNotFound for missing objects.
type Store interface {
    // GetStream retrieves an object and its metadata as a stream.
    //
    // Parameters:
    //   - ctx: Context for cancellation and timeout
    //   - key: Object identifier
    //
    // Returns:
    //   - io.ReadCloser: Object data stream (must be closed)
    //   - *Metadata: Object metadata including ETag and cache time
    //   - error: ErrNotFound if object doesn't exist
    GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
    
    // ... (detailed documentation for other methods)
}
```

### 2. Configuration Documentation

#### Option Functions Documentation
```go
// WithHotStore configures the hot tier storage backend.
//
// The hot tier provides fast access to frequently used objects and is typically
// implemented using memory-based storage. This option is mandatory - the cache
// cannot function without a hot store.
//
// Parameters:
//   - store: Store implementation for hot tier (cannot be nil)
//
// Example:
//   memStore := memstore.New(100*1024*1024, policy.NewLRUPolicy()) // 100MB
//   cache, err := daramjwee.New(logger, daramjwee.WithHotStore(memStore))
//
// Performance: Hot store access patterns should prioritize low latency over capacity.
func WithHotStore(store Store) Option
```

#### Config Struct Documentation
```go
// Config holds all configurable settings for the daramjwee cache.
//
// Config is populated by Option functions and validated during cache creation.
// Default values are provided for most settings to enable quick setup, but
// production deployments should tune parameters based on workload characteristics.
//
// Example configuration for high-throughput scenario:
//   cache, err := daramjwee.New(logger,
//       daramjwee.WithHotStore(memStore),
//       daramjwee.WithColdStore(fileStore),
//       daramjwee.WithWorker("pool", 10, 1000, 30*time.Second),
//       daramjwee.WithCache(10*time.Minute),
//       daramjwee.WithBufferPool(true, 64*1024),
//   )
type Config struct {
    // HotStore provides fast access storage for frequently used objects.
    // This field is mandatory and must be set via WithHotStore option.
    HotStore Store
    
    // ColdStore provides high-capacity storage for less frequently accessed objects.
    // Optional - if not set, a null store is used (no cold tier).
    // Recommended for workloads with large datasets that don't fit in hot tier.
    ColdStore Store
    
    // DefaultTimeout sets the default timeout for cache operations.
    // Applied to Get, Set, and Delete operations when context has no deadline.
    // Default: 30 seconds. Recommended: 5-60 seconds based on storage latency.
    DefaultTimeout time.Duration
    
    // ... (detailed documentation for other fields)
}
```

### 3. Error Documentation Enhancement

#### Error Variables Documentation
```go
// ErrNotFound is returned when an object is not found in the cache or origin.
//
// This error indicates that the requested object does not exist in any cache tier
// and the fetcher was unable to retrieve it from the origin. Applications should
// handle this as a normal condition and may choose to return 404 responses or
// use default values.
//
// Example handling:
//   stream, err := cache.Get(ctx, key, fetcher)
//   if errors.Is(err, daramjwee.ErrNotFound) {
//       // Object doesn't exist - handle appropriately
//       http.NotFound(w, r)
//       return
//   }
var ErrNotFound = errors.New("daramjwee: object not found")

// ErrCacheClosed is returned when operations are attempted on a closed cache.
//
// This error occurs when cache methods are called after Close() has been invoked.
// It indicates that the cache is shutting down or has shut down and cannot process
// new requests. Applications should handle this gracefully during shutdown.
//
// Example handling:
//   stream, err := cache.Get(ctx, key, fetcher)
//   if errors.Is(err, daramjwee.ErrCacheClosed) {
//       // Cache is shutting down - return service unavailable
//       http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
//       return
//   }
var ErrCacheClosed = errors.New("daramjwee: cache is closed")
```

### 4. Storage Backend Documentation

#### MemStore Documentation
```go
// MemStore provides a thread-safe, in-memory implementation of the Store interface.
//
// MemStore is optimized for high-performance scenarios where data fits in memory.
// It supports configurable capacity limits with automatic eviction using pluggable
// policies (LRU, S3-FIFO, SIEVE). Memory usage is tracked precisely and eviction
// occurs when capacity is exceeded.
//
// Key features:
//   - Thread-safe concurrent access with striped locking
//   - Configurable eviction policies for different workload patterns
//   - Precise memory usage tracking and capacity enforcement
//   - Optimized buffer pooling to reduce GC pressure
//
// Performance characteristics:
//   - Get operations: O(1) average, O(log n) worst case
//   - Set operations: O(1) average, may trigger eviction
//   - Memory overhead: ~40 bytes per cached object plus data size
//
// Example usage:
//   // Create 100MB memory store with LRU eviction
//   store := memstore.New(100*1024*1024, policy.NewLRUPolicy())
//
//   // Create with custom locking strategy
//   store := memstore.New(100*1024*1024, policy.NewS3FIFOPolicy(),
//       memstore.WithLocker(lock.NewStripeLock(64)))
//
// Thread safety: All methods are safe for concurrent use.
// Memory usage: Tracks actual memory consumption including metadata overhead.
```

#### FileStore Documentation
```go
// FileStore provides a disk-based implementation of the Store interface.
//
// FileStore offers persistent storage with atomic write guarantees using a
// write-to-temp-then-rename strategy. It supports configurable capacity limits,
// eviction policies, and optional key hashing for better file system distribution.
//
// Key features:
//   - Atomic writes prevent data corruption during failures
//   - Optional copy-and-truncate mode for NFS compatibility
//   - Configurable key hashing with directory partitioning
//   - Precise disk usage tracking and capacity enforcement
//   - Striped locking for concurrent access optimization
//
// Storage format:
//   - Files contain metadata header followed by object data
//   - Metadata is JSON-encoded with length prefix
//   - Supports compression metadata and custom attributes
//
// Example usage:
//   // Basic file store with 1GB capacity
//   store, err := filestore.New("/var/cache/app", logger, 1024*1024*1024, 
//       policy.NewLRUPolicy())
//
//   // File store with hashed keys and NFS compatibility
//   store, err := filestore.New("/nfs/cache", logger, 1024*1024*1024,
//       policy.NewLRUPolicy(),
//       filestore.WithHashedKeys(2, 2),  // /ab/cd/abcdef...
//       filestore.WithCopyAndTruncate()) // NFS-safe writes
//
// Thread safety: All methods are safe for concurrent use.
// Durability: Atomic writes ensure data consistency across failures.
// Performance: Optimized for sequential access patterns with minimal seeks.
```

## Data Models

### Documentation Coverage Metrics

**Target Documentation Standards:**
- All public interfaces: 100% documented with examples
- All public types: 100% documented with usage guidance
- All public functions: 100% documented with parameter descriptions
- All configuration options: 100% documented with recommendations
- All error conditions: 100% documented with handling strategies

### Documentation Quality Criteria

**Completeness Checklist:**
- [ ] Purpose and behavior clearly explained
- [ ] Parameters and return values documented
- [ ] Usage examples provided
- [ ] Thread safety guarantees specified
- [ ] Performance characteristics described
- [ ] Error conditions and handling documented
- [ ] Integration patterns explained

## Error Handling

### Documentation Error Prevention

**Quality Assurance Process:**
1. Automated documentation coverage checking
2. Example code compilation verification
3. Documentation review checklist
4. Consistency validation across packages

**Documentation Standards Enforcement:**
- Linting rules for missing documentation
- Template compliance checking
- Example code testing
- Cross-reference validation

## Testing Strategy

### Documentation Verification

**Automated Testing:**
```bash
# Verify all public symbols are documented
go doc -all ./... | grep -c "^func\|^type\|^var\|^const"

# Test example code compilation
go test -run TestExamples ./...

# Check documentation formatting
golint -set_exit_status ./...
```

**Manual Review Process:**
1. Documentation completeness audit
2. Example code functionality verification
3. User experience testing with documentation
4. Technical accuracy validation

### Integration Testing

**Documentation Integration:**
- README examples match actual API
- Getting started guide works end-to-end
- Configuration examples are valid
- Error handling examples are accurate

## Implementation Phases

### Phase 1: Core Interface Documentation
- Complete Cache interface documentation
- Enhance Store interface documentation
- Document Fetcher and EvictionPolicy interfaces
- Add BufferPool interface documentation

### Phase 2: Configuration Documentation
- Document all Option functions
- Complete Config struct documentation
- Add configuration examples and recommendations
- Document tuning guidelines

### Phase 3: Error and Storage Documentation
- Complete error variable and type documentation
- Document storage backend implementations
- Add troubleshooting guides
- Document performance characteristics

### Phase 4: Extension Documentation
- Document custom implementation guidelines
- Add integration patterns and best practices
- Create comprehensive examples
- Document testing strategies for extensions

## Success Criteria

1. **100% documentation coverage** for all public APIs
2. **Comprehensive examples** for all major use cases
3. **Clear integration guidance** for custom implementations
4. **Consistent documentation style** across all packages
5. **Validated example code** that compiles and runs correctly
6. **Performance and thread safety information** for all components
7. **Complete error handling guidance** for all error conditions