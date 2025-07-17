package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
)

// ErrNotFound is returned when an object is not found in the cache or the origin.
//
// Occurrence scenarios and appropriate responses:
//   - Cache miss in all tiers and origin system doesn't have the resource
//   - Fetcher returns this error for non-existent resources that shouldn't be cached
//   - Temporary failures or transient errors from origin systems
//   - Authentication/authorization failures that shouldn't be cached
//
// This error indicates a definitive "not found" state that should not be cached.
// Unlike ErrCacheableNotFound, this error suggests the failure might be temporary
// or that repeated requests might succeed under different conditions.
//
// Appropriate handling strategies:
//   - Return 404 Not Found to clients for missing resources
//   - Implement retry logic for transient failures
//   - Log the error for monitoring and debugging
//   - Consider fallback mechanisms or default responses
//   - Do not cache this error state (unlike ErrCacheableNotFound)
//
// Example handling in HTTP handlers:
//
//	stream, err := cache.Get(ctx, key, fetcher)
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        http.Error(w, "Resource not found", http.StatusNotFound)
//	        return
//	    }
//	    // Handle other errors...
//	}
//
// Example handling with retry logic:
//
//	var stream io.ReadCloser
//	var err error
//	for attempt := 0; attempt < maxRetries; attempt++ {
//	    stream, err = cache.Get(ctx, key, fetcher)
//	    if err == nil {
//	        break
//	    }
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // Don't retry for definitive not found
//	        break
//	    }
//	    time.Sleep(retryDelay * time.Duration(attempt+1))
//	}
//
// Integration patterns: Applications should distinguish between ErrNotFound
// (temporary/non-cacheable) and ErrCacheableNotFound (permanent/cacheable)
// to implement appropriate caching and retry strategies.
var ErrNotFound = errors.New("daramjwee: object not found")

// ErrNotModified is returned by Fetchers when conditional requests indicate no changes.
//
// Conditional request patterns and cache behavior:
//   - Fetcher receives oldMetadata with ETag or timestamp information
//   - Origin system confirms data hasn't changed since last fetch
//   - HTTP 304 Not Modified responses from REST APIs
//   - Database queries with version/timestamp checks showing no updates
//   - File system checks showing unchanged modification times
//
// This error is a positive signal indicating that cached data is still valid
// and fresh. The cache will continue serving the existing cached data without
// updating it, which is the desired behavior for conditional requests.
//
// Cache behavior when this error is returned:
//   - Existing cached data remains valid and continues to be served
//   - Cache metadata may be updated (e.g., last-checked timestamp)
//   - No new data is written to cache tiers
//   - Background refresh operations complete successfully
//   - Cache hit statistics are updated appropriately
//
// Example Fetcher implementation with conditional requests:
//
//	func (f *HTTPFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
//	    req, err := http.NewRequestWithContext(ctx, "GET", f.URL, nil)
//	    if err != nil {
//	        return nil, err
//	    }
//
//	    // Add conditional request headers
//	    if oldMetadata != nil && oldMetadata.ETag != "" {
//	        req.Header.Set("If-None-Match", oldMetadata.ETag)
//	    }
//
//	    resp, err := f.client.Do(req)
//	    if err != nil {
//	        return nil, err
//	    }
//	    defer resp.Body.Close()
//
//	    if resp.StatusCode == http.StatusNotModified {
//	        return nil, daramjwee.ErrNotModified
//	    }
//
//	    // Handle other response codes...
//	}
//
// Example database Fetcher with version checking:
//
//	func (f *DBFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
//	    if oldMetadata != nil {
//	        // Check if data has changed since last fetch
//	        var currentVersion int64
//	        err := f.db.QueryRowContext(ctx,
//	            "SELECT version FROM table WHERE id = ?", f.id).Scan(&currentVersion)
//	        if err != nil {
//	            return nil, err
//	        }
//
//	        if oldMetadata.ETag == fmt.Sprintf("%d", currentVersion) {
//	            return nil, daramjwee.ErrNotModified
//	        }
//	    }
//
//	    // Fetch fresh data...
//	}
//
// Performance benefits: Conditional requests with ErrNotModified significantly
// reduce bandwidth usage, origin load, and cache storage operations while
// maintaining data freshness guarantees.
//
// Integration patterns: Applications don't typically handle this error directly
// as it's processed internally by the cache. However, monitoring systems should
// track the frequency of conditional request successes for performance insights.
var ErrNotModified = errors.New("daramjwee: resource not modified")

// ErrCacheableNotFound is returned when a resource is not found but this state should be cached.
//
// Negative caching implications and handling:
//   - Resource definitively doesn't exist and this state is stable
//   - HTTP 404 responses that should be cached to prevent repeated requests
//   - Database queries for deleted or non-existent records
//   - File system operations for permanently missing files
//   - API endpoints that return stable "not found" responses
//
// This error indicates a cacheable "not found" state that should be stored
// in the cache to prevent repeated expensive requests to the origin system.
// The cache will create a negative cache entry with the configured TTL.
//
// Negative cache behavior when this error is returned:
//   - A negative cache entry is created with IsNegative=true in metadata
//   - The entry is stored in both hot and cold tiers (if configured)
//   - Subsequent requests for the same key return this cached error
//   - The negative cache entry expires according to NegativeFreshFor TTL
//   - Background refresh may be triggered when the entry becomes stale
//
// Example Fetcher implementation for stable not found states:
//
//	func (f *APIFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
//	    resp, err := f.client.Get(f.buildURL())
//	    if err != nil {
//	        return nil, err
//	    }
//	    defer resp.Body.Close()
//
//	    switch resp.StatusCode {
//	    case http.StatusOK:
//	        return f.handleSuccess(resp)
//	    case http.StatusNotFound:
//	        // Cache this not found state
//	        return nil, daramjwee.ErrCacheableNotFound
//	    case http.StatusServiceUnavailable:
//	        // Don't cache temporary failures
//	        return nil, daramjwee.ErrNotFound
//	    default:
//	        return nil, fmt.Errorf("API error: %d", resp.StatusCode)
//	    }
//	}
//
// Example application handling:
//
//	stream, err := cache.Get(ctx, key, fetcher)
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrCacheableNotFound) {
//	        // This is a cached negative result
//	        http.Error(w, "Resource not found", http.StatusNotFound)
//	        return
//	    }
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // This is a fresh not found (not cached)
//	        http.Error(w, "Resource not found", http.StatusNotFound)
//	        return
//	    }
//	    // Handle other errors...
//	}
//
// Performance benefits of negative caching:
//   - Reduces origin system load from repeated requests for missing resources
//   - Improves response times for known missing resources
//   - Prevents cascading failures during origin system outages
//   - Reduces bandwidth usage for failed requests
//
// Configuration considerations:
//   - Set appropriate NegativeFreshFor TTL based on your use case
//   - Shorter TTLs for resources that might be created later
//   - Longer TTLs for permanently missing or deleted resources
//   - Monitor negative cache hit rates for optimization opportunities
//
// Use cases:
//   - User profiles for deleted accounts
//   - Product pages for discontinued items
//   - Static assets that have been permanently removed
//   - API endpoints for resources that don't exist
var ErrCacheableNotFound = errors.New("daramjwee: resource not found, but this state is cacheable")

// ErrCacheClosed is returned when operations are attempted on a closed cache.
//
// Shutdown scenarios and graceful degradation:
//   - Cache.Close() has been called and shutdown is in progress or complete
//   - Application is shutting down and cache operations are no longer accepted
//   - Background workers have been terminated and cannot process requests
//   - Storage backends have been closed and are no longer accessible
//
// This error indicates that the cache has been shut down and cannot process
// new requests. It's returned by all cache operations (Get, Set, Delete,
// ScheduleRefresh) when called after Close() has been invoked.
//
// Graceful degradation strategies:
//   - Fail fast to prevent hanging operations during shutdown
//   - Return cached responses from application-level fallback caches
//   - Serve static content or default responses
//   - Redirect requests to other cache instances in distributed setups
//   - Log the error for monitoring shutdown behavior
//
// Example handling in HTTP handlers:
//
//	stream, err := cache.Get(ctx, key, fetcher)
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrCacheClosed) {
//	        // Cache is shut down, serve fallback response
//	        http.Error(w, "Service temporarily unavailable", http.StatusServiceUnavailable)
//	        return
//	    }
//	    // Handle other errors...
//	}
//
// Example with fallback cache:
//
//	stream, err := primaryCache.Get(ctx, key, fetcher)
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrCacheClosed) {
//	        // Try fallback cache or direct origin
//	        if fallbackCache != nil {
//	            stream, err = fallbackCache.Get(ctx, key, fetcher)
//	        } else {
//	            // Direct origin fetch as last resort
//	            result, err := fetcher.Fetch(ctx, nil)
//	            if err == nil {
//	                stream = result.Body
//	            }
//	        }
//	    }
//	}
//
// Monitoring and alerting considerations:
//   - Track frequency of ErrCacheClosed errors
//   - Alert on unexpected cache shutdowns
//   - Monitor application behavior during planned shutdowns
//   - Validate graceful degradation mechanisms
//
// Prevention strategies:
//   - Implement proper application lifecycle management
//   - Use health checks to detect cache availability
//   - Coordinate cache shutdown with application shutdown
//   - Consider cache warm-up strategies after restarts
//
// Integration patterns: Applications should handle this error gracefully
// by implementing fallback mechanisms and avoiding cascading failures
// during cache shutdown scenarios.
var ErrCacheClosed = errors.New("daramjwee: cache is closed")

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
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithHotStore(memStore),
//	    daramjwee.WithColdStore(fileStore),
//	    daramjwee.WithCache(5*time.Minute),
//	)
//	if err != nil {
//	    return err
//	}
//	defer cache.Close()
//
//	stream, err := cache.Get(ctx, "key", fetcher)
//	if err != nil {
//	    return err
//	}
//	defer stream.Close()
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
	//
	//	stream, err := cache.Get(ctx, "user:123", userFetcher)
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrNotFound) {
	//	        // Handle not found case
	//	    }
	//	    return err
	//	}
	//	defer stream.Close()
	//	data, err := io.ReadAll(stream)
	//
	// Note: The returned stream must always be closed to prevent resource leaks.
	Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error)

	// Set provides a writer to stream an object into the cache.
	//
	// The cache entry is finalized when the returned writer is closed.
	// This pattern is ideal for use with io.MultiWriter for simultaneous
	// response-to-client and writing-to-cache scenarios. The data is written
	// to both hot and cold tiers according to the configured strategy.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - key: Unique identifier for the cached object
	//   - metadata: Object metadata including ETag and compression info
	//
	// Returns:
	//   - io.WriteCloser: Writer for streaming data (must be closed by caller)
	//   - error: Configuration or storage errors
	//
	// Example:
	//
	//	writer, err := cache.Set(ctx, "user:123", &daramjwee.Metadata{
	//	    ETag: "abc123",
	//	    CachedAt: time.Now(),
	//	})
	//	if err != nil {
	//	    return err
	//	}
	//	defer writer.Close()
	//
	//	// Stream data directly from source
	//	_, err = io.Copy(writer, dataSource)
	//
	// Resource management: The caller is responsible for calling Close() on the returned
	// io.WriteCloser to ensure the cache entry is committed and resources are released.
	// Failure to close may result in incomplete cache entries and resource leaks.
	Set(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error)

	// Delete removes an object from both hot and cold cache tiers.
	//
	// The operation is performed atomically across both tiers. If deletion
	// fails in one tier, the operation continues with the other tier to
	// maintain consistency. Partial failures are logged but do not cause
	// the operation to fail.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - key: Unique identifier for the cached object to remove
	//
	// Returns:
	//   - error: Storage errors or ErrCacheClosed if cache is shut down
	//
	// Example:
	//
	//	err := cache.Delete(ctx, "user:123")
	//	if err != nil {
	//	    log.Printf("Failed to delete cache entry: %v", err)
	//	}
	//
	// Multi-tier behavior: Deletion is attempted on both hot and cold tiers.
	// The operation succeeds if at least one tier deletion succeeds or if
	// the object was not present in either tier.
	Delete(ctx context.Context, key string) error

	// ScheduleRefresh asynchronously refreshes a cache entry using the provided Fetcher.
	//
	// This method schedules a background refresh operation without blocking
	// the caller. It's typically used for stale-while-revalidate patterns
	// where fresh data is fetched in the background while serving stale data.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - key: Unique identifier for the cached object to refresh
	//   - fetcher: Interface for retrieving fresh data from origin
	//
	// Returns:
	//   - error: Worker queue errors or ErrCacheClosed if cache is shut down
	//
	// Example:
	//
	//	// Serve stale data immediately, refresh in background
	//	stream, err := cache.Get(ctx, "user:123", fetcher)
	//	if err == nil {
	//	    // Schedule background refresh for next request
	//	    cache.ScheduleRefresh(context.Background(), "user:123", fetcher)
	//	}
	//
	// Background refresh: The refresh operation runs asynchronously in a worker
	// pool. If the worker pool is full, the refresh may be queued or dropped
	// depending on the configured worker strategy.
	//
	// Use cases: Ideal for high-traffic scenarios where serving slightly stale
	// data is acceptable while ensuring eventual consistency.
	ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error

	// Close gracefully shuts down the cache and its background workers.
	//
	// This method stops accepting new operations, waits for in-flight
	// operations to complete (up to ShutdownTimeout), and releases all
	// resources including worker pools and storage connections.
	//
	// After Close() is called, all cache operations will return ErrCacheClosed.
	// The method blocks until shutdown is complete or the shutdown timeout
	// is reached.
	//
	// Example:
	//
	//	// Graceful shutdown
	//	cache.Close()
	//
	//	// Or with explicit timeout handling
	//	done := make(chan struct{})
	//	go func() {
	//	    cache.Close()
	//	    close(done)
	//	}()
	//
	//	select {
	//	case <-done:
	//	    log.Println("Cache shutdown completed")
	//	case <-time.After(60 * time.Second):
	//	    log.Println("Cache shutdown timed out")
	//	}
	//
	// Graceful shutdown: The method waits for background workers to complete
	// their current tasks before shutting down. Long-running operations may
	// be interrupted if they exceed the configured ShutdownTimeout.
	//
	// Resource cleanup: All storage connections, worker pools, and buffer pools
	// are properly cleaned up during shutdown to prevent resource leaks.
	Close()
}

// Metadata holds essential metadata about a cached item.
// It is designed to be extensible for future needs (e.g., LastModified, Size).
type Metadata struct {
	ETag        string               `json:"etag,omitempty"`
	IsNegative  bool                 `json:"is_negative,omitempty"`
	CachedAt    time.Time            `json:"cached_at"`
	Compression *CompressionMetadata `json:"compression,omitempty"` // Compression-related metadata
}

// FetchResult holds the data and metadata returned from a successful fetch operation.
type FetchResult struct {
	Body     io.ReadCloser
	Metadata *Metadata
}

// Fetcher defines the contract for fetching an object from an origin.
//
// Fetcher implementations handle retrieval of data from origin sources when
// cache misses occur or when refresh operations are needed. The interface
// supports conditional requests using ETag-based optimization to minimize
// unnecessary data transfer.
//
// Key responsibilities:
//   - Fetch data from origin sources (HTTP APIs, databases, files, etc.)
//   - Handle conditional requests using ETag and metadata
//   - Return appropriate errors for different failure scenarios
//   - Support negative caching for "not found" states
//   - Manage origin-specific authentication and configuration
//
// Error handling patterns:
//   - Return ErrNotModified when data hasn't changed (conditional requests)
//   - Return ErrCacheableNotFound for cacheable "not found" states
//   - Return ErrNotFound for non-cacheable "not found" states
//   - Return origin-specific errors for other failure conditions
//
// Example HTTP fetcher implementation:
//
//	type HTTPFetcher struct {
//	    URL    string
//	    Client *http.Client
//	}
//
//	func (f *HTTPFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
//	    req, err := http.NewRequestWithContext(ctx, "GET", f.URL, nil)
//	    if err != nil {
//	        return nil, err
//	    }
//
//	    // Add conditional request headers
//	    if oldMetadata != nil && oldMetadata.ETag != "" {
//	        req.Header.Set("If-None-Match", oldMetadata.ETag)
//	    }
//
//	    resp, err := f.Client.Do(req)
//	    if err != nil {
//	        return nil, err
//	    }
//
//	    switch resp.StatusCode {
//	    case http.StatusNotModified:
//	        resp.Body.Close()
//	        return nil, daramjwee.ErrNotModified
//	    case http.StatusNotFound:
//	        resp.Body.Close()
//	        return nil, daramjwee.ErrCacheableNotFound
//	    case http.StatusOK:
//	        return &daramjwee.FetchResult{
//	            Body: resp.Body,
//	            Metadata: &daramjwee.Metadata{
//	                ETag:     resp.Header.Get("ETag"),
//	                CachedAt: time.Now(),
//	            },
//	        }, nil
//	    default:
//	        resp.Body.Close()
//	        return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
//	    }
//	}
//
// Thread safety: Implementations should be safe for concurrent use.
// Resource management: Returned FetchResult.Body must be closed by the caller.
// Integration patterns: Support various origin types through interface composition.
type Fetcher interface {
	// Fetch retrieves data from the origin source.
	//
	// This method handles the actual data retrieval from origin sources,
	// supporting conditional requests to optimize bandwidth usage. When
	// oldMetadata is provided, implementations should use it for conditional
	// requests (e.g., If-None-Match headers with ETag values).
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - oldMetadata: Previously cached metadata for conditional requests (may be nil)
	//
	// Returns:
	//   - *FetchResult: Contains data stream and fresh metadata
	//   - error: Various error types depending on the fetch outcome
	//
	// Error return patterns:
	//
	// ErrNotModified: Returned when conditional request indicates data hasn't changed.
	// The origin confirms that cached data is still valid.
	//
	//	if oldMetadata != nil && oldMetadata.ETag == currentETag {
	//	    return nil, daramjwee.ErrNotModified
	//	}
	//
	// ErrCacheableNotFound: Returned when resource doesn't exist but this state
	// should be cached to prevent repeated requests.
	//
	//	if resp.StatusCode == http.StatusNotFound {
	//	    return nil, daramjwee.ErrCacheableNotFound
	//	}
	//
	// ErrNotFound: Returned when resource doesn't exist and this state should
	// not be cached (e.g., temporary failures).
	//
	//	if isTemporaryFailure(err) {
	//	    return nil, daramjwee.ErrNotFound
	//	}
	//
	// Origin-specific errors: Return wrapped errors for other failure conditions.
	//
	//	if err != nil {
	//	    return nil, fmt.Errorf("database query failed: %w", err)
	//	}
	//
	// Example usage patterns:
	//
	// HTTP API fetcher with conditional requests:
	//
	//	func (f *APIFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	//	    req := f.buildRequest(ctx)
	//
	//	    // Add conditional headers
	//	    if oldMetadata != nil && oldMetadata.ETag != "" {
	//	        req.Header.Set("If-None-Match", oldMetadata.ETag)
	//	    }
	//
	//	    resp, err := f.client.Do(req)
	//	    if err != nil {
	//	        return nil, fmt.Errorf("API request failed: %w", err)
	//	    }
	//
	//	    return f.handleResponse(resp)
	//	}
	//
	// Database fetcher with change detection:
	//
	//	func (f *DBFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	//	    // Check if data has changed using timestamp or version
	//	    if oldMetadata != nil {
	//	        changed, err := f.hasChanged(ctx, f.query, oldMetadata.CachedAt)
	//	        if err != nil {
	//	            return nil, err
	//	        }
	//	        if !changed {
	//	            return nil, daramjwee.ErrNotModified
	//	        }
	//	    }
	//
	//	    // Fetch fresh data
	//	    data, err := f.queryDatabase(ctx)
	//	    if err != nil {
	//	        if errors.Is(err, sql.ErrNoRows) {
	//	            return nil, daramjwee.ErrCacheableNotFound
	//	        }
	//	        return nil, fmt.Errorf("database error: %w", err)
	//	    }
	//
	//	    return &FetchResult{
	//	        Body: io.NopCloser(bytes.NewReader(data)),
	//	        Metadata: &Metadata{
	//	            ETag:     f.generateETag(data),
	//	            CachedAt: time.Now(),
	//	        },
	//	    }, nil
	//	}
	//
	// Conditional request patterns: When oldMetadata is provided, implementations
	// should attempt to validate whether the cached data is still current before
	// fetching new data. This optimization reduces bandwidth and origin load.
	//
	// ETag handling: ETags should be used consistently for conditional requests.
	// Generate stable ETags that change only when data actually changes.
	//
	// Metadata handling: Always populate fresh metadata in successful responses,
	// including accurate CachedAt timestamps and relevant ETags.
	//
	// Integration patterns: Fetchers can be composed or wrapped to support
	// different origin types, authentication methods, and retry strategies.
	Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error)
}

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
// All Store implementations must provide:
//   - Thread-safe concurrent access to all methods
//   - Streaming I/O to handle large objects efficiently
//   - Proper error handling with standard error types
//   - Metadata preservation across store/retrieve operations
//
// Example custom store implementation:
//
//	type MyStore struct {
//	    // implementation fields
//	}
//
//	func (s *MyStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
//	    // Check if object exists
//	    if !s.exists(key) {
//	        return nil, nil, daramjwee.ErrNotFound
//	    }
//	    // Return stream and metadata
//	    return s.openStream(key), s.getMetadata(key), nil
//	}
//
// Thread safety: All implementations must be safe for concurrent use.
// Error handling: Should return ErrNotFound for missing objects.
// Performance: Optimized for streaming operations and minimal memory allocation.
type Store interface {
	// GetStream retrieves an object and its metadata as a stream.
	//
	// This method provides streaming access to cached objects, allowing
	// efficient handling of large data without loading everything into memory.
	// The metadata contains essential information like ETag, cache time, and
	// compression details.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - key: Unique identifier for the cached object
	//
	// Returns:
	//   - io.ReadCloser: Stream of object data (must be closed by caller)
	//   - *Metadata: Object metadata including ETag and cache timestamp
	//   - error: ErrNotFound if object doesn't exist, storage-specific errors
	//
	// Example:
	//
	//	stream, metadata, err := store.GetStream(ctx, "user:123")
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrNotFound) {
	//	        // Object not in this store tier
	//	        return nil, nil, err
	//	    }
	//	    return nil, nil, fmt.Errorf("storage error: %w", err)
	//	}
	//	defer stream.Close()
	//
	//	// Process metadata
	//	if metadata.IsNegative {
	//	    // Handle negative cache entry
	//	}
	//
	// Streaming patterns: The returned stream should be consumed promptly
	// and always closed to prevent resource leaks. For small objects,
	// consider using io.ReadAll() for convenience.
	//
	// Metadata handling: The metadata must accurately reflect the stored
	// object's state, including compression status and cache timestamps.
	GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)

	// SetWithWriter returns a writer that streams data into the store.
	//
	// This method provides streaming write access for storing objects,
	// enabling efficient handling of large data and simultaneous processing.
	// The write operation is typically atomic - the object becomes available
	// only after the writer is successfully closed.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - key: Unique identifier for the cached object
	//   - metadata: Object metadata to store alongside the data
	//
	// Returns:
	//   - io.WriteCloser: Writer for streaming data (must be closed by caller)
	//   - error: Storage configuration or initialization errors
	//
	// Example:
	//
	//	writer, err := store.SetWithWriter(ctx, "user:123", &daramjwee.Metadata{
	//	    ETag: "abc123",
	//	    CachedAt: time.Now(),
	//	})
	//	if err != nil {
	//	    return fmt.Errorf("failed to create writer: %w", err)
	//	}
	//	defer writer.Close()
	//
	//	// Stream data from source
	//	_, err = io.Copy(writer, dataSource)
	//	if err != nil {
	//	    return fmt.Errorf("failed to write data: %w", err)
	//	}
	//
	// Atomic write guarantees: Implementations should ensure that the object
	// is not visible to readers until the writer is successfully closed.
	// This prevents partial reads during write operations.
	//
	// Error handling: If writing fails, implementations should clean up
	// any partial data to maintain store consistency. The caller must
	// always close the writer, even on error, to ensure proper cleanup.
	//
	// Resource management: The caller is responsible for closing the writer.
	// Failure to close may result in incomplete writes and resource leaks.
	SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error)

	// Delete removes an object from the store.
	//
	// This method removes both the object data and its associated metadata
	// from the store. The operation should be atomic where possible to
	// prevent inconsistent state.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - key: Unique identifier for the cached object to remove
	//
	// Returns:
	//   - error: Storage errors, or nil if object was successfully removed or didn't exist
	//
	// Example:
	//
	//	err := store.Delete(ctx, "user:123")
	//	if err != nil {
	//	    log.Printf("Failed to delete from store: %v", err)
	//	    // Continue with other operations - deletion failure shouldn't be fatal
	//	}
	//
	// Consistency requirements: Implementations should ensure that after
	// successful deletion, subsequent GetStream and Stat calls return
	// ErrNotFound for the deleted key.
	//
	// Error conditions: Implementations may return storage-specific errors
	// for I/O failures, but should not return errors for non-existent objects.
	// Missing objects should be treated as successful deletion.
	//
	// Idempotent operation: Multiple delete calls for the same key should
	// not cause errors - the operation should be idempotent.
	Delete(ctx context.Context, key string) error

	// Stat retrieves metadata for an object without its data.
	//
	// This method provides efficient access to object metadata without
	// the overhead of streaming the actual data. It's useful for cache
	// validation, freshness checks, and conditional requests.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control
	//   - key: Unique identifier for the cached object
	//
	// Returns:
	//   - *Metadata: Object metadata including ETag and cache timestamp
	//   - error: ErrNotFound if object doesn't exist, storage-specific errors
	//
	// Example:
	//
	//	metadata, err := store.Stat(ctx, "user:123")
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrNotFound) {
	//	        // Object not in store
	//	        return nil, err
	//	    }
	//	    return nil, fmt.Errorf("stat failed: %w", err)
	//	}
	//
	//	// Check freshness
	//	if time.Since(metadata.CachedAt) > maxAge {
	//	    // Object is stale, trigger refresh
	//	}
	//
	//	// Check ETag for conditional requests
	//	if metadata.ETag == clientETag {
	//	    // Data hasn't changed
	//	}
	//
	// Metadata-only access patterns: This method should be significantly
	// faster than GetStream for checking object existence and freshness.
	// Implementations should optimize this path for minimal I/O.
	//
	// Performance implications: Stat operations should be lightweight and
	// avoid loading object data. For file-based stores, this might involve
	// reading only file headers or separate metadata files.
	//
	// Consistency: The returned metadata must be consistent with what
	// GetStream would return for the same key at the same point in time.
	Stat(ctx context.Context, key string) (*Metadata, error)
}

// EvictionPolicy defines the contract for a cache eviction strategy.
//
// EvictionPolicy implementations manage which cached objects should be removed
// when storage capacity is exceeded. Different algorithms provide different
// trade-offs between hit rates, computational overhead, and memory usage.
//
// Available algorithms:
//   - LRU (Least Recently Used): Classic algorithm, good general performance
//   - S3-FIFO: Modern algorithm optimized for real-world access patterns
//   - SIEVE: Low-overhead algorithm with competitive hit rates
//
// All implementations must handle concurrent access safely and maintain
// consistent internal state across all operations. The policy tracks access
// patterns and object sizes to make informed eviction decisions.
//
// Example custom policy implementation:
//
//	type CustomPolicy struct {
//	    mu       sync.RWMutex
//	    items    map[string]*Item
//	    maxSize  int64
//	    currSize int64
//	}
//
//	func (p *CustomPolicy) Touch(key string) {
//	    p.mu.Lock()
//	    defer p.mu.Unlock()
//
//	    if item, exists := p.items[key]; exists {
//	        item.lastAccess = time.Now()
//	        // Update access tracking data structures
//	    }
//	}
//
//	func (p *CustomPolicy) Add(key string, size int64) {
//	    p.mu.Lock()
//	    defer p.mu.Unlock()
//
//	    p.items[key] = &Item{
//	        key:        key,
//	        size:       size,
//	        addedAt:    time.Now(),
//	        lastAccess: time.Now(),
//	    }
//	    p.currSize += size
//	}
//
//	func (p *CustomPolicy) Evict() []string {
//	    p.mu.Lock()
//	    defer p.mu.Unlock()
//
//	    if p.currSize <= p.maxSize {
//	        return nil
//	    }
//
//	    // Implement eviction algorithm
//	    return p.selectVictims()
//	}
//
// Thread safety: All implementations must be safe for concurrent use across
// all methods. Use appropriate synchronization primitives (mutexes, atomic
// operations) to protect internal state.
//
// Performance considerations: Eviction policies are called frequently and
// should be optimized for low latency. Consider using efficient data structures
// and minimizing lock contention.
//
// State management: Policies must maintain accurate state about tracked objects,
// including size information and access patterns, to make optimal eviction decisions.
type EvictionPolicy interface {
	// Touch records an access to an existing cached object.
	//
	// This method is called whenever a cached object is accessed (read),
	// allowing the policy to update its access tracking data structures.
	// The frequency and recency of access are key factors in most eviction
	// algorithms.
	//
	// Parameters:
	//   - key: Unique identifier of the accessed object
	//
	// Example usage in cache implementation:
	//
	//	func (c *Cache) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	//	    // ... retrieve object from store ...
	//
	//	    // Record access for eviction policy
	//	    c.policy.Touch(key)
	//
	//	    return stream, nil
	//	}
	//
	// Access pattern tracking requirements:
	//   - Update access timestamps or counters
	//   - Adjust position in access-ordered data structures
	//   - Handle non-existent keys gracefully (no-op)
	//   - Maintain thread safety during concurrent access
	//
	// Performance implications: This method is called on every cache hit,
	// so it must be highly optimized. Consider using lock-free data structures
	// or fine-grained locking to minimize contention.
	//
	// Thread safety: Must be safe for concurrent calls with other policy methods.
	// Multiple threads may call Touch simultaneously for different keys.
	Touch(key string)

	// Add records the addition of a new object to the cache.
	//
	// This method is called when a new object is stored in the cache,
	// providing the policy with size information needed for capacity
	// management and eviction decisions.
	//
	// Parameters:
	//   - key: Unique identifier of the new object
	//   - size: Size of the object in bytes
	//
	// Example usage in cache implementation:
	//
	//	func (c *Cache) Set(ctx context.Context, key string, data []byte) error {
	//	    // ... store object in cache ...
	//
	//	    // Record addition for eviction policy
	//	    c.policy.Add(key, int64(len(data)))
	//
	//	    // Check if eviction is needed
	//	    if victims := c.policy.Evict(); len(victims) > 0 {
	//	        c.evictObjects(victims)
	//	    }
	//
	//	    return nil
	//	}
	//
	// Size tracking and policy state management:
	//   - Maintain accurate total size accounting
	//   - Initialize access tracking for the new object
	//   - Update internal data structures (lists, heaps, etc.)
	//   - Handle duplicate keys appropriately (update vs. error)
	//
	// Policy state management: The policy should track the object's metadata
	// including size, addition time, and initial access information. This data
	// is crucial for making informed eviction decisions.
	//
	// Thread safety: Must be safe for concurrent calls with other policy methods.
	// Coordinate with Touch and Evict operations to maintain consistent state.
	Add(key string, size int64)

	// Remove records the explicit deletion of an object from the cache.
	//
	// This method is called when an object is explicitly deleted (not evicted),
	// allowing the policy to clean up its tracking data structures and update
	// size accounting.
	//
	// Parameters:
	//   - key: Unique identifier of the deleted object
	//
	// Example usage in cache implementation:
	//
	//	func (c *Cache) Delete(ctx context.Context, key string) error {
	//	    // ... remove object from store ...
	//
	//	    // Update eviction policy state
	//	    c.policy.Remove(key)
	//
	//	    return nil
	//	}
	//
	// Explicit removal handling:
	//   - Clean up access tracking data structures
	//   - Update total size accounting
	//   - Remove from internal collections (lists, maps, etc.)
	//   - Handle non-existent keys gracefully (no-op)
	//
	// State cleanup: The policy must properly clean up all internal state
	// associated with the removed object to prevent memory leaks and maintain
	// accurate accounting.
	//
	// Thread safety: Must be safe for concurrent calls with other policy methods.
	// Coordinate with Add, Touch, and Evict operations.
	//
	// Idempotent operation: Multiple Remove calls for the same key should not
	// cause errors or inconsistent state.
	Remove(key string)

	// Evict determines which objects should be removed to free space.
	//
	// This method implements the core eviction algorithm, analyzing tracked
	// objects and returning keys that should be removed from the cache.
	// It's typically called after Add operations when capacity limits are
	// approached or exceeded.
	//
	// Returns:
	//   - []string: List of keys to be evicted (may be empty)
	//
	// Example usage in cache implementation:
	//
	//	func (c *Cache) checkEviction() {
	//	    victims := c.policy.Evict()
	//	    for _, key := range victims {
	//	        if err := c.store.Delete(ctx, key); err != nil {
	//	            log.Printf("Failed to evict %s: %v", key, err)
	//	        } else {
	//	            // Don't call policy.Remove() here - eviction is different from explicit deletion
	//	            c.updateEvictionStats(key)
	//	        }
	//	    }
	//	}
	//
	// Eviction decision algorithms:
	//
	// LRU (Least Recently Used):
	//   - Evict objects that haven't been accessed recently
	//   - Maintain access-ordered list or timestamps
	//   - Good general-purpose algorithm
	//
	// S3-FIFO (Static-Dynamic-FIFO):
	//   - Separate static and dynamic objects
	//   - Use FIFO for static objects, more complex logic for dynamic
	//   - Optimized for real-world access patterns
	//
	// SIEVE:
	//   - Low-overhead algorithm using visit bits
	//   - Competitive hit rates with minimal metadata
	//   - Good for memory-constrained environments
	//
	// Return value handling: The returned slice should contain keys in the
	// order they should be evicted. Empty slice indicates no eviction needed.
	// The policy should update its internal state to reflect the eviction
	// decisions.
	//
	// Thread safety: Must be safe for concurrent calls with other policy methods.
	// The eviction decision process should be atomic and consistent.
	//
	// Performance considerations: This method may be called frequently and
	// should be optimized for low latency. Consider caching eviction candidates
	// or using efficient selection algorithms.
	Evict() []string
}

// Locker defines the interface for a locking mechanism.
type Locker interface {
	Lock(key string)
	Unlock(key string)
	RLock(key string)
	RUnlock(key string)
}

// Compressor defines the interface for compression/decompression functionality
type Compressor interface {
	// Compress compresses the input stream and writes to the output stream
	// Returns the number of compressed bytes and any error
	Compress(dst io.Writer, src io.Reader) (int64, error)

	// Decompress decompresses the input stream and writes to the output stream
	// Returns the number of decompressed bytes and any error
	Decompress(dst io.Writer, src io.Reader) (int64, error)

	// Algorithm returns the compression algorithm name
	Algorithm() string

	// Level returns the compression level
	Level() int
}

// BufferPool defines the interface for buffer pool management.
//
// BufferPool provides optimized buffer allocation and reuse for stream operations,
// significantly reducing garbage collection pressure and memory allocation overhead
// in high-throughput scenarios. The pool manages buffers of various sizes and
// provides specialized operations for common I/O patterns.
//
// Key performance benefits:
//   - Reduced GC pressure through buffer reuse
//   - Lower memory allocation overhead
//   - Optimized I/O operations with pooled buffers
//   - Configurable buffer size limits to prevent memory bloat
//   - Detailed statistics for performance monitoring
//
// Buffer lifecycle management:
//  1. Get() retrieves a buffer from the pool (or allocates new)
//  2. Use the buffer for I/O operations
//  3. Put() returns the buffer to the pool for reuse
//  4. Pool manages buffer retention based on size limits
//
// Example usage patterns:
//
//	// Basic buffer pooling
//	buf := pool.Get(32 * 1024)  // Request 32KB buffer
//	defer pool.Put(buf)         // Always return to pool
//
//	// Use buffer for I/O
//	n, err := src.Read(buf)
//	if err != nil {
//	    return err
//	}
//	_, err = dst.Write(buf[:n])
//
//	// Optimized copy operation
//	copied, err := pool.CopyBuffer(dst, src)
//
//	// Optimized tee operation
//	teeReader := pool.TeeReader(src, dst)
//	data, err := io.ReadAll(teeReader)
//
// Performance implications:
//   - Buffer reuse reduces allocation overhead by 60-80%
//   - GC pressure reduction improves overall application performance
//   - Larger buffers improve I/O throughput but use more memory
//   - Pool contention may occur under extreme concurrency
//
// Thread safety: All implementations must be safe for concurrent use.
// Memory management: Proper buffer lifecycle management prevents memory leaks.
// Monitoring: Statistics provide insights into pool effectiveness and tuning needs.
type BufferPool interface {
	// Get retrieves a buffer from the pool with at least the specified size.
	//
	// This method returns a buffer that is at least as large as requested,
	// but may be larger to optimize reuse patterns. The buffer is ready for
	// immediate use and should be returned to the pool when no longer needed.
	//
	// Parameters:
	//   - size: Minimum required buffer size in bytes
	//
	// Returns:
	//   - []byte: Buffer with capacity >= size (never smaller than requested)
	//
	// Example usage:
	//
	//	// Request buffer for reading data
	//	buf := pool.Get(64 * 1024)  // Request 64KB buffer
	//	defer pool.Put(buf)         // Always return to pool
	//
	//	// Buffer may be larger than requested
	//	fmt.Printf("Requested: %d, Got: %d\n", 64*1024, cap(buf))
	//
	//	// Use buffer for I/O operations
	//	n, err := reader.Read(buf)
	//	if err != nil {
	//	    return err
	//	}
	//
	//	// Process data in buf[:n]
	//	processData(buf[:n])
	//
	// Buffer sizing and reuse patterns:
	//   - Pool may return larger buffers to improve reuse efficiency
	//   - Common sizes (powers of 2) are typically pooled more effectively
	//   - Very large requests may bypass the pool and allocate directly
	//   - Very small requests may be rounded up to minimum pool size
	//
	// Performance benefits: Reusing buffers eliminates allocation overhead
	// and reduces GC pressure, especially important for high-frequency operations.
	//
	// Memory efficiency: The pool balances buffer reuse with memory consumption
	// by limiting the maximum size of pooled buffers.
	Get(size int) []byte

	// Put returns a buffer to the pool for reuse.
	//
	// This method returns a previously obtained buffer to the pool, making it
	// available for future Get() calls. The buffer must not be used after
	// calling Put() as it may be modified by other operations.
	//
	// Parameters:
	//   - buf: Buffer to return to the pool (must not be used after Put)
	//
	// Example usage:
	//
	//	buf := pool.Get(32 * 1024)
	//	defer pool.Put(buf)  // Ensure buffer is always returned
	//
	//	// Use buffer for operations
	//	n, err := src.Read(buf)
	//	if err != nil {
	//	    return err  // defer ensures Put() is called
	//	}
	//
	//	// Process data
	//	result := processData(buf[:n])
	//
	//	// Buffer is automatically returned via defer
	//	return result
	//
	// Buffer return requirements:
	//   - Always call Put() for buffers obtained from Get()
	//   - Use defer to ensure Put() is called even on error paths
	//   - Do not use the buffer after calling Put()
	//   - Do not call Put() multiple times for the same buffer
	//
	// Pool behavior: The pool may choose not to retain buffers that are too
	// large or too small based on its configuration. This prevents memory
	// bloat while maintaining performance benefits.
	//
	// Validation: Implementations may validate buffer sizes and reject buffers
	// that are outside the configured size range.
	Put(buf []byte)

	// CopyBuffer performs optimized copy using pooled buffers.
	//
	// This method provides an optimized version of io.Copy that uses pooled
	// buffers instead of allocating temporary buffers for each operation.
	// It's particularly beneficial for high-frequency copy operations.
	//
	// Parameters:
	//   - dst: Destination writer
	//   - src: Source reader
	//
	// Returns:
	//   - int64: Number of bytes copied
	//   - error: I/O errors from read or write operations
	//
	// Example usage:
	//
	//	// Optimized copy with pooled buffers
	//	copied, err := pool.CopyBuffer(dst, src)
	//	if err != nil {
	//	    return fmt.Errorf("copy failed: %w", err)
	//	}
	//
	//	log.Printf("Copied %d bytes", copied)
	//
	// Performance benefits compared to io.Copy:
	//   - Eliminates buffer allocation overhead
	//   - Reduces GC pressure from temporary buffers
	//   - Uses optimally sized buffers for better I/O performance
	//   - Automatic buffer management (no manual Get/Put required)
	//
	// Fallback behavior: If buffer pooling is disabled, this method should
	// fall back to standard io.Copy behavior to maintain compatibility.
	//
	// Buffer management: The method handles all buffer lifecycle management
	// internally, including Get() and Put() operations.
	//
	// I/O patterns: Optimized for streaming operations where data flows
	// continuously from source to destination.
	CopyBuffer(dst io.Writer, src io.Reader) (int64, error)

	// TeeReader creates an optimized TeeReader using pooled buffers.
	//
	// This method provides an optimized version of io.TeeReader that uses
	// pooled buffers for internal operations, reducing allocation overhead
	// when data needs to be written to multiple destinations.
	//
	// Parameters:
	//   - r: Source reader
	//   - w: Secondary writer (data is written here as it's read)
	//
	// Returns:
	//   - io.Reader: Reader that writes to w as data is read from r
	//
	// Example usage:
	//
	//	// Create optimized tee reader
	//	teeReader := pool.TeeReader(src, dst)
	//
	//	// Read data - it's automatically written to dst
	//	data, err := io.ReadAll(teeReader)
	//	if err != nil {
	//	    return err
	//	}
	//
	//	// data contains the read content
	//	// dst also received the same content
	//	processData(data)
	//
	// Streaming optimization and memory efficiency:
	//   - Uses pooled buffers for internal copy operations
	//   - Reduces memory allocation during tee operations
	//   - Maintains streaming behavior without buffering entire content
	//   - Automatic buffer lifecycle management
	//
	// Use cases:
	//   - Simultaneous cache write and client response
	//   - Data processing with backup/logging
	//   - Multi-destination streaming scenarios
	//
	// Performance characteristics: Significantly reduces allocation overhead
	// compared to io.TeeReader, especially for large data streams or
	// high-frequency operations.
	//
	// Fallback behavior: If buffer pooling is disabled, should fall back
	// to standard io.TeeReader behavior.
	TeeReader(r io.Reader, w io.Writer) io.Reader

	// GetStats returns current buffer pool statistics.
	//
	// This method provides detailed statistics about buffer pool usage,
	// performance, and efficiency. The statistics are useful for monitoring
	// pool effectiveness and tuning configuration parameters.
	//
	// Returns:
	//   - BufferPoolStats: Current pool statistics and performance metrics
	//
	// Example usage:
	//
	//	stats := pool.GetStats()
	//
	//	// Monitor pool effectiveness
	//	hitRate := float64(stats.PoolHits) / float64(stats.TotalGets) * 100
	//	log.Printf("Buffer pool hit rate: %.2f%%", hitRate)
	//
	//	// Check for potential issues
	//	if stats.PoolMisses > stats.PoolHits {
	//	    log.Println("Warning: High buffer pool miss rate")
	//	}
	//
	//	// Monitor active buffer usage
	//	if stats.ActiveBuffers > 1000 {
	//	    log.Printf("High active buffer count: %d", stats.ActiveBuffers)
	//	}
	//
	// Monitoring and performance analysis use cases:
	//
	// Pool effectiveness monitoring:
	//   - Hit rate indicates how often buffers are reused
	//   - Miss rate shows allocation frequency
	//   - Active buffer count indicates current memory usage
	//
	// Performance tuning:
	//   - Low hit rates may indicate suboptimal buffer sizes
	//   - High active buffer counts may indicate buffer leaks
	//   - Get/Put ratio should be approximately 1:1
	//
	// Operational monitoring:
	//   - Track pool performance over time
	//   - Alert on unusual patterns or potential issues
	//   - Validate buffer lifecycle management
	//
	// Statistics accuracy: The returned statistics represent a point-in-time
	// snapshot and may change immediately after the call returns due to
	// concurrent operations.
	//
	// Thread safety: Statistics collection must be thread-safe and should
	// not significantly impact pool performance.
	GetStats() BufferPoolStats
}

// BufferPoolConfig holds configuration for buffer pool behavior.
//
// Buffer pool configuration controls memory allocation optimization for I/O operations,
// providing significant performance benefits through buffer reuse. Proper configuration
// can reduce memory allocation overhead by 60-80% and significantly decrease GC pressure.
//
// Configuration strategy overview:
//   - Enable pooling for high-throughput applications
//   - Size buffers according to typical data patterns
//   - Set appropriate size limits to prevent memory bloat
//   - Enable monitoring for performance insights and tuning
//
// Performance impact: Well-configured buffer pools provide substantial performance
// improvements, especially under high concurrency and frequent I/O operations.
type BufferPoolConfig struct {
	// Enabled controls whether buffer pooling is active.
	//
	// Performance trade-offs and use case recommendations:
	//   - true: Enables buffer pooling with significant performance benefits
	//     * 60-80% reduction in memory allocation overhead
	//     * Substantial GC pressure reduction
	//     * Improved I/O throughput for streaming operations
	//     * Better performance under high concurrency
	//   - false: Disables pooling, falls back to standard library implementations
	//     * Simpler memory profiling and debugging
	//     * No pool management overhead
	//     * Suitable for low-traffic applications
	//
	// Recommendation: Enable for production applications with significant I/O load.
	// Disable only for debugging, testing, or very low-traffic scenarios.
	//
	// Use case guidelines:
	//   - High-throughput web applications: Always enable
	//   - Batch processing systems: Always enable
	//   - Memory-constrained environments: Enable with smaller buffer sizes
	//   - Development/debugging: May disable for simpler analysis
	//   - Low-traffic applications: Benefits may be minimal
	Enabled bool

	// DefaultBufferSize is the default buffer size for pool operations.
	//
	// Sizing strategies and workload considerations:
	//   - Should match the most common data sizes in your application
	//   - Larger sizes improve I/O efficiency but use more memory
	//   - Smaller sizes reduce memory usage but may require more operations
	//   - Consider your typical object sizes and access patterns
	//
	// Recommended values by use case:
	//   - Small objects (< 10KB): 16KB-32KB default size
	//   - Medium objects (10KB-100KB): 32KB-64KB default size
	//   - Large objects (> 100KB): 64KB-128KB default size
	//   - Mixed workloads: 32KB-64KB provides good balance
	//
	// Performance considerations:
	//   - Powers of 2 are typically more efficient (16KB, 32KB, 64KB, 128KB)
	//   - Match common object sizes to minimize waste
	//   - Monitor actual usage patterns and adjust accordingly
	//   - Consider memory constraints in sizing decisions
	//
	// Tuning guidance: Start with 32KB for general use, then adjust based on
	// monitoring data and actual object size distributions in your application.
	DefaultBufferSize int

	// MaxBufferSize is the maximum buffer size that will be pooled.
	//
	// Memory management implications and sizing guidelines:
	//   - Buffers larger than this size are not returned to the pool
	//   - Prevents memory bloat from occasionally large objects
	//   - Should be set based on memory constraints and typical maximum object sizes
	//   - Balances pool effectiveness with memory usage control
	//
	// Sizing strategies:
	//   - Conservative: 2x DefaultBufferSize (good for memory-constrained environments)
	//   - Balanced: 4x DefaultBufferSize (good general-purpose setting)
	//   - Aggressive: 8x DefaultBufferSize (for applications with variable object sizes)
	//
	// Memory management considerations:
	//   - Larger limits allow pooling of bigger buffers but use more memory
	//   - Smaller limits reduce memory usage but may increase allocations
	//   - Monitor buffer size distribution to optimize this setting
	//   - Consider peak memory usage in your environment
	//
	// Example configurations:
	//   - Memory-constrained: MaxBufferSize = 2 * DefaultBufferSize
	//   - General use: MaxBufferSize = 4 * DefaultBufferSize
	//   - Variable workloads: MaxBufferSize = 8 * DefaultBufferSize
	MaxBufferSize int

	// MinBufferSize is the minimum buffer size that will be pooled.
	//
	// Pool efficiency and resource management:
	//   - Buffers smaller than this size are not returned to the pool
	//   - Prevents pool pollution with tiny buffers that provide minimal benefit
	//   - Should be set to avoid overhead of managing very small buffers
	//   - Balances pool simplicity with reuse opportunities
	//
	// Sizing guidelines:
	//   - Typical values: 4KB-16KB depending on application patterns
	//   - Should be significantly smaller than DefaultBufferSize
	//   - Consider the overhead of pool management vs. allocation cost
	//   - Match to your smallest commonly used buffer sizes
	//
	// Performance implications:
	//   - Too small: Pool becomes cluttered with tiny, low-value buffers
	//   - Too large: Miss opportunities to reuse moderately-sized buffers
	//   - Sweet spot: Large enough to be worthwhile, small enough to be inclusive
	//
	// Recommended approach: Set to DefaultBufferSize / 4 to DefaultBufferSize / 8,
	// with a minimum of 4KB for most applications.
	MinBufferSize int

	// EnableLogging controls whether buffer pool performance metrics are logged.
	//
	// Monitoring and debugging guidance:
	//   - true: Enables detailed buffer pool statistics logging
	//     * Provides insights into pool effectiveness
	//     * Helps identify optimization opportunities
	//     * Useful for performance tuning and troubleshooting
	//     * Adds minimal performance overhead
	//   - false: Disables logging for minimal overhead
	//     * Reduces log volume in production
	//     * Eliminates logging performance impact
	//     * Suitable for well-tuned, stable configurations
	//
	// When to enable:
	//   - During initial configuration and tuning
	//   - When investigating performance issues
	//   - For ongoing performance monitoring
	//   - In development and testing environments
	//
	// When to disable:
	//   - In production with stable, well-tuned configuration
	//   - When log volume is a concern
	//   - For maximum performance (minimal impact)
	//
	// Recommendation: Enable during tuning and troubleshooting, consider
	// disabling in stable production environments to reduce log volume.
	EnableLogging bool

	// LoggingInterval specifies how often buffer pool statistics are logged.
	//
	// Monitoring frequency and operational considerations:
	//   - Only used when EnableLogging is true
	//   - Zero value disables periodic logging (log only on significant events)
	//   - Controls the frequency of performance statistics output
	//   - Balances monitoring detail with log volume
	//
	// Recommended intervals:
	//   - Development/tuning: 30s-60s for frequent feedback
	//   - Production monitoring: 5m-15m for trend analysis
	//   - Troubleshooting: 10s-30s for detailed investigation
	//   - Stable production: 30m-60m or disable periodic logging
	//
	// Considerations:
	//   - More frequent logging provides better insights but increases log volume
	//   - Less frequent logging reduces overhead but may miss short-term issues
	//   - Consider your log retention and analysis capabilities
	//   - Adjust based on the stability of your configuration
	//
	// Performance impact: Minimal overhead, but frequent logging can increase
	// log processing load and storage requirements.
	LoggingInterval time.Duration
}

// BufferPoolStats provides monitoring information about buffer pool usage.
type BufferPoolStats struct {
	// TotalGets is the total number of Get operations performed.
	TotalGets int64

	// TotalPuts is the total number of Put operations performed.
	TotalPuts int64

	// PoolHits is the number of times a buffer was successfully retrieved from the pool.
	PoolHits int64

	// PoolMisses is the number of times a new buffer had to be allocated.
	PoolMisses int64

	// ActiveBuffers is the current number of buffers checked out from the pool.
	ActiveBuffers int64
}

// nullEvictionPolicy is a Null Object implementation of EvictionPolicy.
// It performs no operations, effectively disabling eviction.
type nullEvictionPolicy struct{}

// Touch does nothing.
func (p *nullEvictionPolicy) Touch(key string) {}

// Add does nothing.
func (p *nullEvictionPolicy) Add(key string, size int64) {}

// Remove does nothing.
func (p *nullEvictionPolicy) Remove(key string) {}

// Evict returns no keys.
func (p *nullEvictionPolicy) Evict() []string { return nil }

// NewNullEvictionPolicy creates a new no-op eviction policy.
func NewNullEvictionPolicy() EvictionPolicy {
	return &nullEvictionPolicy{}
}

// New creates and configures a new DaramjweeCache instance.
func New(logger log.Logger, opts ...Option) (Cache, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	cfg := Config{
		DefaultTimeout:   30 * time.Second,
		WorkerStrategy:   "pool",
		WorkerPoolSize:   1,
		WorkerQueueSize:  500,
		WorkerJobTimeout: 30 * time.Second,
		ShutdownTimeout:  30 * time.Second,
		PositiveFreshFor: 0 * time.Second,
		NegativeFreshFor: 0 * time.Second,
		BufferPool: BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024, // 32KB default buffer size
			MaxBufferSize:     64 * 1024, // 64KB max buffer size
			MinBufferSize:     4 * 1024,  // 4KB min buffer size
			EnableLogging:     false,     // Disabled by default for performance
			LoggingInterval:   0,         // No periodic logging by default
		},
	}

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	if cfg.HotStore == nil {
		return nil, &ConfigError{"hotStore is required"}
	}

	if cfg.ColdStore == nil {
		level.Debug(logger).Log("msg", "cold store not configured, using null store")
		cfg.ColdStore = newNullStore()
	}

	workerManager, err := worker.NewManager(cfg.WorkerStrategy, logger, cfg.WorkerPoolSize, cfg.WorkerQueueSize, cfg.WorkerJobTimeout)
	if err != nil {
		return nil, err
	}

	// Initialize buffer pool
	var bufferPool BufferPool
	if cfg.BufferPool.Enabled {
		bufferPool = NewDefaultBufferPoolWithLogger(cfg.BufferPool, logger)
		level.Debug(logger).Log("msg", "buffer pool initialized", "enabled", true, "default_size", cfg.BufferPool.DefaultBufferSize, "logging_enabled", cfg.BufferPool.EnableLogging, "logging_interval", cfg.BufferPool.LoggingInterval)
	} else {
		// Create a disabled buffer pool that falls back to standard operations
		bufferPool = NewDefaultBufferPoolWithLogger(BufferPoolConfig{
			Enabled:           false,
			DefaultBufferSize: cfg.BufferPool.DefaultBufferSize,
			MaxBufferSize:     cfg.BufferPool.MaxBufferSize,
			MinBufferSize:     cfg.BufferPool.MinBufferSize,
			EnableLogging:     false,
			LoggingInterval:   0,
		}, logger)
		level.Debug(logger).Log("msg", "buffer pool disabled, using fallback operations")
	}

	c := &DaramjweeCache{
		Logger:           logger,
		HotStore:         cfg.HotStore,
		ColdStore:        cfg.ColdStore,
		Worker:           workerManager,
		BufferPool:       bufferPool,
		DefaultTimeout:   cfg.DefaultTimeout,
		ShutdownTimeout:  cfg.ShutdownTimeout,
		PositiveFreshFor: cfg.PositiveFreshFor,
		NegativeFreshFor: cfg.NegativeFreshFor,
	}

	level.Info(logger).Log("msg", "daramjwee cache initialized", "default_timeout", c.DefaultTimeout)
	return c, nil
}

// CompressionType defines the supported compression algorithms
type CompressionType string

const (
	CompressionGzip CompressionType = "gzip"
	CompressionZstd CompressionType = "zstd"
	CompressionLZ4  CompressionType = "lz4"
	CompressionNone CompressionType = "none"
)

// CompressionMetadata stores compression-related metadata
type CompressionMetadata struct {
	Algorithm        string  `json:"algorithm"`         // Compression algorithm
	Level            int     `json:"level"`             // Compression level
	OriginalSize     int64   `json:"original_size"`     // Original size
	CompressedSize   int64   `json:"compressed_size"`   // Compressed size
	CompressionRatio float64 `json:"compression_ratio"` // Compression ratio (compressed size / original size)
}

// Compression-related error types
// Compression-related error variables with detailed handling guidance
var (
	// ErrCompressionFailed is returned when compression operation fails.
	//
	// Failure conditions and fallback strategies:
	//   - Compressor encounters invalid input data or format issues
	//   - Insufficient memory or resources for compression operation
	//   - I/O errors during compression process
	//   - Compression algorithm internal errors or bugs
	//
	// This error indicates that the compression operation could not be completed
	// successfully. Applications should implement fallback strategies to handle
	// compression failures gracefully without losing data.
	//
	// Fallback strategies:
	//   - Store data uncompressed as fallback
	//   - Try alternative compression algorithms
	//   - Retry with different compression levels
	//   - Log the error and continue with uncompressed storage
	//
	// Example handling:
	//
	//	compressed, err := compressor.Compress(dst, src)
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrCompressionFailed) {
	//	        // Fallback to uncompressed storage
	//	        log.Warn("Compression failed, storing uncompressed", "error", err)
	//	        return storeUncompressed(dst, src)
	//	    }
	//	    return err
	//	}
	//
	// Prevention strategies:
	//   - Validate input data before compression
	//   - Monitor compression success rates
	//   - Use appropriate compression levels for data types
	//   - Implement proper error handling and logging
	ErrCompressionFailed = errors.New("daramjwee: compression failed")

	// ErrDecompressionFailed is returned when decompression operation fails.
	//
	// Failure conditions and recovery patterns:
	//   - Corrupted compressed data that cannot be decompressed
	//   - Mismatched compression algorithm or parameters
	//   - Incomplete or truncated compressed data
	//   - Decompressor internal errors or resource limitations
	//
	// This error indicates that compressed data could not be successfully
	// decompressed. This is often a critical error as it means cached data
	// is inaccessible and may need to be refetched from origin.
	//
	// Recovery strategies:
	//   - Remove corrupted cache entry and refetch from origin
	//   - Try alternative decompression methods if available
	//   - Log detailed error information for debugging
	//   - Implement cache validation to detect corruption early
	//
	// Example handling:
	//
	//	decompressed, err := compressor.Decompress(dst, src)
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrDecompressionFailed) {
	//	        // Remove corrupted entry and refetch
	//	        log.Error("Decompression failed, removing cache entry", "key", key, "error", err)
	//	        cache.Delete(ctx, key)
	//	        return fetchFromOrigin(ctx, key)
	//	    }
	//	    return err
	//	}
	//
	// Prevention strategies:
	//   - Implement data integrity checks (checksums, CRC)
	//   - Use reliable compression algorithms
	//   - Monitor decompression success rates
	//   - Implement proper error handling and recovery
	ErrDecompressionFailed = errors.New("daramjwee: decompression failed")

	// ErrUnsupportedAlgorithm is returned when an unsupported compression algorithm is used.
	//
	// Algorithm compatibility and configuration issues:
	//   - Requested compression algorithm is not available or supported
	//   - Algorithm name or identifier is invalid or unknown
	//   - Missing dependencies or libraries for specific algorithms
	//   - Version compatibility issues between compression implementations
	//
	// This error indicates a configuration or compatibility issue rather than
	// a runtime failure. It typically occurs during initialization or when
	// processing data compressed with unsupported algorithms.
	//
	// Resolution strategies:
	//   - Validate compression algorithm configuration at startup
	//   - Provide fallback to supported algorithms
	//   - Update dependencies or install required libraries
	//   - Document supported algorithms and their requirements
	//
	// Example handling:
	//
	//	compressor, err := comp.NewCompressor(algorithmName)
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrUnsupportedAlgorithm) {
	//	        // Fallback to default supported algorithm
	//	        log.Warn("Unsupported algorithm, using default", "requested", algorithmName, "default", "gzip")
	//	        compressor, err = comp.NewGzipCompressor()
	//	    }
	//	    if err != nil {
	//	        return err
	//	    }
	//	}
	//
	// Configuration validation:
	//   - Check algorithm availability during application startup
	//   - Provide clear error messages for unsupported algorithms
	//   - Document algorithm dependencies and requirements
	//   - Implement graceful fallbacks to supported alternatives
	ErrUnsupportedAlgorithm = errors.New("daramjwee: unsupported compression algorithm")

	// ErrCorruptedData is returned when compressed data is corrupted.
	//
	// Data corruption scenarios and detection:
	//   - Bit flips or data corruption during storage or transmission
	//   - Incomplete writes or partial data loss
	//   - Storage medium failures or corruption
	//   - Network transmission errors affecting cached data
	//
	// This error indicates that the compressed data has been corrupted and
	// cannot be reliably decompressed. This is a serious issue that requires
	// immediate attention and data recovery procedures.
	//
	// Recovery and mitigation strategies:
	//   - Remove corrupted cache entries immediately
	//   - Refetch data from origin systems
	//   - Implement data integrity verification (checksums)
	//   - Monitor corruption rates and investigate root causes
	//
	// Example handling:
	//
	//	data, err := decompressor.Decompress(compressedData)
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrCorruptedData) {
	//	        // Immediate cleanup and recovery
	//	        log.Error("Data corruption detected", "key", key, "error", err)
	//
	//	        // Remove corrupted entry
	//	        if deleteErr := cache.Delete(ctx, key); deleteErr != nil {
	//	            log.Error("Failed to delete corrupted entry", "key", key, "error", deleteErr)
	//	        }
	//
	//	        // Refetch from origin
	//	        return refetchFromOrigin(ctx, key)
	//	    }
	//	    return err
	//	}
	//
	// Prevention and monitoring:
	//   - Implement data integrity checks (CRC, checksums)
	//   - Monitor storage system health and error rates
	//   - Use reliable storage backends with error correction
	//   - Implement regular data validation and health checks
	ErrCorruptedData = errors.New("daramjwee: corrupted compressed data")

	// ErrInvalidCompressionLevel is returned when an invalid compression level is set.
	//
	// Configuration validation and parameter checking:
	//   - Compression level outside the valid range for the algorithm
	//   - Negative compression levels or values exceeding maximum
	//   - Algorithm-specific level constraints not met
	//   - Configuration parameter validation failures
	//
	// This error indicates a configuration issue where the specified compression
	// level is not valid for the chosen compression algorithm. Different algorithms
	// have different valid level ranges and constraints.
	//
	// Resolution and validation strategies:
	//   - Validate compression levels during configuration
	//   - Provide clear documentation of valid level ranges
	//   - Implement parameter validation with helpful error messages
	//   - Use sensible defaults when invalid levels are specified
	//
	// Example handling:
	//
	//	compressor, err := comp.NewGzipCompressor(comp.WithLevel(compressionLevel))
	//	if err != nil {
	//	    if errors.Is(err, daramjwee.ErrInvalidCompressionLevel) {
	//	        // Use default level and log warning
	//	        log.Warn("Invalid compression level, using default",
	//	            "requested", compressionLevel, "default", comp.DefaultLevel)
	//	        compressor, err = comp.NewGzipCompressor(comp.WithLevel(comp.DefaultLevel))
	//	    }
	//	    if err != nil {
	//	        return err
	//	    }
	//	}
	//
	// Algorithm-specific level ranges:
	//   - Gzip: 1-9 (1=fastest, 9=best compression)
	//   - Zstd: 1-22 (1=fastest, 22=best compression)
	//   - LZ4: Typically 1-12 depending on implementation
	//
	// Configuration best practices:
	//   - Document valid level ranges for each algorithm
	//   - Provide configuration validation at startup
	//   - Use reasonable defaults for invalid configurations
	//   - Implement parameter validation with clear error messages
	ErrInvalidCompressionLevel = errors.New("daramjwee: invalid compression level")
)

// NoneCompressor is a pass-through implementation that performs no compression
type NoneCompressor struct{}

// NewNoneCompressor creates a new no-compression implementation
func NewNoneCompressor() Compressor {
	return &NoneCompressor{}
}

// Compress copies data as-is (no compression)
func (n *NoneCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
	return io.Copy(dst, src)
}

// Decompress copies data as-is (no decompression)
func (n *NoneCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
	return io.Copy(dst, src)
}

// Algorithm returns the compression algorithm name
func (n *NoneCompressor) Algorithm() string {
	return "none"
}

// Level returns the compression level (0 since no compression is performed)
func (n *NoneCompressor) Level() int {
	return 0
}

// Helper functions for common use cases with small objects

// GetBytes retrieves an object as a byte slice.
// This is a convenience wrapper around Get that handles stream reading and closing.
// It's ideal for small objects where the streaming API overhead isn't necessary.
func GetBytes(ctx context.Context, cache Cache, key string, fetcher Fetcher) ([]byte, error) {
	stream, err := cache.Get(ctx, key, fetcher)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	return io.ReadAll(stream)
}

// GetString retrieves an object as a string.
// This is a convenience wrapper around GetBytes for text-based content.
func GetString(ctx context.Context, cache Cache, key string, fetcher Fetcher) (string, error) {
	data, err := GetBytes(ctx, cache, key, fetcher)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// SetBytes stores a byte slice in the cache.
// This is a convenience wrapper around Set that handles the streaming write.
func SetBytes(ctx context.Context, cache Cache, key string, data []byte, metadata *Metadata) error {
	writer, err := cache.Set(ctx, key, metadata)
	if err != nil {
		return err
	}
	defer writer.Close()

	_, err = writer.Write(data)
	return err
}

// SetString stores a string in the cache.
// This is a convenience wrapper around SetBytes for text-based content.
func SetString(ctx context.Context, cache Cache, key string, data string, metadata *Metadata) error {
	return SetBytes(ctx, cache, key, []byte(data), metadata)
}

// SimpleFetcher is a convenience type that wraps a simple fetch function.
// It's useful when you don't need the full complexity of implementing the Fetcher interface.
type SimpleFetcher func(ctx context.Context, oldMetadata *Metadata) ([]byte, *Metadata, error)

// Fetch implements the Fetcher interface by calling the wrapped function.
func (f SimpleFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	data, metadata, err := f(ctx, oldMetadata)
	if err != nil {
		return nil, err
	}

	return &FetchResult{
		Body:     io.NopCloser(bytes.NewReader(data)),
		Metadata: metadata,
	}, nil
}

// Configuration Examples for Different Deployment Scenarios
//
// The following examples demonstrate how to configure daramjwee for various
// deployment scenarios, each optimized for specific requirements and constraints.

// Example: High-throughput configuration for web applications
//
// This configuration is optimized for high-traffic web applications that need
// maximum performance and can allocate sufficient resources for caching.
//
//	func NewHighThroughputCache(logger log.Logger) (Cache, error) {
//	    // High-performance memory store for hot tier
//	    hotStore := memstore.New(
//	        memstore.WithCapacity(512 * 1024 * 1024), // 512MB hot tier
//	        memstore.WithEvictionPolicy(policy.NewS3FIFO()), // Modern eviction algorithm
//	    )
//
//	    // Large file-based cold tier for extended coverage
//	    coldStore := filestore.New(
//	        filestore.WithDirectory("/var/cache/daramjwee"),
//	        filestore.WithHashedKeys(true), // Better directory distribution
//	        filestore.WithCapacity(10 * 1024 * 1024 * 1024), // 10GB cold tier
//	    )
//
//	    return daramjwee.New(logger,
//	        // Storage configuration
//	        daramjwee.WithHotStore(hotStore),
//	        daramjwee.WithColdStore(coldStore),
//
//	        // High-performance worker configuration
//	        daramjwee.WithWorker("pool", 8, 400, 60*time.Second),
//
//	        // Aggressive timeout settings
//	        daramjwee.WithDefaultTimeout(30*time.Second),
//	        daramjwee.WithShutdownTimeout(60*time.Second),
//
//	        // Balanced cache freshness
//	        daramjwee.WithCache(15*time.Minute),        // 15min positive TTL
//	        daramjwee.WithNegativeCache(5*time.Minute), // 5min negative TTL
//
//	        // Optimized buffer pool for high throughput
//	        daramjwee.WithBufferPoolAdvanced(daramjwee.BufferPoolConfig{
//	            Enabled:           true,
//	            DefaultBufferSize: 64 * 1024,  // 64KB buffers
//	            MinBufferSize:     16 * 1024,  // 16KB minimum
//	            MaxBufferSize:     256 * 1024, // 256KB maximum
//	            EnableLogging:     true,
//	            LoggingInterval:   300 * time.Second, // 5min logging
//	        }),
//	    )
//	}

// Example: Memory-constrained configuration for resource-limited environments
//
// This configuration is optimized for environments with limited memory and CPU
// resources, such as containers with resource limits or edge deployments.
//
//	func NewMemoryConstrainedCache(logger log.Logger) (Cache, error) {
//	    // Small memory store for hot tier
//	    hotStore := memstore.New(
//	        memstore.WithCapacity(64 * 1024 * 1024), // 64MB hot tier
//	        memstore.WithEvictionPolicy(policy.NewSIEVE()), // Low-overhead eviction
//	    )
//
//	    // Optional small file-based cold tier
//	    coldStore := filestore.New(
//	        filestore.WithDirectory("/tmp/daramjwee-cache"),
//	        filestore.WithCapacity(512 * 1024 * 1024), // 512MB cold tier
//	    )
//
//	    return daramjwee.New(logger,
//	        // Minimal storage configuration
//	        daramjwee.WithHotStore(hotStore),
//	        daramjwee.WithColdStore(coldStore),
//
//	        // Conservative worker configuration
//	        daramjwee.WithWorker("pool", 2, 50, 45*time.Second),
//
//	        // Conservative timeout settings
//	        daramjwee.WithDefaultTimeout(15*time.Second),
//	        daramjwee.WithShutdownTimeout(30*time.Second),
//
//	        // Longer cache freshness to reduce load
//	        daramjwee.WithCache(60*time.Minute),        // 1hr positive TTL
//	        daramjwee.WithNegativeCache(30*time.Minute), // 30min negative TTL
//
//	        // Small buffer pool configuration
//	        daramjwee.WithBufferPool(true, 16*1024), // 16KB buffers
//	    )
//	}

// Example: Development and testing configuration with debugging enabled
//
// This configuration is optimized for development and testing environments,
// with extensive logging and monitoring enabled for debugging purposes.
//
//	func NewDevelopmentCache(logger log.Logger) (Cache, error) {
//	    // Small memory store for quick testing
//	    hotStore := memstore.New(
//	        memstore.WithCapacity(32 * 1024 * 1024), // 32MB hot tier
//	        memstore.WithEvictionPolicy(policy.NewLRU()), // Simple, predictable eviction
//	    )
//
//	    // Local file store for persistence across restarts
//	    coldStore := filestore.New(
//	        filestore.WithDirectory("./dev-cache"),
//	        filestore.WithHashedKeys(false), // Easier debugging
//	    )
//
//	    return daramjwee.New(logger,
//	        // Development storage configuration
//	        daramjwee.WithHotStore(hotStore),
//	        daramjwee.WithColdStore(coldStore),
//
//	        // Simple worker configuration for debugging
//	        daramjwee.WithWorker("pool", 1, 10, 30*time.Second),
//
//	        // Short timeouts for quick feedback
//	        daramjwee.WithDefaultTimeout(10*time.Second),
//	        daramjwee.WithShutdownTimeout(15*time.Second),
//
//	        // Short cache freshness for testing
//	        daramjwee.WithCache(2*time.Minute),         // 2min positive TTL
//	        daramjwee.WithNegativeCache(1*time.Minute), // 1min negative TTL
//
//	        // Buffer pool with extensive logging
//	        daramjwee.WithBufferPoolAdvanced(daramjwee.BufferPoolConfig{
//	            Enabled:           true,
//	            DefaultBufferSize: 32 * 1024, // 32KB buffers
//	            MinBufferSize:     4 * 1024,  // 4KB minimum
//	            MaxBufferSize:     64 * 1024, // 64KB maximum
//	            EnableLogging:     true,
//	            LoggingInterval:   30 * time.Second, // Frequent logging
//	        }),
//	    )
//	}

// Example: Cloud deployment configuration with appropriate timeouts
//
// This configuration is optimized for cloud deployments where network latency
// and distributed storage are considerations, with appropriate timeout settings.
//
//	func NewCloudDeploymentCache(logger log.Logger, bucket objstore.Bucket) (Cache, error) {
//	    // Memory store for hot tier
//	    hotStore := memstore.New(
//	        memstore.WithCapacity(256 * 1024 * 1024), // 256MB hot tier
//	        memstore.WithEvictionPolicy(policy.NewS3FIFO()),
//	    )
//
//	    // Cloud storage for cold tier
//	    coldStore := adapter.NewObjStore(bucket, "cache/")
//
//	    return daramjwee.New(logger,
//	        // Cloud storage configuration
//	        daramjwee.WithHotStore(hotStore),
//	        daramjwee.WithColdStore(coldStore),
//
//	        // Cloud-optimized worker configuration
//	        daramjwee.WithWorker("pool", 6, 300, 120*time.Second),
//
//	        // Cloud-appropriate timeout settings
//	        daramjwee.WithDefaultTimeout(60*time.Second),  // Account for network latency
//	        daramjwee.WithShutdownTimeout(120*time.Second), // Allow for cloud operations
//
//	        // Cloud-optimized cache freshness
//	        daramjwee.WithCache(30*time.Minute),        // 30min positive TTL
//	        daramjwee.WithNegativeCache(10*time.Minute), // 10min negative TTL
//
//	        // Cloud-optimized buffer pool
//	        daramjwee.WithBufferPoolAdvanced(daramjwee.BufferPoolConfig{
//	            Enabled:           true,
//	            DefaultBufferSize: 128 * 1024, // 128KB for network efficiency
//	            MinBufferSize:     32 * 1024,  // 32KB minimum
//	            MaxBufferSize:     512 * 1024, // 512KB maximum
//	            EnableLogging:     true,
//	            LoggingInterval:   600 * time.Second, // 10min logging
//	        }),
//	    )
//	}

// Example: Configuration validation with comprehensive error handling
//
// This example demonstrates proper configuration validation and error handling
// patterns for robust application initialization.
//
//	func NewValidatedCache(logger log.Logger, config CacheConfig) (Cache, error) {
//	    // Validate configuration parameters
//	    if config.HotStoreCapacity <= 0 {
//	        return nil, fmt.Errorf("hot store capacity must be positive: %d", config.HotStoreCapacity)
//	    }
//	    if config.WorkerPoolSize <= 0 {
//	        return nil, fmt.Errorf("worker pool size must be positive: %d", config.WorkerPoolSize)
//	    }
//	    if config.DefaultTimeout <= 0 {
//	        return nil, fmt.Errorf("default timeout must be positive: %v", config.DefaultTimeout)
//	    }
//
//	    // Create hot store with error handling
//	    hotStore := memstore.New(
//	        memstore.WithCapacity(config.HotStoreCapacity),
//	        memstore.WithEvictionPolicy(policy.NewLRU()),
//	    )
//
//	    // Create cold store if configured
//	    var coldStore Store
//	    if config.ColdStoreDirectory != "" {
//	        coldStore = filestore.New(
//	            filestore.WithDirectory(config.ColdStoreDirectory),
//	            filestore.WithHashedKeys(true),
//	        )
//	    }
//
//	    // Build cache with validated configuration
//	    cache, err := daramjwee.New(logger,
//	        daramjwee.WithHotStore(hotStore),
//	        daramjwee.WithColdStore(coldStore), // nil is acceptable
//	        daramjwee.WithWorker("pool", config.WorkerPoolSize, config.WorkerQueueSize, config.WorkerJobTimeout),
//	        daramjwee.WithDefaultTimeout(config.DefaultTimeout),
//	        daramjwee.WithShutdownTimeout(config.ShutdownTimeout),
//	        daramjwee.WithCache(config.PositiveTTL),
//	        daramjwee.WithNegativeCache(config.NegativeTTL),
//	        daramjwee.WithBufferPool(config.BufferPoolEnabled, config.BufferPoolSize),
//	    )
//
//	    if err != nil {
//	        return nil, fmt.Errorf("failed to create cache: %w", err)
//	    }
//
//	    // Log successful configuration
//	    level.Info(logger).Log(
//	        "msg", "cache initialized successfully",
//	        "hot_store_capacity", config.HotStoreCapacity,
//	        "worker_pool_size", config.WorkerPoolSize,
//	        "default_timeout", config.DefaultTimeout,
//	        "buffer_pool_enabled", config.BufferPoolEnabled,
//	    )
//
//	    return cache, nil
//	}
//
//	// CacheConfig holds validated configuration parameters
//	type CacheConfig struct {
//	    HotStoreCapacity     int64
//	    ColdStoreDirectory   string
//	    WorkerPoolSize       int
//	    WorkerQueueSize      int
//	    WorkerJobTimeout     time.Duration
//	    DefaultTimeout       time.Duration
//	    ShutdownTimeout      time.Duration
//	    PositiveTTL          time.Duration
//	    NegativeTTL          time.Duration
//	    BufferPoolEnabled    bool
//	    BufferPoolSize       int
//	}

// Advanced Configuration Patterns and Tuning Guidelines
//
// The following sections provide detailed guidance for advanced configuration
// scenarios, performance tuning, and troubleshooting common issues.

// Worker Pool Sizing Strategies for Different Workload Patterns
//
// Worker pool configuration significantly impacts cache performance and resource
// utilization. The optimal configuration depends on your specific workload
// characteristics and infrastructure constraints.
//
// CPU-bound workloads (data processing, compression, serialization):
//   - Start with worker count = CPU core count
//   - Monitor CPU utilization and worker queue depth
//   - Increase workers if queue builds up and CPU usage is low
//   - Decrease workers if CPU usage is consistently high
//
//	// Example: CPU-bound configuration
//	workerCount := runtime.NumCPU()
//	queueSize := workerCount * 20
//	jobTimeout := 60 * time.Second
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithWorker("pool", workerCount, queueSize, jobTimeout),
//	    // ... other options
//	)
//
// I/O-bound workloads (network requests, database queries, file operations):
//   - Start with worker count = 2-4x CPU core count
//   - Monitor I/O wait times and origin response latencies
//   - Increase workers if origins can handle more concurrent requests
//   - Consider origin system capacity and rate limits
//
//	// Example: I/O-bound configuration
//	workerCount := runtime.NumCPU() * 3
//	queueSize := workerCount * 15
//	jobTimeout := 120 * time.Second // Longer timeout for network operations
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithWorker("pool", workerCount, queueSize, jobTimeout),
//	    // ... other options
//	)
//
// Mixed workloads (combination of CPU and I/O operations):
//   - Start with worker count = 1.5-2x CPU core count
//   - Monitor both CPU usage and I/O wait times
//   - Adjust based on the dominant workload type
//   - Consider separate worker pools for different operation types
//
//	// Example: Mixed workload configuration
//	workerCount := int(float64(runtime.NumCPU()) * 1.5)
//	queueSize := workerCount * 25
//	jobTimeout := 90 * time.Second
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithWorker("pool", workerCount, queueSize, jobTimeout),
//	    // ... other options
//	)

// Timeout Configuration for Different Network and Storage Latencies
//
// Timeout configuration must account for the latency characteristics of your
// storage backends and origin systems. Proper timeout tuning prevents
// unnecessary failures while maintaining responsive behavior.
//
// Local storage scenarios (in-memory, local SSD):
//   - Very low latency, short timeouts acceptable
//   - Focus on preventing hung operations
//   - Consider application response time requirements
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithDefaultTimeout(5*time.Second),
//	    daramjwee.WithShutdownTimeout(15*time.Second),
//	    daramjwee.WithWorker("pool", 4, 100, 30*time.Second),
//	    // ... other options
//	)
//
// Network storage scenarios (NFS, cloud storage, distributed systems):
//   - Higher latency, longer timeouts required
//   - Account for network variability and congestion
//   - Consider retry strategies and circuit breakers
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithDefaultTimeout(30*time.Second),
//	    daramjwee.WithShutdownTimeout(60*time.Second),
//	    daramjwee.WithWorker("pool", 6, 200, 120*time.Second),
//	    // ... other options
//	)
//
// High-latency origin scenarios (slow APIs, batch systems, legacy systems):
//   - Very high latency, generous timeouts needed
//   - Balance timeout duration with user experience
//   - Consider asynchronous patterns and background refresh
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithDefaultTimeout(120*time.Second),
//	    daramjwee.WithShutdownTimeout(300*time.Second),
//	    daramjwee.WithWorker("pool", 8, 400, 300*time.Second),
//	    // ... other options
//	)

// Eviction Policy Selection Criteria and Performance Characteristics
//
// Different eviction policies provide different trade-offs between hit rates,
// computational overhead, and memory usage. Choose based on your access
// patterns and performance requirements.
//
// LRU (Least Recently Used):
//   - Best for: General-purpose applications with temporal locality
//   - Characteristics: Good hit rates, moderate overhead, predictable behavior
//   - Use when: Access patterns show clear temporal locality
//
//	hotStore := memstore.New(
//	    memstore.WithCapacity(256 * 1024 * 1024),
//	    memstore.WithEvictionPolicy(policy.NewLRU()),
//	)
//
// S3-FIFO (Static-Dynamic FIFO):
//   - Best for: Modern web applications with mixed access patterns
//   - Characteristics: Excellent hit rates, low overhead, optimized for real-world workloads
//   - Use when: You have mixed hot/cold data with varying access frequencies
//
//	hotStore := memstore.New(
//	    memstore.WithCapacity(256 * 1024 * 1024),
//	    memstore.WithEvictionPolicy(policy.NewS3FIFO()),
//	)
//
// SIEVE:
//   - Best for: Memory-constrained environments, high-frequency operations
//   - Characteristics: Very low overhead, competitive hit rates, minimal metadata
//   - Use when: CPU and memory overhead must be minimized
//
//	hotStore := memstore.New(
//	    memstore.WithCapacity(256 * 1024 * 1024),
//	    memstore.WithEvictionPolicy(policy.NewSIEVE()),
//	)

// Buffer Pool Optimization for Different Object Size Distributions
//
// Buffer pool configuration should match your actual data size patterns for
// optimal performance. Analyze your object size distribution and configure
// accordingly.
//
// Small object workloads (< 10KB average):
//   - Use smaller default buffer sizes to reduce memory waste
//   - Narrow size range to improve reuse rates
//   - Monitor pool hit rates and adjust accordingly
//
//	bufferConfig := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 16 * 1024,  // 16KB default
//	    MinBufferSize:     4 * 1024,   // 4KB minimum
//	    MaxBufferSize:     32 * 1024,  // 32KB maximum
//	    EnableLogging:     true,
//	    LoggingInterval:   300 * time.Second,
//	}
//
// Large object workloads (> 100KB average):
//   - Use larger default buffer sizes for better I/O efficiency
//   - Wider size range to accommodate size variation
//   - Monitor memory usage and adjust limits accordingly
//
//	bufferConfig := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 256 * 1024, // 256KB default
//	    MinBufferSize:     64 * 1024,  // 64KB minimum
//	    MaxBufferSize:     1024 * 1024, // 1MB maximum
//	    EnableLogging:     true,
//	    LoggingInterval:   600 * time.Second,
//	}
//
// Mixed size workloads (wide size distribution):
//   - Use moderate default size with wide range
//   - Monitor size distribution and adjust based on actual patterns
//   - Consider multiple buffer pools for different size classes
//
//	bufferConfig := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 64 * 1024,  // 64KB default
//	    MinBufferSize:     8 * 1024,   // 8KB minimum
//	    MaxBufferSize:     512 * 1024, // 512KB maximum
//	    EnableLogging:     true,
//	    LoggingInterval:   300 * time.Second,
//	}

// Troubleshooting Guide for Common Configuration Issues
//
// This section provides guidance for diagnosing and resolving common
// configuration problems that can impact cache performance and reliability.
//
// Issue: High cache miss rates
// Symptoms: Frequent origin requests, poor response times, high origin load
// Diagnosis:
//   - Check hot store capacity vs. working set size
//   - Monitor eviction rates and patterns
//   - Analyze access patterns and temporal locality
//   - Review TTL settings and refresh behavior
// Solutions:
//   - Increase hot store capacity
//   - Adjust eviction policy based on access patterns
//   - Optimize TTL settings for your data freshness requirements
//   - Consider adding or expanding cold tier
//
//	// Example: Increase capacity and optimize eviction
//	hotStore := memstore.New(
//	    memstore.WithCapacity(1024 * 1024 * 1024), // Increase to 1GB
//	    memstore.WithEvictionPolicy(policy.NewS3FIFO()), // Better algorithm
//	)
//
// Issue: Worker queue overflow
// Symptoms: Dropped background refresh jobs, stale data serving, queue full errors
// Diagnosis:
//   - Monitor worker queue depth and utilization
//   - Check worker job completion times
//   - Analyze origin response times and failure rates
//   - Review worker pool sizing vs. workload
// Solutions:
//   - Increase worker pool size
//   - Increase queue size for burst handling
//   - Optimize origin systems or add caching layers
//   - Adjust job timeouts based on actual requirements
//
//	// Example: Scale up worker configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithWorker("pool", 12, 600, 90*time.Second), // More workers, bigger queue
//	    // ... other options
//	)
//
// Issue: High memory usage
// Symptoms: Memory growth, OOM errors, GC pressure, poor performance
// Diagnosis:
//   - Monitor cache capacity vs. actual usage
//   - Check buffer pool statistics and sizing
//   - Analyze object size distribution
//   - Review eviction policy effectiveness
// Solutions:
//   - Reduce cache capacity or implement stricter limits
//   - Optimize buffer pool sizing for your workload
//   - Implement more aggressive eviction policies
//   - Consider compression for large objects
//
//	// Example: Memory-optimized configuration
//	hotStore := memstore.New(
//	    memstore.WithCapacity(128 * 1024 * 1024), // Reduce capacity
//	    memstore.WithEvictionPolicy(policy.NewSIEVE()), // Low-overhead eviction
//	)
//
//	bufferConfig := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 16 * 1024, // Smaller buffers
//	    MinBufferSize:     4 * 1024,
//	    MaxBufferSize:     32 * 1024, // Strict size limits
//	    EnableLogging:     false,     // Reduce overhead
//	}
//
// Issue: Slow shutdown times
// Symptoms: Application takes long time to terminate, hung shutdown processes
// Diagnosis:
//   - Monitor in-flight operations during shutdown
//   - Check worker job completion times
//   - Review storage backend flush operations
//   - Analyze shutdown timeout vs. actual requirements
// Solutions:
//   - Adjust shutdown timeout based on actual operation times
//   - Optimize background job processing
//   - Implement graceful degradation during shutdown
//   - Consider force termination for non-critical operations
//
//	// Example: Optimized shutdown configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithShutdownTimeout(45*time.Second), // Balanced timeout
//	    daramjwee.WithWorker("pool", 4, 100, 30*time.Second), // Shorter job timeout
//	    // ... other options
//	)
//
// Issue: Poor buffer pool performance
// Symptoms: High allocation rates, GC pressure, low pool hit rates
// Diagnosis:
//   - Monitor buffer pool statistics (hit/miss rates)
//   - Analyze actual buffer size usage patterns
//   - Check pool configuration vs. workload characteristics
//   - Review buffer lifecycle management
// Solutions:
//   - Adjust buffer sizes to match actual usage patterns
//   - Optimize size ranges for better reuse
//   - Enable logging to gather performance insights
//   - Consider workload-specific buffer pool configurations
//
//	// Example: Optimized buffer pool configuration
//	bufferConfig := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 32 * 1024,  // Match common sizes
//	    MinBufferSize:     8 * 1024,   // Capture smaller buffers
//	    MaxBufferSize:     128 * 1024, // Reasonable upper limit
//	    EnableLogging:     true,       // Monitor performance
//	    LoggingInterval:   60 * time.Second, // Frequent feedback
//	}
// Error Handling Examples and Patterns
//
// The following examples demonstrate comprehensive error handling patterns
// for production deployments, including HTTP handlers, retry logic, graceful
// degradation, and monitoring strategies.

// HTTP Handler Examples with Proper Error Response Mapping
//
// These examples show how to handle different cache errors in HTTP handlers,
// providing appropriate HTTP status codes and response messages for clients.

// Example: Basic HTTP handler with comprehensive error handling
//
//	func CacheHandler(cache daramjwee.Cache, fetcher daramjwee.Fetcher) http.HandlerFunc {
//	    return func(w http.ResponseWriter, r *http.Request) {
//	        key := r.URL.Path
//	        ctx := r.Context()
//
//	        stream, err := cache.Get(ctx, key, fetcher)
//	        if err != nil {
//	            handleCacheError(w, r, err, key)
//	            return
//	        }
//	        defer stream.Close()
//
//	        // Set appropriate headers
//	        w.Header().Set("Content-Type", "application/json")
//	        w.Header().Set("Cache-Control", "public, max-age=300")
//
//	        // Stream response to client
//	        if _, err := io.Copy(w, stream); err != nil {
//	            log.Printf("Failed to stream response: %v", err)
//	        }
//	    }
//	}
//
//	func handleCacheError(w http.ResponseWriter, r *http.Request, err error, key string) {
//	    switch {
//	    case errors.Is(err, daramjwee.ErrNotFound):
//	        // Resource not found - return 404
//	        http.Error(w, "Resource not found", http.StatusNotFound)
//	        log.Printf("Resource not found: %s", key)
//
//	    case errors.Is(err, daramjwee.ErrCacheableNotFound):
//	        // Cached not found - return 404 but log differently
//	        http.Error(w, "Resource not found", http.StatusNotFound)
//	        log.Printf("Cached not found result: %s", key)
//
//	    case errors.Is(err, daramjwee.ErrCacheClosed):
//	        // Cache is shut down - return 503 Service Unavailable
//	        http.Error(w, "Service temporarily unavailable", http.StatusServiceUnavailable)
//	        log.Printf("Cache closed during request: %s", key)
//
//	    case errors.Is(err, context.DeadlineExceeded):
//	        // Request timeout - return 504 Gateway Timeout
//	        http.Error(w, "Request timeout", http.StatusGatewayTimeout)
//	        log.Printf("Request timeout for key: %s", key)
//
//	    case errors.Is(err, context.Canceled):
//	        // Client canceled request - log but don't respond
//	        log.Printf("Request canceled by client: %s", key)
//	        return
//
//	    default:
//	        // Internal server error for other cases
//	        http.Error(w, "Internal server error", http.StatusInternalServerError)
//	        log.Printf("Cache error for key %s: %v", key, err)
//	    }
//	}

// Example: Advanced HTTP handler with fallback mechanisms
//
//	func AdvancedCacheHandler(primaryCache, fallbackCache daramjwee.Cache, fetcher daramjwee.Fetcher) http.HandlerFunc {
//	    return func(w http.ResponseWriter, r *http.Request) {
//	        key := r.URL.Path
//	        ctx := r.Context()
//
//	        // Try primary cache first
//	        stream, err := primaryCache.Get(ctx, key, fetcher)
//	        if err != nil {
//	            // Handle primary cache errors with fallback
//	            stream, err = handlePrimaryCacheError(ctx, fallbackCache, fetcher, key, err)
//	            if err != nil {
//	                handleFinalError(w, r, err, key)
//	                return
//	            }
//	        }
//	        defer stream.Close()
//
//	        // Set cache headers based on source
//	        setCacheHeaders(w, err == nil) // true if from primary cache
//
//	        // Stream response
//	        if _, err := io.Copy(w, stream); err != nil {
//	            log.Printf("Failed to stream response: %v", err)
//	        }
//	    }
//	}
//
//	func handlePrimaryCacheError(ctx context.Context, fallbackCache daramjwee.Cache,
//	    fetcher daramjwee.Fetcher, key string, primaryErr error) (io.ReadCloser, error) {
//
//	    switch {
//	    case errors.Is(primaryErr, daramjwee.ErrCacheClosed):
//	        // Primary cache is down, try fallback
//	        log.Printf("Primary cache closed, trying fallback: %s", key)
//	        if fallbackCache != nil {
//	            return fallbackCache.Get(ctx, key, fetcher)
//	        }
//	        // No fallback available, fetch directly from origin
//	        return fetchDirectFromOrigin(ctx, fetcher)
//
//	    case errors.Is(primaryErr, context.DeadlineExceeded):
//	        // Primary cache timeout, try fallback with shorter timeout
//	        log.Printf("Primary cache timeout, trying fallback: %s", key)
//	        if fallbackCache != nil {
//	            shortCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
//	            defer cancel()
//	            return fallbackCache.Get(shortCtx, key, fetcher)
//	        }
//	        return nil, primaryErr
//
//	    default:
//	        // For other errors, return the original error
//	        return nil, primaryErr
//	    }
//	}
//
//	func fetchDirectFromOrigin(ctx context.Context, fetcher daramjwee.Fetcher) (io.ReadCloser, error) {
//	    result, err := fetcher.Fetch(ctx, nil)
//	    if err != nil {
//	        return nil, fmt.Errorf("direct origin fetch failed: %w", err)
//	    }
//	    return result.Body, nil
//	}

// Retry Logic Examples for Transient Failures
//
// These examples demonstrate how to implement retry logic for handling
// transient failures and temporary errors in cache operations.

// Example: Exponential backoff retry for cache operations
//
//	type RetryConfig struct {
//	    MaxRetries    int
//	    BaseDelay     time.Duration
//	    MaxDelay      time.Duration
//	    Multiplier    float64
//	    Jitter        bool
//	}
//
//	func GetWithRetry(ctx context.Context, cache daramjwee.Cache, key string,
//	    fetcher daramjwee.Fetcher, config RetryConfig) (io.ReadCloser, error) {
//
//	    var lastErr error
//	    delay := config.BaseDelay
//
//	    for attempt := 0; attempt <= config.MaxRetries; attempt++ {
//	        if attempt > 0 {
//	            // Wait before retry
//	            select {
//	            case <-time.After(delay):
//	            case <-ctx.Done():
//	                return nil, ctx.Err()
//	            }
//
//	            // Calculate next delay with exponential backoff
//	            delay = time.Duration(float64(delay) * config.Multiplier)
//	            if delay > config.MaxDelay {
//	                delay = config.MaxDelay
//	            }
//
//	            // Add jitter to prevent thundering herd
//	            if config.Jitter {
//	                jitter := time.Duration(rand.Int63n(int64(delay / 4)))
//	                delay += jitter
//	            }
//	        }
//
//	        stream, err := cache.Get(ctx, key, fetcher)
//	        if err == nil {
//	            return stream, nil
//	        }
//
//	        lastErr = err
//
//	        // Check if error is retryable
//	        if !isRetryableError(err) {
//	            break
//	        }
//
//	        log.Printf("Cache operation failed (attempt %d/%d): %v",
//	            attempt+1, config.MaxRetries+1, err)
//	    }
//
//	    return nil, fmt.Errorf("cache operation failed after %d attempts: %w",
//	        config.MaxRetries+1, lastErr)
//	}
//
//	func isRetryableError(err error) bool {
//	    switch {
//	    case errors.Is(err, context.DeadlineExceeded):
//	        return true // Timeout errors are retryable
//	    case errors.Is(err, daramjwee.ErrCacheClosed):
//	        return false // Cache closed is not retryable
//	    case errors.Is(err, daramjwee.ErrNotFound):
//	        return false // Not found is not retryable
//	    case errors.Is(err, daramjwee.ErrCacheableNotFound):
//	        return false // Cacheable not found is not retryable
//	    default:
//	        // For unknown errors, check if it's a temporary network error
//	        return isTemporaryNetworkError(err)
//	    }
//	}
//
//	func isTemporaryNetworkError(err error) bool {
//	    // Check for common temporary network errors
//	    if netErr, ok := err.(net.Error); ok {
//	        return netErr.Temporary()
//	    }
//
//	    // Check for specific error patterns
//	    errStr := err.Error()
//	    temporaryPatterns := []string{
//	        "connection refused",
//	        "connection reset",
//	        "timeout",
//	        "temporary failure",
//	        "service unavailable",
//	    }
//
//	    for _, pattern := range temporaryPatterns {
//	        if strings.Contains(strings.ToLower(errStr), pattern) {
//	            return true
//	        }
//	    }
//
//	    return false
//	}

// Graceful Degradation Examples for Storage Failures
//
// These examples show how to implement graceful degradation when storage
// backends fail, ensuring application availability even during failures.

// Example: Multi-tier graceful degradation
//
//	type GracefulCache struct {
//	    primary   daramjwee.Cache
//	    secondary daramjwee.Cache
//	    fallback  map[string][]byte // In-memory fallback
//	    mu        sync.RWMutex
//	}
//
//	func NewGracefulCache(primary, secondary daramjwee.Cache) *GracefulCache {
//	    return &GracefulCache{
//	        primary:   primary,
//	        secondary: secondary,
//	        fallback:  make(map[string][]byte),
//	    }
//	}
//
//	func (gc *GracefulCache) Get(ctx context.Context, key string, fetcher daramjwee.Fetcher) (io.ReadCloser, error) {
//	    // Try primary cache
//	    if stream, err := gc.tryCache(ctx, gc.primary, key, fetcher, "primary"); err == nil {
//	        return stream, nil
//	    }
//
//	    // Try secondary cache
//	    if gc.secondary != nil {
//	        if stream, err := gc.tryCache(ctx, gc.secondary, key, fetcher, "secondary"); err == nil {
//	            return stream, nil
//	        }
//	    }
//
//	    // Try in-memory fallback
//	    if data := gc.getFallback(key); data != nil {
//	        log.Printf("Serving from fallback cache: %s", key)
//	        return io.NopCloser(bytes.NewReader(data)), nil
//	    }
//
//	    // Last resort: fetch from origin and store in fallback
//	    return gc.fetchAndStoreFallback(ctx, key, fetcher)
//	}
//
//	func (gc *GracefulCache) tryCache(ctx context.Context, cache daramjwee.Cache,
//	    key string, fetcher daramjwee.Fetcher, tier string) (io.ReadCloser, error) {
//
//	    stream, err := cache.Get(ctx, key, fetcher)
//	    if err != nil {
//	        log.Printf("Cache %s failed for key %s: %v", tier, key, err)
//	        return nil, err
//	    }
//
//	    // Store successful result in fallback for future failures
//	    go gc.updateFallback(key, stream)
//
//	    return stream, nil
//	}
//
//	func (gc *GracefulCache) getFallback(key string) []byte {
//	    gc.mu.RLock()
//	    defer gc.mu.RUnlock()
//	    return gc.fallback[key]
//	}
//
//	func (gc *GracefulCache) updateFallback(key string, stream io.ReadCloser) {
//	    defer stream.Close()
//
//	    data, err := io.ReadAll(stream)
//	    if err != nil {
//	        log.Printf("Failed to read stream for fallback: %v", err)
//	        return
//	    }
//
//	    gc.mu.Lock()
//	    defer gc.mu.Unlock()
//
//	    // Limit fallback cache size
//	    if len(gc.fallback) > 1000 {
//	        // Simple eviction: remove random entry
//	        for k := range gc.fallback {
//	            delete(gc.fallback, k)
//	            break
//	        }
//	    }
//
//	    gc.fallback[key] = data
//	}
//
//	func (gc *GracefulCache) fetchAndStoreFallback(ctx context.Context, key string, fetcher daramjwee.Fetcher) (io.ReadCloser, error) {
//	    result, err := fetcher.Fetch(ctx, nil)
//	    if err != nil {
//	        return nil, fmt.Errorf("all cache tiers failed, origin fetch failed: %w", err)
//	    }
//
//	    // Read data to store in fallback
//	    data, err := io.ReadAll(result.Body)
//	    result.Body.Close()
//	    if err != nil {
//	        return nil, fmt.Errorf("failed to read origin data: %w", err)
//	    }
//
//	    // Store in fallback
//	    gc.mu.Lock()
//	    gc.fallback[key] = data
//	    gc.mu.Unlock()
//
//	    return io.NopCloser(bytes.NewReader(data)), nil
//	}

// Monitoring and Alerting Examples for Error Conditions
//
// These examples demonstrate how to implement comprehensive monitoring
// and alerting for cache error conditions in production environments.

// Example: Error metrics collection and alerting
//
//	type ErrorMetrics struct {
//	    notFoundCount       int64
//	    cacheClosedCount    int64
//	    timeoutCount        int64
//	    compressionErrors   int64
//	    configErrors        int64
//	    totalErrors         int64
//	    mu                  sync.RWMutex
//	}
//
//	func NewErrorMetrics() *ErrorMetrics {
//	    return &ErrorMetrics{}
//	}
//
//	func (em *ErrorMetrics) RecordError(err error) {
//	    em.mu.Lock()
//	    defer em.mu.Unlock()
//
//	    em.totalErrors++
//
//	    switch {
//	    case errors.Is(err, daramjwee.ErrNotFound) || errors.Is(err, daramjwee.ErrCacheableNotFound):
//	        em.notFoundCount++
//	    case errors.Is(err, daramjwee.ErrCacheClosed):
//	        em.cacheClosedCount++
//	    case errors.Is(err, context.DeadlineExceeded):
//	        em.timeoutCount++
//	    case errors.Is(err, daramjwee.ErrCompressionFailed) || errors.Is(err, daramjwee.ErrDecompressionFailed):
//	        em.compressionErrors++
//	    case isConfigError(err):
//	        em.configErrors++
//	    }
//	}
//
//	func (em *ErrorMetrics) GetStats() map[string]int64 {
//	    em.mu.RLock()
//	    defer em.mu.RUnlock()
//
//	    return map[string]int64{
//	        "total_errors":       em.totalErrors,
//	        "not_found":          em.notFoundCount,
//	        "cache_closed":       em.cacheClosedCount,
//	        "timeouts":           em.timeoutCount,
//	        "compression_errors": em.compressionErrors,
//	        "config_errors":      em.configErrors,
//	    }
//	}
//
//	func (em *ErrorMetrics) CheckAlerts() []Alert {
//	    stats := em.GetStats()
//	    var alerts []Alert
//
//	    // Alert on high error rates
//	    if stats["total_errors"] > 100 {
//	        alerts = append(alerts, Alert{
//	            Level:   "warning",
//	            Message: fmt.Sprintf("High error rate: %d total errors", stats["total_errors"]),
//	        })
//	    }
//
//	    // Alert on cache closed errors
//	    if stats["cache_closed"] > 0 {
//	        alerts = append(alerts, Alert{
//	            Level:   "critical",
//	            Message: fmt.Sprintf("Cache closed errors detected: %d", stats["cache_closed"]),
//	        })
//	    }
//
//	    // Alert on high timeout rates
//	    if stats["timeouts"] > 50 {
//	        alerts = append(alerts, Alert{
//	            Level:   "warning",
//	            Message: fmt.Sprintf("High timeout rate: %d timeouts", stats["timeouts"]),
//	        })
//	    }
//
//	    return alerts
//	}
//
//	type Alert struct {
//	    Level   string
//	    Message string
//	}
//
//	func isConfigError(err error) bool {
//	    var configErr *daramjwee.ConfigError
//	    return errors.As(err, &configErr)
//	}

// Error Handling Best Practices and Common Pitfalls
//
// Best practices for robust error handling:
//
// 1. Always check for specific error types using errors.Is() or errors.As()
// 2. Implement appropriate fallback mechanisms for each error type
// 3. Log errors with sufficient context for debugging
// 4. Monitor error rates and patterns for operational insights
// 5. Implement circuit breakers for cascading failure prevention
// 6. Use structured logging for better error analysis
// 7. Implement graceful degradation strategies
// 8. Test error handling paths thoroughly
//
// Common pitfalls to avoid:
//
// 1. Ignoring specific error types and treating all errors the same
// 2. Not implementing proper fallback mechanisms
// 3. Insufficient logging or context in error messages
// 4. Not monitoring error rates and patterns
// 5. Blocking operations during error handling
// 6. Not testing error scenarios in development
// 7. Improper resource cleanup during error conditions
// 8. Not implementing timeout and cancellation handling
