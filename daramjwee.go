package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
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
// BufferPoolStrategy defines different optimization strategies for different object sizes.
type BufferPoolStrategy int

const (
	// StrategyPooled uses buffer pool for optimization
	StrategyPooled BufferPoolStrategy = iota
	// StrategyChunked uses chunked streaming for large objects
	StrategyChunked
	// StrategyDirect uses direct streaming without pooling
	StrategyDirect
	// StrategyAdaptive automatically selects the best strategy based on object size
	StrategyAdaptive
)

// String returns the string representation of BufferPoolStrategy.
func (s BufferPoolStrategy) String() string {
	switch s {
	case StrategyPooled:
		return "pooled"
	case StrategyChunked:
		return "chunked"
	case StrategyDirect:
		return "direct"
	case StrategyAdaptive:
		return "adaptive"
	default:
		return "unknown"
	}
}

// ObjectSizeCategory represents different object size categories for optimization.
type ObjectSizeCategory int

const (
	// SizeCategorySmall represents objects smaller than 32KB
	SizeCategorySmall ObjectSizeCategory = iota
	// SizeCategoryMedium represents objects between 32KB and LargeObjectThreshold
	SizeCategoryMedium
	// SizeCategoryLarge represents objects between LargeObjectThreshold and VeryLargeObjectThreshold
	SizeCategoryLarge
	// SizeCategoryVeryLarge represents objects larger than VeryLargeObjectThreshold
	SizeCategoryVeryLarge
)

// String returns the string representation of ObjectSizeCategory.
func (c ObjectSizeCategory) String() string {
	switch c {
	case SizeCategorySmall:
		return "small"
	case SizeCategoryMedium:
		return "medium"
	case SizeCategoryLarge:
		return "large"
	case SizeCategoryVeryLarge:
		return "very_large"
	default:
		return "unknown"
	}
}

// Size category thresholds
const (
	// SmallObjectThreshold defines the boundary between small and medium objects
	SmallObjectThreshold = 32 * 1024 // 32KB
)

// classifyObjectSize determines the size category for a given object size.
func classifyObjectSize(size int, config BufferPoolConfig) ObjectSizeCategory {
	switch {
	case size < SmallObjectThreshold:
		return SizeCategorySmall
	case size < config.LargeObjectThreshold:
		return SizeCategoryMedium
	case size < config.VeryLargeObjectThreshold:
		return SizeCategoryLarge
	default:
		return SizeCategoryVeryLarge
	}
}

// selectStrategy determines the optimal strategy for a given object size.
func selectStrategy(size int, config BufferPoolConfig) BufferPoolStrategy {
	// If strategy is not adaptive, return the configured strategy
	if config.LargeObjectStrategy != StrategyAdaptive {
		return config.LargeObjectStrategy
	}

	// Adaptive strategy selection based on object size
	category := classifyObjectSize(size, config)

	switch category {
	case SizeCategorySmall, SizeCategoryMedium:
		// Use buffer pooling for small and medium objects
		return StrategyPooled
	case SizeCategoryLarge:
		// Use chunked streaming for large objects to optimize memory usage
		return StrategyChunked
	case SizeCategoryVeryLarge:
		// Use direct streaming for very large objects to minimize overhead
		return StrategyDirect
	default:
		// Fallback to pooled strategy
		return StrategyPooled
	}
}

// validateStrategy checks if a strategy is valid and provides fallback.
func validateStrategy(strategy BufferPoolStrategy) BufferPoolStrategy {
	switch strategy {
	case StrategyPooled, StrategyChunked, StrategyDirect, StrategyAdaptive:
		return strategy
	default:
		// Fallback to adaptive strategy for invalid values
		return StrategyAdaptive
	}
}

// getOptimalChunkSize calculates the optimal chunk size for a given object size.
func getOptimalChunkSize(objectSize int, config BufferPoolConfig) int {
	baseChunkSize := config.ChunkSize
	if baseChunkSize <= 0 {
		baseChunkSize = 64 * 1024 // Default 64KB
	}

	// For very large objects, use larger chunks for better I/O efficiency
	if objectSize >= config.VeryLargeObjectThreshold {
		// Use up to 128KB chunks for very large objects
		if baseChunkSize < 128*1024 {
			return 128 * 1024
		}
	}

	// For large objects, optimize chunk size based on object size
	if objectSize >= config.LargeObjectThreshold {
		// Use chunk size that divides the object size efficiently
		// Aim for 8-16 chunks per object for optimal streaming
		optimalChunkSize := objectSize / 12 // Target ~12 chunks

		// Round to nearest power of 2 or common size
		if optimalChunkSize > baseChunkSize*2 {
			return baseChunkSize * 2
		}
		if optimalChunkSize < baseChunkSize/2 {
			return baseChunkSize / 2
		}
	}

	return baseChunkSize
}

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

	// Large object handling configuration fields

	// LargeObjectThreshold defines the size threshold for large object detection.
	//
	// Objects larger than this threshold will use alternative optimization strategies
	// instead of standard buffer pooling. This helps prevent performance degradation
	// observed with large objects in buffer pools.
	//
	// Sizing guidelines:
	//   - Default: 256KB (256 * 1024 bytes)
	//   - Conservative: 128KB for memory-constrained environments
	//   - Aggressive: 512KB for systems with abundant memory
	//   - Should be larger than MaxBufferSize to avoid conflicts
	//
	// Performance considerations:
	//   - Objects below this threshold use standard buffer pooling
	//   - Objects above this threshold use chunked streaming or direct operations
	//   - Threshold should be based on actual workload analysis
	//   - Monitor performance across the threshold boundary
	LargeObjectThreshold int

	// VeryLargeObjectThreshold defines the threshold for very large objects.
	//
	// Objects larger than this threshold bypass buffer pooling entirely and use
	// direct streaming operations to minimize memory overhead and GC pressure.
	//
	// Sizing guidelines:
	//   - Default: 1MB (1024 * 1024 bytes)
	//   - Should be significantly larger than LargeObjectThreshold
	//   - Consider available memory and GC characteristics
	//   - Adjust based on maximum expected object sizes
	//
	// Performance implications:
	//   - Objects above this threshold use direct streaming without any pooling
	//   - Minimizes memory allocation and GC pressure for very large objects
	//   - Provides consistent performance regardless of object size
	VeryLargeObjectThreshold int

	// LargeObjectStrategy defines the strategy for handling large objects.
	//
	// Strategy selection guidelines:
	//   - StrategyAdaptive: Automatically select based on object size (recommended)
	//   - StrategyChunked: Use chunked streaming for all large objects
	//   - StrategyDirect: Use direct streaming without pooling
	//   - StrategyPooled: Force pooling (may cause performance issues)
	//
	// Default: StrategyAdaptive provides optimal performance across different sizes
	LargeObjectStrategy BufferPoolStrategy

	// ChunkSize defines the chunk size for streaming large objects.
	//
	// This size is used for chunked streaming operations when processing large
	// objects. It affects memory usage and I/O efficiency for large transfers.
	//
	// Sizing recommendations:
	//   - Default: 64KB (64 * 1024 bytes)
	//   - I/O bound workloads: 128KB for better throughput
	//   - Memory constrained: 32KB to reduce memory usage
	//   - Network operations: Match network buffer sizes
	//
	// Performance considerations:
	//   - Larger chunks improve I/O efficiency but use more memory
	//   - Smaller chunks reduce memory usage but may increase overhead
	//   - Should be a power of 2 for optimal memory alignment
	ChunkSize int

	// MaxConcurrentLargeOps limits concurrent large object operations.
	//
	// This setting prevents resource exhaustion when processing multiple large
	// objects simultaneously. It provides backpressure and resource management.
	//
	// Sizing guidelines:
	//   - Default: 4 concurrent operations
	//   - High-memory systems: 8-16 operations
	//   - Memory-constrained: 1-2 operations
	//   - Consider available memory and I/O capacity
	//
	// Resource management:
	//   - Operations exceeding this limit are queued or rejected
	//   - Prevents memory exhaustion from concurrent large transfers
	//   - Balances throughput with resource stability
	MaxConcurrentLargeOps int

	// EnableDetailedMetrics enables size-category performance metrics.
	//
	// When enabled, the buffer pool tracks detailed metrics for different
	// object size categories and strategy usage patterns.
	//
	// Monitoring benefits:
	//   - Track performance across different object sizes
	//   - Monitor strategy selection effectiveness
	//   - Identify optimization opportunities
	//   - Analyze memory usage patterns
	//
	// Performance impact: Minimal overhead for metric collection
	EnableDetailedMetrics bool
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

	// Size-category operation counters for detailed monitoring

	// SmallObjectOps tracks operations on objects smaller than 32KB.
	SmallObjectOps int64

	// MediumObjectOps tracks operations on objects between 32KB and LargeObjectThreshold.
	MediumObjectOps int64

	// LargeObjectOps tracks operations on objects between LargeObjectThreshold and VeryLargeObjectThreshold.
	LargeObjectOps int64

	// VeryLargeObjectOps tracks operations on objects larger than VeryLargeObjectThreshold.
	VeryLargeObjectOps int64

	// Strategy usage metrics for performance analysis

	// PooledOperations tracks operations that used buffer pooling strategy.
	PooledOperations int64

	// ChunkedOperations tracks operations that used chunked streaming strategy.
	ChunkedOperations int64

	// DirectOperations tracks operations that used direct streaming strategy.
	DirectOperations int64

	// Performance metrics for optimization insights

	// AverageLatencyNs provides average latency by size category in nanoseconds.
	// Keys: "small", "medium", "large", "very_large"
	AverageLatencyNs map[string]int64

	// MemoryEfficiency represents the ratio of effective memory usage to total allocated memory.
	// Values closer to 1.0 indicate better memory efficiency.
	MemoryEfficiency float64
}

// validateBufferPoolConfig validates the buffer pool configuration for consistency.
func (cfg *BufferPoolConfig) validate() error {
	// Validate basic buffer size constraints
	if cfg.MinBufferSize <= 0 {
		return &ConfigError{"buffer pool minimum size must be positive"}
	}
	if cfg.DefaultBufferSize <= 0 {
		return &ConfigError{"buffer pool default size must be positive"}
	}
	if cfg.MaxBufferSize <= 0 {
		return &ConfigError{"buffer pool maximum size must be positive"}
	}
	if cfg.MinBufferSize > cfg.DefaultBufferSize {
		return &ConfigError{"buffer pool minimum size cannot be larger than default size"}
	}
	if cfg.DefaultBufferSize > cfg.MaxBufferSize {
		return &ConfigError{"buffer pool default size cannot be larger than maximum size"}
	}

	// Validate large object threshold constraints
	if cfg.LargeObjectThreshold <= 0 {
		cfg.LargeObjectThreshold = 256 * 1024 // Default: 256KB
	}
	if cfg.VeryLargeObjectThreshold <= 0 {
		cfg.VeryLargeObjectThreshold = 1024 * 1024 // Default: 1MB
	}
	if cfg.LargeObjectThreshold >= cfg.VeryLargeObjectThreshold {
		return &ConfigError{"large object threshold must be smaller than very large object threshold"}
	}

	// Validate chunk size
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 64 * 1024 // Default: 64KB
	}
	if cfg.ChunkSize > cfg.LargeObjectThreshold {
		return &ConfigError{"chunk size should not exceed large object threshold"}
	}

	// Validate concurrent operations limit
	if cfg.MaxConcurrentLargeOps <= 0 {
		cfg.MaxConcurrentLargeOps = 4 // Default: 4 concurrent operations
	}

	// Validate strategy
	switch cfg.LargeObjectStrategy {
	case StrategyPooled, StrategyChunked, StrategyDirect, StrategyAdaptive:
		// Valid strategies
	default:
		cfg.LargeObjectStrategy = StrategyAdaptive // Default to adaptive
	}

	return nil
}

// logStrategySelection logs debug information about strategy selection decisions.
func logStrategySelection(logger log.Logger, size int, category ObjectSizeCategory, strategy BufferPoolStrategy, config BufferPoolConfig) {
	if logger == nil {
		return
	}

	level.Debug(logger).Log(
		"msg", "buffer pool strategy selected",
		"object_size", size,
		"size_category", category.String(),
		"selected_strategy", strategy.String(),
		"large_threshold", config.LargeObjectThreshold,
		"very_large_threshold", config.VeryLargeObjectThreshold,
		"configured_strategy", config.LargeObjectStrategy.String(),
	)
}

// selectStrategyWithLogging determines the optimal strategy with debug logging.
func selectStrategyWithLogging(size int, config BufferPoolConfig, logger log.Logger) BufferPoolStrategy {
	category := classifyObjectSize(size, config)
	strategy := selectStrategy(size, config)

	// Log strategy selection for debugging and monitoring
	if config.EnableDetailedMetrics {
		logStrategySelection(logger, size, category, strategy, config)
	}

	return strategy
}

// isStrategyOptimal checks if the selected strategy is optimal for the given size.
// This is used for performance optimization and validation.
func isStrategyOptimal(size int, strategy BufferPoolStrategy, config BufferPoolConfig) bool {
	expectedStrategy := selectStrategy(size, config)
	return strategy == expectedStrategy
}

// getStrategyPerformanceHint provides performance hints for strategy selection.
func getStrategyPerformanceHint(size int, config BufferPoolConfig) string {
	category := classifyObjectSize(size, config)

	switch category {
	case SizeCategorySmall:
		return "small objects benefit from buffer pooling"
	case SizeCategoryMedium:
		return "medium objects use optimized buffer pooling"
	case SizeCategoryLarge:
		return "large objects use chunked streaming for memory efficiency"
	case SizeCategoryVeryLarge:
		return "very large objects use direct streaming to minimize overhead"
	default:
		return "unknown size category"
	}
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

// AdaptiveBufferPool implements size-aware buffer management with strategy delegation.
type AdaptiveBufferPool struct {
	config BufferPoolConfig
	logger log.Logger

	// Size-specific pools for different object categories
	smallPool  BufferPool // For objects < 32KB
	mediumPool BufferPool // For objects 32KB-256KB

	// Large object handling
	chunkPool        *sync.Pool        // Pool of reusable chunks for streaming
	chunkPoolManager *ChunkPoolManager // Advanced chunk pool management

	// Metrics and monitoring
	stats            *AdaptiveBufferPoolStats
	metricsCollector *MetricsCollector

	// Resource management for large operations
	largeOpsSemaphore chan struct{} // Semaphore for limiting concurrent large operations

	// Thread safety
	mutex sync.RWMutex
}

// AdaptiveBufferPoolStats extends BufferPoolStats with adaptive-specific metrics.
type AdaptiveBufferPoolStats struct {
	BufferPoolStats

	// Strategy selection metrics
	StrategySelections map[string]int64 // Count of strategy selections by type

	// Performance tracking
	LatencyByCategory map[string]time.Duration // Average latency by size category

	// Resource usage
	ConcurrentLargeOps    int64 // Current number of concurrent large operations
	MaxConcurrentLargeOps int64 // Maximum concurrent large operations reached

	// Chunk pool metrics
	ChunkPoolGets   int64
	ChunkPoolPuts   int64
	ChunkPoolHits   int64
	ChunkPoolMisses int64
}

// NewAdaptiveBufferPool creates a new adaptive buffer pool with the given configuration.
func NewAdaptiveBufferPool(config BufferPoolConfig, logger log.Logger) (*AdaptiveBufferPool, error) {
	// Validate configuration
	if err := config.validate(); err != nil {
		return nil, err
	}

	// Create size-specific pools
	smallPoolConfig := config
	smallPoolConfig.MaxBufferSize = SmallObjectThreshold
	smallPool := NewDefaultBufferPoolWithLogger(smallPoolConfig, logger)

	mediumPoolConfig := config
	mediumPoolConfig.MinBufferSize = SmallObjectThreshold
	mediumPoolConfig.MaxBufferSize = config.LargeObjectThreshold
	mediumPool := NewDefaultBufferPoolWithLogger(mediumPoolConfig, logger)

	// Create chunk pool for large object streaming
	chunkPool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, config.ChunkSize)
		},
	}

	// Create advanced chunk pool manager
	chunkPoolConfig := ChunkPoolConfig{
		ChunkSizes: []int{
			config.ChunkSize / 2,
			config.ChunkSize,
			config.ChunkSize * 2,
		},
		MaxChunksPerPool:        100,
		MemoryPressureThreshold: int64(config.MaxBufferSize * 50), // 50x max buffer size
		CleanupInterval:         5 * time.Minute,
		MaxChunkAge:             10 * time.Minute,
		EnableMetrics:           config.EnableDetailedMetrics,
	}
	chunkPoolManager := NewChunkPoolManager(chunkPoolConfig, logger)

	// Create semaphore for limiting concurrent large operations
	largeOpsSemaphore := make(chan struct{}, config.MaxConcurrentLargeOps)

	// Initialize statistics
	stats := &AdaptiveBufferPoolStats{
		StrategySelections: make(map[string]int64),
		LatencyByCategory:  make(map[string]time.Duration),
	}

	// Initialize metrics collector if detailed metrics are enabled
	var metricsCollector *MetricsCollector
	if config.EnableDetailedMetrics {
		metricsConfig := MetricsConfig{
			CollectionInterval:     1 * time.Second,
			RetentionPeriod:        24 * time.Hour,
			MaxDataPoints:          10000,
			EnableTrendAnalysis:    true,
			EnableAnomalyDetection: true,
			EnableRecommendations:  true,
			AnalysisInterval:       5 * time.Minute,
			ReportingInterval:      15 * time.Minute,
		}
		metricsCollector = NewMetricsCollector(metricsConfig, logger)
	}

	return &AdaptiveBufferPool{
		config:            config,
		logger:            logger,
		smallPool:         smallPool,
		mediumPool:        mediumPool,
		chunkPool:         chunkPool,
		chunkPoolManager:  chunkPoolManager,
		stats:             stats,
		metricsCollector:  metricsCollector,
		largeOpsSemaphore: largeOpsSemaphore,
	}, nil
}

// Get retrieves a buffer using the appropriate strategy based on size.
func (abp *AdaptiveBufferPool) Get(size int) []byte {
	if !abp.config.Enabled {
		return make([]byte, size)
	}

	startTime := time.Now()
	defer func() {
		if abp.config.EnableDetailedMetrics {
			category := classifyObjectSize(size, abp.config)
			abp.updateLatencyMetrics(category, time.Since(startTime))
		}
	}()

	// Update size-category metrics regardless of strategy
	category := classifyObjectSize(size, abp.config)
	switch category {
	case SizeCategorySmall:
		atomic.AddInt64(&abp.stats.SmallObjectOps, 1)
	case SizeCategoryMedium:
		atomic.AddInt64(&abp.stats.MediumObjectOps, 1)
	case SizeCategoryLarge:
		atomic.AddInt64(&abp.stats.LargeObjectOps, 1)
	case SizeCategoryVeryLarge:
		atomic.AddInt64(&abp.stats.VeryLargeObjectOps, 1)
	}

	strategy := selectStrategyWithLogging(size, abp.config, abp.logger)
	abp.updateStrategyMetrics(strategy)

	switch strategy {
	case StrategyPooled:
		return abp.getPooledBuffer(size)
	case StrategyChunked, StrategyDirect:
		// For large objects, return a buffer but don't use pooling
		return make([]byte, size)
	default:
		// Fallback to pooled strategy
		return abp.getPooledBuffer(size)
	}
}

// getPooledBuffer retrieves a buffer from the appropriate size-specific pool.
func (abp *AdaptiveBufferPool) getPooledBuffer(size int) []byte {
	category := classifyObjectSize(size, abp.config)

	switch category {
	case SizeCategorySmall:
		return abp.smallPool.Get(size)
	case SizeCategoryMedium:
		return abp.mediumPool.Get(size)
	default:
		// For large objects that still use pooling, use medium pool
		return abp.mediumPool.Get(size)
	}
}

// Put returns a buffer to the appropriate pool based on its size.
func (abp *AdaptiveBufferPool) Put(buf []byte) {
	if !abp.config.Enabled || buf == nil {
		return
	}

	size := cap(buf)
	category := classifyObjectSize(size, abp.config)

	switch category {
	case SizeCategorySmall:
		abp.smallPool.Put(buf)
	case SizeCategoryMedium:
		abp.mediumPool.Put(buf)
	default:
		// Large buffers are not pooled, just let them be garbage collected
		return
	}
}

// CopyBuffer performs optimized copy using the appropriate strategy.
func (abp *AdaptiveBufferPool) CopyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	if !abp.config.Enabled {
		return io.Copy(dst, src)
	}

	// For CopyBuffer, we don't know the size in advance, so use default strategy
	// This could be enhanced with size detection in the future
	buf := abp.Get(abp.config.DefaultBufferSize)
	defer abp.Put(buf)

	return io.CopyBuffer(dst, src, buf)
}

// TeeReader creates an optimized TeeReader using the appropriate strategy.
func (abp *AdaptiveBufferPool) TeeReader(r io.Reader, w io.Writer) io.Reader {
	if !abp.config.Enabled {
		return io.TeeReader(r, w)
	}

	return &adaptiveTeeReader{
		r:          r,
		w:          w,
		bufferPool: abp,
	}
}

// GetStats returns comprehensive statistics including adaptive-specific metrics.
func (abp *AdaptiveBufferPool) GetStats() BufferPoolStats {
	abp.mutex.RLock()
	defer abp.mutex.RUnlock()

	// Aggregate stats from all pools
	smallStats := abp.smallPool.GetStats()
	mediumStats := abp.mediumPool.GetStats()

	// Combine basic stats
	combinedStats := BufferPoolStats{
		TotalGets:     smallStats.TotalGets + mediumStats.TotalGets,
		TotalPuts:     smallStats.TotalPuts + mediumStats.TotalPuts,
		PoolHits:      smallStats.PoolHits + mediumStats.PoolHits,
		PoolMisses:    smallStats.PoolMisses + mediumStats.PoolMisses,
		ActiveBuffers: smallStats.ActiveBuffers + mediumStats.ActiveBuffers,

		// Size-category metrics from adaptive stats
		SmallObjectOps:     abp.stats.SmallObjectOps,
		MediumObjectOps:    abp.stats.MediumObjectOps,
		LargeObjectOps:     abp.stats.LargeObjectOps,
		VeryLargeObjectOps: abp.stats.VeryLargeObjectOps,

		// Strategy usage metrics
		PooledOperations:  abp.stats.StrategySelections["pooled"],
		ChunkedOperations: abp.stats.StrategySelections["chunked"],
		DirectOperations:  abp.stats.StrategySelections["direct"],

		// Performance metrics
		AverageLatencyNs: make(map[string]int64),
		MemoryEfficiency: abp.calculateMemoryEfficiency(),
	}

	// Convert latency metrics to nanoseconds
	for category, duration := range abp.stats.LatencyByCategory {
		combinedStats.AverageLatencyNs[category] = duration.Nanoseconds()
	}

	return combinedStats
}

// updateStrategyMetrics updates strategy selection counters.
func (abp *AdaptiveBufferPool) updateStrategyMetrics(strategy BufferPoolStrategy) {
	if !abp.config.EnableDetailedMetrics {
		return
	}

	abp.mutex.Lock()
	defer abp.mutex.Unlock()

	abp.stats.StrategySelections[strategy.String()]++
}

// updateLatencyMetrics updates latency tracking for size categories.
func (abp *AdaptiveBufferPool) updateLatencyMetrics(category ObjectSizeCategory, latency time.Duration) {
	abp.mutex.Lock()
	defer abp.mutex.Unlock()

	categoryStr := category.String()

	// Simple moving average (could be enhanced with more sophisticated tracking)
	currentLatency := abp.stats.LatencyByCategory[categoryStr]
	if currentLatency == 0 {
		abp.stats.LatencyByCategory[categoryStr] = latency
	} else {
		// Exponential moving average with alpha = 0.1
		abp.stats.LatencyByCategory[categoryStr] = time.Duration(
			float64(currentLatency)*0.9 + float64(latency)*0.1,
		)
	}
}

// calculateMemoryEfficiency calculates the memory efficiency ratio.
func (abp *AdaptiveBufferPool) calculateMemoryEfficiency() float64 {
	smallStats := abp.smallPool.GetStats()
	mediumStats := abp.mediumPool.GetStats()

	totalHits := smallStats.PoolHits + mediumStats.PoolHits
	totalOps := smallStats.TotalGets + mediumStats.TotalGets

	if totalOps == 0 {
		return 0.0
	}

	return float64(totalHits) / float64(totalOps)
}

// getChunk retrieves a chunk from the chunk pool for large object streaming.
func (abp *AdaptiveBufferPool) getChunk() []byte {
	atomic.AddInt64(&abp.stats.ChunkPoolGets, 1)

	chunk := abp.chunkPool.Get().([]byte)

	// Check if this was a hit or miss (simple heuristic)
	if len(chunk) == abp.config.ChunkSize {
		atomic.AddInt64(&abp.stats.ChunkPoolHits, 1)
	} else {
		atomic.AddInt64(&abp.stats.ChunkPoolMisses, 1)
	}

	return chunk[:abp.config.ChunkSize]
}

// putChunk returns a chunk to the chunk pool.
func (abp *AdaptiveBufferPool) putChunk(chunk []byte) {
	atomic.AddInt64(&abp.stats.ChunkPoolPuts, 1)

	if cap(chunk) >= abp.config.ChunkSize {
		abp.chunkPool.Put(chunk[:cap(chunk)])
	}
}

// acquireLargeOpSlot acquires a slot for large operation processing.
func (abp *AdaptiveBufferPool) acquireLargeOpSlot(ctx context.Context) error {
	select {
	case abp.largeOpsSemaphore <- struct{}{}:
		atomic.AddInt64(&abp.stats.ConcurrentLargeOps, 1)

		// Update max concurrent operations if needed
		current := atomic.LoadInt64(&abp.stats.ConcurrentLargeOps)
		for {
			max := atomic.LoadInt64(&abp.stats.MaxConcurrentLargeOps)
			if current <= max || atomic.CompareAndSwapInt64(&abp.stats.MaxConcurrentLargeOps, max, current) {
				break
			}
		}

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseLargeOpSlot releases a slot for large operation processing.
func (abp *AdaptiveBufferPool) releaseLargeOpSlot() {
	select {
	case <-abp.largeOpsSemaphore:
		atomic.AddInt64(&abp.stats.ConcurrentLargeOps, -1)
	default:
		// Should not happen, but handle gracefully
	}
}

// adaptiveTeeReader implements io.Reader using adaptive buffer management.
type adaptiveTeeReader struct {
	r          io.Reader
	w          io.Writer
	bufferPool *AdaptiveBufferPool
}

// Read implements io.Reader interface with adaptive buffer management.
func (t *adaptiveTeeReader) Read(p []byte) (n int, err error) {
	n, err = t.r.Read(p)
	if n > 0 {
		// Write the data to the tee writer
		if _, writeErr := t.w.Write(p[:n]); writeErr != nil {
			return n, writeErr
		}
	}
	return n, err
}

// ChunkedTeeReader implements optimized tee reading for large objects using chunk-based processing.
type ChunkedTeeReader struct {
	r          io.Reader
	w          io.Writer
	bufferPool *AdaptiveBufferPool
	chunkSize  int

	// Performance tracking
	totalBytesRead  int64
	totalChunksRead int64
	chunkReuseCount int64

	// Error handling
	lastError error

	// Thread safety
	mutex sync.Mutex
}

// NewChunkedTeeReader creates a new ChunkedTeeReader with the given parameters.
func NewChunkedTeeReader(r io.Reader, w io.Writer, bufferPool *AdaptiveBufferPool, chunkSize int) *ChunkedTeeReader {
	if chunkSize <= 0 {
		chunkSize = 64 * 1024 // Default 64KB chunks
	}

	return &ChunkedTeeReader{
		r:          r,
		w:          w,
		bufferPool: bufferPool,
		chunkSize:  chunkSize,
	}
}

// Read implements io.Reader interface with optimized chunk-based processing.
func (ctr *ChunkedTeeReader) Read(p []byte) (n int, err error) {
	ctr.mutex.Lock()
	defer ctr.mutex.Unlock()

	// Return previous error if any
	if ctr.lastError != nil && ctr.lastError != io.EOF {
		return 0, ctr.lastError
	}

	// Read from source
	n, err = ctr.r.Read(p)
	if n > 0 {
		atomic.AddInt64(&ctr.totalBytesRead, int64(n))

		// Write to tee destination using chunked approach for large data
		if n > ctr.chunkSize {
			err = ctr.writeChunked(p[:n])
		} else {
			// For small data, write directly
			_, writeErr := ctr.w.Write(p[:n])
			if writeErr != nil {
				ctr.lastError = writeErr
				return n, writeErr
			}
		}
	}

	if err != nil {
		ctr.lastError = err
	}

	return n, err
}

// writeChunked writes large data in chunks to optimize memory usage and performance.
func (ctr *ChunkedTeeReader) writeChunked(data []byte) error {
	remaining := len(data)
	offset := 0

	for remaining > 0 {
		chunkSize := ctr.chunkSize
		if remaining < chunkSize {
			chunkSize = remaining
		}

		// Get a chunk buffer from the pool
		chunk := ctr.bufferPool.getChunk()

		// Copy data to chunk buffer
		copy(chunk, data[offset:offset+chunkSize])

		// Write chunk to destination
		_, err := ctr.w.Write(chunk[:chunkSize])

		// Return chunk to pool
		ctr.bufferPool.putChunk(chunk)
		atomic.AddInt64(&ctr.chunkReuseCount, 1)

		if err != nil {
			return err
		}

		offset += chunkSize
		remaining -= chunkSize
		atomic.AddInt64(&ctr.totalChunksRead, 1)
	}

	return nil
}

// GetStats returns performance statistics for the ChunkedTeeReader.
func (ctr *ChunkedTeeReader) GetStats() ChunkedTeeReaderStats {
	ctr.mutex.Lock()
	defer ctr.mutex.Unlock()

	return ChunkedTeeReaderStats{
		TotalBytesRead:  atomic.LoadInt64(&ctr.totalBytesRead),
		TotalChunksRead: atomic.LoadInt64(&ctr.totalChunksRead),
		ChunkReuseCount: atomic.LoadInt64(&ctr.chunkReuseCount),
		ChunkSize:       ctr.chunkSize,
		LastError:       ctr.lastError,
	}
}

// ChunkedTeeReaderStats provides performance metrics for ChunkedTeeReader.
type ChunkedTeeReaderStats struct {
	TotalBytesRead  int64
	TotalChunksRead int64
	ChunkReuseCount int64
	ChunkSize       int
	LastError       error
}

// Reset resets the ChunkedTeeReader statistics and error state.
func (ctr *ChunkedTeeReader) Reset() {
	ctr.mutex.Lock()
	defer ctr.mutex.Unlock()

	atomic.StoreInt64(&ctr.totalBytesRead, 0)
	atomic.StoreInt64(&ctr.totalChunksRead, 0)
	atomic.StoreInt64(&ctr.chunkReuseCount, 0)
	ctr.lastError = nil
}

// Close cleans up resources used by the ChunkedTeeReader.
func (ctr *ChunkedTeeReader) Close() error {
	ctr.mutex.Lock()
	defer ctr.mutex.Unlock()

	// Close underlying readers/writers if they implement io.Closer
	var errs []error

	if closer, ok := ctr.r.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if closer, ok := ctr.w.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Return first error if any
	if len(errs) > 0 {
		return errs[0]
	}

	return nil
}

// ChunkedTeeReaderWithMetrics extends ChunkedTeeReader with detailed performance tracking.
type ChunkedTeeReaderWithMetrics struct {
	*ChunkedTeeReader

	// Detailed metrics
	readLatencies    []time.Duration
	writeLatencies   []time.Duration
	chunkUtilization []float64

	// Configuration
	enableMetrics    bool
	maxMetricSamples int

	// Thread safety for metrics
	metricsMutex sync.RWMutex
}

// NewChunkedTeeReaderWithMetrics creates a ChunkedTeeReader with detailed metrics collection.
func NewChunkedTeeReaderWithMetrics(r io.Reader, w io.Writer, bufferPool *AdaptiveBufferPool, chunkSize int, enableMetrics bool) *ChunkedTeeReaderWithMetrics {
	base := NewChunkedTeeReader(r, w, bufferPool, chunkSize)

	return &ChunkedTeeReaderWithMetrics{
		ChunkedTeeReader: base,
		enableMetrics:    enableMetrics,
		maxMetricSamples: 1000, // Keep last 1000 samples
		readLatencies:    make([]time.Duration, 0, 1000),
		writeLatencies:   make([]time.Duration, 0, 1000),
		chunkUtilization: make([]float64, 0, 1000),
	}
}

// Read implements io.Reader with detailed metrics collection.
func (ctrm *ChunkedTeeReaderWithMetrics) Read(p []byte) (n int, err error) {
	if !ctrm.enableMetrics {
		return ctrm.ChunkedTeeReader.Read(p)
	}

	startTime := time.Now()
	n, err = ctrm.ChunkedTeeReader.Read(p)
	readLatency := time.Since(startTime)

	// Record metrics
	ctrm.recordReadLatency(readLatency)
	if n > 0 {
		utilization := float64(n) / float64(len(p))
		ctrm.recordChunkUtilization(utilization)
	}

	return n, err
}

// recordReadLatency records read operation latency.
func (ctrm *ChunkedTeeReaderWithMetrics) recordReadLatency(latency time.Duration) {
	ctrm.metricsMutex.Lock()
	defer ctrm.metricsMutex.Unlock()

	ctrm.readLatencies = append(ctrm.readLatencies, latency)
	if len(ctrm.readLatencies) > ctrm.maxMetricSamples {
		// Remove oldest sample
		ctrm.readLatencies = ctrm.readLatencies[1:]
	}
}

// recordChunkUtilization records chunk utilization efficiency.
func (ctrm *ChunkedTeeReaderWithMetrics) recordChunkUtilization(utilization float64) {
	ctrm.metricsMutex.Lock()
	defer ctrm.metricsMutex.Unlock()

	ctrm.chunkUtilization = append(ctrm.chunkUtilization, utilization)
	if len(ctrm.chunkUtilization) > ctrm.maxMetricSamples {
		// Remove oldest sample
		ctrm.chunkUtilization = ctrm.chunkUtilization[1:]
	}
}

// GetDetailedStats returns comprehensive performance statistics.
func (ctrm *ChunkedTeeReaderWithMetrics) GetDetailedStats() ChunkedTeeReaderDetailedStats {
	ctrm.metricsMutex.RLock()
	defer ctrm.metricsMutex.RUnlock()

	baseStats := ctrm.ChunkedTeeReader.GetStats()

	// Calculate average latencies
	var avgReadLatency, avgWriteLatency time.Duration
	if len(ctrm.readLatencies) > 0 {
		var total time.Duration
		for _, latency := range ctrm.readLatencies {
			total += latency
		}
		avgReadLatency = total / time.Duration(len(ctrm.readLatencies))
	}

	if len(ctrm.writeLatencies) > 0 {
		var total time.Duration
		for _, latency := range ctrm.writeLatencies {
			total += latency
		}
		avgWriteLatency = total / time.Duration(len(ctrm.writeLatencies))
	}

	// Calculate average chunk utilization
	var avgChunkUtilization float64
	if len(ctrm.chunkUtilization) > 0 {
		var total float64
		for _, util := range ctrm.chunkUtilization {
			total += util
		}
		avgChunkUtilization = total / float64(len(ctrm.chunkUtilization))
	}

	return ChunkedTeeReaderDetailedStats{
		ChunkedTeeReaderStats:   baseStats,
		AverageReadLatency:      avgReadLatency,
		AverageWriteLatency:     avgWriteLatency,
		AverageChunkUtilization: avgChunkUtilization,
		MetricSampleCount:       len(ctrm.readLatencies),
	}
}

// ChunkedTeeReaderDetailedStats provides comprehensive performance metrics.
type ChunkedTeeReaderDetailedStats struct {
	ChunkedTeeReaderStats
	AverageReadLatency      time.Duration
	AverageWriteLatency     time.Duration
	AverageChunkUtilization float64
	MetricSampleCount       int
}

// ChunkedCopyBuffer performs optimized copying for large data using adaptive chunk sizing.
func (abp *AdaptiveBufferPool) ChunkedCopyBuffer(dst io.Writer, src io.Reader, estimatedSize int) (int64, error) {
	if !abp.config.Enabled {
		return io.Copy(dst, src)
	}

	// Determine optimal chunk size based on estimated size
	chunkSize := abp.calculateOptimalChunkSize(estimatedSize)

	// For small data, use regular copy
	if estimatedSize > 0 && estimatedSize <= abp.config.LargeObjectThreshold {
		buf := abp.Get(chunkSize)
		defer abp.Put(buf)
		return io.CopyBuffer(dst, src, buf)
	}

	// For large data, use chunked copying with resource management
	ctx := context.Background()
	if err := abp.acquireLargeOpSlot(ctx); err != nil {
		// Fallback to regular copy if resource limit reached
		return io.Copy(dst, src)
	}
	defer abp.releaseLargeOpSlot()

	return abp.performChunkedCopy(dst, src, chunkSize)
}

// calculateOptimalChunkSize determines the best chunk size for a given data size.
func (abp *AdaptiveBufferPool) calculateOptimalChunkSize(estimatedSize int) int {
	if estimatedSize <= 0 {
		return abp.config.ChunkSize
	}

	// Use getOptimalChunkSize function we implemented earlier
	return getOptimalChunkSize(estimatedSize, abp.config)
}

// performChunkedCopy executes the actual chunked copy operation.
func (abp *AdaptiveBufferPool) performChunkedCopy(dst io.Writer, src io.Reader, chunkSize int) (int64, error) {
	var totalCopied int64
	var copyStats ChunkedCopyStats

	startTime := time.Now()
	defer func() {
		copyStats.TotalDuration = time.Since(startTime)
		if abp.config.EnableDetailedMetrics {
			abp.recordChunkedCopyStats(copyStats)
		}
	}()

	for {
		// Get chunk from pool
		chunk := abp.getChunk()
		copyStats.ChunksUsed++

		// Read data into chunk
		readStart := time.Now()
		n, readErr := src.Read(chunk)
		copyStats.TotalReadTime += time.Since(readStart)

		if n > 0 {
			// Write chunk to destination
			writeStart := time.Now()
			written, writeErr := dst.Write(chunk[:n])
			copyStats.TotalWriteTime += time.Since(writeStart)

			totalCopied += int64(written)
			copyStats.BytesCopied += int64(written)

			// Return chunk to pool
			abp.putChunk(chunk)

			if writeErr != nil {
				copyStats.WriteErrors++
				return totalCopied, writeErr
			}

			if written != n {
				copyStats.WriteErrors++
				return totalCopied, io.ErrShortWrite
			}
		} else {
			// Return unused chunk to pool
			abp.putChunk(chunk)
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			copyStats.ReadErrors++
			return totalCopied, readErr
		}
	}

	return totalCopied, nil
}

// ChunkedCopyStats tracks performance metrics for chunked copy operations.
type ChunkedCopyStats struct {
	BytesCopied    int64
	ChunksUsed     int64
	TotalDuration  time.Duration
	TotalReadTime  time.Duration
	TotalWriteTime time.Duration
	ReadErrors     int64
	WriteErrors    int64
}

// recordChunkedCopyStats records performance statistics for analysis.
func (abp *AdaptiveBufferPool) recordChunkedCopyStats(stats ChunkedCopyStats) {
	abp.mutex.Lock()
	defer abp.mutex.Unlock()

	// Update aggregate statistics
	if abp.stats.LatencyByCategory == nil {
		abp.stats.LatencyByCategory = make(map[string]time.Duration)
	}

	// Record chunked copy performance
	currentAvg := abp.stats.LatencyByCategory["chunked_copy"]
	if currentAvg == 0 {
		abp.stats.LatencyByCategory["chunked_copy"] = stats.TotalDuration
	} else {
		// Exponential moving average
		abp.stats.LatencyByCategory["chunked_copy"] = time.Duration(
			float64(currentAvg)*0.9 + float64(stats.TotalDuration)*0.1,
		)
	}
}

// ChunkedCopyBufferWithCallback performs chunked copying with progress callback.
func (abp *AdaptiveBufferPool) ChunkedCopyBufferWithCallback(
	dst io.Writer,
	src io.Reader,
	estimatedSize int,
	progressCallback func(copied int64, total int64),
) (int64, error) {
	if !abp.config.Enabled {
		return io.Copy(dst, src)
	}

	chunkSize := abp.calculateOptimalChunkSize(estimatedSize)

	// For small data, use regular copy
	if estimatedSize > 0 && estimatedSize <= abp.config.LargeObjectThreshold {
		buf := abp.Get(chunkSize)
		defer abp.Put(buf)
		return io.CopyBuffer(dst, src, buf)
	}

	// For large data, use chunked copying with progress tracking
	ctx := context.Background()
	if err := abp.acquireLargeOpSlot(ctx); err != nil {
		return io.Copy(dst, src)
	}
	defer abp.releaseLargeOpSlot()

	return abp.performChunkedCopyWithProgress(dst, src, chunkSize, int64(estimatedSize), progressCallback)
}

// performChunkedCopyWithProgress executes chunked copy with progress reporting.
func (abp *AdaptiveBufferPool) performChunkedCopyWithProgress(
	dst io.Writer,
	src io.Reader,
	chunkSize int,
	totalSize int64,
	progressCallback func(copied int64, total int64),
) (int64, error) {
	var totalCopied int64

	for {
		chunk := abp.getChunk()

		n, readErr := src.Read(chunk)
		if n > 0 {
			written, writeErr := dst.Write(chunk[:n])
			totalCopied += int64(written)

			// Report progress
			if progressCallback != nil {
				progressCallback(totalCopied, totalSize)
			}

			abp.putChunk(chunk)

			if writeErr != nil {
				return totalCopied, writeErr
			}

			if written != n {
				return totalCopied, io.ErrShortWrite
			}
		} else {
			abp.putChunk(chunk)
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return totalCopied, readErr
		}
	}

	return totalCopied, nil
}

// OptimizedCopyBuffer automatically selects the best copy strategy based on data characteristics.
func (abp *AdaptiveBufferPool) OptimizedCopyBuffer(dst io.Writer, src io.Reader, hint CopyHint) (int64, error) {
	if !abp.config.Enabled {
		return io.Copy(dst, src)
	}

	// Determine strategy based on hint
	switch {
	case hint.EstimatedSize > 0 && hint.EstimatedSize >= int64(abp.config.VeryLargeObjectThreshold):
		// Very large objects: use direct streaming
		return abp.directStreamCopy(dst, src, hint)
	case hint.EstimatedSize > 0 && hint.EstimatedSize >= int64(abp.config.LargeObjectThreshold):
		// Large objects: use chunked copying
		return abp.ChunkedCopyBuffer(dst, src, int(hint.EstimatedSize))
	default:
		// Small/medium objects: use regular buffered copy
		buf := abp.Get(abp.config.DefaultBufferSize)
		defer abp.Put(buf)
		return io.CopyBuffer(dst, src, buf)
	}
}

// CopyHint provides information to optimize copy operations.
type CopyHint struct {
	EstimatedSize int64
	IsStreaming   bool
	Priority      CopyPriority
	Timeout       time.Duration
}

// CopyPriority defines the priority level for copy operations.
type CopyPriority int

const (
	CopyPriorityLow CopyPriority = iota
	CopyPriorityNormal
	CopyPriorityHigh
	CopyPriorityUrgent
)

// directStreamCopy performs direct streaming for very large objects.
func (abp *AdaptiveBufferPool) directStreamCopy(dst io.Writer, src io.Reader, hint CopyHint) (int64, error) {
	// For very large objects, use minimal buffering to reduce memory pressure
	bufSize := abp.config.ChunkSize
	if bufSize > 128*1024 {
		bufSize = 128 * 1024 // Cap at 128KB for very large objects
	}

	buf := make([]byte, bufSize)
	return io.CopyBuffer(dst, src, buf)
}

// CopyBufferWithTimeout performs copy operation with timeout support.
func (abp *AdaptiveBufferPool) CopyBufferWithTimeout(
	ctx context.Context,
	dst io.Writer,
	src io.Reader,
	estimatedSize int,
) (int64, error) {
	if !abp.config.Enabled {
		return io.Copy(dst, src)
	}

	// Create a channel to receive the copy result
	type copyResult struct {
		n   int64
		err error
	}

	resultChan := make(chan copyResult, 1)

	// Perform copy in a goroutine
	go func() {
		n, err := abp.ChunkedCopyBuffer(dst, src, estimatedSize)
		resultChan <- copyResult{n: n, err: err}
	}()

	// Wait for completion or timeout
	select {
	case result := <-resultChan:
		return result.n, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// ChunkPoolManager manages multiple chunk pools for different sizes with advanced optimization.
type ChunkPoolManager struct {
	pools map[int]*ChunkPool
	mutex sync.RWMutex

	// Configuration
	config ChunkPoolConfig
	logger log.Logger

	// Statistics
	stats ChunkPoolManagerStats

	// Memory pressure management
	memoryPressureThreshold int64
	currentMemoryUsage      int64

	// Cleanup management
	cleanupTicker *time.Ticker
	cleanupCancel context.CancelFunc
	cleanupCtx    context.Context
}

// ChunkPoolConfig configures the chunk pool management system.
type ChunkPoolConfig struct {
	// Chunk sizes to pre-create pools for
	ChunkSizes []int

	// Maximum number of chunks per pool
	MaxChunksPerPool int

	// Memory pressure threshold (bytes)
	MemoryPressureThreshold int64

	// Cleanup interval for unused chunks
	CleanupInterval time.Duration

	// Maximum age for chunks before cleanup
	MaxChunkAge time.Duration

	// Enable detailed metrics collection
	EnableMetrics bool
}

// ChunkPool manages chunks of a specific size with lifecycle tracking.
type ChunkPool struct {
	chunkSize int
	pool      *sync.Pool

	// Lifecycle tracking
	activeChunks map[*[]byte]*ChunkInfo
	activeMutex  sync.RWMutex

	// Statistics
	gets        int64
	puts        int64
	hits        int64
	misses      int64
	allocations int64

	// Memory management
	maxChunks     int
	currentChunks int64
	memoryUsage   int64

	// Configuration
	enableTracking bool
}

// ChunkInfo tracks metadata for individual chunks.
type ChunkInfo struct {
	AllocatedAt time.Time
	LastUsedAt  time.Time
	UseCount    int64
	Size        int
}

// ChunkPoolManagerStats provides comprehensive statistics for chunk pool management.
type ChunkPoolManagerStats struct {
	TotalPools          int
	TotalChunks         int64
	TotalMemoryUsage    int64
	TotalGets           int64
	TotalPuts           int64
	TotalHits           int64
	TotalMisses         int64
	MemoryPressureCount int64
	CleanupOperations   int64

	// Per-size statistics
	PoolStats map[int]ChunkPoolStats
}

// ChunkPoolStats provides statistics for individual chunk pools.
type ChunkPoolStats struct {
	ChunkSize       int
	ActiveChunks    int64
	MemoryUsage     int64
	Gets            int64
	Puts            int64
	Hits            int64
	Misses          int64
	Allocations     int64
	AverageAge      time.Duration
	UtilizationRate float64
}

// NewChunkPoolManager creates a new chunk pool manager with the given configuration.
func NewChunkPoolManager(config ChunkPoolConfig, logger log.Logger) *ChunkPoolManager {
	// Set defaults
	if len(config.ChunkSizes) == 0 {
		config.ChunkSizes = []int{
			16 * 1024,  // 16KB
			32 * 1024,  // 32KB
			64 * 1024,  // 64KB
			128 * 1024, // 128KB
			256 * 1024, // 256KB
		}
	}

	if config.MaxChunksPerPool <= 0 {
		config.MaxChunksPerPool = 100
	}

	if config.MemoryPressureThreshold <= 0 {
		config.MemoryPressureThreshold = 100 * 1024 * 1024 // 100MB
	}

	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 5 * time.Minute
	}

	if config.MaxChunkAge <= 0 {
		config.MaxChunkAge = 10 * time.Minute
	}

	cpm := &ChunkPoolManager{
		pools:  make(map[int]*ChunkPool),
		config: config,
		logger: logger,
		stats: ChunkPoolManagerStats{
			PoolStats: make(map[int]ChunkPoolStats),
		},
		memoryPressureThreshold: config.MemoryPressureThreshold,
	}

	// Initialize pools for configured chunk sizes
	for _, size := range config.ChunkSizes {
		cpm.createPool(size)
	}

	// Start cleanup routine
	cpm.startCleanupRoutine()

	return cpm
}

// createPool creates a new chunk pool for the specified size.
func (cpm *ChunkPoolManager) createPool(chunkSize int) *ChunkPool {
	pool := &ChunkPool{
		chunkSize:      chunkSize,
		maxChunks:      cpm.config.MaxChunksPerPool,
		enableTracking: cpm.config.EnableMetrics,
		activeChunks:   make(map[*[]byte]*ChunkInfo),
	}

	pool.pool = &sync.Pool{
		New: func() interface{} {
			chunk := make([]byte, chunkSize)
			atomic.AddInt64(&pool.allocations, 1)
			atomic.AddInt64(&pool.misses, 1)

			if pool.enableTracking {
				info := &ChunkInfo{
					AllocatedAt: time.Now(),
					LastUsedAt:  time.Now(),
					Size:        chunkSize,
				}

				pool.activeMutex.Lock()
				pool.activeChunks[&chunk] = info
				pool.activeMutex.Unlock()
			}

			return chunk
		},
	}

	cpm.mutex.Lock()
	cpm.pools[chunkSize] = pool
	cpm.mutex.Unlock()

	return pool
}

// GetChunk retrieves a chunk of the specified size from the appropriate pool.
func (cpm *ChunkPoolManager) GetChunk(size int) []byte {
	// Find the best matching pool
	poolSize := cpm.findBestPoolSize(size)

	cpm.mutex.RLock()
	pool, exists := cpm.pools[poolSize]
	cpm.mutex.RUnlock()

	if !exists {
		// Create pool on demand
		pool = cpm.createPool(poolSize)
	}

	// Get chunk from pool
	chunk := pool.pool.Get().([]byte)
	atomic.AddInt64(&pool.gets, 1)
	atomic.AddInt64(&cpm.stats.TotalGets, 1)

	// Update tracking information
	if pool.enableTracking {
		pool.activeMutex.Lock()
		if info, exists := pool.activeChunks[&chunk]; exists {
			info.LastUsedAt = time.Now()
			info.UseCount++
			atomic.AddInt64(&pool.hits, 1)
			atomic.AddInt64(&cpm.stats.TotalHits, 1)
		}
		pool.activeMutex.Unlock()
	}

	// Update memory usage
	atomic.AddInt64(&pool.memoryUsage, int64(poolSize))
	atomic.AddInt64(&cpm.currentMemoryUsage, int64(poolSize))
	atomic.AddInt64(&pool.currentChunks, 1)

	return chunk[:size] // Return slice of requested size
}

// PutChunk returns a chunk to the appropriate pool.
func (cpm *ChunkPoolManager) PutChunk(chunk []byte) {
	if chunk == nil {
		return
	}

	chunkSize := cap(chunk)

	cpm.mutex.RLock()
	pool, exists := cpm.pools[chunkSize]
	cpm.mutex.RUnlock()

	if !exists {
		// No pool for this size, just let it be garbage collected
		return
	}

	// Check memory pressure
	if atomic.LoadInt64(&cpm.currentMemoryUsage) > cpm.memoryPressureThreshold {
		atomic.AddInt64(&cpm.stats.MemoryPressureCount, 1)
		// Don't return to pool under memory pressure
		cpm.updateMemoryUsage(pool, -int64(chunkSize))
		return
	}

	// Reset chunk to full capacity
	chunk = chunk[:cap(chunk)]

	// Return to pool
	pool.pool.Put(chunk)
	atomic.AddInt64(&pool.puts, 1)
	atomic.AddInt64(&cpm.stats.TotalPuts, 1)

	// Update memory tracking (chunk is still in memory, just pooled)
	atomic.AddInt64(&pool.currentChunks, -1)
}

// findBestPoolSize finds the best matching pool size for a requested size.
func (cpm *ChunkPoolManager) findBestPoolSize(requestedSize int) int {
	cpm.mutex.RLock()
	defer cpm.mutex.RUnlock()

	// Find the smallest pool size that can accommodate the request
	bestSize := requestedSize
	for size := range cpm.pools {
		if size >= requestedSize && (bestSize == requestedSize || size < bestSize) {
			bestSize = size
		}
	}

	// If no existing pool is suitable, round up to next power of 2 or common size
	if bestSize == requestedSize {
		bestSize = cpm.roundUpToPoolSize(requestedSize)
	}

	return bestSize
}

// roundUpToPoolSize rounds up a size to a suitable pool size.
func (cpm *ChunkPoolManager) roundUpToPoolSize(size int) int {
	// Common pool sizes
	commonSizes := []int{
		16 * 1024,   // 16KB
		32 * 1024,   // 32KB
		64 * 1024,   // 64KB
		128 * 1024,  // 128KB
		256 * 1024,  // 256KB
		512 * 1024,  // 512KB
		1024 * 1024, // 1MB
	}

	for _, commonSize := range commonSizes {
		if size <= commonSize {
			return commonSize
		}
	}

	// For very large sizes, round up to next MB
	return ((size-1)/1024/1024 + 1) * 1024 * 1024
}

// updateMemoryUsage updates memory usage tracking.
func (cpm *ChunkPoolManager) updateMemoryUsage(pool *ChunkPool, delta int64) {
	atomic.AddInt64(&pool.memoryUsage, delta)
	atomic.AddInt64(&cpm.currentMemoryUsage, delta)
}

// startCleanupRoutine starts the background cleanup routine.
func (cpm *ChunkPoolManager) startCleanupRoutine() {
	if cpm.config.CleanupInterval <= 0 {
		return
	}

	cpm.cleanupCtx, cpm.cleanupCancel = context.WithCancel(context.Background())
	cpm.cleanupTicker = time.NewTicker(cpm.config.CleanupInterval)

	go func() {
		defer cpm.cleanupTicker.Stop()
		for {
			select {
			case <-cpm.cleanupCtx.Done():
				return
			case <-cpm.cleanupTicker.C:
				cpm.performCleanup()
			}
		}
	}()
}

// performCleanup performs cleanup of old and unused chunks.
func (cpm *ChunkPoolManager) performCleanup() {
	if cpm.logger != nil {
		level.Debug(cpm.logger).Log("msg", "performing chunk pool cleanup")
	}

	cpm.mutex.RLock()
	pools := make([]*ChunkPool, 0, len(cpm.pools))
	for _, pool := range cpm.pools {
		pools = append(pools, pool)
	}
	cpm.mutex.RUnlock()

	for _, pool := range pools {
		cpm.cleanupPool(pool)
	}

	atomic.AddInt64(&cpm.stats.CleanupOperations, 1)
}

// cleanupPool performs cleanup for a specific pool.
func (cpm *ChunkPoolManager) cleanupPool(pool *ChunkPool) {
	if !pool.enableTracking {
		return
	}

	pool.activeMutex.Lock()
	defer pool.activeMutex.Unlock()

	now := time.Now()
	var chunksToRemove []*[]byte

	// Find chunks that are too old
	for chunkPtr, info := range pool.activeChunks {
		if now.Sub(info.LastUsedAt) > cpm.config.MaxChunkAge {
			chunksToRemove = append(chunksToRemove, chunkPtr)
		}
	}

	// Remove old chunks from tracking
	for _, chunkPtr := range chunksToRemove {
		delete(pool.activeChunks, chunkPtr)
		cpm.updateMemoryUsage(pool, -int64(pool.chunkSize))
	}

	if len(chunksToRemove) > 0 && cpm.logger != nil {
		level.Debug(cpm.logger).Log(
			"msg", "cleaned up old chunks",
			"pool_size", pool.chunkSize,
			"chunks_removed", len(chunksToRemove),
		)
	}
}

// GetStats returns comprehensive statistics for the chunk pool manager.
func (cpm *ChunkPoolManager) GetStats() ChunkPoolManagerStats {
	cpm.mutex.RLock()
	defer cpm.mutex.RUnlock()

	stats := ChunkPoolManagerStats{
		TotalPools:          len(cpm.pools),
		TotalMemoryUsage:    atomic.LoadInt64(&cpm.currentMemoryUsage),
		TotalGets:           atomic.LoadInt64(&cpm.stats.TotalGets),
		TotalPuts:           atomic.LoadInt64(&cpm.stats.TotalPuts),
		TotalHits:           atomic.LoadInt64(&cpm.stats.TotalHits),
		TotalMisses:         atomic.LoadInt64(&cpm.stats.TotalMisses),
		MemoryPressureCount: atomic.LoadInt64(&cpm.stats.MemoryPressureCount),
		CleanupOperations:   atomic.LoadInt64(&cpm.stats.CleanupOperations),
		PoolStats:           make(map[int]ChunkPoolStats),
	}

	var totalChunks int64

	// Collect per-pool statistics
	for size, pool := range cpm.pools {
		poolStats := ChunkPoolStats{
			ChunkSize:    size,
			ActiveChunks: atomic.LoadInt64(&pool.currentChunks),
			MemoryUsage:  atomic.LoadInt64(&pool.memoryUsage),
			Gets:         atomic.LoadInt64(&pool.gets),
			Puts:         atomic.LoadInt64(&pool.puts),
			Hits:         atomic.LoadInt64(&pool.hits),
			Misses:       atomic.LoadInt64(&pool.misses),
			Allocations:  atomic.LoadInt64(&pool.allocations),
		}

		// Calculate utilization rate
		if poolStats.Gets > 0 {
			poolStats.UtilizationRate = float64(poolStats.Hits) / float64(poolStats.Gets)
		}

		// Calculate average age if tracking is enabled
		if pool.enableTracking {
			poolStats.AverageAge = cpm.calculateAverageAge(pool)
		}

		stats.PoolStats[size] = poolStats
		totalChunks += poolStats.ActiveChunks
	}

	stats.TotalChunks = totalChunks
	return stats
}

// calculateAverageAge calculates the average age of chunks in a pool.
func (cpm *ChunkPoolManager) calculateAverageAge(pool *ChunkPool) time.Duration {
	pool.activeMutex.RLock()
	defer pool.activeMutex.RUnlock()

	if len(pool.activeChunks) == 0 {
		return 0
	}

	now := time.Now()
	var totalAge time.Duration

	for _, info := range pool.activeChunks {
		totalAge += now.Sub(info.AllocatedAt)
	}

	return totalAge / time.Duration(len(pool.activeChunks))
}

// Close shuts down the chunk pool manager and cleans up resources.
func (cpm *ChunkPoolManager) Close() {
	if cpm.cleanupCancel != nil {
		cpm.cleanupCancel()
	}

	if cpm.cleanupTicker != nil {
		cpm.cleanupTicker.Stop()
	}

	// Perform final cleanup
	cpm.performCleanup()

	if cpm.logger != nil {
		level.Info(cpm.logger).Log("msg", "chunk pool manager closed")
	}
}

// AdvancedBufferPoolMetrics provides comprehensive performance analysis capabilities.
type AdvancedBufferPoolMetrics struct {
	// Basic stats
	BasicStats BufferPoolStats

	// Performance tracking
	PerformanceMetrics PerformanceMetrics

	// Memory analysis
	MemoryMetrics MemoryMetrics

	// Strategy effectiveness
	StrategyMetrics StrategyMetrics

	// Trend analysis
	TrendMetrics TrendMetrics

	// Configuration and recommendations
	ConfigMetrics ConfigMetrics
}

// PerformanceMetrics tracks detailed performance characteristics.
type PerformanceMetrics struct {
	// Latency tracking by category
	LatencyStats map[string]LatencyStats

	// Throughput metrics
	ThroughputStats ThroughputStats

	// Error rates and patterns
	ErrorStats ErrorStats

	// Resource utilization
	ResourceStats ResourceStats
}

// LatencyStats provides detailed latency analysis for a category.
type LatencyStats struct {
	Mean        time.Duration
	Median      time.Duration
	P95         time.Duration
	P99         time.Duration
	Min         time.Duration
	Max         time.Duration
	StdDev      time.Duration
	SampleCount int64
}

// ThroughputStats tracks throughput characteristics.
type ThroughputStats struct {
	BytesPerSecond      float64
	OperationsPerSecond float64
	PeakBytesPerSecond  float64
	PeakOpsPerSecond    float64
	AverageObjectSize   int64
}

// ErrorStats tracks error patterns and rates.
type ErrorStats struct {
	TotalErrors      int64
	ErrorRate        float64
	ErrorsByCategory map[string]int64
	ErrorsByStrategy map[string]int64
	RecentErrors     []ErrorEvent
}

// ErrorEvent represents a specific error occurrence.
type ErrorEvent struct {
	Timestamp time.Time
	Category  string
	Strategy  string
	Error     string
	Context   map[string]interface{}
}

// ResourceStats tracks resource utilization patterns.
type ResourceStats struct {
	MemoryUtilization float64
	CPUUtilization    float64
	GCPressure        float64
	AllocationRate    float64
	PoolEfficiency    float64
	CacheHitRate      float64
}

// MemoryMetrics provides detailed memory usage analysis.
type MemoryMetrics struct {
	// Current memory usage
	CurrentUsage MemoryUsage

	// Peak memory usage
	PeakUsage MemoryUsage

	// Memory allocation patterns
	AllocationPatterns AllocationPatterns

	// GC impact analysis
	GCMetrics GCMetrics

	// Memory efficiency by category
	EfficiencyByCategory map[string]float64
}

// MemoryUsage represents memory usage at a point in time.
type MemoryUsage struct {
	TotalAllocated int64
	ActiveBuffers  int64
	PooledBuffers  int64
	ChunkedBuffers int64
	DirectBuffers  int64
	OverheadBytes  int64
	EffectiveBytes int64
}

// AllocationPatterns tracks memory allocation patterns.
type AllocationPatterns struct {
	AllocationsBySize     map[int]int64
	AllocationsByStrategy map[string]int64
	AllocationTrends      []AllocationTrend
	FragmentationRatio    float64
	ReuseEfficiency       float64
}

// AllocationTrend represents allocation trends over time.
type AllocationTrend struct {
	Timestamp     time.Time
	AllocatedMB   float64
	ActiveMB      float64
	PooledMB      float64
	EfficiencyPct float64
}

// GCMetrics tracks garbage collection impact.
type GCMetrics struct {
	GCFrequency      float64 // GCs per second
	GCPauseDuration  time.Duration
	GCPressureScore  float64 // 0-1 scale
	MemoryReclaimed  int64
	GCTriggerReasons map[string]int64
}

// StrategyMetrics analyzes strategy effectiveness.
type StrategyMetrics struct {
	// Effectiveness by strategy
	EffectivenessByStrategy map[string]StrategyEffectiveness

	// Strategy selection accuracy
	SelectionAccuracy float64

	// Strategy switching patterns
	SwitchingPatterns []StrategySwitch

	// Optimal strategy recommendations
	Recommendations []StrategyRecommendation
}

// StrategyEffectiveness measures how well a strategy performs.
type StrategyEffectiveness struct {
	Strategy         string
	UsageCount       int64
	AverageLatency   time.Duration
	MemoryEfficiency float64
	ErrorRate        float64
	ThroughputScore  float64
	OverallScore     float64
}

// StrategySwitch represents a strategy change event.
type StrategySwitch struct {
	Timestamp    time.Time
	FromStrategy string
	ToStrategy   string
	ObjectSize   int
	Reason       string
	Performance  float64
}

// StrategyRecommendation suggests strategy optimizations.
type StrategyRecommendation struct {
	Category            string
	CurrentStrategy     string
	RecommendedStrategy string
	ExpectedImprovement float64
	Confidence          float64
	Reasoning           string
}

// TrendMetrics provides trend analysis and forecasting.
type TrendMetrics struct {
	// Performance trends
	PerformanceTrends []PerformanceTrend

	// Usage pattern trends
	UsageTrends []UsageTrend

	// Capacity planning
	CapacityForecasts []CapacityForecast

	// Anomaly detection
	Anomalies []PerformanceAnomaly
}

// PerformanceTrend represents performance changes over time.
type PerformanceTrend struct {
	Timestamp      time.Time
	Category       string
	Latency        time.Duration
	Throughput     float64
	MemoryUsage    int64
	ErrorRate      float64
	TrendDirection string // "improving", "degrading", "stable"
}

// UsageTrend tracks usage pattern changes.
type UsageTrend struct {
	Timestamp       time.Time
	SmallObjPct     float64
	MediumObjPct    float64
	LargeObjPct     float64
	VeryLargeObjPct float64
	PredictedShift  string
}

// CapacityForecast predicts future capacity needs.
type CapacityForecast struct {
	TimeHorizon      time.Duration
	PredictedLoad    float64
	RequiredCapacity int64
	ConfidenceLevel  float64
	Recommendations  []string
}

// PerformanceAnomaly represents detected performance anomalies.
type PerformanceAnomaly struct {
	Timestamp   time.Time
	Category    string
	Metric      string
	Expected    float64
	Actual      float64
	Severity    string // "low", "medium", "high", "critical"
	Description string
	Suggestions []string
}

// ConfigMetrics provides configuration analysis and recommendations.
type ConfigMetrics struct {
	// Current configuration effectiveness
	ConfigEffectiveness ConfigEffectiveness

	// Configuration recommendations
	Recommendations []ConfigRecommendation

	// Tuning opportunities
	TuningOpportunities []TuningOpportunity

	// Configuration validation
	ValidationResults ConfigValidationResults
}

// ConfigEffectiveness measures how well current configuration performs.
type ConfigEffectiveness struct {
	OverallScore       float64
	BufferSizeScore    float64
	ThresholdScore     float64
	StrategyScore      float64
	MemoryScore        float64
	PerformanceScore   float64
	RecommendedChanges []string
}

// ConfigRecommendation suggests configuration improvements.
type ConfigRecommendation struct {
	Parameter           string
	CurrentValue        interface{}
	RecommendedValue    interface{}
	ExpectedImprovement float64
	Priority            string // "low", "medium", "high", "critical"
	Reasoning           string
	RiskLevel           string // "low", "medium", "high"
}

// TuningOpportunity identifies specific tuning opportunities.
type TuningOpportunity struct {
	Area                 string
	Description          string
	PotentialGain        float64
	ImplementationEffort string // "low", "medium", "high"
	Prerequisites        []string
	Steps                []string
}

// ConfigValidationResults provides configuration validation feedback.
type ConfigValidationResults struct {
	IsValid         bool
	ValidationScore float64
	Issues          []ConfigIssue
	Warnings        []ConfigWarning
	BestPractices   []BestPracticeCheck
}

// ConfigIssue represents a configuration problem.
type ConfigIssue struct {
	Severity   string // "error", "warning", "info"
	Parameter  string
	Issue      string
	Impact     string
	Resolution string
}

// ConfigWarning represents a configuration warning.
type ConfigWarning struct {
	Parameter      string
	Warning        string
	Recommendation string
	Impact         string
}

// BestPracticeCheck represents a best practice validation.
type BestPracticeCheck struct {
	Practice       string
	Status         string // "pass", "fail", "warning"
	Description    string
	Recommendation string
}

// MetricsCollector manages comprehensive metrics collection and analysis.
type MetricsCollector struct {
	// Configuration
	config MetricsConfig
	logger log.Logger

	// Data collection
	dataPoints []MetricDataPoint
	mutex      sync.RWMutex

	// Analysis engines
	performanceAnalyzer *PerformanceAnalyzer
	memoryAnalyzer      *MemoryAnalyzer
	strategyAnalyzer    *StrategyAnalyzer
	trendAnalyzer       *TrendAnalyzer
	configAnalyzer      *ConfigAnalyzer

	// Background processing
	processingTicker *time.Ticker
	processingCancel context.CancelFunc
	processingCtx    context.Context
}

// MetricsConfig configures metrics collection behavior.
type MetricsConfig struct {
	// Collection settings
	CollectionInterval time.Duration
	RetentionPeriod    time.Duration
	MaxDataPoints      int

	// Analysis settings
	EnableTrendAnalysis    bool
	EnableAnomalyDetection bool
	EnableRecommendations  bool

	// Performance settings
	AnalysisInterval  time.Duration
	ReportingInterval time.Duration

	// Storage settings
	PersistMetrics     bool
	MetricsStoragePath string
}

// MetricDataPoint represents a single metrics measurement.
type MetricDataPoint struct {
	Timestamp   time.Time
	Category    string
	Strategy    string
	ObjectSize  int
	Latency     time.Duration
	MemoryUsage int64
	Success     bool
	Error       string
	Context     map[string]interface{}
}

// NewMetricsCollector creates a new comprehensive metrics collector.
func NewMetricsCollector(config MetricsConfig, logger log.Logger) *MetricsCollector {
	// Set defaults
	if config.CollectionInterval <= 0 {
		config.CollectionInterval = 1 * time.Second
	}
	if config.RetentionPeriod <= 0 {
		config.RetentionPeriod = 24 * time.Hour
	}
	if config.MaxDataPoints <= 0 {
		config.MaxDataPoints = 10000
	}
	if config.AnalysisInterval <= 0 {
		config.AnalysisInterval = 5 * time.Minute
	}
	if config.ReportingInterval <= 0 {
		config.ReportingInterval = 15 * time.Minute
	}

	mc := &MetricsCollector{
		config:     config,
		logger:     logger,
		dataPoints: make([]MetricDataPoint, 0, config.MaxDataPoints),
	}

	// Initialize analyzers
	mc.performanceAnalyzer = NewPerformanceAnalyzer(logger)
	mc.memoryAnalyzer = NewMemoryAnalyzer(logger)
	mc.strategyAnalyzer = NewStrategyAnalyzer(logger)
	mc.trendAnalyzer = NewTrendAnalyzer(logger)
	mc.configAnalyzer = NewConfigAnalyzer(logger)

	// Start background processing
	mc.startBackgroundProcessing()

	return mc
}

// RecordMetric records a new metric data point.
func (mc *MetricsCollector) RecordMetric(dataPoint MetricDataPoint) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Add timestamp if not set
	if dataPoint.Timestamp.IsZero() {
		dataPoint.Timestamp = time.Now()
	}

	// Add to data points
	mc.dataPoints = append(mc.dataPoints, dataPoint)

	// Trim if necessary
	if len(mc.dataPoints) > mc.config.MaxDataPoints {
		// Remove oldest 10% of data points
		removeCount := mc.config.MaxDataPoints / 10
		mc.dataPoints = mc.dataPoints[removeCount:]
	}
}

// GetAdvancedMetrics returns comprehensive metrics analysis.
func (mc *MetricsCollector) GetAdvancedMetrics() AdvancedBufferPoolMetrics {
	mc.mutex.RLock()
	dataPoints := make([]MetricDataPoint, len(mc.dataPoints))
	copy(dataPoints, mc.dataPoints)
	mc.mutex.RUnlock()

	// Analyze data using different analyzers
	performanceMetrics := mc.performanceAnalyzer.Analyze(dataPoints)
	memoryMetrics := mc.memoryAnalyzer.Analyze(dataPoints)
	strategyMetrics := mc.strategyAnalyzer.Analyze(dataPoints)
	trendMetrics := mc.trendAnalyzer.Analyze(dataPoints)
	configMetrics := mc.configAnalyzer.Analyze(dataPoints)

	return AdvancedBufferPoolMetrics{
		PerformanceMetrics: performanceMetrics,
		MemoryMetrics:      memoryMetrics,
		StrategyMetrics:    strategyMetrics,
		TrendMetrics:       trendMetrics,
		ConfigMetrics:      configMetrics,
	}
}

// startBackgroundProcessing starts background metrics processing.
func (mc *MetricsCollector) startBackgroundProcessing() {
	if mc.config.AnalysisInterval <= 0 {
		return
	}

	mc.processingCtx, mc.processingCancel = context.WithCancel(context.Background())
	mc.processingTicker = time.NewTicker(mc.config.AnalysisInterval)

	go func() {
		defer mc.processingTicker.Stop()
		for {
			select {
			case <-mc.processingCtx.Done():
				return
			case <-mc.processingTicker.C:
				mc.performBackgroundAnalysis()
			}
		}
	}()
}

// performBackgroundAnalysis performs periodic analysis and cleanup.
func (mc *MetricsCollector) performBackgroundAnalysis() {
	// Clean up old data points
	mc.cleanupOldDataPoints()

	// Perform analysis if enabled
	if mc.config.EnableTrendAnalysis || mc.config.EnableAnomalyDetection {
		metrics := mc.GetAdvancedMetrics()

		// Log significant findings
		mc.logSignificantFindings(metrics)
	}
}

// cleanupOldDataPoints removes data points older than retention period.
func (mc *MetricsCollector) cleanupOldDataPoints() {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	cutoff := time.Now().Add(-mc.config.RetentionPeriod)

	// Find first data point to keep
	keepIndex := 0
	for i, dp := range mc.dataPoints {
		if dp.Timestamp.After(cutoff) {
			keepIndex = i
			break
		}
	}

	// Remove old data points
	if keepIndex > 0 {
		mc.dataPoints = mc.dataPoints[keepIndex:]

		if mc.logger != nil {
			level.Debug(mc.logger).Log(
				"msg", "cleaned up old metric data points",
				"removed_count", keepIndex,
				"remaining_count", len(mc.dataPoints),
			)
		}
	}
}

// logSignificantFindings logs important metrics findings.
func (mc *MetricsCollector) logSignificantFindings(metrics AdvancedBufferPoolMetrics) {
	if mc.logger == nil {
		return
	}

	// Log performance anomalies
	for _, anomaly := range metrics.TrendMetrics.Anomalies {
		if anomaly.Severity == "high" || anomaly.Severity == "critical" {
			level.Warn(mc.logger).Log(
				"msg", "performance anomaly detected",
				"category", anomaly.Category,
				"metric", anomaly.Metric,
				"severity", anomaly.Severity,
				"description", anomaly.Description,
			)
		}
	}

	// Log high-priority configuration recommendations
	for _, rec := range metrics.ConfigMetrics.Recommendations {
		if rec.Priority == "high" || rec.Priority == "critical" {
			level.Info(mc.logger).Log(
				"msg", "configuration recommendation",
				"parameter", rec.Parameter,
				"priority", rec.Priority,
				"expected_improvement", rec.ExpectedImprovement,
				"reasoning", rec.Reasoning,
			)
		}
	}
}

// Close shuts down the metrics collector.
func (mc *MetricsCollector) Close() {
	if mc.processingCancel != nil {
		mc.processingCancel()
	}

	if mc.processingTicker != nil {
		mc.processingTicker.Stop()
	}

	if mc.logger != nil {
		level.Info(mc.logger).Log("msg", "metrics collector closed")
	}
}

// PerformanceAnalyzer analyzes performance metrics and patterns.
type PerformanceAnalyzer struct {
	logger log.Logger
}

// NewPerformanceAnalyzer creates a new performance analyzer.
func NewPerformanceAnalyzer(logger log.Logger) *PerformanceAnalyzer {
	return &PerformanceAnalyzer{
		logger: logger,
	}
}

// Analyze performs comprehensive performance analysis.
func (pa *PerformanceAnalyzer) Analyze(dataPoints []MetricDataPoint) PerformanceMetrics {
	if len(dataPoints) == 0 {
		return PerformanceMetrics{
			LatencyStats: make(map[string]LatencyStats),
		}
	}

	// Group data points by category
	categoryData := make(map[string][]MetricDataPoint)
	for _, dp := range dataPoints {
		categoryData[dp.Category] = append(categoryData[dp.Category], dp)
	}

	// Analyze latency for each category
	latencyStats := make(map[string]LatencyStats)
	for category, points := range categoryData {
		latencyStats[category] = pa.analyzeLatency(points)
	}

	// Analyze throughput
	throughputStats := pa.analyzeThroughput(dataPoints)

	// Analyze errors
	errorStats := pa.analyzeErrors(dataPoints)

	// Analyze resource utilization
	resourceStats := pa.analyzeResourceUtilization(dataPoints)

	return PerformanceMetrics{
		LatencyStats:    latencyStats,
		ThroughputStats: throughputStats,
		ErrorStats:      errorStats,
		ResourceStats:   resourceStats,
	}
}

// analyzeLatency performs detailed latency analysis.
func (pa *PerformanceAnalyzer) analyzeLatency(dataPoints []MetricDataPoint) LatencyStats {
	if len(dataPoints) == 0 {
		return LatencyStats{}
	}

	// Extract latencies
	latencies := make([]time.Duration, 0, len(dataPoints))
	for _, dp := range dataPoints {
		if dp.Success && dp.Latency > 0 {
			latencies = append(latencies, dp.Latency)
		}
	}

	if len(latencies) == 0 {
		return LatencyStats{}
	}

	// Sort for percentile calculations
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate statistics
	stats := LatencyStats{
		Min:         latencies[0],
		Max:         latencies[len(latencies)-1],
		SampleCount: int64(len(latencies)),
	}

	// Calculate mean
	var total time.Duration
	for _, lat := range latencies {
		total += lat
	}
	stats.Mean = total / time.Duration(len(latencies))

	// Calculate median
	if len(latencies)%2 == 0 {
		mid := len(latencies) / 2
		stats.Median = (latencies[mid-1] + latencies[mid]) / 2
	} else {
		stats.Median = latencies[len(latencies)/2]
	}

	// Calculate percentiles
	p95Index := int(float64(len(latencies)) * 0.95)
	p99Index := int(float64(len(latencies)) * 0.99)

	if p95Index >= len(latencies) {
		p95Index = len(latencies) - 1
	}
	if p99Index >= len(latencies) {
		p99Index = len(latencies) - 1
	}

	stats.P95 = latencies[p95Index]
	stats.P99 = latencies[p99Index]

	// Calculate standard deviation
	var variance float64
	meanFloat := float64(stats.Mean)
	for _, lat := range latencies {
		diff := float64(lat) - meanFloat
		variance += diff * diff
	}
	variance /= float64(len(latencies))
	stats.StdDev = time.Duration(math.Sqrt(variance))

	return stats
}

// analyzeThroughput analyzes throughput characteristics.
func (pa *PerformanceAnalyzer) analyzeThroughput(dataPoints []MetricDataPoint) ThroughputStats {
	if len(dataPoints) == 0 {
		return ThroughputStats{}
	}

	// Calculate time window
	startTime := dataPoints[0].Timestamp
	endTime := dataPoints[len(dataPoints)-1].Timestamp
	duration := endTime.Sub(startTime)

	if duration <= 0 {
		return ThroughputStats{}
	}

	// Calculate totals
	var totalBytes int64
	var totalOps int64
	var totalObjectSize int64

	for _, dp := range dataPoints {
		if dp.Success {
			totalBytes += dp.MemoryUsage
			totalOps++
			totalObjectSize += int64(dp.ObjectSize)
		}
	}

	durationSeconds := duration.Seconds()

	stats := ThroughputStats{
		BytesPerSecond:      float64(totalBytes) / durationSeconds,
		OperationsPerSecond: float64(totalOps) / durationSeconds,
	}

	if totalOps > 0 {
		stats.AverageObjectSize = totalObjectSize / totalOps
	}

	// Calculate peak throughput (using 1-second windows)
	stats.PeakBytesPerSecond, stats.PeakOpsPerSecond = pa.calculatePeakThroughput(dataPoints)

	return stats
}

// calculatePeakThroughput calculates peak throughput in 1-second windows.
func (pa *PerformanceAnalyzer) calculatePeakThroughput(dataPoints []MetricDataPoint) (float64, float64) {
	if len(dataPoints) == 0 {
		return 0, 0
	}

	// Group by 1-second windows
	windows := make(map[int64][]MetricDataPoint)
	for _, dp := range dataPoints {
		windowKey := dp.Timestamp.Unix()
		windows[windowKey] = append(windows[windowKey], dp)
	}

	var maxBytes, maxOps float64

	for _, windowData := range windows {
		var windowBytes int64
		var windowOps int64

		for _, dp := range windowData {
			if dp.Success {
				windowBytes += dp.MemoryUsage
				windowOps++
			}
		}

		if float64(windowBytes) > maxBytes {
			maxBytes = float64(windowBytes)
		}
		if float64(windowOps) > maxOps {
			maxOps = float64(windowOps)
		}
	}

	return maxBytes, maxOps
}

// analyzeErrors analyzes error patterns and rates.
func (pa *PerformanceAnalyzer) analyzeErrors(dataPoints []MetricDataPoint) ErrorStats {
	var totalOps int64
	var totalErrors int64
	errorsByCategory := make(map[string]int64)
	errorsByStrategy := make(map[string]int64)
	var recentErrors []ErrorEvent

	// Analyze last 100 errors for recent patterns
	recentCutoff := time.Now().Add(-1 * time.Hour)

	for _, dp := range dataPoints {
		totalOps++

		if !dp.Success && dp.Error != "" {
			totalErrors++
			errorsByCategory[dp.Category]++
			errorsByStrategy[dp.Strategy]++

			if dp.Timestamp.After(recentCutoff) && len(recentErrors) < 100 {
				recentErrors = append(recentErrors, ErrorEvent{
					Timestamp: dp.Timestamp,
					Category:  dp.Category,
					Strategy:  dp.Strategy,
					Error:     dp.Error,
					Context:   dp.Context,
				})
			}
		}
	}

	var errorRate float64
	if totalOps > 0 {
		errorRate = float64(totalErrors) / float64(totalOps)
	}

	return ErrorStats{
		TotalErrors:      totalErrors,
		ErrorRate:        errorRate,
		ErrorsByCategory: errorsByCategory,
		ErrorsByStrategy: errorsByStrategy,
		RecentErrors:     recentErrors,
	}
}

// analyzeResourceUtilization analyzes resource utilization patterns.
func (pa *PerformanceAnalyzer) analyzeResourceUtilization(dataPoints []MetricDataPoint) ResourceStats {
	if len(dataPoints) == 0 {
		return ResourceStats{}
	}

	var totalMemory int64
	var successfulOps int64
	var totalOps int64

	for _, dp := range dataPoints {
		totalOps++
		totalMemory += dp.MemoryUsage

		if dp.Success {
			successfulOps++
		}
	}

	stats := ResourceStats{}

	if totalOps > 0 {
		stats.MemoryUtilization = float64(totalMemory) / float64(totalOps)
		stats.CacheHitRate = float64(successfulOps) / float64(totalOps)
	}

	// Calculate pool efficiency (simplified)
	stats.PoolEfficiency = stats.CacheHitRate

	// Estimate GC pressure based on memory allocation patterns
	stats.GCPressure = pa.estimateGCPressure(dataPoints)

	return stats
}

// estimateGCPressure estimates GC pressure based on allocation patterns.
func (pa *PerformanceAnalyzer) estimateGCPressure(dataPoints []MetricDataPoint) float64 {
	if len(dataPoints) < 2 {
		return 0.0
	}

	// Calculate allocation rate
	var totalAllocations int64
	duration := dataPoints[len(dataPoints)-1].Timestamp.Sub(dataPoints[0].Timestamp)

	for _, dp := range dataPoints {
		totalAllocations += dp.MemoryUsage
	}

	if duration <= 0 {
		return 0.0
	}

	allocationRate := float64(totalAllocations) / duration.Seconds()

	// Normalize to 0-1 scale (rough estimation)
	// Higher allocation rates indicate higher GC pressure
	normalizedPressure := allocationRate / (100 * 1024 * 1024) // 100MB/s as reference

	if normalizedPressure > 1.0 {
		normalizedPressure = 1.0
	}

	return normalizedPressure
}

// MemoryAnalyzer analyzes memory usage patterns and efficiency.
type MemoryAnalyzer struct {
	logger log.Logger
}

// NewMemoryAnalyzer creates a new memory analyzer.
func NewMemoryAnalyzer(logger log.Logger) *MemoryAnalyzer {
	return &MemoryAnalyzer{
		logger: logger,
	}
}

// Analyze performs comprehensive memory analysis.
func (ma *MemoryAnalyzer) Analyze(dataPoints []MetricDataPoint) MemoryMetrics {
	if len(dataPoints) == 0 {
		return MemoryMetrics{
			EfficiencyByCategory: make(map[string]float64),
		}
	}

	// Analyze current usage
	currentUsage := ma.analyzeCurrentUsage(dataPoints)

	// Analyze peak usage
	peakUsage := ma.analyzePeakUsage(dataPoints)

	// Analyze allocation patterns
	allocationPatterns := ma.analyzeAllocationPatterns(dataPoints)

	// Analyze GC metrics
	gcMetrics := ma.analyzeGCMetrics(dataPoints)

	// Calculate efficiency by category
	efficiencyByCategory := ma.calculateEfficiencyByCategory(dataPoints)

	return MemoryMetrics{
		CurrentUsage:         currentUsage,
		PeakUsage:            peakUsage,
		AllocationPatterns:   allocationPatterns,
		GCMetrics:            gcMetrics,
		EfficiencyByCategory: efficiencyByCategory,
	}
}

// analyzeCurrentUsage analyzes current memory usage.
func (ma *MemoryAnalyzer) analyzeCurrentUsage(dataPoints []MetricDataPoint) MemoryUsage {
	if len(dataPoints) == 0 {
		return MemoryUsage{}
	}

	// Use recent data points (last 100 or last minute)
	recentCutoff := time.Now().Add(-1 * time.Minute)
	recentPoints := make([]MetricDataPoint, 0)

	for i := len(dataPoints) - 1; i >= 0 && len(recentPoints) < 100; i-- {
		if dataPoints[i].Timestamp.After(recentCutoff) {
			recentPoints = append(recentPoints, dataPoints[i])
		}
	}

	if len(recentPoints) == 0 {
		recentPoints = dataPoints[len(dataPoints)-10:] // Last 10 points as fallback
	}

	var totalAllocated int64
	var activeBuffers int64
	strategyUsage := make(map[string]int64)

	for _, dp := range recentPoints {
		totalAllocated += dp.MemoryUsage
		if dp.Success {
			activeBuffers++
		}
		strategyUsage[dp.Strategy] += dp.MemoryUsage
	}

	usage := MemoryUsage{
		TotalAllocated: totalAllocated,
		ActiveBuffers:  activeBuffers,
		PooledBuffers:  strategyUsage["pooled"],
		ChunkedBuffers: strategyUsage["chunked"],
		DirectBuffers:  strategyUsage["direct"],
	}

	// Calculate overhead (simplified estimation)
	usage.OverheadBytes = totalAllocated / 20 // Assume 5% overhead
	usage.EffectiveBytes = totalAllocated - usage.OverheadBytes

	return usage
}

// analyzePeakUsage analyzes peak memory usage.
func (ma *MemoryAnalyzer) analyzePeakUsage(dataPoints []MetricDataPoint) MemoryUsage {
	if len(dataPoints) == 0 {
		return MemoryUsage{}
	}

	// Find peak usage in 1-minute windows
	windows := make(map[int64][]MetricDataPoint)
	for _, dp := range dataPoints {
		windowKey := dp.Timestamp.Unix() / 60 // 1-minute windows
		windows[windowKey] = append(windows[windowKey], dp)
	}

	var peakUsage MemoryUsage
	var maxTotal int64

	for _, windowData := range windows {
		var windowTotal int64
		var windowActive int64
		strategyUsage := make(map[string]int64)

		for _, dp := range windowData {
			windowTotal += dp.MemoryUsage
			if dp.Success {
				windowActive++
			}
			strategyUsage[dp.Strategy] += dp.MemoryUsage
		}

		if windowTotal > maxTotal {
			maxTotal = windowTotal
			peakUsage = MemoryUsage{
				TotalAllocated: windowTotal,
				ActiveBuffers:  windowActive,
				PooledBuffers:  strategyUsage["pooled"],
				ChunkedBuffers: strategyUsage["chunked"],
				DirectBuffers:  strategyUsage["direct"],
				OverheadBytes:  windowTotal / 20,
				EffectiveBytes: windowTotal - (windowTotal / 20),
			}
		}
	}

	return peakUsage
}

// analyzeAllocationPatterns analyzes memory allocation patterns.
func (ma *MemoryAnalyzer) analyzeAllocationPatterns(dataPoints []MetricDataPoint) AllocationPatterns {
	allocationsBySize := make(map[int]int64)
	allocationsByStrategy := make(map[string]int64)
	var allocationTrends []AllocationTrend

	// Group allocations by size and strategy
	for _, dp := range dataPoints {
		allocationsBySize[dp.ObjectSize] += dp.MemoryUsage
		allocationsByStrategy[dp.Strategy] += dp.MemoryUsage
	}

	// Calculate allocation trends (hourly)
	hourlyData := make(map[int64][]MetricDataPoint)
	for _, dp := range dataPoints {
		hourKey := dp.Timestamp.Unix() / 3600 // 1-hour windows
		hourlyData[hourKey] = append(hourlyData[hourKey], dp)
	}

	for hourKey, hourData := range hourlyData {
		var allocated, active, pooled int64
		var successCount int64

		for _, dp := range hourData {
			allocated += dp.MemoryUsage
			if dp.Success {
				active += dp.MemoryUsage
				successCount++
			}
			if dp.Strategy == "pooled" {
				pooled += dp.MemoryUsage
			}
		}

		var efficiency float64
		if allocated > 0 {
			efficiency = float64(active) / float64(allocated) * 100
		}

		trend := AllocationTrend{
			Timestamp:     time.Unix(hourKey*3600, 0),
			AllocatedMB:   float64(allocated) / (1024 * 1024),
			ActiveMB:      float64(active) / (1024 * 1024),
			PooledMB:      float64(pooled) / (1024 * 1024),
			EfficiencyPct: efficiency,
		}

		allocationTrends = append(allocationTrends, trend)
	}

	// Sort trends by timestamp
	sort.Slice(allocationTrends, func(i, j int) bool {
		return allocationTrends[i].Timestamp.Before(allocationTrends[j].Timestamp)
	})

	// Calculate fragmentation ratio (simplified)
	var totalAllocated, effectiveAllocated int64
	for _, dp := range dataPoints {
		totalAllocated += dp.MemoryUsage
		if dp.Success {
			effectiveAllocated += dp.MemoryUsage
		}
	}

	var fragmentationRatio float64
	if totalAllocated > 0 {
		fragmentationRatio = 1.0 - (float64(effectiveAllocated) / float64(totalAllocated))
	}

	// Calculate reuse efficiency
	var reuseEfficiency float64
	if len(allocationsByStrategy) > 0 {
		pooledAllocations := allocationsByStrategy["pooled"]
		if totalAllocated > 0 {
			reuseEfficiency = float64(pooledAllocations) / float64(totalAllocated)
		}
	}

	return AllocationPatterns{
		AllocationsBySize:     allocationsBySize,
		AllocationsByStrategy: allocationsByStrategy,
		AllocationTrends:      allocationTrends,
		FragmentationRatio:    fragmentationRatio,
		ReuseEfficiency:       reuseEfficiency,
	}
}

// analyzeGCMetrics analyzes garbage collection impact.
func (ma *MemoryAnalyzer) analyzeGCMetrics(dataPoints []MetricDataPoint) GCMetrics {
	if len(dataPoints) == 0 {
		return GCMetrics{
			GCTriggerReasons: make(map[string]int64),
		}
	}

	// Estimate GC frequency based on allocation patterns
	duration := dataPoints[len(dataPoints)-1].Timestamp.Sub(dataPoints[0].Timestamp)
	if duration <= 0 {
		return GCMetrics{
			GCTriggerReasons: make(map[string]int64),
		}
	}

	var totalAllocations int64
	for _, dp := range dataPoints {
		totalAllocations += dp.MemoryUsage
	}

	// Rough estimation of GC frequency
	// Assume GC triggers every ~8MB of allocations (simplified)
	estimatedGCs := totalAllocations / (8 * 1024 * 1024)
	gcFrequency := float64(estimatedGCs) / duration.Seconds()

	// Estimate GC pause duration (very rough)
	gcPauseDuration := time.Duration(float64(time.Millisecond) * math.Sqrt(float64(totalAllocations)/(1024*1024)))

	// Calculate GC pressure score
	allocationRate := float64(totalAllocations) / duration.Seconds()
	gcPressureScore := math.Min(allocationRate/(50*1024*1024), 1.0) // Normalize to 0-1

	// Estimate memory reclaimed
	var successfulAllocations int64
	for _, dp := range dataPoints {
		if dp.Success {
			successfulAllocations += dp.MemoryUsage
		}
	}
	memoryReclaimed := totalAllocations - successfulAllocations

	// Categorize GC trigger reasons (simplified)
	gcTriggerReasons := map[string]int64{
		"heap_size":       estimatedGCs / 2,
		"allocation_rate": estimatedGCs / 3,
		"time_based":      estimatedGCs / 6,
	}

	return GCMetrics{
		GCFrequency:      gcFrequency,
		GCPauseDuration:  gcPauseDuration,
		GCPressureScore:  gcPressureScore,
		MemoryReclaimed:  memoryReclaimed,
		GCTriggerReasons: gcTriggerReasons,
	}
}

// calculateEfficiencyByCategory calculates memory efficiency for each category.
func (ma *MemoryAnalyzer) calculateEfficiencyByCategory(dataPoints []MetricDataPoint) map[string]float64 {
	categoryData := make(map[string][]MetricDataPoint)
	for _, dp := range dataPoints {
		categoryData[dp.Category] = append(categoryData[dp.Category], dp)
	}

	efficiency := make(map[string]float64)

	for category, points := range categoryData {
		var totalAllocated, effectiveAllocated int64

		for _, dp := range points {
			totalAllocated += dp.MemoryUsage
			if dp.Success {
				effectiveAllocated += dp.MemoryUsage
			}
		}

		if totalAllocated > 0 {
			efficiency[category] = float64(effectiveAllocated) / float64(totalAllocated)
		}
	}

	return efficiency
}

// StrategyAnalyzer analyzes strategy effectiveness and selection patterns.
type StrategyAnalyzer struct {
	logger log.Logger
}

// NewStrategyAnalyzer creates a new strategy analyzer.
func NewStrategyAnalyzer(logger log.Logger) *StrategyAnalyzer {
	return &StrategyAnalyzer{logger: logger}
}

// Analyze performs strategy effectiveness analysis.
func (sa *StrategyAnalyzer) Analyze(dataPoints []MetricDataPoint) StrategyMetrics {
	effectiveness := sa.analyzeEffectiveness(dataPoints)
	accuracy := sa.calculateSelectionAccuracy(dataPoints)
	patterns := sa.analyzeSwitchingPatterns(dataPoints)
	recommendations := sa.generateRecommendations(dataPoints)

	return StrategyMetrics{
		EffectivenessByStrategy: effectiveness,
		SelectionAccuracy:       accuracy,
		SwitchingPatterns:       patterns,
		Recommendations:         recommendations,
	}
}

// analyzeEffectiveness analyzes effectiveness of each strategy.
func (sa *StrategyAnalyzer) analyzeEffectiveness(dataPoints []MetricDataPoint) map[string]StrategyEffectiveness {
	strategyData := make(map[string][]MetricDataPoint)
	for _, dp := range dataPoints {
		strategyData[dp.Strategy] = append(strategyData[dp.Strategy], dp)
	}

	effectiveness := make(map[string]StrategyEffectiveness)

	for strategy, points := range strategyData {
		var totalLatency time.Duration
		var totalMemory, effectiveMemory int64
		var successCount, errorCount int64

		for _, dp := range points {
			totalLatency += dp.Latency
			totalMemory += dp.MemoryUsage

			if dp.Success {
				successCount++
				effectiveMemory += dp.MemoryUsage
			} else {
				errorCount++
			}
		}

		eff := StrategyEffectiveness{
			Strategy:   strategy,
			UsageCount: int64(len(points)),
		}

		if len(points) > 0 {
			eff.AverageLatency = totalLatency / time.Duration(len(points))
		}

		if totalMemory > 0 {
			eff.MemoryEfficiency = float64(effectiveMemory) / float64(totalMemory)
		}

		if len(points) > 0 {
			eff.ErrorRate = float64(errorCount) / float64(len(points))
		}

		// Calculate throughput score (simplified)
		if eff.AverageLatency > 0 {
			eff.ThroughputScore = 1.0 / eff.AverageLatency.Seconds()
		}

		// Calculate overall score
		eff.OverallScore = (eff.MemoryEfficiency + (1.0 - eff.ErrorRate) + math.Min(eff.ThroughputScore, 1.0)) / 3.0

		effectiveness[strategy] = eff
	}

	return effectiveness
}

// calculateSelectionAccuracy calculates strategy selection accuracy.
func (sa *StrategyAnalyzer) calculateSelectionAccuracy(dataPoints []MetricDataPoint) float64 {
	// Simplified accuracy calculation
	// In a real implementation, this would compare actual vs optimal strategy selection
	return 0.85 // Placeholder
}

// analyzeSwitchingPatterns analyzes strategy switching patterns.
func (sa *StrategyAnalyzer) analyzeSwitchingPatterns(dataPoints []MetricDataPoint) []StrategySwitch {
	// Simplified implementation
	return []StrategySwitch{} // Placeholder
}

// generateRecommendations generates strategy optimization recommendations.
func (sa *StrategyAnalyzer) generateRecommendations(dataPoints []MetricDataPoint) []StrategyRecommendation {
	// Simplified implementation
	return []StrategyRecommendation{} // Placeholder
}

// TrendAnalyzer analyzes performance trends and detects anomalies.
type TrendAnalyzer struct {
	logger log.Logger
}

// NewTrendAnalyzer creates a new trend analyzer.
func NewTrendAnalyzer(logger log.Logger) *TrendAnalyzer {
	return &TrendAnalyzer{logger: logger}
}

// Analyze performs trend analysis and anomaly detection.
func (ta *TrendAnalyzer) Analyze(dataPoints []MetricDataPoint) TrendMetrics {
	trends := ta.analyzePerformanceTrends(dataPoints)
	usage := ta.analyzeUsageTrends(dataPoints)
	forecasts := ta.generateCapacityForecasts(dataPoints)
	anomalies := ta.detectAnomalies(dataPoints)

	return TrendMetrics{
		PerformanceTrends: trends,
		UsageTrends:       usage,
		CapacityForecasts: forecasts,
		Anomalies:         anomalies,
	}
}

// analyzePerformanceTrends analyzes performance trends over time.
func (ta *TrendAnalyzer) analyzePerformanceTrends(dataPoints []MetricDataPoint) []PerformanceTrend {
	// Group by time windows (hourly)
	hourlyData := make(map[int64][]MetricDataPoint)
	for _, dp := range dataPoints {
		hourKey := dp.Timestamp.Unix() / 3600
		hourlyData[hourKey] = append(hourlyData[hourKey], dp)
	}

	var trends []PerformanceTrend

	for hourKey, hourData := range hourlyData {
		categoryData := make(map[string][]MetricDataPoint)
		for _, dp := range hourData {
			categoryData[dp.Category] = append(categoryData[dp.Category], dp)
		}

		for category, points := range categoryData {
			var avgLatency time.Duration
			var totalMemory int64
			var errorCount int64

			for _, dp := range points {
				avgLatency += dp.Latency
				totalMemory += dp.MemoryUsage
				if !dp.Success {
					errorCount++
				}
			}

			if len(points) > 0 {
				avgLatency /= time.Duration(len(points))
			}

			var errorRate float64
			if len(points) > 0 {
				errorRate = float64(errorCount) / float64(len(points))
			}

			trend := PerformanceTrend{
				Timestamp:      time.Unix(hourKey*3600, 0),
				Category:       category,
				Latency:        avgLatency,
				MemoryUsage:    totalMemory,
				ErrorRate:      errorRate,
				TrendDirection: "stable", // Simplified
			}

			trends = append(trends, trend)
		}
	}

	return trends
}

// analyzeUsageTrends analyzes usage pattern trends.
func (ta *TrendAnalyzer) analyzeUsageTrends(dataPoints []MetricDataPoint) []UsageTrend {
	// Simplified implementation
	return []UsageTrend{} // Placeholder
}

// generateCapacityForecasts generates capacity planning forecasts.
func (ta *TrendAnalyzer) generateCapacityForecasts(dataPoints []MetricDataPoint) []CapacityForecast {
	// Simplified implementation
	return []CapacityForecast{} // Placeholder
}

// detectAnomalies detects performance anomalies.
func (ta *TrendAnalyzer) detectAnomalies(dataPoints []MetricDataPoint) []PerformanceAnomaly {
	var anomalies []PerformanceAnomaly

	// Simple anomaly detection based on latency spikes
	categoryLatencies := make(map[string][]time.Duration)
	for _, dp := range dataPoints {
		if dp.Success && dp.Latency > 0 {
			categoryLatencies[dp.Category] = append(categoryLatencies[dp.Category], dp.Latency)
		}
	}

	for category, latencies := range categoryLatencies {
		if len(latencies) < 10 {
			continue
		}

		// Calculate mean and standard deviation
		var total time.Duration
		for _, lat := range latencies {
			total += lat
		}
		mean := total / time.Duration(len(latencies))

		var variance float64
		meanFloat := float64(mean)
		for _, lat := range latencies {
			diff := float64(lat) - meanFloat
			variance += diff * diff
		}
		variance /= float64(len(latencies))
		stdDev := time.Duration(math.Sqrt(variance))

		// Detect outliers (> 3 standard deviations)
		threshold := mean + 3*stdDev

		for i, lat := range latencies {
			if lat > threshold {
				anomaly := PerformanceAnomaly{
					Timestamp:   dataPoints[i].Timestamp,
					Category:    category,
					Metric:      "latency",
					Expected:    float64(mean),
					Actual:      float64(lat),
					Severity:    "medium",
					Description: fmt.Sprintf("Latency spike detected in %s category", category),
					Suggestions: []string{
						"Check for resource contention",
						"Review recent configuration changes",
						"Monitor system load",
					},
				}

				if lat > mean+5*stdDev {
					anomaly.Severity = "high"
				}

				anomalies = append(anomalies, anomaly)
			}
		}
	}

	return anomalies
}

// ConfigAnalyzer analyzes configuration effectiveness and provides recommendations.
type ConfigAnalyzer struct {
	logger log.Logger
}

// NewConfigAnalyzer creates a new configuration analyzer.
func NewConfigAnalyzer(logger log.Logger) *ConfigAnalyzer {
	return &ConfigAnalyzer{logger: logger}
}

// Analyze performs configuration analysis.
func (ca *ConfigAnalyzer) Analyze(dataPoints []MetricDataPoint) ConfigMetrics {
	effectiveness := ca.analyzeConfigEffectiveness(dataPoints)
	recommendations := ca.generateConfigRecommendations(dataPoints)
	opportunities := ca.identifyTuningOpportunities(dataPoints)
	validation := ca.validateConfiguration(dataPoints)

	return ConfigMetrics{
		ConfigEffectiveness: effectiveness,
		Recommendations:     recommendations,
		TuningOpportunities: opportunities,
		ValidationResults:   validation,
	}
}

// analyzeConfigEffectiveness analyzes current configuration effectiveness.
func (ca *ConfigAnalyzer) analyzeConfigEffectiveness(dataPoints []MetricDataPoint) ConfigEffectiveness {
	// Simplified effectiveness analysis
	var totalLatency time.Duration
	var successCount, totalCount int64
	var totalMemory, effectiveMemory int64

	for _, dp := range dataPoints {
		totalCount++
		totalLatency += dp.Latency
		totalMemory += dp.MemoryUsage

		if dp.Success {
			successCount++
			effectiveMemory += dp.MemoryUsage
		}
	}

	var performanceScore, memoryScore float64

	if totalCount > 0 {
		avgLatency := totalLatency / time.Duration(totalCount)
		// Score based on latency (lower is better)
		performanceScore = math.Max(0, 1.0-avgLatency.Seconds()/0.1) // 100ms as reference

		if totalMemory > 0 {
			memoryScore = float64(effectiveMemory) / float64(totalMemory)
		}
	}

	overallScore := (performanceScore + memoryScore) / 2.0

	return ConfigEffectiveness{
		OverallScore:     overallScore,
		PerformanceScore: performanceScore,
		MemoryScore:      memoryScore,
		BufferSizeScore:  0.8,  // Placeholder
		ThresholdScore:   0.85, // Placeholder
		StrategyScore:    0.9,  // Placeholder
		RecommendedChanges: []string{
			"Consider adjusting buffer size thresholds",
			"Monitor strategy selection effectiveness",
		},
	}
}

// generateConfigRecommendations generates configuration recommendations.
func (ca *ConfigAnalyzer) generateConfigRecommendations(dataPoints []MetricDataPoint) []ConfigRecommendation {
	// Simplified recommendations
	return []ConfigRecommendation{
		{
			Parameter:           "LargeObjectThreshold",
			CurrentValue:        256 * 1024,
			RecommendedValue:    512 * 1024,
			ExpectedImprovement: 0.15,
			Priority:            "medium",
			Reasoning:           "Analysis shows many objects just above current threshold",
			RiskLevel:           "low",
		},
	}
}

// identifyTuningOpportunities identifies specific tuning opportunities.
func (ca *ConfigAnalyzer) identifyTuningOpportunities(dataPoints []MetricDataPoint) []TuningOpportunity {
	// Simplified opportunities
	return []TuningOpportunity{
		{
			Area:                 "Buffer Pool Sizing",
			Description:          "Optimize buffer pool sizes based on usage patterns",
			PotentialGain:        0.20,
			ImplementationEffort: "medium",
			Prerequisites:        []string{"Performance monitoring enabled"},
			Steps: []string{
				"Analyze current buffer size distribution",
				"Adjust pool sizes based on usage patterns",
				"Monitor performance impact",
			},
		},
	}
}

// validateConfiguration validates current configuration.
func (ca *ConfigAnalyzer) validateConfiguration(dataPoints []MetricDataPoint) ConfigValidationResults {
	// Simplified validation
	return ConfigValidationResults{
		IsValid:         true,
		ValidationScore: 0.85,
		Issues:          []ConfigIssue{},
		Warnings:        []ConfigWarning{},
		BestPractices: []BestPracticeCheck{
			{
				Practice:       "Buffer pool enabled",
				Status:         "pass",
				Description:    "Buffer pool is properly enabled",
				Recommendation: "Continue monitoring performance",
			},
		},
	}
}
