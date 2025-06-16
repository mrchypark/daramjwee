// Package daramjwee provides a simple, "Good Enough" hybrid caching middleware.
// It is designed with a "stream-only" philosophy to ensure memory safety
// and high performance, particularly for proxying use cases.
// The core component is the Cache interface, implemented by DaramjweeCache,
// which orchestrates interactions between hot and optional cold storage tiers,
// and manages background tasks like data fetching and cache refreshing.
package daramjwee

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
)

// --- Sentinel Errors ---

var (
	// ErrNotFound is returned when an object is not found in the cache or the origin.
	ErrNotFound = errors.New("daramjwee: object not found")

	// ErrNotModified is a sentinel error returned by a Fetcher when the resource
	// at the origin has not changed compared to the cached version.
	ErrNotModified = errors.New("daramjwee: resource not modified")
)

// --- Primary Public Interface ---

// Cache is the primary public interface for interacting with daramjwee.
// It defines a set of operations for getting, setting, deleting, and managing cache entries.
// All operations are designed to be stream-based to ensure memory safety and efficiency.
type Cache interface {
	// Get retrieves an object as a stream (io.ReadCloser) identified by a key.
	// If the object is not found in the cache (cache miss), it uses the provided Fetcher
	// to retrieve the data from the origin. The context is used for timeout and cancellation.
	// The caller is responsible for closing the returned io.ReadCloser after use.
	// Parameters:
	//   ctx: Context for managing the operation's lifecycle, including timeouts.
	//   key: The unique identifier for the cached object.
	//   fetcher: The Fetcher implementation used to get data from the origin on a cache miss.
	// Returns:
	//   An io.ReadCloser for the object's data stream.
	//   An error if the object cannot be retrieved or any other issue occurs.
	Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error)

	// Set provides an io.WriteCloser to stream an object directly into the cache,
	// associating it with the given key and ETag.
	// The cache entry is typically finalized and made visible only when the returned
	// io.WriteCloser is successfully closed. This pattern is useful for scenarios like
	// using io.MultiWriter to simultaneously send a response to a client and write to the cache.
	// IMPORTANT: The caller MUST call Close() on the returned io.WriteCloser to finalize
	// the operation and release associated resources.
	// Parameters:
	//   ctx: Context for managing the operation's lifecycle.
	//   key: The unique identifier for the cached object.
	//   etag: The ETag (entity tag) for the object, used for cache validation.
	// Returns:
	//   An io.WriteCloser to which the object's data can be written.
	//   An error if the cache write operation cannot be initiated.
	Set(ctx context.Context, key string, etag string) (io.WriteCloser, error)

	// Delete removes an object identified by key from all cache tiers.
	// Parameters:
	//   ctx: Context for managing the operation's lifecycle.
	//   key: The unique identifier for the cached object to be deleted.
	// Returns:
	//   An error if the deletion fails for any cache tier.
	Delete(ctx context.Context, key string) error

	// ScheduleRefresh asynchronously refreshes a cache entry identified by key.
	// It uses the provided Fetcher to get the latest data from the origin.
	// This operation is non-blocking and performed by a background worker.
	// Parameters:
	//   ctx: Context for managing the scheduling of the refresh operation.
	//   key: The unique identifier for the cached object to be refreshed.
	//   fetcher: The Fetcher implementation used to get updated data from the origin.
	// Returns:
	//   An error if the refresh job cannot be scheduled (e.g., worker not available).
	ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error

	// Close gracefully shuts down the cache system, including its background workers.
	// It ensures that pending operations are completed or properly terminated.
	// This method should be called when the cache is no longer needed to free resources.
	Close()
}

// --- Fetcher and Metadata Types ---

// Metadata holds essential metadata about a cached item.
// It is designed to be extensible for future needs.
type Metadata struct {
	// ETag (Entity Tag) is a version identifier for a resource.
	// It's used to determine if a cached item is still fresh or needs an update.
	ETag string
	// TODO: Consider adding fields like LastModified (time.Time) or Size (int64) in the future.
}

// FetchResult holds the data stream and metadata returned from a successful fetch operation
// by a Fetcher.
type FetchResult struct {
	// Body is a readable stream of the fetched object's data.
	// The caller is responsible for closing this stream.
	Body io.ReadCloser
	// Metadata contains information about the fetched object, such as its ETag.
	Metadata *Metadata
}

// Fetcher defines the contract for fetching an object from an origin source.
// Implementations of this interface are responsible for retrieving data when a cache miss occurs
// or when a cache entry needs to be refreshed.
type Fetcher interface {
	// Fetch attempts to retrieve an object from the origin.
	// The oldETag parameter can be used by the origin to determine if the resource
	// has changed (e.g., by checking If-None-Match HTTP header).
	// If the resource has not changed, Fetch should return ErrNotModified.
	// Parameters:
	//   ctx: Context for managing the fetch operation's lifecycle.
	//   oldETag: The ETag of the currently cached version, if any. Can be an empty string.
	// Returns:
	//   A pointer to FetchResult containing the data stream and metadata on success.
	//   ErrNotModified if the resource at the origin has not changed since the oldETag.
	//   Any other error if the fetch operation fails.
	Fetch(ctx context.Context, oldETag string) (*FetchResult, error)
}

// Store defines the interface for a single cache storage tier (e.g., memory, disk).
// Each store implementation is responsible for the low-level operations of
// getting, setting, deleting, and stat-ing cache entries.
type Store interface {
	// GetStream retrieves an object and its metadata as a stream from the store.
	// Parameters:
	//   ctx: Context for the operation.
	//   key: The key of the object to retrieve.
	// Returns:
	//   An io.ReadCloser for the object's data, its *Metadata, and an error if any.
	//   Returns ErrNotFound if the key is not found in the store.
	GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)

	// SetWithWriter returns an io.WriteCloser that streams data directly into the store
	// for the given key and ETag. The entry is finalized upon closing the writer.
	// Parameters:
	//   ctx: Context for the operation.
	//   key: The key of the object to set.
	//   etag: The ETag of the object.
	// Returns:
	//   An io.WriteCloser to write the object's data, and an error if any.
	SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error)

	// Delete removes an object from the store.
	// Parameters:
	//   ctx: Context for the operation.
	//   key: The key of the object to delete.
	// Returns:
	//   An error if any issue occurs. Should return nil if key is not found or successfully deleted.
	Delete(ctx context.Context, key string) error

	// Stat retrieves metadata for an object without fetching its data.
	// Parameters:
	//   ctx: Context for the operation.
	//   key: The key of the object to stat.
	// Returns:
	//   The *Metadata for the object and an error if any.
	//   Returns ErrNotFound if the key is not found.
	Stat(ctx context.Context, key string) (*Metadata, error)
}

// EvictionPolicy defines the contract for a cache eviction strategy.
// Implementations determine which items to remove when a cache (typically a Store)
// reaches its capacity limits.
type EvictionPolicy interface {
	// Touch is called when an item with the given key is accessed (e.g., cache hit).
	// This allows the policy to update its internal state (e.g., LRU, LFU).
	Touch(key string)

	// Add is called when a new item with the given key and size (in bytes) is added to the cache.
	// This allows the policy to track new items and their resource consumption.
	Add(key string, size int64)

	// Remove is called when an item with the given key is explicitly deleted from the cache.
	// This allows the policy to update its state accordingly.
	Remove(key string)

	// Evict is called by the cache store when it needs to free up space.
	// The policy should analyze its current state and return a slice of keys
	// that should be evicted. It may return nil or an empty slice if no items
	// are eligible for eviction based on the policy's criteria.
	Evict() []string
}

// nullEvictionPolicy is a Null Object pattern implementation of the EvictionPolicy interface.
// It performs no operations, effectively disabling active eviction. This can be useful
// for stores that manage their own eviction (e.g., an external Redis cache) or when
// eviction is not desired.
type nullEvictionPolicy struct{}

// Touch implements EvictionPolicy.Touch for nullEvictionPolicy. It performs no action.
func (p *nullEvictionPolicy) Touch(key string) {}

// Add implements EvictionPolicy.Add for nullEvictionPolicy. It performs no action.
func (p *nullEvictionPolicy) Add(key string, size int64) {}

// Remove implements EvictionPolicy.Remove for nullEvictionPolicy. It performs no action.
func (p *nullEvictionPolicy) Remove(key string) {}

// Evict implements EvictionPolicy.Evict for nullEvictionPolicy. It returns nil, indicating no items to evict.
func (p *nullEvictionPolicy) Evict() []string { return nil }

// NewNullEvictionPolicy creates a new no-op eviction policy (null object pattern).
// This policy can be used when eviction is handled externally or not needed.
func NewNullEvictionPolicy() EvictionPolicy {
	return &nullEvictionPolicy{}
}

// New creates and configures a new DaramjweeCache instance, which implements the Cache interface.
// It initializes the cache with default settings that can be overridden by providing Option functions.
//
// Parameters:
//   logger: A log.Logger instance for logging messages. If nil, a no-op logger is used.
//   opts: A variadic slice of Option functions. Each Option function takes a pointer to a Config
//         struct and modifies it. This allows for flexible configuration of the cache,
//         such as setting custom hot/cold stores, timeouts, and worker parameters.
//
// Returns:
//   A Cache interface instance (specifically, a *DaramjweeCache).
//   An error if the configuration is invalid (e.g., missing required HotStore) or if
//   any part of the initialization fails (e.g., worker manager creation).
//
// The function performs the following steps:
// 1. Initializes a Config struct with default values (e.g., DefaultTimeout, worker settings).
// 2. Applies each provided Option function to the Config struct, allowing customization.
// 3. Validates the final Config (e.g., ensures HotStore is provided). If ColdStore is not
//    provided, a NullStore (no-op store) is used by default.
// 4. Creates a worker manager based on the configured strategy and parameters.
// 5. Instantiates and returns the DaramjweeCache with the configured components.
func New(logger log.Logger, opts ...Option) (Cache, error) {
	if logger == nil {
		logger = log.NewNopLogger() // Default to a no-op logger if none provided.
	}

	// 1. Create a Config struct with default values.
	cfg := Config{
		DefaultTimeout:   30 * time.Second,    // Default timeout for cache operations.
		WorkerStrategy:   "pool",              // Default worker strategy.
		WorkerPoolSize:   10,                  // Default worker pool size.
		WorkerQueueSize:  100,                 // Default worker queue size.
		WorkerJobTimeout: 30 * time.Second,    // Default timeout for individual worker jobs.
	}

	// 2. Apply user-provided Options to modify the Config.
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err // Return error if an option fails to apply.
		}
	}

	// 3. Validate the final Config.
	if cfg.HotStore == nil {
		return nil, &ConfigError{"hotStore is required"} // HotStore is mandatory.
	}

	if cfg.ColdStore == nil {
		level.Debug(logger).Log("msg", "cold store not configured, using null store")
		cfg.ColdStore = newNullStore() // Default to a no-op ColdStore if not provided.
	}

	// 4. Create the internal worker manager based on the Config.
	workerManager, err := worker.NewManager(cfg.WorkerStrategy, logger, cfg.WorkerPoolSize, cfg.WorkerQueueSize, cfg.WorkerJobTimeout)
	if err != nil {
		return nil, err // Return error if worker manager creation fails.
	}

	// 5. Instantiate DaramjweeCache with the configured components.
	c := &DaramjweeCache{
		Logger:         logger,
		HotStore:       cfg.HotStore,
		ColdStore:      cfg.ColdStore,
		Worker:         workerManager,
		DefaultTimeout: cfg.DefaultTimeout,
	}

	level.Info(logger).Log("msg", "daramjwee cache initialized", "default_timeout", c.DefaultTimeout)
	return c, nil
}
