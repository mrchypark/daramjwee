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

// ErrNotFound is returned when an object is not found in the cache or the origin.
var ErrNotFound = errors.New("daramjwee: object not found")

// ErrNotModified is a sentinel error returned by a Fetcher when the resource
// at the origin has not changed compared to the cached version.
var ErrNotModified = errors.New("daramjwee: resource not modified")

// ErrCacheableNotFound is returned when a resource is not found, but this state
// is cacheable (e.g., a negative cache entry).
var ErrCacheableNotFound = errors.New("daramjwee: resource not found, but this state is cacheable")

// Cache is the primary public interface for interacting with daramjwee.
// It enforces a memory-safe, stream-based interaction model.
type Cache interface {
	// Get retrieves an object as a stream.
	// On a cache miss, it uses the provided Fetcher to retrieve data from the origin.
	// The caller is responsible for closing the returned io.ReadCloser.
	Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error)

	// Set provides a writer to stream an object into the cache.
	// The cache entry is finalized when the returned writer is closed.
	// This pattern is ideal for use with io.MultiWriter for simultaneous
	// response-to-client and writing-to-cache scenarios.
	// NOTE: The caller is responsible for calling Close() on the returned
	// io.WriteCloser to ensure the cache entry is committed and resources are released.
	Set(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error)

	// Delete removes an object from the cache.
	Delete(ctx context.Context, key string) error

	// ScheduleRefresh asynchronously refreshes a cache entry using the provided Fetcher.
	ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error

	// Close gracefully shuts down the cache and its background workers.
	Close()
}

// Metadata holds essential metadata about a cached item.
// It is designed to be extensible for future needs (e.g., LastModified, Size).
type Metadata struct {
	ETag       string
	IsNegative bool
	CachedAt   time.Time
}

// FetchResult holds the data and metadata returned from a successful fetch operation.
type FetchResult struct {
	Body     io.ReadCloser
	Metadata *Metadata
}

// Fetcher defines the contract for fetching an object from an origin.
type Fetcher interface {
	Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error)
}

// Store defines the interface for a single cache storage tier (e.g., memory, disk).
type Store interface {
	// GetStream retrieves an object and its metadata as a stream.
	GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
	// SetWithWriter returns a writer that streams data into the store.
	SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error)
	// Delete removes an object from the store.
	Delete(ctx context.Context, key string) error
	// Stat retrieves metadata for an object without its data.
	Stat(ctx context.Context, key string) (*Metadata, error)
}

// EvictionPolicy defines the contract for a cache eviction strategy.
type EvictionPolicy interface {
	// Touch is called when an item is accessed.
	Touch(key string)
	// Add is called when a new item is added, along with its size in bytes.
	Add(key string, size int64)
	// Remove is called when an item is explicitly deleted.
	Remove(key string)
	// Evict is called to determine which item(s) should be evicted.
	// It should return one or more keys to be removed.
	Evict() []string
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
	}

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	if cfg.HotStore == nil {
		return nil, &ConfigError{"hotStore is required"}
	}

	// Prevent using the same store instance for both hot and cold tiers
	if cfg.ColdStore != nil && cfg.HotStore == cfg.ColdStore {
		return nil, &ConfigError{"hot store and cold store cannot be the same instance"}
	}

	if cfg.ColdStore == nil {
		level.Debug(logger).Log("msg", "cold store not configured, using null store")
		cfg.ColdStore = newNullStore()
	}

	workerManager, err := worker.NewManager(cfg.WorkerStrategy, logger, cfg.WorkerPoolSize, cfg.WorkerQueueSize, cfg.WorkerJobTimeout)
	if err != nil {
		return nil, err
	}

	c := &DaramjweeCache{
		Logger:                    logger,
		HotStore:                  cfg.HotStore,
		ColdStore:                 cfg.ColdStore,
		Worker:                    workerManager,
		DefaultTimeout:            cfg.DefaultTimeout,
		ShutdownTimeout:           cfg.ShutdownTimeout,
		PositiveFreshFor:          cfg.PositiveFreshFor,
		NegativeFreshFor:          cfg.NegativeFreshFor,
		ColdStorePositiveFreshFor: cfg.ColdStorePositiveFreshFor,
		ColdStoreNegativeFreshFor: cfg.ColdStoreNegativeFreshFor,
	}

	level.Info(logger).Log("msg", "daramjwee cache initialized", "default_timeout", c.DefaultTimeout)
	return c, nil
}
