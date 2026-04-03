package daramjwee

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
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

// ErrNilFetcher is returned when a cache operation that may call the origin
// is invoked without a Fetcher.
var ErrNilFetcher = errors.New("daramjwee: nil fetcher")

// Cache is the primary public interface for interacting with daramjwee.
// It enforces a memory-safe, stream-based interaction model.
type Cache interface {
	// Get retrieves an object as a stream.
	// On a cache miss, it uses the provided Fetcher to retrieve data from the origin.
	// The fetcher must not be nil.
	// The caller is responsible for closing the returned io.ReadCloser.
	Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error)

	// Set provides a writer to stream an object into the cache.
	// The cache entry is finalized when the returned writer is closed.
	// This pattern is ideal for use with io.MultiWriter for simultaneous
	// response-to-client and writing-to-cache scenarios.
	// NOTE: The caller is responsible for calling Close() or Abort() on the
	// returned sink to ensure resources are released.
	Set(ctx context.Context, key string, metadata *Metadata) (WriteSink, error)

	// Delete removes an object from the cache.
	Delete(ctx context.Context, key string) error

	// ScheduleRefresh asynchronously refreshes a cache entry using the provided Fetcher.
	// The fetcher must not be nil.
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

// FetchUsesContext is an optional Fetcher extension for fetchers whose returned
// body continues using the provided context after Fetch returns.
type FetchUsesContext interface {
	FetchUsesContext() bool
}

// WriteSink is the terminal write contract for cache stores.
// Close publishes the staged write, and Abort discards it.
type WriteSink interface {
	io.WriteCloser
	Abort() error
}

// Store defines the interface for a single cache storage tier (e.g., memory, disk).
type Store interface {
	// GetStream retrieves an object and its metadata as a stream.
	// Successful lookups must return non-nil metadata.
	GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
	// BeginSet returns a sink that stages data into the store.
	BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error)
	// Delete removes an object from the store.
	Delete(ctx context.Context, key string) error
	// Stat retrieves metadata for an object without its data.
	Stat(ctx context.Context, key string) (*Metadata, error)
}

// GetStreamUsesContext is an optional Store extension for backends whose
// returned readers continue using the provided context after GetStream returns.
type GetStreamUsesContext interface {
	GetStreamUsesContext() bool
}

// BeginSetUsesContext is an optional Store extension for backends whose
// returned sinks continue using the provided context after BeginSet returns.
type BeginSetUsesContext interface {
	BeginSetUsesContext() bool
}

// TierValidator is an optional Store extension for stores that restrict which
// positions they can safely occupy in the ordered tier chain.
type TierValidator interface {
	ValidateTier(index int) error
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
		OpTimeout:         30 * time.Second,
		Workers:           1,
		WorkerQueue:       500,
		WorkerTimeout:     30 * time.Second,
		CloseTimeout:      30 * time.Second,
		PositiveFreshness: 0,
		NegativeFreshness: 0,
	}

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	if len(cfg.Tiers) == 0 {
		return nil, &ConfigError{"at least one tier is required"}
	}

	seen := make([]Store, 0, len(cfg.Tiers))
	for idx, tier := range cfg.Tiers {
		if isNilStore(tier) {
			return nil, &ConfigError{"tier cannot be nil"}
		}
		if containsSameStore(seen, tier) {
			return nil, &ConfigError{"duplicate tier store instance"}
		}
		if validator, ok := tier.(TierValidator); ok {
			if err := validator.ValidateTier(idx); err != nil {
				return nil, &ConfigError{fmt.Sprintf("tier %d %s", idx, err.Error())}
			}
		}
		seen = append(seen, tier)
	}
	for idx := range cfg.TierFreshnessOverrides {
		if idx >= len(cfg.Tiers) {
			return nil, &ConfigError{fmt.Sprintf("tier freshness override index %d is out of range", idx)}
		}
	}

	workerManager, err := worker.NewManager("pool", logger, cfg.Workers, cfg.WorkerQueue, cfg.WorkerTimeout)
	if err != nil {
		return nil, err
	}

	c := &DaramjweeCache{
		Logger:                 logger,
		Tiers:                  append([]Store(nil), cfg.Tiers...),
		Worker:                 workerManager,
		OpTimeout:              cfg.OpTimeout,
		CloseTimeout:           cfg.CloseTimeout,
		PositiveFreshness:      cfg.PositiveFreshness,
		NegativeFreshness:      cfg.NegativeFreshness,
		TierFreshnessOverrides: cloneTierFreshnessOverrides(cfg.TierFreshnessOverrides),
		loggingDisabled:        isNoopLogger(logger),
	}

	level.Info(logger).Log("msg", "daramjwee cache initialized", "op_timeout", c.OpTimeout)
	return c, nil
}

func containsSameStore(stores []Store, candidate Store) bool {
	for _, existing := range stores {
		if sameStoreInstance(existing, candidate) {
			return true
		}
	}
	return false
}

func cloneTierFreshnessOverrides(src map[int]TierFreshnessOverride) map[int]TierFreshnessOverride {
	if len(src) == 0 {
		return nil
	}

	cloned := make(map[int]TierFreshnessOverride, len(src))
	for index, override := range src {
		copied := TierFreshnessOverride{}
		if override.Positive != nil {
			copied.Positive = durationPtr(*override.Positive)
		}
		if override.Negative != nil {
			copied.Negative = durationPtr(*override.Negative)
		}
		cloned[index] = copied
	}
	return cloned
}

func sameStoreInstance(a, b Store) bool {
	if isNilStore(a) || isNilStore(b) {
		return isNilStore(a) == isNilStore(b)
	}

	ta := reflect.TypeOf(a)
	tb := reflect.TypeOf(b)
	if ta != tb {
		return false
	}

	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	switch va.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return va.Pointer() == vb.Pointer()
	default:
		return false
	}
}

func isNilStore(store Store) bool {
	if store == nil {
		return true
	}

	v := reflect.ValueOf(store)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}
