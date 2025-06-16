// Package daramjwee provides a simple, "Good Enough" hybrid caching middleware.
// It is designed with a "stream-only" philosophy to ensure memory safety
// and high performance for proxying use cases.
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
	Set(ctx context.Context, key string, etag string) (io.WriteCloser, error)

	// Delete removes an object from the cache.
	Delete(ctx context.Context, key string) error

	// ScheduleRefresh asynchronously refreshes a cache entry using the provided Fetcher.
	ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error

	// Close gracefully shuts down the cache and its background workers.
	Close()
}

// --- Fetcher and Metadata Types ---

// Metadata holds essential metadata about a cached item.
// It is designed to be extensible for future needs (e.g., LastModified, Size).
type Metadata struct {
	ETag string
}

// FetchResult holds the data and metadata returned from a successful fetch operation.
type FetchResult struct {
	Body     io.ReadCloser
	Metadata *Metadata
}

// Fetcher defines the contract for fetching an object from an origin.
type Fetcher interface {
	Fetch(ctx context.Context, oldETag string) (*FetchResult, error)
}

// Store defines the interface for a single cache storage tier (e.g., memory, disk).
type Store interface {
	// GetStream retrieves an object and its metadata as a stream.
	GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
	// SetWithWriter returns a writer that streams data into the store.
	SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error)
	// Delete removes an object from the store.
	Delete(ctx context.Context, key string) error
	// Stat retrieves metadata for an object without its data.
	Stat(ctx context.Context, key string) (*Metadata, error)
}

// New creates and configures a new DaramjweeCache instance.
func New(logger log.Logger, opts ...Option) (Cache, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	// 1. 기본값이 담긴 Config 구조체를 생성합니다.
	cfg := Config{
		DefaultTimeout:   30 * time.Second,
		WorkerStrategy:   "pool", // 기본 워커 전략
		WorkerPoolSize:   10,     // 기본 워커 풀 사이즈
		WorkerQueueSize:  100,    // 기본 워커 큐 사이즈,
		WorkerJobTimeout: 30 * time.Second,
	}

	// 2. 사용자가 제공한 Option들을 적용하여 Config를 수정합니다.
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	// 3. 최종 Config를 검증합니다.
	if cfg.HotStore == nil {
		return nil, &ConfigError{"hotStore is required"}
	}

	if cfg.ColdStore == nil {
		level.Debug(logger).Log("msg", "cold store not configured, using null store")
		cfg.ColdStore = newNullStore()
	}

	// 4. Config를 바탕으로 내부 구현체를 생성합니다.
	workerManager, err := worker.NewManager(cfg.WorkerStrategy, logger, cfg.WorkerPoolSize, cfg.WorkerQueueSize, cfg.WorkerJobTimeout)
	if err != nil {
		return nil, err
	}

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
