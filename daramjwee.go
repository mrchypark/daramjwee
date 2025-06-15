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
	Set(ctx context.Context, key string, meta Metadata) (io.WriteCloser, error)

	// Delete removes an object from the cache.
	Delete(ctx context.Context, key string) error

	// ScheduleRefresh asynchronously refreshes a cache entry using the provided Fetcher.
	ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error

	// Close gracefully shuts down the cache and its background workers.
	Close()
}

// --- Fetcher and Metadata Types ---

// Metadata holds essential metadata about a cached item.
type Metadata struct {
	Key        string
	ETag       string
	CreatedAt  time.Time
	TTL        time.Duration
	IsNegative bool
}

// FetchResult holds the data and metadata returned from a successful fetch operation.
type FetchResult struct {
	Body io.ReadCloser
	ETag string
	TTL  time.Duration
	// NOTE: The []byte Data field is removed to enforce a stream-only workflow.
}

// Fetcher defines the contract for fetching an object from an origin.
type Fetcher interface {
	Fetch(ctx context.Context, oldETag string) (*FetchResult, error)
}

type Store interface {
	GetStream(key string) (io.ReadCloser, Metadata, error)
	SetWithWriter(key string, meta Metadata) (io.WriteCloser, error)
	Delete(key string) error
	Stat(key string) (Metadata, error)
}

type ContextAwareStore interface {
	GetStreamContext(ctx context.Context, key string) (io.ReadCloser, Metadata, error)
	SetWithWriterContext(ctx context.Context, key string, meta Metadata) (io.WriteCloser, error)
	DeleteContext(ctx context.Context, key string) error
	StatContext(ctx context.Context, key string) (Metadata, error)
}

func New(logger log.Logger, opts ...Option) (Cache, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	// 1. 기본값이 담긴 Config 구조체를 생성합니다.
	cfg := Config{
		DefaultTimeout:   30 * time.Second,
		WorkerStrategy:   "pool", // 기본 워커 전략
		WorkerPoolSize:   10,     // 기본 워커 풀 사이즈
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

	// 4. Config를 바탕으로 내부 구현체를 생성합니다.
	// 이 로직은 worker와 같은 내부 패키지를 알 필요가 있어, 별도 함수로 분리할 수 있습니다.
	// 여기서는 설명을 위해 New 함수 안에 직접 작성합니다.
	workerManager, err := worker.NewManager(cfg.WorkerStrategy, logger, cfg.WorkerPoolSize, cfg.WorkerJobTimeout)
	if err != nil {
		return nil, err
	}

	c := &DaramjweeCache{
		Logger:           logger,
		HotStore:         cfg.HotStore,
		ColdStore:        cfg.ColdStore,
		Worker:           workerManager,
		DefaultTimeout:   cfg.DefaultTimeout,
		NegativeCacheTTL: cfg.NegativeCacheTTL,
	}

	level.Info(logger).Log("msg", "daramjwee cache initialized", "default_timeout", c.DefaultTimeout)
	return c, nil
}
