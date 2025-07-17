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
// It provides optimized buffer allocation and reuse for stream operations.
type BufferPool interface {
	// Get retrieves a buffer from the pool with the specified size.
	// The returned buffer may be larger than requested but never smaller.
	Get(size int) []byte

	// Put returns a buffer to the pool for reuse.
	// The buffer should not be used after calling Put.
	Put(buf []byte)

	// CopyBuffer performs optimized copy using pooled buffers.
	// It's equivalent to io.Copy but uses buffer pool for better performance.
	CopyBuffer(dst io.Writer, src io.Reader) (int64, error)

	// TeeReader creates an optimized TeeReader using pooled buffers.
	// It's equivalent to io.TeeReader but uses buffer pool for better performance.
	TeeReader(r io.Reader, w io.Writer) io.Reader

	// GetStats returns current buffer pool statistics.
	GetStats() BufferPoolStats
}

// BufferPoolConfig holds configuration for buffer pool behavior.
type BufferPoolConfig struct {
	// Enabled controls whether buffer pooling is active.
	// When false, operations fall back to standard library implementations.
	Enabled bool

	// DefaultBufferSize is the default buffer size for pool operations.
	// This should match common use cases for optimal performance.
	DefaultBufferSize int

	// MaxBufferSize is the maximum buffer size that will be pooled.
	// Buffers larger than this will not be returned to the pool to prevent memory bloat.
	MaxBufferSize int

	// MinBufferSize is the minimum buffer size that will be pooled.
	// Buffers smaller than this will not be returned to the pool.
	MinBufferSize int

	// EnableLogging controls whether buffer pool performance metrics are logged.
	// When false, no performance logging is performed to avoid overhead.
	EnableLogging bool

	// LoggingInterval specifies how often buffer pool statistics are logged.
	// Only used when EnableLogging is true. Zero value disables periodic logging.
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
var (
	// ErrCompressionFailed is returned when compression operation fails
	ErrCompressionFailed = errors.New("daramjwee: compression failed")

	// ErrDecompressionFailed is returned when decompression operation fails
	ErrDecompressionFailed = errors.New("daramjwee: decompression failed")

	// ErrUnsupportedAlgorithm is returned when an unsupported compression algorithm is used
	ErrUnsupportedAlgorithm = errors.New("daramjwee: unsupported compression algorithm")

	// ErrCorruptedData is returned when compressed data is corrupted
	ErrCorruptedData = errors.New("daramjwee: corrupted compressed data")

	// ErrInvalidCompressionLevel is returned when an invalid compression level is set
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
