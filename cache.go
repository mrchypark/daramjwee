// Package daramjwee contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
)

// DaramjweeCache is a concrete implementation of the Cache interface with N-tier architecture.
//
// N-tier Architecture Design:
//   - Stores slice represents cache hierarchy from fastest (stores[0]) to slowest (stores[n-1])
//   - stores[0] is the primary tier for writes and first lookup
//   - Sequential lookup through all tiers on cache miss
//   - Automatic promotion from lower tiers to upper tiers on cache hit
//   - Simplified logic compared to legacy hot/cold tier implementation
//
// Backward Compatibility:
//   - Legacy WithHotStore/WithColdStore options are automatically converted to Stores slice
//   - Existing applications continue to work without code changes
//   - New WithStores() option enables N-tier configurations
type DaramjweeCache struct {
	Stores           []Store // N-tier cache hierarchy from fastest to slowest
	Logger           log.Logger
	Worker           *worker.Manager
	BufferPool       BufferPool
	DefaultTimeout   time.Duration
	ShutdownTimeout  time.Duration
	PositiveFreshFor time.Duration
	NegativeFreshFor time.Duration
	isClosed         atomic.Bool
}

var _ Cache = (*DaramjweeCache)(nil)

// Get retrieves data using N-tier sequential lookup strategy.
// It checks stores in order from fastest (stores[0]) to slowest (stores[n-1]),
// then fetches from origin if not found in any tier.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	level.Debug(c.Logger).Log("msg", "starting sequential lookup across tiers", "key", key, "total_tiers", len(c.Stores))

	// Sequential lookup through all tiers
	for i, store := range c.Stores {
		level.Debug(c.Logger).Log("msg", "checking tier for cache hit", "key", key, "tier", i)

		stream, metadata, err := c.getStreamFromStore(ctx, store, key)
		if err == nil {
			if i == 0 {
				// Primary tier hit - handle staleness and background refresh
				return c.handlePrimaryHit(ctx, key, fetcher, stream, metadata)
			} else {
				// Lower tier hit - promote to upper tiers
				return c.handleTierHit(ctx, key, i, stream, metadata)
			}
		}
		if !errors.Is(err, ErrNotFound) {
			// Wrap error with tier information for better debugging
			tierErr := NewTierError(i, "get", key, err)
			level.Error(c.Logger).Log("msg", "tier lookup failed with error", "key", key, "tier", i, "err", tierErr)
		} else {
			level.Debug(c.Logger).Log("msg", "cache miss in tier", "key", key, "tier", i)
		}
	}

	// Cache miss - fetch from origin
	return c.handleMiss(ctx, key, fetcher)
}

// Set returns a WriteCloser to directly write data to the cache.
// The data is written to the primary tier (stores[0]).
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	if c.isClosed.Load() {
		level.Debug(c.Logger).Log("msg", "set operation rejected, cache is closed", "key", key)
		return nil, ErrCacheClosed
	}
	if len(c.Stores) == 0 {
		level.Error(c.Logger).Log("msg", "set operation failed, no stores configured", "key", key)
		return nil, &ConfigError{"no stores configured"}
	}

	level.Debug(c.Logger).Log("msg", "initiating set operation", "key", key, "tier", 0, "is_negative", metadata != nil && metadata.IsNegative)

	ctx, cancel := c.newCtxWithTimeout(ctx)

	if metadata == nil {
		metadata = &Metadata{}
	}
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToStore(ctx, c.Stores[0], key, metadata)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get writer for set operation", "key", key, "tier", 0, "err", err)
		cancel()
		return nil, err
	}

	level.Debug(c.Logger).Log("msg", "set operation writer created successfully", "key", key, "tier", 0)
	return newCancelWriteCloser(wc, cancel), nil
}

// Delete sequentially deletes an object from all cache tiers.
// It attempts to delete from all stores, continuing on failure to ensure cleanup.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	if c.isClosed.Load() {
		level.Debug(c.Logger).Log("msg", "delete operation rejected, cache is closed", "key", key)
		return ErrCacheClosed
	}

	level.Debug(c.Logger).Log("msg", "initiating delete operation across all tiers", "key", key, "total_tiers", len(c.Stores))

	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	var firstErr error
	var successCount int

	// Delete from all tiers, continuing on failure
	for i, store := range c.Stores {
		if err := c.deleteFromStore(ctx, store, key); err != nil && !errors.Is(err, ErrNotFound) {
			tierErr := NewTierError(i, "delete", key, err)
			level.Error(c.Logger).Log("msg", "failed to delete from tier", "key", key, "tier", i, "err", tierErr)
			if firstErr == nil {
				firstErr = tierErr
			}
		} else if err == nil {
			level.Debug(c.Logger).Log("msg", "successfully deleted from tier", "key", key, "tier", i)
			successCount++
		} else {
			// ErrNotFound case
			level.Debug(c.Logger).Log("msg", "key not found in tier during delete", "key", key, "tier", i)
		}
	}

	if firstErr == nil {
		level.Debug(c.Logger).Log("msg", "delete operation completed successfully", "key", key, "successful_tiers", successCount, "total_tiers", len(c.Stores))
	} else {
		level.Warn(c.Logger).Log("msg", "delete operation completed with errors", "key", key, "successful_tiers", successCount, "total_tiers", len(c.Stores), "first_error", firstErr)
	}

	return firstErr
}

// ScheduleRefresh submits a background cache refresh job to the worker.
func (c *DaramjweeCache) ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error {
	if c.isClosed.Load() {
		return ErrCacheClosed
	}

	if c.Worker == nil {
		return errors.New("worker is not configured, cannot schedule refresh")
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background refresh", "key", key, "tier", 0)

		var oldMetadata *Metadata
		if len(c.Stores) > 0 {
			if meta, err := c.statFromStore(jobCtx, c.Stores[0], key); err == nil && meta != nil {
				oldMetadata = meta
				level.Debug(c.Logger).Log("msg", "retrieved existing metadata for refresh", "key", key, "tier", 0, "cached_at", meta.CachedAt, "is_negative", meta.IsNegative)
			} else {
				level.Debug(c.Logger).Log("msg", "no existing metadata found for refresh", "key", key, "tier", 0)
			}
		}

		result, err := fetcher.Fetch(jobCtx, oldMetadata)
		if err != nil {
			if errors.Is(err, ErrCacheableNotFound) {
				level.Info(c.Logger).Log("msg", "background refresh resulted in not found, caching as negative entry", "key", key, "tier", 0)
				c.handleNegativeCache(jobCtx, key)
			} else if errors.Is(err, ErrNotModified) {
				level.Info(c.Logger).Log("msg", "background refresh: object not modified, keeping existing cache", "key", key, "tier", 0)
			} else {
				level.Error(c.Logger).Log("msg", "background fetch failed during refresh", "key", key, "tier", 0, "err", err)
			}
			return
		}
		defer result.Body.Close()

		if result.Metadata == nil {
			result.Metadata = &Metadata{}
		}
		result.Metadata.CachedAt = time.Now()

		if len(c.Stores) == 0 {
			level.Error(c.Logger).Log("msg", "no stores configured for background refresh", "key", key)
			return
		}

		level.Debug(c.Logger).Log("msg", "writing refreshed data to primary tier", "key", key, "tier", 0, "is_negative", result.Metadata.IsNegative)

		writer, err := c.setStreamToStore(jobCtx, c.Stores[0], key, result.Metadata)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer for background refresh", "key", key, "tier", 0, "err", err)
			return
		}

		var copyErr error
		if c.BufferPool != nil {
			_, copyErr = c.BufferPool.CopyBuffer(writer, result.Body)
		} else {
			// Fallback to standard io.Copy if buffer pool is not available
			_, copyErr = io.Copy(writer, result.Body)
		}
		closeErr := writer.Close()

		if copyErr != nil || closeErr != nil {
			level.Error(c.Logger).Log("msg", "failed to write refreshed data", "key", key, "tier", 0, "copy_err", copyErr, "close_err", closeErr)
		} else {
			level.Info(c.Logger).Log("msg", "background refresh completed successfully", "key", key, "tier", 0, "is_negative", result.Metadata.IsNegative)
		}
	}

	c.Worker.Submit(job)
	return nil
}

// Close safely shuts down the worker.
func (c *DaramjweeCache) Close() {
	if c.isClosed.Swap(true) {
		// Already closed, do nothing (prevent duplicate calls)
		level.Debug(c.Logger).Log("msg", "close called on already closed cache")
		return
	}

	level.Info(c.Logger).Log("msg", "initiating daramjwee cache shutdown", "total_tiers", len(c.Stores), "shutdown_timeout", c.ShutdownTimeout)

	if c.Worker != nil {
		level.Info(c.Logger).Log("msg", "shutting down worker manager")
		if err := c.Worker.Shutdown(c.ShutdownTimeout); err != nil {
			level.Error(c.Logger).Log("msg", "worker shutdown failed", "err", err, "timeout", c.ShutdownTimeout)
		} else {
			level.Info(c.Logger).Log("msg", "worker shutdown completed successfully")
		}
	} else {
		level.Debug(c.Logger).Log("msg", "no worker configured, skipping worker shutdown")
	}

	level.Info(c.Logger).Log("msg", "daramjwee cache shutdown complete")
}

// handlePrimaryHit processes cache hits from the primary tier (stores[0]).
// It handles staleness detection and background refresh scheduling.
func (c *DaramjweeCache) handlePrimaryHit(ctx context.Context, key string, fetcher Fetcher, stream io.ReadCloser, meta *Metadata) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "cache hit in primary tier", "key", key, "tier", 0, "is_negative", meta.IsNegative, "cached_at", meta.CachedAt)

	var isStale bool
	// Calculate expiration using metadata and FreshFor duration
	if meta.IsNegative {
		freshnessLifetime := c.NegativeFreshFor
		if freshnessLifetime == 0 || (freshnessLifetime > 0 && time.Now().After(meta.CachedAt.Add(freshnessLifetime))) {
			isStale = true
		}
	} else {
		freshnessLifetime := c.PositiveFreshFor
		if freshnessLifetime == 0 || (freshnessLifetime > 0 && time.Now().After(meta.CachedAt.Add(freshnessLifetime))) {
			isStale = true
		}
	}

	streamCloser := newCloserWithCallback(stream, func() {})

	if isStale {
		level.Debug(c.Logger).Log("msg", "primary cache is stale, scheduling background refresh", "key", key, "tier", 0, "cached_at", meta.CachedAt, "is_negative", meta.IsNegative)
		streamCloser = newCloserWithCallback(stream, func() { c.ScheduleRefresh(context.Background(), key, fetcher) })
	} else {
		level.Debug(c.Logger).Log("msg", "primary cache is fresh", "key", key, "tier", 0, "cached_at", meta.CachedAt, "is_negative", meta.IsNegative)
	}

	if meta.IsNegative {
		streamCloser.Close()
		return nil, ErrNotFound
	}

	return streamCloser, nil
}

// handleTierHit processes cache hits from lower tiers (stores[i] where i > 0).
// It promotes the data to upper tiers while streaming to the client.
func (c *DaramjweeCache) handleTierHit(ctx context.Context, key string, tierIndex int, stream io.ReadCloser, meta *Metadata) (io.ReadCloser, error) {
	level.Info(c.Logger).Log("msg", "cache hit in lower tier, initiating promotion", "key", key, "source_tier", tierIndex, "is_negative", meta.IsNegative, "cached_at", meta.CachedAt)

	// Create a copy of the metadata for promotion
	// This prevents data races if multiple goroutines handle tier hits concurrently
	metaToPromote := &Metadata{}
	if meta != nil {
		// Copy values from existing metadata
		*metaToPromote = *meta
	}
	// Update the CachedAt field to current time for promotion
	metaToPromote.CachedAt = time.Now()

	// Promote to all upper tiers (stores[0] through stores[tierIndex-1])
	return c.promoteToUpperTiers(ctx, key, tierIndex, metaToPromote, stream)
}

// handleMiss processes the logic when an object is not found in any cache tier.
func (c *DaramjweeCache) handleMiss(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	level.Info(c.Logger).Log("msg", "cache miss in all tiers, fetching from origin", "key", key, "total_tiers", len(c.Stores))

	var oldMetadata *Metadata
	if len(c.Stores) > 0 {
		if meta, err := c.statFromStore(ctx, c.Stores[0], key); err == nil {
			oldMetadata = meta
		}
	}

	result, err := fetcher.Fetch(ctx, oldMetadata)
	if err != nil {
		if errors.Is(err, ErrCacheableNotFound) {
			return c.handleNegativeCache(ctx, key)
		}
		if errors.Is(err, ErrNotModified) {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from primary cache again", "key", key)
			if len(c.Stores) > 0 {
				stream, meta, err := c.getStreamFromStore(ctx, c.Stores[0], key)
				if err != nil {
					level.Warn(c.Logger).Log("msg", "failed to refetch from primary cache after 304", "key", key, "err", err)
					return nil, ErrNotFound
				}
				if meta.IsNegative {
					stream.Close()
					return nil, ErrNotFound
				}
				return stream, nil
			}
			return nil, ErrNotFound
		}
		return nil, err
	}

	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata.CachedAt = time.Now()

	teeStream, err := c.cacheAndTeeStream(ctx, key, result)
	if err != nil {
		return result.Body, nil
	}

	return teeStream, nil
}

// handleNegativeCache processes the logic for storing a negative cache entry.
func (c *DaramjweeCache) handleNegativeCache(ctx context.Context, key string) (io.ReadCloser, error) {
	level.Info(c.Logger).Log("msg", "storing negative cache entry", "key", key, "tier", 0, "negative_fresh_for", c.NegativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	if len(c.Stores) > 0 {
		writer, err := c.setStreamToStore(ctx, c.Stores[0], key, meta)
		if err != nil {
			level.Warn(c.Logger).Log("msg", "failed to get writer for negative cache entry", "key", key, "tier", 0, "err", err)
		} else {
			if closeErr := writer.Close(); closeErr != nil {
				level.Warn(c.Logger).Log("msg", "failed to close writer for negative cache entry", "key", key, "tier", 0, "err", closeErr)
			} else {
				level.Debug(c.Logger).Log("msg", "negative cache entry stored successfully", "key", key, "tier", 0)
			}
		}
	}
	return nil, ErrNotFound
}

// newCtxWithTimeout applies the default timeout to the context if no deadline is set.
func (c *DaramjweeCache) newCtxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.DefaultTimeout)
}

// getStreamFromStore is a wrapper that calls the Store interface's GetStream method.
func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, *Metadata, error) {
	return store.GetStream(ctx, key)
}

// setStreamToStore is a wrapper that calls the Store interface's SetWithWriter method.
func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, metadata *Metadata) (io.WriteCloser, error) {
	return store.SetWithWriter(ctx, key, metadata)
}

// deleteFromStore is a wrapper that calls the Store interface's Delete method.
func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	return store.Delete(ctx, key)
}

// statFromStore is a wrapper that calls the Store interface's Stat method.
func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (*Metadata, error) {
	return store.Stat(ctx, key)
}

// scheduleSetToStore schedules an asynchronous copy of the primary cache content to destination store.
func (c *DaramjweeCache) scheduleSetToStore(ctx context.Context, destStore Store, key string) {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule set", "key", key)
		return
	}

	if len(c.Stores) == 0 {
		level.Warn(c.Logger).Log("msg", "no stores configured, cannot schedule set", "key", key)
		return
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background set", "key", key, "dest", "secondary")

		srcStream, meta, err := c.getStreamFromStore(jobCtx, c.Stores[0], key)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get stream from primary store for background set", "key", key, "err", err)
			return
		}
		defer srcStream.Close()

		destWriter, err := c.setStreamToStore(jobCtx, destStore, key, meta)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get writer for dest store for background set", "key", key, "err", err)
			return
		}

		var copyErr error
		if c.BufferPool != nil {
			_, copyErr = c.BufferPool.CopyBuffer(destWriter, srcStream)
		} else {
			// Fallback to standard io.Copy if buffer pool is not available
			_, copyErr = io.Copy(destWriter, srcStream)
		}
		closeErr := destWriter.Close()

		if copyErr != nil || closeErr != nil {
			level.Error(c.Logger).Log("msg", "failed background set", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			level.Info(c.Logger).Log("msg", "background set successful", "key", key, "dest", "secondary")
		}
	}

	c.Worker.Submit(job)
}

// promoteAndTeeStream promotes a lower tier stream to primary tier while simultaneously returning it to the user.
func (c *DaramjweeCache) promoteAndTeeStream(ctx context.Context, key string, metadata *Metadata, stream io.ReadCloser) (io.ReadCloser, error) {
	if len(c.Stores) == 0 {
		level.Error(c.Logger).Log("msg", "no stores configured for promotion", "key", key)
		return stream, nil
	}

	primaryWriter, err := c.setStreamToStore(ctx, c.Stores[0], key, metadata)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get primary store writer for promotion", "key", key, "err", err)
		return stream, nil
	}

	var teeReader io.Reader
	if c.BufferPool != nil {
		teeReader = c.BufferPool.TeeReader(stream, primaryWriter)
	} else {
		// Fallback to standard io.TeeReader if buffer pool is not available
		teeReader = io.TeeReader(stream, primaryWriter)
	}
	return newMultiCloser(teeReader, []io.Closer{stream, primaryWriter}), nil
}

// cacheAndTeeStream caches the origin stream in the primary cache while simultaneously returning it to the user.
func (c *DaramjweeCache) cacheAndTeeStream(ctx context.Context, key string, result *FetchResult) (io.ReadCloser, error) {
	if len(c.Stores) > 0 && result.Metadata != nil {
		level.Debug(c.Logger).Log("msg", "caching origin data to primary tier", "key", key, "tier", 0, "is_negative", result.Metadata.IsNegative)

		cacheWriter, err := c.setStreamToStore(ctx, c.Stores[0], key, result.Metadata)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer for origin data", "key", key, "tier", 0, "err", err)
			return result.Body, err
		}

		var teeReader io.Reader
		if c.BufferPool != nil {
			teeReader = c.BufferPool.TeeReader(result.Body, cacheWriter)
		} else {
			// Fallback to standard io.TeeReader if buffer pool is not available
			teeReader = io.TeeReader(result.Body, cacheWriter)
		}

		level.Debug(c.Logger).Log("msg", "origin data tee stream created, serving to client and caching simultaneously", "key", key, "tier", 0)
		return newMultiCloserWithLogging(teeReader, []io.Closer{result.Body, cacheWriter}, c.Logger, key, "origin_caching"), nil
	}

	level.Debug(c.Logger).Log("msg", "no caching configured, serving origin data directly", "key", key)
	return result.Body, nil
}

// promoteToUpperTiers promotes data from a lower tier to all upper tiers simultaneously.
// It uses io.MultiWriter to write to multiple upper tiers concurrently while streaming to the client.
func (c *DaramjweeCache) promoteToUpperTiers(ctx context.Context, key string, tierIndex int, metadata *Metadata, stream io.ReadCloser) (io.ReadCloser, error) {
	if tierIndex <= 0 || tierIndex >= len(c.Stores) {
		level.Error(c.Logger).Log("msg", "invalid tier index for promotion", "key", key, "source_tier", tierIndex, "total_tiers", len(c.Stores))
		return stream, nil
	}

	level.Info(c.Logger).Log("msg", "starting promotion to upper tiers", "key", key, "source_tier", tierIndex, "target_tier_count", tierIndex, "is_negative", metadata.IsNegative)

	// Create writers for all upper tiers (stores[0] through stores[tierIndex-1])
	var writers []io.WriteCloser
	var closers []io.Closer
	var successfulWriters []int

	// Add the original stream to closers
	closers = append(closers, stream)

	for i := 0; i < tierIndex; i++ {
		writer, err := c.setStreamToStore(ctx, c.Stores[i], key, metadata)
		if err != nil {
			tierErr := NewTierError(i, "set", key, err)
			level.Error(c.Logger).Log("msg", "failed to create promotion writer", "key", key, "target_tier", i, "source_tier", tierIndex, "err", tierErr)
			// Close any writers we've already created
			for _, w := range writers {
				w.Close()
			}
			level.Warn(c.Logger).Log("msg", "promotion failed, serving from source tier only", "key", key, "source_tier", tierIndex, "failed_target_tier", i)
			return stream, nil
		}
		level.Debug(c.Logger).Log("msg", "promotion writer created successfully", "key", key, "target_tier", i, "source_tier", tierIndex)
		writers = append(writers, writer)
		closers = append(closers, writer)
		successfulWriters = append(successfulWriters, i)
	}

	if len(writers) == 0 {
		level.Warn(c.Logger).Log("msg", "no promotion writers created", "key", key, "source_tier", tierIndex)
		return stream, nil
	}

	// Create MultiWriter to write to all upper tiers simultaneously
	var multiWriter io.Writer
	if len(writers) == 1 {
		multiWriter = writers[0]
	} else {
		// Convert []io.WriteCloser to []io.Writer
		ioWriters := make([]io.Writer, len(writers))
		for i, w := range writers {
			ioWriters[i] = w
		}
		multiWriter = io.MultiWriter(ioWriters...)
	}

	// Create TeeReader to simultaneously read from stream and write to upper tiers
	var teeReader io.Reader
	if c.BufferPool != nil {
		teeReader = c.BufferPool.TeeReader(stream, multiWriter)
	} else {
		// Fallback to standard io.TeeReader if buffer pool is not available
		teeReader = io.TeeReader(stream, multiWriter)
	}

	level.Info(c.Logger).Log("msg", "promotion setup completed, streaming to client and upper tiers", "key", key, "source_tier", tierIndex, "target_tiers", successfulWriters)

	// Return a multiCloser that will close all writers and the original stream
	return newMultiCloserWithLogging(teeReader, closers, c.Logger, key, "promotion"), nil
}

// multiCloser combines multiple io.Closer instances into one.
type multiCloser struct {
	reader  io.Reader
	closers []io.Closer
	logger  log.Logger
	key     string
	context string
}

func newMultiCloser(r io.Reader, closers []io.Closer) io.ReadCloser {
	return &multiCloser{reader: r, closers: closers}
}

func newMultiCloserWithLogging(r io.Reader, closers []io.Closer, logger log.Logger, key, context string) io.ReadCloser {
	return &multiCloser{reader: r, closers: closers, logger: logger, key: key, context: context}
}

func (mc *multiCloser) Read(p []byte) (n int, err error) { return mc.reader.Read(p) }

func (mc *multiCloser) Close() error {
	var firstErr error
	var successCount int

	for i, c := range mc.closers {
		if err := c.Close(); err != nil {
			if mc.logger != nil {
				level.Error(mc.logger).Log("msg", "failed to close resource", "key", mc.key, "context", mc.context, "closer_index", i, "err", err)
			}
			if firstErr == nil {
				firstErr = err
			}
		} else {
			successCount++
		}
	}

	if mc.logger != nil && mc.context != "" {
		if firstErr == nil {
			level.Debug(mc.logger).Log("msg", "all resources closed successfully", "key", mc.key, "context", mc.context, "total_closers", len(mc.closers))
		} else {
			level.Warn(mc.logger).Log("msg", "some resources failed to close", "key", mc.key, "context", mc.context, "successful_closers", successCount, "total_closers", len(mc.closers), "first_error", firstErr)
		}
	}

	return firstErr
}

// cancelWriteCloser cancels the context when the WriteCloser is closed.
type cancelWriteCloser struct {
	io.WriteCloser
	cancel context.CancelFunc
}

func newCancelWriteCloser(wc io.WriteCloser, cancel context.CancelFunc) io.WriteCloser {
	return &cancelWriteCloser{WriteCloser: wc, cancel: cancel}
}
func (cwc *cancelWriteCloser) Close() error {
	defer cwc.cancel()
	return cwc.WriteCloser.Close()
}

// closerWithCallback wraps an io.ReadCloser and executes a callback function upon Close.
type closerWithCallback struct {
	io.ReadCloser
	callback func()
}

// newCloserWithCallback creates a new ReadCloser that executes a callback function
// after the underlying ReadCloser is closed.
func newCloserWithCallback(rc io.ReadCloser, cb func()) io.ReadCloser {
	return &closerWithCallback{
		ReadCloser: rc,
		callback:   cb,
	}
}

// Close closes the underlying ReadCloser and then executes the callback function.
// It returns the error from the underlying ReadCloser's Close method.
func (c *closerWithCallback) Close() error {
	// Use defer to ensure the callback is executed even if the underlying Close fails.
	defer c.callback()
	return c.ReadCloser.Close()
}
