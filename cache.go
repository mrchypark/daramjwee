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

// DaramjweeCache is a concrete implementation of the Cache interface.
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

	// Sequential lookup through all tiers
	for i, store := range c.Stores {
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
			level.Error(c.Logger).Log("msg", "store get failed", "key", key, "tier", i, "err", tierErr)
		}
	}

	// Cache miss - fetch from origin
	return c.handleMiss(ctx, key, fetcher)
}

// Set returns a WriteCloser to directly write data to the cache.
// The data is written to the primary tier (stores[0]).
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	if len(c.Stores) == 0 {
		return nil, &ConfigError{"no stores configured"}
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)

	if metadata == nil {
		metadata = &Metadata{}
	}
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToStore(ctx, c.Stores[0], key, metadata)
	if err != nil {
		cancel()
		return nil, err
	}
	return newCancelWriteCloser(wc, cancel), nil
}

// Delete sequentially deletes an object from all cache tiers.
// It attempts to delete from all stores, continuing on failure to ensure cleanup.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	if c.isClosed.Load() {
		return ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	var firstErr error

	// Delete from all tiers, continuing on failure
	for i, store := range c.Stores {
		if err := c.deleteFromStore(ctx, store, key); err != nil && !errors.Is(err, ErrNotFound) {
			tierErr := NewTierError(i, "delete", key, err)
			level.Error(c.Logger).Log("msg", "failed to delete from store", "key", key, "tier", i, "err", tierErr)
			if firstErr == nil {
				firstErr = tierErr
			}
		}
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
		level.Info(c.Logger).Log("msg", "starting background refresh", "key", key)

		var oldMetadata *Metadata
		if len(c.Stores) > 0 {
			if meta, err := c.statFromStore(jobCtx, c.Stores[0], key); err == nil && meta != nil {
				oldMetadata = meta
			}
		}

		result, err := fetcher.Fetch(jobCtx, oldMetadata)
		if err != nil {
			if errors.Is(err, ErrCacheableNotFound) {
				level.Debug(c.Logger).Log("msg", "re-caching as negative entry during background refresh", "key", key)
				c.handleNegativeCache(jobCtx, key)
			} else if errors.Is(err, ErrNotModified) {
				level.Debug(c.Logger).Log("msg", "background refresh: object not modified", "key", key)
			} else {
				level.Error(c.Logger).Log("msg", "background fetch failed", "key", key, "err", err)
			}
			return
		}
		defer result.Body.Close()

		if result.Metadata == nil {
			result.Metadata = &Metadata{}
		}
		result.Metadata.CachedAt = time.Now()

		if len(c.Stores) == 0 {
			level.Error(c.Logger).Log("msg", "no stores configured for refresh", "key", key)
			return
		}

		writer, err := c.setStreamToStore(jobCtx, c.Stores[0], key, result.Metadata)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer for refresh", "key", key, "err", err)
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
			level.Error(c.Logger).Log("msg", "failed background set", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			level.Info(c.Logger).Log("msg", "background set successful", "key", key)
		}
	}

	c.Worker.Submit(job)
	return nil
}

// Close safely shuts down the worker.
func (c *DaramjweeCache) Close() {
	if c.isClosed.Swap(true) {
		// Already closed, do nothing (prevent duplicate calls)
		return
	}

	if c.Worker != nil {
		level.Info(c.Logger).Log("msg", "shutting down daramjwee cache")
		if err := c.Worker.Shutdown(c.ShutdownTimeout); err != nil {
			level.Error(c.Logger).Log("msg", "graceful shutdown failed", "err", err)
		} else {
			level.Info(c.Logger).Log("msg", "daramjwee cache shutdown complete")
		}
	}
}

// handlePrimaryHit processes cache hits from the primary tier (stores[0]).
// It handles staleness detection and background refresh scheduling.
func (c *DaramjweeCache) handlePrimaryHit(ctx context.Context, key string, fetcher Fetcher, stream io.ReadCloser, meta *Metadata) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "primary tier cache hit", "key", key, "tier", 0)

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
		level.Debug(c.Logger).Log("msg", "primary cache is stale, scheduling refresh", "key", key)
		streamCloser = newCloserWithCallback(stream, func() { c.ScheduleRefresh(context.Background(), key, fetcher) })
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
	level.Debug(c.Logger).Log("msg", "tier cache hit, promoting to upper tiers", "key", key, "tier", tierIndex)

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
	level.Debug(c.Logger).Log("msg", "full cache miss, fetching from origin", "key", key)

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
	level.Debug(c.Logger).Log("msg", "caching as negative entry", "key", key, "NegativeFreshFor", c.NegativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	if len(c.Stores) > 0 {
		writer, err := c.setStreamToStore(ctx, c.Stores[0], key, meta)
		if err != nil {
			level.Warn(c.Logger).Log("msg", "failed to get writer for negative cache entry", "key", key, "err", err)
		} else {
			if closeErr := writer.Close(); closeErr != nil {
				level.Warn(c.Logger).Log("msg", "failed to close writer for negative cache entry", "key", key, "err", closeErr)
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
		cacheWriter, err := c.setStreamToStore(ctx, c.Stores[0], key, result.Metadata)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer", "key", key, "err", err)
			return result.Body, err
		}
		var teeReader io.Reader
		if c.BufferPool != nil {
			teeReader = c.BufferPool.TeeReader(result.Body, cacheWriter)
		} else {
			// Fallback to standard io.TeeReader if buffer pool is not available
			teeReader = io.TeeReader(result.Body, cacheWriter)
		}
		return newMultiCloser(teeReader, []io.Closer{result.Body, cacheWriter}), nil
	}
	return result.Body, nil
}

// promoteToUpperTiers promotes data from a lower tier to all upper tiers simultaneously.
// It uses io.MultiWriter to write to multiple upper tiers concurrently while streaming to the client.
func (c *DaramjweeCache) promoteToUpperTiers(ctx context.Context, key string, tierIndex int, metadata *Metadata, stream io.ReadCloser) (io.ReadCloser, error) {
	if tierIndex <= 0 || tierIndex >= len(c.Stores) {
		level.Error(c.Logger).Log("msg", "invalid tier index for promotion", "key", key, "tier", tierIndex, "total_stores", len(c.Stores))
		return stream, nil
	}

	// Create writers for all upper tiers (stores[0] through stores[tierIndex-1])
	var writers []io.WriteCloser
	var closers []io.Closer

	// Add the original stream to closers
	closers = append(closers, stream)

	for i := 0; i < tierIndex; i++ {
		writer, err := c.setStreamToStore(ctx, c.Stores[i], key, metadata)
		if err != nil {
			tierErr := NewTierError(i, "set", key, err)
			level.Error(c.Logger).Log("msg", "failed to get writer for promotion", "key", key, "tier", i, "err", tierErr)
			// Close any writers we've already created
			for _, w := range writers {
				w.Close()
			}
			return stream, nil
		}
		writers = append(writers, writer)
		closers = append(closers, writer)
	}

	if len(writers) == 0 {
		level.Debug(c.Logger).Log("msg", "no writers created for promotion", "key", key, "tier", tierIndex)
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

	level.Debug(c.Logger).Log("msg", "promoting to upper tiers", "key", key, "source_tier", tierIndex, "target_tiers", tierIndex)

	// Return a multiCloser that will close all writers and the original stream
	return newMultiCloser(teeReader, closers), nil
}

// multiCloser combines multiple io.Closer instances into one.
type multiCloser struct {
	reader  io.Reader
	closers []io.Closer
}

func newMultiCloser(r io.Reader, closers []io.Closer) io.ReadCloser {
	return &multiCloser{reader: r, closers: closers}
}
func (mc *multiCloser) Read(p []byte) (n int, err error) { return mc.reader.Read(p) }
func (mc *multiCloser) Close() error {
	var firstErr error
	for _, c := range mc.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
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
