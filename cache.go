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
	HotStore         Store
	ColdStore        Store // Optional
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

// Get retrieves data based on the requested caching strategy.
// It first checks the hot cache, then the cold cache, and finally fetches from the origin.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	// 1. Check Hot Cache
	hotStream, hotMeta, err := c.getStreamFromStore(ctx, c.HotStore, key)
	if err == nil {
		return c.handleHotHit(ctx, key, fetcher, hotStream, hotMeta)
	}
	if !errors.Is(err, ErrNotFound) {
		level.Error(c.Logger).Log("msg", "hot store get failed", "key", key, "err", err)
	}

	// 2. Check Cold Cache
	coldStream, coldMeta, err := c.getStreamFromStore(ctx, c.ColdStore, key)
	if err == nil {
		return c.handleColdHit(ctx, key, coldStream, coldMeta)
	}
	if !errors.Is(err, ErrNotFound) {
		level.Error(c.Logger).Log("msg", "cold store get failed", "key", key, "err", err)
	}

	// 3. Fetch from Origin
	return c.handleMiss(ctx, key, fetcher)
}

// Set returns a WriteCloser to directly write data to the cache.
// The data is written to the hot store.
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	if c.HotStore == nil {
		return nil, &ConfigError{"hotStore is not configured"}
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)

	if metadata == nil {
		metadata = &Metadata{}
	}
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToStore(ctx, c.HotStore, key, metadata)
	if err != nil {
		cancel()
		return nil, err
	}
	return newCancelWriteCloser(wc, cancel), nil
}

// Delete sequentially deletes an object from all cache tiers to prevent deadlocks.
// It attempts to delete from the hot store first, then the cold store.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	if c.isClosed.Load() {
		return ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	var firstErr error

	// 1. Always delete from Hot Store first.
	if c.HotStore != nil {
		if err := c.deleteFromStore(ctx, c.HotStore, key); err != nil && !errors.Is(err, ErrNotFound) {
			level.Error(c.Logger).Log("msg", "failed to delete from hot store", "key", key, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// 2. Then delete from Cold Store.
	if err := c.deleteFromStore(ctx, c.ColdStore, key); err != nil && !errors.Is(err, ErrNotFound) {
		level.Error(c.Logger).Log("msg", "failed to delete from cold store", "key", key, "err", err)
		if firstErr == nil {
			firstErr = err
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
		if meta, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil && meta != nil {
			oldMetadata = meta
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

		writer, err := c.setStreamToStore(jobCtx, c.HotStore, key, result.Metadata)
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

// handleHotHit processes the logic when an object is found in the hot cache.
func (c *DaramjweeCache) handleHotHit(ctx context.Context, key string, fetcher Fetcher, hotStream io.ReadCloser, meta *Metadata) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "hot cache hit", "key", key)

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

	hotStreamCloser := newCloserWithCallback(hotStream, func() {})

	if isStale {
		level.Debug(c.Logger).Log("msg", "hot cache is stale, scheduling refresh", "key", key)
		hotStreamCloser = newCloserWithCallback(hotStream, func() { c.ScheduleRefresh(context.Background(), key, fetcher) })
	}

	if meta.IsNegative {
		hotStreamCloser.Close()
		return nil, ErrNotFound
	}

	return hotStreamCloser, nil
}

// handleColdHit processes the logic when an object is found in the cold cache.
func (c *DaramjweeCache) handleColdHit(ctx context.Context, key string, coldStream io.ReadCloser, coldMeta *Metadata) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "cold cache hit, promoting to hot", "key", key)

	// Create a copy of the metadata to promote to the hot cache.
	// This prevents data races if multiple goroutines handle Cold Hit concurrently
	// by not modifying the original coldMeta object directly.
	metaToPromote := &Metadata{}
	if coldMeta != nil {
		// Copy values from existing metadata.
		*metaToPromote = *coldMeta
	}
	// Only update the CachedAt field of the copy to the current time.
	metaToPromote.CachedAt = time.Now()

	// Pass the modified copy to the promotion logic.
	return c.promoteAndTeeStream(ctx, key, metaToPromote, coldStream)
}

// handleMiss processes the logic when an object is not found in either hot or cold cache.
func (c *DaramjweeCache) handleMiss(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "full cache miss, fetching from origin", "key", key)

	var oldMetadata *Metadata
	if meta, err := c.statFromStore(ctx, c.HotStore, key); err == nil {
		oldMetadata = meta
	}

	result, err := fetcher.Fetch(ctx, oldMetadata)
	if err != nil {
		if errors.Is(err, ErrCacheableNotFound) {
			return c.handleNegativeCache(ctx, key)
		}
		if errors.Is(err, ErrNotModified) {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from hot cache again", "key", key)
			stream, meta, err := c.getStreamFromStore(ctx, c.HotStore, key)
			if err != nil {
				level.Warn(c.Logger).Log("msg", "failed to refetch from hot cache after 304", "key", key, "err", err)
				return nil, ErrNotFound
			}
			if meta.IsNegative {
				stream.Close()
				return nil, ErrNotFound
			}
			return stream, nil
		}
		return nil, err
	}

	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata.CachedAt = time.Now()

	hotTeeStream, err := c.cacheAndTeeStream(ctx, key, result)
	if err != nil {
		return result.Body, nil
	}

	c.scheduleSetToStore(context.Background(), c.ColdStore, key)
	return hotTeeStream, nil
}

// handleNegativeCache processes the logic for storing a negative cache entry.
func (c *DaramjweeCache) handleNegativeCache(ctx context.Context, key string) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "caching as negative entry", "key", key, "NegativeFreshFor", c.NegativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	writer, err := c.setStreamToStore(ctx, c.HotStore, key, meta)
	if err != nil {
		level.Warn(c.Logger).Log("msg", "failed to get writer for negative cache entry", "key", key, "err", err)
	} else {
		if closeErr := writer.Close(); closeErr != nil {
			level.Warn(c.Logger).Log("msg", "failed to close writer for negative cache entry", "key", key, "err", closeErr)
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

// scheduleSetToStore schedules an asynchronous copy of the hot cache content to the cold cache.
func (c *DaramjweeCache) scheduleSetToStore(ctx context.Context, destStore Store, key string) {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule set", "key", key)
		return
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background set", "key", key, "dest", "cold")

		srcStream, meta, err := c.getStreamFromStore(jobCtx, c.HotStore, key)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get stream from hot store for background set", "key", key, "err", err)
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
			level.Info(c.Logger).Log("msg", "background set successful", "key", key, "dest", "cold")
		}
	}

	c.Worker.Submit(job)
}

// promoteAndTeeStream promotes a cold stream to hot while simultaneously returning it to the user.
func (c *DaramjweeCache) promoteAndTeeStream(ctx context.Context, key string, metadata *Metadata, coldStream io.ReadCloser) (io.ReadCloser, error) {
	hotWriter, err := c.setStreamToStore(ctx, c.HotStore, key, metadata)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get hot store writer for promotion", "key", key, "err", err)
		return coldStream, nil
	}

	var teeReader io.Reader
	if c.BufferPool != nil {
		teeReader = c.BufferPool.TeeReader(coldStream, hotWriter)
	} else {
		// Fallback to standard io.TeeReader if buffer pool is not available
		teeReader = io.TeeReader(coldStream, hotWriter)
	}
	return newMultiCloser(teeReader, coldStream, hotWriter), nil
}

// cacheAndTeeStream caches the origin stream in the hot cache while simultaneously returning it to the user.
func (c *DaramjweeCache) cacheAndTeeStream(ctx context.Context, key string, result *FetchResult) (io.ReadCloser, error) {
	if c.HotStore != nil && result.Metadata != nil {
		cacheWriter, err := c.setStreamToStore(ctx, c.HotStore, key, result.Metadata)
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
		return newMultiCloser(teeReader, result.Body, cacheWriter), nil
	}
	return result.Body, nil
}

// multiCloser combines multiple io.Closer instances into one.
type multiCloser struct {
	reader  io.Reader
	closers []io.Closer
}

func newMultiCloser(r io.Reader, closers ...io.Closer) io.ReadCloser {
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
