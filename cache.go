// Package daramjwee contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
		"github.com/mrchypark/daramjwee/internal/worker"
)

var ErrCacheClosed = errors.New("daramjwee: cache is closed")
var ErrNilMetadata = errors.New("daramjwee: nil metadata encountered")
var ErrBackgroundJobRejected = errors.New("daramjwee: background job rejected")

// DaramjweeCache is a concrete implementation of the Cache interface.
type DaramjweeCache struct {
	HotStore                  Store
	ColdStore                 Store // Optional
	Logger                    log.Logger
	Worker                    *worker.Manager
	DefaultTimeout            time.Duration
	ShutdownTimeout           time.Duration
	PositiveFreshFor          time.Duration
	NegativeFreshFor          time.Duration
	ColdStorePositiveFreshFor time.Duration
	ColdStoreNegativeFreshFor time.Duration
	loggingDisabled           bool
	isClosed                  atomic.Bool
}

var _ Cache = (*DaramjweeCache)(nil)

// Get retrieves data based on the requested caching strategy.
// It first checks the hot cache, then the cold cache, and finally fetches from the origin.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	if fetcher == nil {
		return nil, ErrNilFetcher
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)

	// 1. Check Hot Cache
	hotStream, hotMeta, err := c.getStreamFromStore(ctx, c.HotStore, key)
	if err == nil {
		stream, streamErr := c.handleHotHit(ctx, key, fetcher, hotStream, hotMeta, cancel)
		if streamErr != nil {
			cancel()
			return nil, streamErr
		}
		return stream, nil
	}
	if errors.Is(err, ErrNilMetadata) {
		cancel()
		return nil, err
	}
	if !errors.Is(err, ErrNotFound) {
		c.errorLog("msg", "hot store get failed", "key", key, "err", err)
	}

	// 2. Check Cold Cache
	coldStream, coldMeta, err := c.getStreamFromStore(ctx, c.ColdStore, key)
	if err == nil {
		stream, streamErr := c.handleColdHit(ctx, key, coldStream, coldMeta, cancel)
		if streamErr != nil {
			cancel()
			return nil, streamErr
		}
		return stream, nil
	}
	if errors.Is(err, ErrNilMetadata) {
		cancel()
		return nil, err
	}
	if !errors.Is(err, ErrNotFound) {
		c.errorLog("msg", "cold store get failed", "key", key, "err", err)
	}

	// 3. Fetch from Origin
	stream, streamErr := c.handleMiss(ctx, key, fetcher, cancel)
	if streamErr != nil {
		cancel()
		return nil, streamErr
	}
	return stream, nil
}

// Set returns a WriteCloser to directly write data to the cache.
// The data is written to the hot store.
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
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
	return newCancelWriteSink(wc, cancel), nil
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
				c.errorLog("msg", "failed to delete from hot store", "key", key, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// 2. Then delete from Cold Store.
	if err := c.deleteFromStore(ctx, c.ColdStore, key); err != nil && !errors.Is(err, ErrNotFound) {
			c.errorLog("msg", "failed to delete from cold store", "key", key, "err", err)
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
	if fetcher == nil {
		return ErrNilFetcher
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if c.Worker == nil {
		return errors.New("worker is not configured, cannot schedule refresh")
	}

	job := func(jobCtx context.Context) {
		jobCtx, cancel := mergeContexts(jobCtx, ctx)
		defer cancel()

		c.infoLog("msg", "starting background refresh", "key", key)

		var oldMetadata *Metadata
		if meta, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil && meta != nil {
			oldMetadata = meta
		}

		result, err := fetcher.Fetch(jobCtx, oldMetadata)
		if err != nil {
			if errors.Is(err, ErrCacheableNotFound) {
				c.debugLog("msg", "re-caching as negative entry during background refresh", "key", key)
				c.handleNegativeCache(jobCtx, key)
			} else if errors.Is(err, ErrNotModified) {
				c.debugLog("msg", "background refresh: object not modified", "key", key)
			} else {
				c.errorLog("msg", "background fetch failed", "key", key, "err", err)
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
			c.errorLog("msg", "failed to get cache writer for refresh", "key", key, "err", err)
			return
		}

		_, copyErr := io.Copy(writer, result.Body)
		var closeErr error
		if copyErr != nil {
			closeErr = writer.Abort()
		} else {
			closeErr = writer.Close()
		}

		if copyErr != nil || closeErr != nil {
			c.errorLog("msg", "failed background set", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			c.infoLog("msg", "background set successful", "key", key)

			if hasRealColdStore(c.ColdStore) {
				var coldMetadata *Metadata
				if meta, err := c.statFromStore(jobCtx, c.ColdStore, key); err == nil {
					coldMetadata = meta
				}
				isStale := c.isColdStoreCachedStale(coldMetadata)

				if isStale {
					// schedule background copy to cold store
					c.scheduleSetToStore(jobCtx, c.ColdStore, key)
				}
			}
		}
	}

	if !c.Worker.Submit(job) {
		return ErrBackgroundJobRejected
	}
	return nil
}

func (c *DaramjweeCache) isColdStoreCachedStale(oldMeta *Metadata) bool {
	if !hasRealColdStore(c.ColdStore) {
		return false
	}
	if oldMeta == nil {
		return true
	}

	freshnessLifetime := c.ColdStorePositiveFreshFor
	if oldMeta.IsNegative {
		freshnessLifetime = c.ColdStoreNegativeFreshFor
	}

	return time.Now().After(oldMeta.CachedAt.Add(freshnessLifetime))
}

// Close safely shuts down the worker.
func (c *DaramjweeCache) Close() {
	if c.isClosed.Swap(true) {
		// Already closed, do nothing (prevent duplicate calls)
		return
	}

	if c.Worker != nil {
		c.infoLog("msg", "shutting down daramjwee cache")
		if err := c.Worker.Shutdown(c.ShutdownTimeout); err != nil {
			c.errorLog("msg", "graceful shutdown failed", "err", err)
		} else {
			c.infoLog("msg", "daramjwee cache shutdown complete")
		}
	}
}

// handleHotHit processes the logic when an object is found in the hot cache.
func (c *DaramjweeCache) handleHotHit(_ context.Context, key string, fetcher Fetcher, hotStream io.ReadCloser, meta *Metadata, cancel context.CancelFunc) (io.ReadCloser, error) {
	c.debugLog("msg", "hot cache hit", "key", key)

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

	callback := func() {
		cancel()
	}
	if isStale {
		c.debugLog("msg", "hot cache is stale, scheduling refresh", "key", key)
		callback = func() {
			defer cancel()
			if err := c.ScheduleRefresh(context.Background(), key, fetcher); err != nil {
				c.warnLog("msg", "failed to schedule stale refresh", "key", key, "err", err)
			}
		}
	}
	hotStreamCloser := newSafeCloser(hotStream, callback)

	if meta.IsNegative {
		hotStreamCloser.Close()
		return nil, ErrNotFound
	}

	return hotStreamCloser, nil
}

// handleColdHit processes the logic when an object is found in the cold cache.
func (c *DaramjweeCache) handleColdHit(ctx context.Context, key string, coldStream io.ReadCloser, coldMeta *Metadata, cancel context.CancelFunc) (io.ReadCloser, error) {
	c.debugLog("msg", "cold cache hit, promoting to hot", "key", key)

	// Create a copy of the metadata to promote to the hot cache.
	// This prevents data races if multiple goroutines handle Cold Hit concurrently
	// by not modifying the original coldMeta object directly.
	metaToPromote := &Metadata{}
	if coldMeta != nil {
		// Copy values from existing metadata.
		*metaToPromote = *coldMeta
	}

	writer, err := c.setStreamToStore(ctx, c.HotStore, key, metaToPromote)
	if err != nil {
		c.warnLog("msg", "failed to acquire hot sink for promotion", "key", key, "err", err)
		return newCancelOnCloseReadCloser(coldStream, cancel), nil
	}
	return newStreamingReadCloser(coldStream, writer, cancel, nil), nil
}

// handleMiss processes the logic when an object is not found in either hot or cold cache.
func (c *DaramjweeCache) handleMiss(ctx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc) (io.ReadCloser, error) {
	c.debugLog("msg", "full cache miss, fetching from origin", "key", key)

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
			c.debugLog("msg", "object not modified, serving from hot cache again", "key", key)
			stream, meta, err := c.getStreamFromStore(ctx, c.HotStore, key)
			if err != nil {
				if errors.Is(err, ErrNilMetadata) {
					return nil, err
				}
				c.warnLog("msg", "failed to refetch from hot cache after 304", "key", key, "err", err)
				return nil, ErrNotFound
			}
			if meta.IsNegative {
				stream.Close()
				return nil, ErrNotFound
			}
			return newCancelReadCloser(stream, cancel), nil
		}
		return nil, err
	}

	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata.CachedAt = time.Now()

	writer, err := c.setStreamToStore(ctx, c.HotStore, key, result.Metadata)
	if err != nil {
		c.warnLog("msg", "failed to acquire hot sink on miss", "key", key, "err", err)
		return newCancelOnCloseReadCloser(result.Body, cancel), nil
	}

	return newStreamingReadCloser(result.Body, writer, cancel, func() {
		if hasRealColdStore(c.ColdStore) {
			c.scheduleSetToStore(context.Background(), c.ColdStore, key)
		}
	}), nil
}

// handleNegativeCache processes the logic for storing a negative cache entry.
func (c *DaramjweeCache) handleNegativeCache(ctx context.Context, key string) (io.ReadCloser, error) {
	c.debugLog("msg", "caching as negative entry", "key", key, "NegativeFreshFor", c.NegativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	writer, err := c.setStreamToStore(ctx, c.HotStore, key, meta)
	if err != nil {
		c.warnLog("msg", "failed to get writer for negative cache entry", "key", key, "err", err)
	} else {
		if closeErr := writer.Close(); closeErr != nil {
			c.warnLog("msg", "failed to close writer for negative cache entry", "key", key, "err", closeErr)
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
	stream, meta, err := store.GetStream(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if meta == nil {
		if stream != nil {
			_ = stream.Close()
		}
		return nil, nil, ErrNilMetadata
	}
	return stream, meta, nil
}

// setStreamToStore is a wrapper that calls the Store interface's BeginSet method.
func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, metadata *Metadata) (WriteSink, error) {
	return store.BeginSet(ctx, key, metadata)
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
func (c *DaramjweeCache) scheduleSetToStore(_ context.Context, destStore Store, key string) {
	if !hasRealColdStore(destStore) {
		return
	}
	if c.Worker == nil {
		c.warnLog("msg", "worker is not configured, cannot schedule set", "key", key)
		return
	}

	job := func(jobCtx context.Context) {
		c.infoLog("msg", "starting background set", "key", key, "dest", "cold")

		srcStream, meta, err := c.getStreamFromStore(jobCtx, c.HotStore, key)
		if err != nil {
			c.errorLog("msg", "failed to get stream from hot store for background set", "key", key, "err", err)
			return
		}
		defer srcStream.Close()

		destWriter, err := c.setStreamToStore(jobCtx, destStore, key, meta)
		if err != nil {
			c.errorLog("msg", "failed to get writer for dest store for background set", "key", key, "err", err)
			return
		}

		_, copyErr := io.Copy(destWriter, srcStream)
		var closeErr error
		if copyErr != nil {
			closeErr = destWriter.Abort()
		} else {
			closeErr = destWriter.Close()
		}

		if copyErr != nil || closeErr != nil {
			c.errorLog("msg", "failed background set", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			c.infoLog("msg", "background set successful", "key", key, "dest", "cold")
		}
	}

	if !c.Worker.Submit(job) {
		c.warnLog("msg", "background set rejected", "key", key, "dest", "cold")
	}
}

func hasRealColdStore(store Store) bool {
	if store == nil {
		return false
	}
	_, isNullStore := store.(*nullStore)
	return !isNullStore
}

// cancelWriteCloser cancels the context when the WriteCloser is closed.
type cancelReadCloser struct {
	io.ReadCloser
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

func newCancelReadCloser(rc io.ReadCloser, cancel context.CancelFunc) io.ReadCloser {
	return &cancelReadCloser{ReadCloser: rc, cancel: cancel}
}

func (crc *cancelReadCloser) Read(p []byte) (n int, err error) {
	n, err = crc.ReadCloser.Read(p)
	if err == io.EOF {
		_ = crc.Close()
	}
	return n, err
}

func (crc *cancelReadCloser) Close() error {
	crc.closeOnce.Do(func() {
		defer crc.cancel()
		crc.closeErr = crc.ReadCloser.Close()
	})
	return crc.closeErr
}

// safeCloser wraps an io.ReadCloser and executes a callback function upon Close.
// It automatically closes when EOF is reached and prevents duplicate closes using sync.Once.
type safeCloser struct {
	io.ReadCloser
	callback  func()
	closeOnce sync.Once
	closeErr  error
}

// newSafeCloser creates a new ReadCloser that executes a callback function
// after the underlying ReadCloser is closed, with automatic EOF detection and safe duplicate close handling.
func newSafeCloser(rc io.ReadCloser, cb func()) *safeCloser {
	return &safeCloser{
		ReadCloser: rc,
		callback:   cb,
	}
}

// Read reads from the underlying ReadCloser and automatically closes when EOF is reached.
func (c *safeCloser) Read(p []byte) (n int, err error) {
	n, err = c.ReadCloser.Read(p)
	if err == io.EOF {
		c.Close() // 자동으로 닫기
	}
	return n, err
}

// Close closes the underlying ReadCloser and executes the callback function.
// It uses sync.Once to ensure the close operation and callback are executed only once.
func (c *safeCloser) Close() error {
	c.closeOnce.Do(func() {
		defer c.callback()
		c.closeErr = c.ReadCloser.Close()
	})
	return c.closeErr
}

// ReadAll reads all data from the safeCloser and returns it as a byte slice.
// It leverages the safeCloser's automatic EOF handling and close callback execution.
// Unlike io.ReadAll, this method benefits from the automatic resource cleanup
// provided by safeCloser when EOF is reached.
func (c *safeCloser) ReadAll() ([]byte, error) {
	// Use a reasonable initial buffer size to minimize allocations
	buf := make([]byte, 0, 512)
	readBuf := make([]byte, 512)

	for {
		n, err := c.Read(readBuf)
		if n > 0 {
			buf = append(buf, readBuf[:n]...)
		}
		if err == io.EOF {
			// safeCloser automatically closes on EOF, so we're done
			return buf, nil
		}
		if err != nil {
			// For any other error, manually close and return the error
			c.Close()
			return buf, err
		}
	}
}

// ReadAll attempts to use safeCloser.ReadAll() if possible, otherwise falls back to io.ReadAll.
// This helper function allows seamless usage regardless of the underlying ReadCloser type.
func ReadAll(rc io.ReadCloser) ([]byte, error) {
	if sc, ok := rc.(*safeCloser); ok {
		return sc.ReadAll()
	}
	return io.ReadAll(rc)
}

func mergeContexts(primary, secondary context.Context) (context.Context, context.CancelFunc) {
	if secondary == nil || secondary.Done() == nil {
		return primary, func() {}
	}
	ctx, cancel := context.WithCancel(primary)
	stop := context.AfterFunc(secondary, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}
