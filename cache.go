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
	Tiers                []Store
	Logger               log.Logger
	Worker               *worker.Manager
	DefaultTimeout       time.Duration
	ShutdownTimeout      time.Duration
	TierPositiveFreshFor time.Duration
	TierNegativeFreshFor time.Duration
	loggingDisabled      bool
	isClosed             atomic.Bool
}

var _ Cache = (*DaramjweeCache)(nil)

// Get retrieves data based on the requested caching strategy.
// It checks ordered tiers from top to bottom and finally fetches from the origin.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	if fetcher == nil {
		return nil, ErrNilFetcher
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)

	for i, tier := range c.Tiers {
		tierStream, tierMeta, err := c.getStreamFromStore(ctx, tier, key)
		if err == nil {
			if i == 0 {
				stream, streamErr := c.handleTopTierHit(ctx, key, fetcher, tierStream, tierMeta, cancel)
				if streamErr != nil {
					cancel()
					return nil, streamErr
				}
				return stream, nil
			}
			stream, streamErr := c.handleLowerTierHit(ctx, key, i, fetcher, tierStream, tierMeta, cancel)
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
			c.errorLog("msg", "tier get failed", "key", key, "tier_index", i, "err", err)
		}
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
// The data is written to tier 0.
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	target := c.topWriteStore()
	if !hasRealStore(target) {
		return nil, &ConfigError{"no writable tier is configured"}
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)

	if metadata == nil {
		metadata = &Metadata{}
	}
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToStore(ctx, target, key, metadata)
	if err != nil {
		cancel()
		return nil, err
	}
	return newCancelWriteSink(wc, cancel), nil
}

// Delete sequentially deletes an object from all tiers to prevent deadlocks.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	if c.isClosed.Load() {
		return ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	var firstErr error

	for i, tier := range c.Tiers {
		if err := c.deleteFromStore(ctx, tier, key); err != nil && !errors.Is(err, ErrNotFound) {
			c.errorLog("msg", "failed to delete from tier", "key", key, "tier_index", i, "err", err)
			if firstErr == nil {
				firstErr = err
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
		if meta, err := c.statFromStore(jobCtx, c.topWriteStore(), key); err == nil && meta != nil {
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

		target := c.topWriteStore()
		writer, err := c.setStreamToStore(jobCtx, target, key, result.Metadata)
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
			c.schedulePersistFromTop(jobCtx, key, c.persistDestinationsAfterTop()...)
		}
	}

	if !c.Worker.Submit(job) {
		return ErrBackgroundJobRejected
	}
	return nil
}

func (c *DaramjweeCache) isCachedStale(oldMeta *Metadata, positive, negative time.Duration) bool {
	if oldMeta == nil {
		return true
	}
	if oldMeta.CachedAt.IsZero() {
		return false
	}

	freshnessLifetime := positive
	if oldMeta.IsNegative {
		freshnessLifetime = negative
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

// handleTopTierHit processes the logic when an object is found in tier 0.
func (c *DaramjweeCache) handleTopTierHit(_ context.Context, key string, fetcher Fetcher, stream io.ReadCloser, meta *Metadata, cancel context.CancelFunc) (io.ReadCloser, error) {
	c.debugLog("msg", "top tier hit", "key", key)

	isStale := c.isCachedStale(meta, c.TierPositiveFreshFor, c.TierNegativeFreshFor)

	callback := func() {
		cancel()
	}
	if isStale {
		c.debugLog("msg", "top tier is stale, scheduling refresh", "key", key)
		callback = c.refreshOnCloseCallback(key, fetcher, cancel)
	}
	streamCloser := newSafeCloser(stream, callback)

	if meta.IsNegative {
		streamCloser.Close()
		return nil, ErrNotFound
	}

	return streamCloser, nil
}

// handleLowerTierHit processes the logic when an object is found in a lower tier.
func (c *DaramjweeCache) handleLowerTierHit(ctx context.Context, key string, tierIndex int, fetcher Fetcher, src io.ReadCloser, meta *Metadata, cancel context.CancelFunc) (io.ReadCloser, error) {
	c.debugLog("msg", "lower tier hit, promoting to top tier", "key", key, "tier_index", tierIndex)

	metaToPromote := &Metadata{}
	if meta != nil {
		*metaToPromote = *meta
	}

	if c.isCachedStale(meta, c.TierPositiveFreshFor, c.TierNegativeFreshFor) {
		c.debugLog("msg", "lower tier is stale, serving stale and scheduling refresh", "key", key, "tier_index", tierIndex)
		streamCloser := newSafeCloser(src, c.refreshOnCloseCallback(key, fetcher, cancel))
		if meta.IsNegative {
			streamCloser.Close()
			return nil, ErrNotFound
		}
		return streamCloser, nil
	}

	if meta.IsNegative {
		writer, err := c.setStreamToStore(ctx, c.Tiers[0], key, metaToPromote)
		if err != nil {
			c.warnLog("msg", "failed to acquire top-tier sink for negative promotion", "key", key, "err", err)
			_ = src.Close()
			cancel()
			return nil, ErrNotFound
		}

		_ = src.Close()
		closeErr := writer.Close()
		if closeErr == nil {
			if destinations := c.regularFanoutDestinations(tierIndex); len(destinations) > 0 {
				c.schedulePersistFromTop(context.Background(), key, destinations...)
			}
		}
		cancel()
		if closeErr != nil {
			c.warnLog("msg", "failed to publish negative entry to top tier", "key", key, "err", closeErr)
		}
		return nil, ErrNotFound
	}

	writer, err := c.setStreamToStore(ctx, c.Tiers[0], key, metaToPromote)
	if err != nil {
		c.warnLog("msg", "failed to acquire top-tier sink for promotion", "key", key, "err", err)
		return newCancelOnCloseReadCloser(src, cancel), nil
	}

	var onPublish func()
	destinations := c.regularFanoutDestinations(tierIndex)
	if len(destinations) > 0 {
		onPublish = func() {
			c.schedulePersistFromTop(context.Background(), key, destinations...)
		}
	}
	return streamThrough(src, writer, cancel, onPublish), nil
}

func (c *DaramjweeCache) refreshOnCloseCallback(key string, fetcher Fetcher, cancel context.CancelFunc) func() {
	return func() {
		defer cancel()
		if err := c.ScheduleRefresh(context.Background(), key, fetcher); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "err", err)
		}
	}
}

// handleMiss processes the logic when an object is not found in any tier.
func (c *DaramjweeCache) handleMiss(ctx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc) (io.ReadCloser, error) {
	c.debugLog("msg", "full cache miss, fetching from origin", "key", key)

	var oldMetadata *Metadata
	if meta, err := c.statFromStore(ctx, c.topWriteStore(), key); err == nil {
		oldMetadata = meta
	}

	result, err := fetcher.Fetch(ctx, oldMetadata)
	if err != nil {
		if errors.Is(err, ErrCacheableNotFound) {
			return c.handleNegativeCache(ctx, key)
		}
		if errors.Is(err, ErrNotModified) {
			c.debugLog("msg", "object not modified, serving from hot cache again", "key", key)
			stream, meta, err := c.getStreamFromStore(ctx, c.topWriteStore(), key)
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
			return newCancelOnCloseReadCloser(stream, cancel), nil
		}
		return nil, err
	}

	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata.CachedAt = time.Now()

	target := c.topWriteStore()
	writer, err := c.setStreamToStore(ctx, target, key, result.Metadata)
	if err != nil {
		c.warnLog("msg", "failed to acquire top sink on miss", "key", key, "err", err)
		return newCancelOnCloseReadCloser(result.Body, cancel), nil
	}

	return streamThrough(result.Body, writer, cancel, func() {
		c.schedulePersistFromTop(context.Background(), key, c.persistDestinationsAfterTop()...)
	}), nil
}

// handleNegativeCache processes the logic for storing a negative cache entry.
func (c *DaramjweeCache) handleNegativeCache(ctx context.Context, key string) (io.ReadCloser, error) {
	c.debugLog("msg", "caching as negative entry", "key", key, "negative_fresh_for", c.TierNegativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	writer, err := c.setStreamToStore(ctx, c.topWriteStore(), key, meta)
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

func hasRealStore(store Store) bool {
	if store == nil {
		return false
	}
	_, isNullStore := store.(*nullStore)
	return !isNullStore
}

func (c *DaramjweeCache) topWriteStore() Store {
	if len(c.Tiers) == 0 {
		return nil
	}
	return c.Tiers[0]
}

func (c *DaramjweeCache) persistDestinationsAfterTop() []Store {
	if len(c.Tiers) <= 1 {
		return nil
	}

	dests := make([]Store, 0, len(c.Tiers)-1)
	for _, tier := range c.Tiers[1:] {
		if hasRealStore(tier) {
			dests = append(dests, tier)
		}
	}
	return dests
}

func (c *DaramjweeCache) regularFanoutDestinations(sourceIndex int) []Store {
	if sourceIndex <= 1 {
		return nil
	}

	dests := make([]Store, 0, sourceIndex-1)
	for _, tier := range c.Tiers[1:sourceIndex] {
		if hasRealStore(tier) {
			dests = append(dests, tier)
		}
	}
	return dests
}

func (c *DaramjweeCache) schedulePersistFromTop(_ context.Context, key string, destStores ...Store) {
	srcStore := c.topWriteStore()
	if !hasRealStore(srcStore) || len(destStores) == 0 {
		return
	}
	if c.Worker == nil {
		c.warnLog("msg", "worker is not configured, cannot schedule persistence", "key", key)
		return
	}

	for idx, destStore := range destStores {
		if !hasRealStore(destStore) || destStore == srcStore {
			continue
		}

		destIndex := idx
		dest := destStore
		job := func(jobCtx context.Context) {
			c.infoLog("msg", "starting background set", "key", key, "dest_index", destIndex)

			srcStream, meta, err := c.getStreamFromStore(jobCtx, srcStore, key)
			if err != nil {
				c.errorLog("msg", "failed to get stream from top store for background set", "key", key, "err", err)
				return
			}
			defer srcStream.Close()

			destWriter, err := c.setStreamToStore(jobCtx, dest, key, meta)
			if err != nil {
				c.errorLog("msg", "failed to get writer for destination store", "key", key, "err", err)
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
				c.errorLog("msg", "failed background set", "key", key, "dest_index", destIndex, "copyErr", copyErr, "closeErr", closeErr)
				return
			}
			c.infoLog("msg", "background set successful", "key", key, "dest_index", destIndex)
		}

		if !c.Worker.Submit(job) {
			c.warnLog("msg", "background set rejected", "key", key, "dest_index", destIndex)
		}
	}
}

// cancelWriteCloser cancels the context when the WriteCloser is closed.
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
