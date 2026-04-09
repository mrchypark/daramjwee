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

var errTopWriteInvalidated = errors.New("daramjwee: top-tier write invalidated")

// DaramjweeCache is a concrete implementation of the Cache interface.
type DaramjweeCache struct {
	Tiers                  []Store
	Logger                 log.Logger
	Worker                 *worker.Manager
	OpTimeout              time.Duration
	CloseTimeout           time.Duration
	WorkerTimeout          time.Duration
	PositiveFreshness      time.Duration
	NegativeFreshness      time.Duration
	TierFreshnessOverrides map[int]TierFreshnessOverride
	loggingDisabled        bool
	isClosed               atomic.Bool
	topWriteStates         sync.Map
	topWritePruneCounter   atomic.Uint64
	fanoutTopologyOnce     sync.Once
	persistAfterTop        []tierDestination
	regularFanoutBySource  [][]tierDestination
}

var _ Cache = (*DaramjweeCache)(nil)

// Get retrieves data based on the requested caching strategy.
// It checks ordered tiers from top to bottom and finally fetches from the origin.
func (c *DaramjweeCache) Get(ctx context.Context, key string, req GetRequest, fetcher Fetcher) (*GetResponse, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	if fetcher == nil {
		return nil, ErrNilFetcher
	}
	setupCtx, cancel := c.newCtxWithTimeout(ctx)

	for i, tier := range c.Tiers {
		tierStream, tierMeta, err := c.getStreamFromStore(c.getStreamContextForStore(ctx, setupCtx, tier), tier, key)
		if err == nil {
			if i == 0 {
				resp, respErr := c.handleTopTierHit(setupCtx, key, req, fetcher, tierStream, tierMeta, cancel)
				if respErr != nil {
					cancel()
					return nil, respErr
				}
				return resp, nil
			}
			resp, respErr := c.handleLowerTierHit(ctx, setupCtx, key, i, req, fetcher, tierStream, tierMeta, cancel)
			if respErr != nil {
				cancel()
				return nil, respErr
			}
			return resp, nil
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
	resp, respErr := c.handleMiss(ctx, setupCtx, key, req, fetcher, cancel)
	if respErr != nil {
		cancel()
		return nil, respErr
	}
	return resp, nil
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
	setupCtx, cancel := c.newCtxWithTimeout(ctx)

	if metadata == nil {
		metadata = &Metadata{}
	}
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToStore(c.beginSetContextForStore(ctx, setupCtx, target), target, key, metadata)
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
	c.noteTopWriteGeneration(key)
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
	return c.scheduleRefreshWithMetadata(ctx, key, fetcher, nil, nil)
}

func (c *DaramjweeCache) scheduleRefreshWithMetadata(ctx context.Context, key string, fetcher Fetcher, fallbackMetadata *Metadata, fallbackSource *tierDestination) error {
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
		c.infoLog("msg", "starting background refresh", "key", key)
		checkCtx, cancelChecks := c.newCtxWithTimeout(jobCtx)
		defer cancelChecks()

		var oldMetadata *Metadata
		hadTopEntry := false
		if meta, err := c.statFromStore(checkCtx, c.topWriteStore(), key); err == nil && meta != nil {
			oldMetadata = meta
			hadTopEntry = true
		} else if fallbackMetadata != nil {
			copied := *fallbackMetadata
			oldMetadata = &copied
		}
		startGeneration := c.currentTopWriteGeneration(key)

		result, err := fetcher.Fetch(jobCtx, oldMetadata)
		if err != nil {
			if errors.Is(err, ErrCacheableNotFound) {
				shouldPublish, checkErr := c.backgroundRefreshStillOwnsTop(checkCtx, key, hadTopEntry, oldMetadata, startGeneration)
				if checkErr != nil {
					c.warnLog("msg", "failed to validate top-tier state before negative refresh publish", "key", key, "err", checkErr)
					return
				}
				if !shouldPublish {
					c.infoLog("msg", "skipping negative refresh publish because top-tier state changed", "key", key)
					return
				}
				c.debugLog("msg", "re-caching as negative entry during background refresh", "key", key)
				c.handleNegativeCacheWithGeneration(jobCtx, jobCtx, key, nil, &startGeneration)
			} else if errors.Is(err, ErrNotModified) {
				c.debugLog("msg", "background refresh: object not modified", "key", key)
				if fallbackSource != nil {
					if promoteErr := c.promoteRefreshFallbackToTop(jobCtx, key, *fallbackSource, fallbackMetadata, startGeneration); promoteErr != nil {
						c.warnLog("msg", "failed to promote fallback entry after not-modified refresh", "key", key, "source_tier", fallbackSource.tierIndex, "err", promoteErr)
					}
				} else if refreshErr := c.refreshTopEntryCachedAt(jobCtx, key, oldMetadata, startGeneration); refreshErr != nil {
					c.warnLog("msg", "failed to refresh top-tier metadata after not-modified refresh", "key", key, "err", refreshErr)
				}
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

		shouldPublish, checkErr := c.backgroundRefreshStillOwnsTop(checkCtx, key, hadTopEntry, oldMetadata, startGeneration)
		if checkErr != nil {
			c.warnLog("msg", "failed to validate top-tier state before background refresh publish", "key", key, "err", checkErr)
			return
		}
		if !shouldPublish {
			c.infoLog("msg", "skipping background refresh publish because top-tier state changed", "key", key)
			return
		}

		target := c.topWriteStore()
		writer, err := c.setStreamToStoreWithTopGeneration(jobCtx, target, key, result.Metadata, &startGeneration)
		if err != nil {
			if errors.Is(err, errTopWriteInvalidated) {
				c.infoLog("msg", "skipping background refresh publish because top-tier generation changed", "key", key)
				return
			}
			c.errorLog("msg", "failed to get cache writer for refresh", "key", key, "err", err)
			return
		}

		_, copyErr := io.Copy(writer, result.Body)
		var closeErr error
		if copyErr != nil {
			closeErr = writer.Abort()
		} else {
			stillOwnsTop, ownErr := c.backgroundRefreshMatchesSnapshot(checkCtx, key, hadTopEntry, oldMetadata)
			if ownErr != nil {
				closeErr = errors.Join(writer.Abort(), ownErr)
			} else if !stillOwnsTop {
				closeErr = writer.Abort()
			} else {
				closeErr = writer.Close()
			}
		}

		if copyErr != nil || closeErr != nil {
			c.errorLog("msg", "failed background set", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			c.infoLog("msg", "background set successful", "key", key)
			if destinations := c.persistDestinationsAfterTop(); len(destinations) > 0 {
				c.schedulePersistFromTop(key, *result.Metadata, destinations...)
			}
		}
	}

	if !c.Worker.Submit(job) {
		return ErrBackgroundJobRejected
	}
	return nil
}

func (c *DaramjweeCache) backgroundRefreshStillOwnsTop(ctx context.Context, key string, hadTopEntry bool, oldMetadata *Metadata, expectedGeneration uint64) (bool, error) {
	matchesSnapshot, err := c.backgroundRefreshMatchesSnapshot(ctx, key, hadTopEntry, oldMetadata)
	if !matchesSnapshot || err != nil {
		return matchesSnapshot, err
	}
	return c.currentTopWriteGeneration(key) == expectedGeneration, nil
}

func (c *DaramjweeCache) backgroundRefreshMatchesSnapshot(ctx context.Context, key string, hadTopEntry bool, oldMetadata *Metadata) (bool, error) {
	target := c.topWriteStore()
	if !hasRealStore(target) {
		return false, nil
	}

	currentMeta, err := c.statFromStore(ctx, target, key)
	if errors.Is(err, ErrNotFound) {
		return !hadTopEntry, nil
	}
	if err != nil {
		return false, err
	}
	if currentMeta == nil {
		return false, ErrNilMetadata
	}
	if !hadTopEntry || oldMetadata == nil {
		return false, nil
	}

	sameTag := currentMeta.CacheTag == oldMetadata.CacheTag
	sameNegative := currentMeta.IsNegative == oldMetadata.IsNegative
	sameCachedAt := currentMeta.CachedAt.Equal(oldMetadata.CachedAt)
	if !sameTag || !sameNegative || !sameCachedAt {
		return false, nil
	}
	return true, nil
}

func (c *DaramjweeCache) isCachedStale(oldMeta *Metadata, positive, negative time.Duration) bool {
	if oldMeta == nil {
		return true
	}

	freshnessLifetime := positive
	if oldMeta.IsNegative {
		freshnessLifetime = negative
	}
	if oldMeta.CachedAt.IsZero() {
		return true
	}

	return time.Now().After(oldMeta.CachedAt.Add(freshnessLifetime))
}

func (c *DaramjweeCache) tierFreshness(index int) (time.Duration, time.Duration) {
	override, ok := c.TierFreshnessOverrides[index]
	if !ok {
		return c.PositiveFreshness, c.NegativeFreshness
	}
	return override.Positive, override.Negative
}

func (c *DaramjweeCache) isTierCachedStale(oldMeta *Metadata, index int) bool {
	positive, negative := c.tierFreshness(index)
	return c.isCachedStale(oldMeta, positive, negative)
}

// Close safely shuts down the worker.
func (c *DaramjweeCache) Close() {
	if c.isClosed.Swap(true) {
		// Already closed, do nothing (prevent duplicate calls)
		return
	}

	if c.Worker != nil {
		c.infoLog("msg", "shutting down daramjwee cache")
		if err := c.Worker.Shutdown(c.CloseTimeout); err != nil {
			c.errorLog("msg", "graceful shutdown failed", "err", err)
		} else {
			c.infoLog("msg", "daramjwee cache shutdown complete")
		}
	}
}

// handleTopTierHit processes the logic when an object is found in tier 0.
func (c *DaramjweeCache) handleTopTierHit(_ context.Context, key string, req GetRequest, fetcher Fetcher, stream io.ReadCloser, meta *Metadata, cancel context.CancelFunc) (*GetResponse, error) {
	c.debugLog("msg", "top tier hit", "key", key)

	isStale := c.isTierCachedStale(meta, 0)
	if !meta.IsNegative && ifNoneMatchMatchesCacheTag(req.IfNoneMatch, meta.CacheTag) {
		if err := stream.Close(); err != nil {
			cancel()
			return nil, err
		}
		if isStale {
			if err := c.scheduleRefreshWithMetadata(context.Background(), key, fetcher, cloneMetadata(meta), nil); err != nil {
				c.warnLog("msg", "failed to schedule stale refresh", "key", key, "err", err)
			}
		}
		cancel()
		return newGetResponse(GetStatusNotModified, nil, meta), nil
	}

	callback := func() {
		cancel()
	}
	if isStale {
		c.debugLog("msg", "top tier is stale, scheduling refresh", "key", key)
		callback = c.refreshOnCloseCallback(key, fetcher, cancel, meta)
	}
	streamCloser := newSafeCloser(stream, callback)

	if meta.IsNegative {
		streamCloser.Close()
		return newGetResponse(GetStatusNotFound, nil, meta), nil
	}

	return newGetResponse(GetStatusOK, streamCloser, meta), nil
}

// handleLowerTierHit processes the logic when an object is found in a lower tier.
func (c *DaramjweeCache) handleLowerTierHit(requestCtx, setupCtx context.Context, key string, tierIndex int, req GetRequest, fetcher Fetcher, src io.ReadCloser, meta *Metadata, cancel context.CancelFunc) (*GetResponse, error) {
	c.debugLog("msg", "lower tier hit, promoting to top tier", "key", key, "tier_index", tierIndex)

	metaToPromote := &Metadata{}
	if meta != nil {
		*metaToPromote = *meta
	}

	isStale := c.isTierCachedStale(meta, tierIndex)
	if !meta.IsNegative && ifNoneMatchMatchesCacheTag(req.IfNoneMatch, meta.CacheTag) {
		if !isStale {
			if err := c.promoteLowerTierHitToTop(requestCtx, setupCtx, key, tierIndex, src, metaToPromote); err != nil {
				c.warnLog("msg", "failed to promote conditional lower-tier hit to top tier", "key", key, "tier_index", tierIndex, "err", err)
			}
		} else if err := src.Close(); err != nil {
			cancel()
			return nil, err
		}
		if isStale {
			if err := c.scheduleRefreshWithMetadata(context.Background(), key, fetcher, cloneMetadata(meta), &tierDestination{tierIndex: tierIndex, store: c.Tiers[tierIndex]}); err != nil {
				c.warnLog("msg", "failed to schedule stale refresh", "key", key, "source_tier", tierIndex, "err", err)
			}
		}
		cancel()
		return newGetResponse(GetStatusNotModified, nil, meta), nil
	}

	if isStale {
		c.debugLog("msg", "lower tier is stale, serving stale and scheduling refresh", "key", key, "tier_index", tierIndex)
		streamCloser := newSafeCloser(src, c.lowerTierRefreshOnCloseCallback(key, fetcher, cancel, meta, tierDestination{tierIndex: tierIndex, store: c.Tiers[tierIndex]}))
		if meta.IsNegative {
			streamCloser.Close()
			return newGetResponse(GetStatusNotFound, nil, meta), nil
		}
		return newGetResponse(GetStatusOK, streamCloser, meta), nil
	}

	if meta.IsNegative {
		target := c.topWriteStore()
		writer, err := c.setStreamToStore(c.beginSetContextForStore(requestCtx, setupCtx, target), target, key, metaToPromote)
		if err != nil {
			c.warnLog("msg", "failed to acquire top-tier sink for negative promotion", "key", key, "err", err)
			_ = src.Close()
			cancel()
			return newGetResponse(GetStatusNotFound, nil, meta), nil
		}

		_ = src.Close()
		closeErr := writer.Close()
		if closeErr == nil {
			if destinations := c.regularFanoutDestinations(tierIndex); len(destinations) > 0 {
				c.schedulePersistFromTop(key, *metaToPromote, destinations...)
			}
		}
		cancel()
		if closeErr != nil {
			c.warnLog("msg", "failed to publish negative entry to top tier", "key", key, "err", closeErr)
		}
		return newGetResponse(GetStatusNotFound, nil, meta), nil
	}

	target := c.topWriteStore()
	writer, err := c.setStreamToStore(c.beginSetContextForStore(requestCtx, setupCtx, target), target, key, metaToPromote)
	if err != nil {
		c.warnLog("msg", "failed to acquire top-tier sink for promotion", "key", key, "err", err)
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(src, cancel), meta), nil
	}

	var onPublish func()
	destinations := c.regularFanoutDestinations(tierIndex)
	if len(destinations) > 0 {
		onPublish = func() {
			c.schedulePersistFromTop(key, *metaToPromote, destinations...)
		}
	}
	return newGetResponse(GetStatusOK, streamThrough(src, writer, cancel, onPublish), meta), nil
}

func (c *DaramjweeCache) promoteLowerTierHitToTop(requestCtx, setupCtx context.Context, key string, tierIndex int, src io.ReadCloser, metadata *Metadata) error {
	defer src.Close()

	target := c.topWriteStore()
	writer, err := c.setStreamToStore(c.beginSetContextForStore(requestCtx, setupCtx, target), target, key, metadata)
	if err != nil {
		return err
	}
	if _, copyErr := io.Copy(writer, src); copyErr != nil {
		abortErr := writer.Abort()
		return errors.Join(copyErr, abortErr)
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if destinations := c.regularFanoutDestinations(tierIndex); len(destinations) > 0 {
		c.schedulePersistFromTop(key, *metadata, destinations...)
	}
	return nil
}

func (c *DaramjweeCache) refreshOnCloseCallback(key string, fetcher Fetcher, cancel context.CancelFunc, oldMetadata *Metadata) func() {
	return func() {
		defer cancel()
		if err := c.scheduleRefreshWithMetadata(context.Background(), key, fetcher, cloneMetadata(oldMetadata), nil); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "err", err)
		}
	}
}

func (c *DaramjweeCache) lowerTierRefreshOnCloseCallback(key string, fetcher Fetcher, cancel context.CancelFunc, oldMetadata *Metadata, source tierDestination) func() {
	return func() {
		defer cancel()
		if err := c.scheduleRefreshWithMetadata(context.Background(), key, fetcher, cloneMetadata(oldMetadata), &source); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "source_tier", source.tierIndex, "err", err)
		}
	}
}

func (c *DaramjweeCache) promoteRefreshFallbackToTop(ctx context.Context, key string, source tierDestination, fallbackMetadata *Metadata, expectedGeneration uint64) error {
	target := c.topWriteStore()
	if !hasRealStore(target) || !hasRealStore(source.store) || sameStoreInstance(source.store, target) {
		return nil
	}
	if _, err := c.statFromStore(ctx, target, key); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	metaToPromote := &Metadata{}
	if fallbackMetadata != nil {
		*metaToPromote = *fallbackMetadata
	}
	metaToPromote.CachedAt = time.Now()

	if metaToPromote.IsNegative {
		writer, err := c.setStreamToStoreWithTopGeneration(ctx, target, key, metaToPromote, &expectedGeneration)
		if err != nil {
			if errors.Is(err, errTopWriteInvalidated) {
				return nil
			}
			return err
		}
		if !c.topEntryAbsent(ctx, key) {
			return writer.Abort()
		}
		if err := writer.Close(); err != nil {
			return err
		}
		if destinations := c.regularFanoutDestinations(source.tierIndex); len(destinations) > 0 {
			c.schedulePersistFromTop(key, *metaToPromote, destinations...)
		}
		return nil
	}

	srcStream, _, err := c.getStreamFromStore(ctx, source.store, key)
	if err != nil {
		return err
	}
	defer srcStream.Close()

	writer, err := c.setStreamToStoreWithTopGeneration(ctx, target, key, metaToPromote, &expectedGeneration)
	if err != nil {
		if errors.Is(err, errTopWriteInvalidated) {
			return nil
		}
		return err
	}

	if _, copyErr := io.Copy(writer, srcStream); copyErr != nil {
		abortErr := writer.Abort()
		return errors.Join(copyErr, abortErr)
	}
	if !c.topEntryAbsent(ctx, key) {
		return writer.Abort()
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if destinations := c.regularFanoutDestinations(source.tierIndex); len(destinations) > 0 {
		c.schedulePersistFromTop(key, *metaToPromote, destinations...)
	}
	return nil
}

func (c *DaramjweeCache) refreshTopEntryCachedAt(ctx context.Context, key string, oldMetadata *Metadata, expectedGeneration uint64) error {
	target := c.topWriteStore()
	if !hasRealStore(target) {
		return nil
	}
	srcStream, currentMeta, err := c.getStreamFromStore(ctx, target, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if currentMeta == nil {
		return ErrNilMetadata
	}
	if !c.isTierCachedStale(currentMeta, 0) {
		_ = srcStream.Close()
		return nil
	}
	if oldMetadata != nil && !metadataEqual(currentMeta, oldMetadata) {
		_ = srcStream.Close()
		return nil
	}

	metaToRefresh := *currentMeta
	metaToRefresh.CachedAt = time.Now()

	if metaToRefresh.IsNegative {
		srcCloseErr := srcStream.Close()
		if srcCloseErr != nil {
			return srcCloseErr
		}
		writer, err := c.setStreamToStoreWithTopGeneration(ctx, target, key, &metaToRefresh, &expectedGeneration)
		if err != nil {
			if errors.Is(err, errTopWriteInvalidated) {
				return nil
			}
			return err
		}
		if !c.topMetadataMatches(ctx, key, currentMeta) {
			return writer.Abort()
		}
		return writer.Close()
	}

	content, copyErr := io.ReadAll(srcStream)
	srcCloseErr := srcStream.Close()
	if copyErr != nil || srcCloseErr != nil {
		return errors.Join(copyErr, srcCloseErr)
	}

	writer, err := c.setStreamToStoreWithTopGeneration(ctx, target, key, &metaToRefresh, &expectedGeneration)
	if err != nil {
		if errors.Is(err, errTopWriteInvalidated) {
			return nil
		}
		return err
	}

	if _, writeErr := writer.Write(content); writeErr != nil {
		abortErr := writer.Abort()
		return errors.Join(writeErr, abortErr)
	}
	if !c.topMetadataMatches(ctx, key, currentMeta) {
		abortErr := writer.Abort()
		return abortErr
	}
	return writer.Close()
}

// handleMiss processes the logic when an object is not found in any tier.
func (c *DaramjweeCache) handleMiss(requestCtx, setupCtx context.Context, key string, req GetRequest, fetcher Fetcher, cancel context.CancelFunc) (*GetResponse, error) {
	c.debugLog("msg", "full cache miss, fetching from origin", "key", key)

	var oldMetadata *Metadata
	if meta, err := c.statFromStore(setupCtx, c.topWriteStore(), key); err == nil {
		oldMetadata = meta
	}

	result, err := c.fetchFromOrigin(c.fetchContextForFetcher(requestCtx, setupCtx, fetcher), fetcher, oldMetadata)
	if err != nil {
		if errors.Is(err, ErrCacheableNotFound) {
			return c.handleNegativeCache(requestCtx, setupCtx, key, cancel)
		}
		if errors.Is(err, ErrNotModified) {
			c.debugLog("msg", "object not modified, serving from hot cache again", "key", key)
			stream, meta, err := c.getStreamFromStore(c.getStreamContextForStore(requestCtx, setupCtx, c.topWriteStore()), c.topWriteStore(), key)
			if err != nil {
				if errors.Is(err, ErrNilMetadata) {
					return nil, err
				}
				c.warnLog("msg", "hot cache entry not found after 304 from origin", "key", key, "err", err)
				cancel()
				return newGetResponse(GetStatusNotFound, nil, nil), nil
			}
			if meta.IsNegative {
				stream.Close()
				cancel()
				return newGetResponse(GetStatusNotFound, nil, meta), nil
			}
			if ifNoneMatchMatchesCacheTag(req.IfNoneMatch, meta.CacheTag) {
				if err := stream.Close(); err != nil {
					cancel()
					return nil, err
				}
				cancel()
				return newGetResponse(GetStatusNotModified, nil, meta), nil
			}
			return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(stream, cancel), meta), nil
		}
		return nil, err
	}
	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata.CachedAt = time.Now()

	target := c.topWriteStore()
	writer, err := c.setStreamToStore(c.beginSetContextForStore(requestCtx, setupCtx, target), target, key, result.Metadata)
	if err != nil {
		c.warnLog("msg", "failed to acquire top sink on miss", "key", key, "err", err)
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(result.Body, cancel), result.Metadata), nil
	}

	return newGetResponse(GetStatusOK, streamThrough(result.Body, writer, cancel, func() {
		if destinations := c.persistDestinationsAfterTop(); len(destinations) > 0 {
			c.schedulePersistFromTop(key, *result.Metadata, destinations...)
		}
	}), result.Metadata), nil
}

// handleNegativeCache processes the logic for storing a negative cache entry.
func (c *DaramjweeCache) handleNegativeCache(requestCtx, setupCtx context.Context, key string, cancel context.CancelFunc) (*GetResponse, error) {
	return c.handleNegativeCacheWithGeneration(requestCtx, setupCtx, key, cancel, nil)
}

func (c *DaramjweeCache) handleNegativeCacheWithGeneration(requestCtx, setupCtx context.Context, key string, cancel context.CancelFunc, expectedGeneration *uint64) (*GetResponse, error) {
	_, negativeFreshFor := c.tierFreshness(0)
	c.debugLog("msg", "caching as negative entry", "key", key, "negative_fresh_for", negativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	writer, err := c.setStreamToStoreWithTopGeneration(c.beginSetContextForStore(requestCtx, setupCtx, c.topWriteStore()), c.topWriteStore(), key, meta, expectedGeneration)
	if err != nil {
		if !errors.Is(err, errTopWriteInvalidated) {
			c.warnLog("msg", "failed to get writer for negative cache entry", "key", key, "err", err)
		}
	} else {
		if closeErr := writer.Close(); closeErr != nil {
			if !errors.Is(closeErr, errTopWriteInvalidated) {
				c.warnLog("msg", "failed to close writer for negative cache entry", "key", key, "err", closeErr)
			}
		}
	}
	if cancel != nil {
		cancel()
	}
	return newGetResponse(GetStatusNotFound, nil, meta), nil
}

// newCtxWithTimeout applies the operation timeout to the context if no deadline is set.
func (c *DaramjweeCache) newCtxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.OpTimeout)
}

func usesContextAfterGetStream(store Store) bool {
	sensitive, ok := store.(GetStreamUsesContext)
	return ok && sensitive.GetStreamUsesContext()
}

func usesContextAfterBeginSet(store Store) bool {
	sensitive, ok := store.(BeginSetUsesContext)
	return ok && sensitive.BeginSetUsesContext()
}

func usesContextAfterFetch(fetcher Fetcher) bool {
	sensitive, ok := fetcher.(FetchUsesContext)
	return ok && sensitive.FetchUsesContext()
}

func (c *DaramjweeCache) getStreamContextForStore(requestCtx, setupCtx context.Context, store Store) context.Context {
	if usesContextAfterGetStream(store) {
		return requestCtx
	}
	return setupCtx
}

func (c *DaramjweeCache) beginSetContextForStore(requestCtx, setupCtx context.Context, store Store) context.Context {
	if usesContextAfterBeginSet(store) {
		return requestCtx
	}
	return setupCtx
}

func (c *DaramjweeCache) fetchContextForFetcher(requestCtx, setupCtx context.Context, fetcher Fetcher) context.Context {
	if usesContextAfterFetch(fetcher) {
		return requestCtx
	}
	return setupCtx
}

func cloneMetadata(meta *Metadata) *Metadata {
	if meta == nil {
		return nil
	}
	cloned := *meta
	return &cloned
}

func metadataEqual(a, b *Metadata) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.CacheTag == b.CacheTag && a.IsNegative == b.IsNegative && a.CachedAt.Equal(b.CachedAt)
}

func newGetResponse(status GetStatus, body io.ReadCloser, meta *Metadata) *GetResponse {
	resp := &GetResponse{
		Status: status,
		Body:   body,
	}
	if meta != nil {
		resp.Metadata = *meta
	}
	return resp
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
	return c.setStreamToStoreWithTopGeneration(ctx, store, key, metadata, nil)
}

func (c *DaramjweeCache) setStreamToStoreWithTopGeneration(ctx context.Context, store Store, key string, metadata *Metadata, expectedGeneration *uint64) (WriteSink, error) {
	if !sameStoreInstance(store, c.topWriteStore()) {
		return store.BeginSet(ctx, key, metadata)
	}

	state, generation, ok := c.beginTopWriteReservation(key, expectedGeneration)
	if !ok {
		return nil, errTopWriteInvalidated
	}

	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		c.cancelTopWriteReservation(state, generation)
		return nil, err
	}
	c.finishTopWriteReservation(state)
	return &topWriteSink{
		WriteSink:  sink,
		cache:      c,
		key:        key,
		store:      store,
		state:      state,
		generation: generation,
	}, nil
}

// deleteFromStore is a wrapper that calls the Store interface's Delete method.
func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	return store.Delete(ctx, key)
}

// statFromStore is a wrapper that calls the Store interface's Stat method.
func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (*Metadata, error) {
	opCtx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()
	return store.Stat(opCtx, key)
}

func (c *DaramjweeCache) topMetadataMatches(ctx context.Context, key string, expected *Metadata) bool {
	currentMeta, err := c.statFromStore(ctx, c.topWriteStore(), key)
	if err != nil {
		return false
	}
	return metadataEqual(currentMeta, expected)
}

func (c *DaramjweeCache) topEntryAbsent(ctx context.Context, key string) bool {
	_, err := c.statFromStore(ctx, c.topWriteStore(), key)
	return errors.Is(err, ErrNotFound)
}

func (c *DaramjweeCache) deleteDestinationIfMetadataMatches(ctx context.Context, store Store, key string, expected *Metadata) error {
	currentMeta, err := c.statFromStore(ctx, store, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !metadataEqual(currentMeta, expected) {
		return nil
	}
	return c.deleteFromStore(ctx, store, key)
}

type topWriteState struct {
	beginMu     sync.Mutex
	commitMu    sync.Mutex
	generation  atomic.Uint64
	lastTouched atomic.Int64
}

var testHookAfterTopWriteReservationValidation func()

func (c *DaramjweeCache) topWriteStateForKey(key string) *topWriteState {
	if state, ok := c.topWriteStates.Load(key); ok {
		typed := state.(*topWriteState)
		typed.lastTouched.Store(time.Now().UnixNano())
		return typed
	}

	state := &topWriteState{}
	state.lastTouched.Store(time.Now().UnixNano())
	actual, _ := c.topWriteStates.LoadOrStore(key, state)
	typed := actual.(*topWriteState)
	typed.lastTouched.Store(time.Now().UnixNano())
	c.maybePruneTopWriteStates(time.Now())
	return typed
}

func (c *DaramjweeCache) currentTopWriteGeneration(key string) uint64 {
	state, ok := c.topWriteStates.Load(key)
	if !ok {
		return 0
	}
	typed := state.(*topWriteState)
	typed.lastTouched.Store(time.Now().UnixNano())
	return typed.generation.Load()
}

func (c *DaramjweeCache) beginTopWriteReservation(key string, expectedGeneration *uint64) (*topWriteState, uint64, bool) {
	state := c.topWriteStateForKey(key)
	state.beginMu.Lock()
	for {
		currentGeneration := state.generation.Load()
		if expectedGeneration != nil && currentGeneration != *expectedGeneration {
			state.beginMu.Unlock()
			return nil, 0, false
		}
		if testHookAfterTopWriteReservationValidation != nil {
			testHookAfterTopWriteReservationValidation()
		}
		generation := currentGeneration + 1
		if state.generation.CompareAndSwap(currentGeneration, generation) {
			state.lastTouched.Store(time.Now().UnixNano())
			return state, generation, true
		}
	}
}

func (c *DaramjweeCache) finishTopWriteReservation(state *topWriteState) {
	state.lastTouched.Store(time.Now().UnixNano())
	state.beginMu.Unlock()
}

func (c *DaramjweeCache) cancelTopWriteReservation(state *topWriteState, generation uint64) {
	state.generation.CompareAndSwap(generation, generation-1)
	state.lastTouched.Store(time.Now().UnixNano())
	state.beginMu.Unlock()
}

func (c *DaramjweeCache) noteTopWriteGeneration(key string) {
	state := c.topWriteStateForKey(key)
	state.generation.Add(1)
	state.lastTouched.Store(time.Now().UnixNano())
}

type topWriteSink struct {
	WriteSink
	cache      *DaramjweeCache
	key        string
	store      Store
	state      *topWriteState
	generation uint64
	once       sync.Once
	err        error
}

func (s *topWriteSink) Close() error {
	s.once.Do(func() {
		s.state.commitMu.Lock()
		if s.generation != s.state.generation.Load() {
			abortErr := s.WriteSink.Abort()
			s.err = errTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			s.state.commitMu.Unlock()
			return
		}
		s.err = s.WriteSink.Close()
		if s.err == nil && s.generation != s.state.generation.Load() {
			s.err = s.cache.cleanupInvalidatedTopWrite(s.store, s.key)
		}
		s.state.lastTouched.Store(time.Now().UnixNano())
		s.state.commitMu.Unlock()
	})
	return s.err
}

func (s *topWriteSink) Abort() error {
	s.once.Do(func() {
		s.err = s.WriteSink.Abort()
		s.state.lastTouched.Store(time.Now().UnixNano())
	})
	return s.err
}

func (s *topWriteSink) Write(p []byte) (int, error) {
	return s.WriteSink.Write(p)
}

func (c *DaramjweeCache) cleanupInvalidatedTopWrite(store Store, key string) error {
	ctx, cancel := c.newCtxWithTimeout(context.Background())
	defer cancel()
	return c.deleteFromStore(ctx, store, key)
}

func (c *DaramjweeCache) topWriteStateRetention() time.Duration {
	retention := c.OpTimeout
	if c.WorkerTimeout > retention {
		retention = c.WorkerTimeout
	}
	if c.CloseTimeout > retention {
		retention = c.CloseTimeout
	}
	if retention <= 0 {
		retention = 30 * time.Second
	}
	return retention*2 + time.Second
}

func (c *DaramjweeCache) maybePruneTopWriteStates(now time.Time) {
	if c.topWritePruneCounter.Add(1)%256 != 0 {
		return
	}
	c.pruneIdleTopWriteStates(now)
}

func (c *DaramjweeCache) pruneIdleTopWriteStates(now time.Time) {
	cutoff := now.Add(-c.topWriteStateRetention()).UnixNano()
	c.topWriteStates.Range(func(key, value any) bool {
		state := value.(*topWriteState)
		if state.lastTouched.Load() > cutoff {
			return true
		}
		if !state.beginMu.TryLock() {
			return true
		}
		defer state.beginMu.Unlock()
		if !state.commitMu.TryLock() {
			return true
		}
		defer state.commitMu.Unlock()
		if state.lastTouched.Load() > cutoff {
			return true
		}
		current, ok := c.topWriteStates.Load(key)
		if ok && current == state {
			c.topWriteStates.CompareAndDelete(key, state)
		}
		return true
	})
}

func (c *DaramjweeCache) fetchFromOrigin(ctx context.Context, fetcher Fetcher, oldMetadata *Metadata) (*FetchResult, error) {
	return fetcher.Fetch(ctx, oldMetadata)
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

type tierDestination struct {
	tierIndex int
	store     Store
}

func (c *DaramjweeCache) persistDestinationsAfterTop() []tierDestination {
	c.initFanoutTopology()
	return c.persistAfterTop
}

func (c *DaramjweeCache) regularFanoutDestinations(sourceIndex int) []tierDestination {
	c.initFanoutTopology()
	if sourceIndex <= 1 || sourceIndex >= len(c.regularFanoutBySource) {
		return nil
	}
	return c.regularFanoutBySource[sourceIndex]
}

func (c *DaramjweeCache) initFanoutTopology() {
	c.fanoutTopologyOnce.Do(func() {
		if len(c.Tiers) <= 1 {
			return
		}

		persist := make([]tierDestination, 0, len(c.Tiers)-1)
		regularBySource := make([][]tierDestination, len(c.Tiers))
		for sourceIndex := 1; sourceIndex < len(c.Tiers); sourceIndex++ {
			tier := c.Tiers[sourceIndex]
			if hasRealStore(tier) {
				dest := tierDestination{tierIndex: sourceIndex, store: tier}
				persist = append(persist, dest)
			}

			if sourceIndex <= 1 {
				continue
			}

			dests := make([]tierDestination, 0, sourceIndex-1)
			for idx, lowerTier := range c.Tiers[1:sourceIndex] {
				if hasRealStore(lowerTier) {
					dests = append(dests, tierDestination{tierIndex: idx + 1, store: lowerTier})
				}
			}
			regularBySource[sourceIndex] = dests
		}
		c.persistAfterTop = persist
		c.regularFanoutBySource = regularBySource
	})
}

func (c *DaramjweeCache) schedulePersistFromTop(key string, expectedTopMetadata Metadata, destinations ...tierDestination) {
	srcStore := c.topWriteStore()
	if !hasRealStore(srcStore) || len(destinations) == 0 {
		return
	}
	if c.Worker == nil {
		c.warnLog("msg", "worker is not configured, cannot schedule persistence", "key", key)
		return
	}

	for _, destination := range destinations {
		destStore := destination.store
		if !hasRealStore(destStore) || sameStoreInstance(destStore, srcStore) {
			continue
		}

		destTierIndex := destination.tierIndex
		dest := destStore
		job := func(jobCtx context.Context) {
			checkCtx, cancelChecks := c.newCtxWithTimeout(jobCtx)
			defer cancelChecks()

			if !c.topMetadataMatches(checkCtx, key, &expectedTopMetadata) {
				c.infoLog("msg", "skipping background set because top-tier state changed", "key", key, "dest_tier", destTierIndex)
				return
			}

			c.infoLog("msg", "starting background set", "key", key, "dest_tier", destTierIndex)

			srcStream, meta, err := c.getStreamFromStore(jobCtx, srcStore, key)
			if err != nil {
				c.errorLog("msg", "failed to get stream from top store for background set", "key", key, "err", err)
				return
			}
			defer srcStream.Close()
			if !metadataEqual(meta, &expectedTopMetadata) {
				c.infoLog("msg", "skipping background set because top-tier stream metadata changed", "key", key, "dest_tier", destTierIndex)
				return
			}

			destWriter, err := c.setStreamToStore(jobCtx, dest, key, meta)
			if err != nil {
				c.errorLog("msg", "failed to get writer for destination store", "key", key, "dest_tier", destTierIndex, "err", err)
				return
			}
			if !c.topMetadataMatches(checkCtx, key, &expectedTopMetadata) {
				_ = destWriter.Abort()
				c.infoLog("msg", "aborted background set because top-tier state changed", "key", key, "dest_tier", destTierIndex)
				return
			}

			_, copyErr := io.Copy(destWriter, srcStream)
			var closeErr error
			if copyErr != nil {
				closeErr = destWriter.Abort()
			} else if !c.topMetadataMatches(checkCtx, key, &expectedTopMetadata) {
				closeErr = destWriter.Abort()
				c.infoLog("msg", "aborted background set before publish because top-tier state changed", "key", key, "dest_tier", destTierIndex)
			} else {
				closeErr = destWriter.Close()
			}

			if copyErr != nil || closeErr != nil {
				c.errorLog("msg", "failed background set", "key", key, "dest_tier", destTierIndex, "copyErr", copyErr, "closeErr", closeErr)
				return
			}
			if !c.topMetadataMatches(checkCtx, key, &expectedTopMetadata) {
				if cleanupErr := c.deleteDestinationIfMetadataMatches(checkCtx, dest, key, &expectedTopMetadata); cleanupErr != nil {
					c.warnLog("msg", "failed to clean up stale background set after top-tier change", "key", key, "dest_tier", destTierIndex, "err", cleanupErr)
				} else {
					c.infoLog("msg", "cleaned up stale background set after top-tier change", "key", key, "dest_tier", destTierIndex)
				}
				return
			}
			c.infoLog("msg", "background set successful", "key", key, "dest_tier", destTierIndex)
		}

		if !c.Worker.Submit(job) {
			c.warnLog("msg", "background set rejected", "key", key, "dest_tier", destTierIndex)
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
