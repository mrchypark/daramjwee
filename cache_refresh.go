package daramjwee

import (
	"context"
	"errors"
	"io"
	"time"
)

// ScheduleRefresh submits a background cache refresh job to the worker.
func (c *DaramjweeCache) ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error {
	return c.scheduleRefreshWithMetadata(ctx, key, fetcher, nil, nil, nil)
}

func (c *DaramjweeCache) scheduleRefreshWithMetadata(ctx context.Context, key string, fetcher Fetcher, fallbackMetadata *Metadata, fallbackSource *tierDestination, observedGeneration *uint64) error {
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

	if c.runtime == nil {
		return errors.New("worker is not configured, cannot schedule refresh")
	}

	expectedGeneration := c.currentTopWriteGeneration(key)
	if observedGeneration != nil {
		expectedGeneration = *observedGeneration
	}
	valueCtx := detachedValueContext(ctx)
	job := func(jobCtx context.Context) {
		refreshCtx := overlayContextValues(jobCtx, valueCtx)
		c.infoLog("msg", "starting background refresh", "key", key)

		var oldMetadata *Metadata
		if meta, err := c.statFromStore(refreshCtx, c.topWriteStore(), key); err == nil && meta != nil {
			oldMetadata = meta
		} else if fallbackMetadata != nil {
			copied := *fallbackMetadata
			oldMetadata = &copied
		}

		result, err := fetcher.Fetch(refreshCtx, oldMetadata)
		if err != nil {
			if errors.Is(err, ErrCacheableNotFound) {
				c.debugLog("msg", "re-caching as negative entry during background refresh", "key", key)
				c.handleNegativeCacheWithGeneration(refreshCtx, refreshCtx, key, nil, &expectedGeneration)
			} else if errors.Is(err, ErrNotModified) {
				c.debugLog("msg", "background refresh: object not modified", "key", key)
				if fallbackSource != nil {
					if promoteErr := c.promoteRefreshFallbackToTop(refreshCtx, key, *fallbackSource, fallbackMetadata, expectedGeneration); promoteErr != nil {
						c.warnLog("msg", "failed to promote fallback entry after not-modified refresh", "key", key, "source_tier", fallbackSource.tierIndex, "err", promoteErr)
					}
				} else if refreshErr := c.refreshTopEntryCachedAt(refreshCtx, key, oldMetadata, expectedGeneration); refreshErr != nil {
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

		writer, err := c.setStreamToTopStoreWithGeneration(refreshCtx, key, result.Metadata, &expectedGeneration)
		if err != nil {
			if errors.Is(err, ErrTopWriteInvalidated) {
				c.infoLog("msg", "skipping background refresh publish because top-tier state changed", "key", key)
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
			closeErr = writer.Close()
		}

		if copyErr != nil || closeErr != nil {
			c.errorLog("msg", "failed background set", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			c.infoLog("msg", "background set successful", "key", key)
			c.schedulePersistFromCurrentTop(refreshCtx, key, c.persistDestinationsAfterTop()...)
		}
	}

	if !c.runtime.Submit(c.cacheID, JobKindRefresh, job) {
		return ErrBackgroundJobRejected
	}
	return nil
}

func (c *DaramjweeCache) refreshOnCloseCallback(requestCtx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc, oldMetadata *Metadata, observedGeneration uint64) func() {
	return func() {
		defer cancel()
		if err := c.scheduleRefreshWithMetadata(detachedValueContext(requestCtx), key, fetcher, cloneMetadata(oldMetadata), nil, &observedGeneration); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "err", err)
		}
	}
}

func (c *DaramjweeCache) lowerTierRefreshOnCloseCallback(requestCtx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc, oldMetadata *Metadata, source tierDestination, observedGeneration uint64) func() {
	return func() {
		defer cancel()
		if err := c.scheduleRefreshWithMetadata(detachedValueContext(requestCtx), key, fetcher, cloneMetadata(oldMetadata), &source, &observedGeneration); err != nil {
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
		writer, err := c.setStreamToTopStoreWithGeneration(ctx, key, metaToPromote, &expectedGeneration)
		if err != nil {
			if errors.Is(err, ErrTopWriteInvalidated) {
				return nil
			}
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		if destinations := c.regularFanoutDestinations(source.tierIndex); len(destinations) > 0 {
			c.schedulePersistFromCurrentTop(ctx, key, destinations...)
		}
		return nil
	}

	srcStream, _, err := c.getStreamFromStore(ctx, source.store, key)
	if err != nil {
		return err
	}
	defer srcStream.Close()

	writer, err := c.setStreamToTopStoreWithGeneration(ctx, key, metaToPromote, &expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			return nil
		}
		return err
	}

	if _, copyErr := io.Copy(writer, srcStream); copyErr != nil {
		abortErr := writer.Abort()
		return errors.Join(copyErr, abortErr)
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if destinations := c.regularFanoutDestinations(source.tierIndex); len(destinations) > 0 {
		c.schedulePersistFromCurrentTop(ctx, key, destinations...)
	}
	return nil
}

func (c *DaramjweeCache) refreshTopEntryCachedAt(ctx context.Context, key string, oldMetadata *Metadata, expectedGeneration uint64) error {
	target := c.topWriteStore()
	if !hasRealStore(target) {
		return nil
	}

	currentMeta, err := c.statFromStore(ctx, target, key)
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
		return nil
	}
	if oldMetadata != nil {
		if currentMeta.CacheTag != oldMetadata.CacheTag || currentMeta.IsNegative != oldMetadata.IsNegative || !currentMeta.CachedAt.Equal(oldMetadata.CachedAt) {
			return nil
		}
	}

	metaToRefresh := *currentMeta
	metaToRefresh.CachedAt = time.Now()

	if metaToRefresh.IsNegative {
		writer, err := c.setStreamToTopStoreWithGeneration(ctx, key, &metaToRefresh, &expectedGeneration)
		if err != nil {
			if errors.Is(err, ErrTopWriteInvalidated) {
				return nil
			}
			return err
		}
		return writer.Close()
	}

	srcStream, _, err := c.getStreamFromStore(ctx, target, key)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(srcStream)
	closeErr := srcStream.Close()
	if err != nil {
		return errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return closeErr
	}

	writer, err := c.setStreamToTopStoreWithGeneration(ctx, key, &metaToRefresh, &expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			return nil
		}
		return err
	}
	if _, copyErr := writer.Write(content); copyErr != nil {
		abortErr := writer.Abort()
		return errors.Join(copyErr, abortErr)
	}
	return writer.Close()
}

func (c *DaramjweeCache) handleNegativeCacheWithGeneration(requestCtx, setupCtx context.Context, key string, cancel context.CancelFunc, expectedGeneration *uint64) (*GetResponse, error) {
	_, negativeFreshFor := c.tierFreshness(0)
	c.debugLog("msg", "caching as negative entry", "key", key, "negative_fresh_for", negativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	writer, err := c.setStreamToTopStoreWithGeneration(c.beginSetContextForStore(requestCtx, setupCtx, c.topWriteStore()), key, meta, expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			if cancel != nil {
				cancel()
			}
			return newGetResponse(GetStatusNotFound, nil, meta), nil
		}
		c.warnLog("msg", "failed to get writer for negative cache entry", "key", key, "err", err)
	} else {
		if closeErr := writer.Close(); closeErr != nil {
			c.warnLog("msg", "failed to close writer for negative cache entry", "key", key, "err", closeErr)
		}
	}
	if cancel != nil {
		cancel()
	}
	return newGetResponse(GetStatusNotFound, nil, meta), nil
}
