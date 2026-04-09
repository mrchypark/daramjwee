package daramjwee

import (
	"context"
	"errors"
	"io"
	"time"
)

// handleTopTierHit processes the logic when an object is found in tier 0.
func (c *DaramjweeCache) handleTopTierHit(requestCtx context.Context, key string, req GetRequest, fetcher Fetcher, stream io.ReadCloser, meta *Metadata, cancel context.CancelFunc, observedGeneration uint64) (*GetResponse, error) {
	c.debugLog("msg", "top tier hit", "key", key)

	isStale := c.isTierCachedStale(meta, 0)
	if c.isConditionalRequestSatisfied(req, meta) {
		return c.handleConditionalTopTierHit(requestCtx, key, fetcher, stream, meta, cancel, isStale, observedGeneration)
	}

	callback := c.topTierCloseCallback(requestCtx, key, fetcher, cancel, meta, isStale, observedGeneration)
	streamCloser := newSafeCloser(stream, callback)

	if meta.IsNegative {
		streamCloser.Close()
		return newGetResponse(GetStatusNotFound, nil, meta), nil
	}

	return newGetResponse(GetStatusOK, streamCloser, meta), nil
}

func (c *DaramjweeCache) handleConditionalTopTierHit(requestCtx context.Context, key string, fetcher Fetcher, stream io.ReadCloser, meta *Metadata, cancel context.CancelFunc, isStale bool, observedGeneration uint64) (*GetResponse, error) {
	if err := stream.Close(); err != nil {
		cancel()
		return nil, err
	}
	if isStale {
		if err := c.scheduleRefreshWithMetadata(detachedValueContext(requestCtx), key, fetcher, cloneMetadata(meta), nil, &observedGeneration); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "err", err)
		}
	}
	cancel()
	return newGetResponse(GetStatusNotModified, nil, meta), nil
}

func (c *DaramjweeCache) topTierCloseCallback(requestCtx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc, meta *Metadata, isStale bool, observedGeneration uint64) func() {
	if !isStale {
		return func() {
			cancel()
		}
	}

	c.debugLog("msg", "top tier is stale, scheduling refresh", "key", key)
	return c.refreshOnCloseCallback(requestCtx, key, fetcher, cancel, meta, observedGeneration)
}

// handleLowerTierHit processes the logic when an object is found in a lower tier.
func (c *DaramjweeCache) handleLowerTierHit(requestCtx, setupCtx context.Context, key string, tierIndex int, req GetRequest, fetcher Fetcher, src io.ReadCloser, meta *Metadata, cancel context.CancelFunc, expectedGeneration uint64) (*GetResponse, error) {
	c.debugLog("msg", "lower tier hit, promoting to top tier", "key", key, "tier_index", tierIndex)

	metaToPromote := cloneMetadata(meta)
	if metaToPromote == nil {
		metaToPromote = &Metadata{}
	}

	isStale := c.isTierCachedStale(meta, tierIndex)
	if c.isConditionalRequestSatisfied(req, meta) {
		return c.handleConditionalLowerTierHit(requestCtx, setupCtx, key, tierIndex, fetcher, src, meta, metaToPromote, cancel, isStale, expectedGeneration)
	}
	if isStale {
		return c.handleStaleLowerTierHit(requestCtx, key, tierIndex, fetcher, src, meta, cancel, expectedGeneration), nil
	}
	if meta.IsNegative {
		return c.promoteNegativeLowerTierHit(requestCtx, setupCtx, key, tierIndex, src, meta, metaToPromote, cancel, expectedGeneration), nil
	}
	return c.promotePositiveLowerTierHit(requestCtx, setupCtx, key, tierIndex, src, meta, metaToPromote, cancel, expectedGeneration), nil
}

func (c *DaramjweeCache) handleConditionalLowerTierHit(requestCtx, setupCtx context.Context, key string, tierIndex int, fetcher Fetcher, src io.ReadCloser, meta, metaToPromote *Metadata, cancel context.CancelFunc, isStale bool, expectedGeneration uint64) (*GetResponse, error) {
	if !isStale {
		if err := c.promoteLowerTierHitToTop(requestCtx, setupCtx, key, tierIndex, src, metaToPromote, expectedGeneration); err != nil {
			return c.handleConditionalLowerTierPromotionError(key, tierIndex, err, src, meta, cancel), nil
		}
	} else if err := src.Close(); err != nil {
		cancel()
		return nil, err
	}

	if isStale {
		source := tierDestination{tierIndex: tierIndex, store: c.tiers[tierIndex]}
		if err := c.scheduleRefreshWithMetadata(detachedValueContext(requestCtx), key, fetcher, cloneMetadata(meta), &source, &expectedGeneration); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "source_tier", tierIndex, "err", err)
		}
	}
	cancel()
	return newGetResponse(GetStatusNotModified, nil, meta), nil
}

func (c *DaramjweeCache) handleConditionalLowerTierPromotionError(key string, tierIndex int, err error, src io.ReadCloser, meta *Metadata, cancel context.CancelFunc) *GetResponse {
	var invalidated lowerTierPromotionInvalidatedError
	if errors.As(err, &invalidated) {
		if invalidated.preserveBody {
			return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(src, cancel), meta)
		}
		cancel()
		return newGetResponse(GetStatusNotFound, nil, nil)
	}
	if errors.Is(err, ErrTopWriteInvalidated) {
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(src, cancel), meta)
	}
	c.warnLog("msg", "failed to promote conditional lower-tier hit to top tier", "key", key, "tier_index", tierIndex, "err", err)
	cancel()
	return newGetResponse(GetStatusNotModified, nil, meta)
}

func (c *DaramjweeCache) handleStaleLowerTierHit(requestCtx context.Context, key string, tierIndex int, fetcher Fetcher, src io.ReadCloser, meta *Metadata, cancel context.CancelFunc, expectedGeneration uint64) *GetResponse {
	c.debugLog("msg", "lower tier is stale, serving stale and scheduling refresh", "key", key, "tier_index", tierIndex)
	source := tierDestination{tierIndex: tierIndex, store: c.tiers[tierIndex]}
	streamCloser := newSafeCloser(src, c.lowerTierRefreshOnCloseCallback(requestCtx, key, fetcher, cancel, meta, source, expectedGeneration))
	if meta.IsNegative {
		streamCloser.Close()
		return newGetResponse(GetStatusNotFound, nil, meta)
	}
	return newGetResponse(GetStatusOK, streamCloser, meta)
}

func (c *DaramjweeCache) promoteNegativeLowerTierHit(requestCtx, setupCtx context.Context, key string, tierIndex int, src io.ReadCloser, meta, metaToPromote *Metadata, cancel context.CancelFunc, expectedGeneration uint64) *GetResponse {
	target := c.topWriteStore()
	writer, err := c.setStreamToTopStoreWithGeneration(c.beginSetContextForStore(requestCtx, setupCtx, target), key, metaToPromote, &expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			_ = src.Close()
			cancel()
			return newGetResponse(GetStatusNotFound, nil, meta)
		}
		c.warnLog("msg", "failed to acquire top-tier sink for negative promotion", "key", key, "err", err)
		_ = src.Close()
		cancel()
		return newGetResponse(GetStatusNotFound, nil, meta)
	}

	_ = src.Close()
	closeErr := writer.Close()
	if closeErr == nil {
		if destinations := c.regularFanoutDestinations(tierIndex); len(destinations) > 0 {
			c.schedulePersistFromCurrentTop(requestCtx, key, destinations...)
		}
	}
	cancel()
	if closeErr != nil {
		c.warnLog("msg", "failed to publish negative entry to top tier", "key", key, "err", closeErr)
	}
	return newGetResponse(GetStatusNotFound, nil, meta)
}

func (c *DaramjweeCache) promotePositiveLowerTierHit(requestCtx, setupCtx context.Context, key string, tierIndex int, src io.ReadCloser, meta, metaToPromote *Metadata, cancel context.CancelFunc, expectedGeneration uint64) *GetResponse {
	target := c.topWriteStore()
	writer, err := c.setStreamToTopStoreWithGeneration(c.beginSetContextForStore(requestCtx, setupCtx, target), key, metaToPromote, &expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(src, cancel), meta)
		}
		c.warnLog("msg", "failed to acquire top-tier sink for promotion", "key", key, "err", err)
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(src, cancel), meta)
	}

	var onPublish func()
	destinations := c.regularFanoutDestinations(tierIndex)
	if len(destinations) > 0 {
		onPublish = func() {
			c.schedulePersistFromCurrentTop(requestCtx, key, destinations...)
		}
	}
	return newGetResponse(GetStatusOK, streamThrough(src, writer, cancel, onPublish), meta)
}

func (c *DaramjweeCache) promoteLowerTierHitToTop(requestCtx, setupCtx context.Context, key string, tierIndex int, src io.ReadCloser, metadata *Metadata, expectedGeneration uint64) error {
	target := c.topWriteStore()
	writer, err := c.setStreamToTopStoreWithGeneration(c.beginSetContextForStore(requestCtx, setupCtx, target), key, metadata, &expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			return lowerTierPromotionInvalidatedError{preserveBody: true}
		}
		if !errors.Is(err, ErrTopWriteInvalidated) {
			_ = src.Close()
		}
		return err
	}
	if _, copyErr := io.Copy(writer, src); copyErr != nil {
		abortErr := writer.Abort()
		closeErr := src.Close()
		return errors.Join(copyErr, abortErr, closeErr)
	}
	closeErr := writer.Close()
	srcErr := src.Close()
	if closeErr != nil || srcErr != nil {
		if errors.Is(closeErr, ErrTopWriteInvalidated) {
			return errors.Join(lowerTierPromotionInvalidatedError{preserveBody: false}, closeErr, srcErr)
		}
		return errors.Join(closeErr, srcErr)
	}
	if destinations := c.regularFanoutDestinations(tierIndex); len(destinations) > 0 {
		c.schedulePersistFromCurrentTop(requestCtx, key, destinations...)
	}
	return nil
}

// handleMiss processes the logic when an object is not found in any tier.
func (c *DaramjweeCache) handleMiss(requestCtx, setupCtx context.Context, key string, req GetRequest, fetcher Fetcher, cancel context.CancelFunc, expectedGeneration uint64) (*GetResponse, error) {
	c.debugLog("msg", "full cache miss, fetching from origin", "key", key)

	var oldMetadata *Metadata
	if meta, err := c.statFromStore(setupCtx, c.topWriteStore(), key); err == nil {
		oldMetadata = meta
	}

	result, err := c.fetchFromOrigin(c.fetchContextForFetcher(requestCtx, setupCtx, fetcher), fetcher, oldMetadata)
	if err != nil {
		return c.handleMissFetchError(requestCtx, setupCtx, key, req, cancel, fetcher, err, expectedGeneration)
	}

	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata.CachedAt = time.Now()

	return c.publishMissResult(requestCtx, setupCtx, key, result, cancel, expectedGeneration), nil
}

func (c *DaramjweeCache) handleMissFetchError(requestCtx, setupCtx context.Context, key string, req GetRequest, cancel context.CancelFunc, fetcher Fetcher, fetchErr error, expectedGeneration uint64) (*GetResponse, error) {
	if errors.Is(fetchErr, ErrCacheableNotFound) {
		return c.handleNegativeCacheWithGeneration(requestCtx, setupCtx, key, cancel, &expectedGeneration)
	}
	if errors.Is(fetchErr, ErrNotModified) {
		return c.replayTopTierAfterNotModified(requestCtx, setupCtx, key, req, cancel)
	}
	return nil, fetchErr
}

func (c *DaramjweeCache) replayTopTierAfterNotModified(requestCtx, setupCtx context.Context, key string, req GetRequest, cancel context.CancelFunc) (*GetResponse, error) {
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
	if c.isConditionalRequestSatisfied(req, meta) {
		if err := stream.Close(); err != nil {
			cancel()
			return nil, err
		}
		cancel()
		return newGetResponse(GetStatusNotModified, nil, meta), nil
	}
	return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(stream, cancel), meta), nil
}

func (c *DaramjweeCache) publishMissResult(requestCtx, setupCtx context.Context, key string, result *FetchResult, cancel context.CancelFunc, expectedGeneration uint64) *GetResponse {
	target := c.topWriteStore()
	writer, err := c.setStreamToTopStoreWithGeneration(c.beginSetContextForStore(requestCtx, setupCtx, target), key, result.Metadata, &expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(result.Body, cancel), result.Metadata)
		}
		c.warnLog("msg", "failed to acquire top sink on miss", "key", key, "err", err)
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(result.Body, cancel), result.Metadata)
	}

	return newGetResponse(GetStatusOK, streamThrough(result.Body, writer, cancel, func() {
		c.schedulePersistFromCurrentTop(requestCtx, key, c.persistDestinationsAfterTop()...)
	}), result.Metadata)
}

func (c *DaramjweeCache) isConditionalRequestSatisfied(req GetRequest, meta *Metadata) bool {
	return meta != nil && !meta.IsNegative && ifNoneMatchMatchesCacheTag(req.IfNoneMatch, meta.CacheTag)
}
