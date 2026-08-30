package daramjwee

import (
	"context"
	"errors"
	"io"
	"time"
)

// handleTopTierHit processes the logic when an object is found in tier 0.
// observedGeneration may be nil for fast-path hits: the generation snapshot
// (snapCoord/snapGen, taken before the tier-0 read) is materialized lazily
// when the entry is stale and a background refresh must be scheduled.
func (c *DaramjweeCache) handleTopTierHit(requestCtx context.Context, key string, req GetRequest, fetcher Fetcher, stream io.ReadCloser, meta *Metadata, cancel context.CancelFunc, observedGeneration *topWriteGeneration, snapCoord *writeCoordinator, snapGen uint64) (*GetResponse, error) {
	if c.debugEnabled() {
		c.debugLog("msg", "top tier hit", "key", key)
	}

	isStale := c.isTierCachedStale(meta, 0)
	if isStale && observedGeneration == nil {
		observedGeneration = c.staleGenerationFromSnapshot(key, snapCoord, snapGen)
		defer observedGeneration.release()
	}
	if c.isConditionalRequestSatisfied(req, meta) {
		return c.handleConditionalTopTierHit(requestCtx, key, fetcher, stream, meta, cancel, isStale, observedGeneration)
	}

	callback := c.topTierCloseCallback(requestCtx, key, fetcher, cancel, meta, isStale, observedGeneration)
	streamCloser := newSafeCloser(stream, callback)

	if meta.IsNegative {
		if err := streamCloser.Close(); err != nil {
			return nil, err
		}
		return newGetResponse(GetStatusNotFound, nil, meta), nil
	}

	return newGetResponse(GetStatusOK, streamCloser, meta), nil
}

// staleGenerationFromSnapshot materializes a live topWriteGeneration for a
// stale-hit refresh from the pre-read generation snapshot. When the snapshot
// coordinator still exists, it is retained and its generation is used so any
// write committed after the snapshot invalidates the refresh. When no
// coordinator existed at snapshot time, the refresh is gated on generation 0
// of a newly created coordinator, which preserves the same invalidation
// semantics against a concurrent first write.
func (c *DaramjweeCache) staleGenerationFromSnapshot(key string, snapCoord *writeCoordinator, snapGen uint64) *topWriteGeneration {
	if snapCoord != nil && snapCoord.retainReference() {
		return &topWriteGeneration{coord: snapCoord, generation: snapGen}
	}
	coord := c.topWrites.coordinator(key)
	return &topWriteGeneration{coord: coord, generation: 0}
}

func (c *DaramjweeCache) handleConditionalTopTierHit(requestCtx context.Context, key string, fetcher Fetcher, stream io.ReadCloser, meta *Metadata, cancel context.CancelFunc, isStale bool, observedGeneration *topWriteGeneration) (*GetResponse, error) {
	if err := stream.Close(); err != nil {
		cancel()
		return nil, err
	}
	if isStale {
		if err := c.scheduleRefreshWithMetadata(detachedValueContext(requestCtx), key, fetcher, cloneMetadata(meta), nil, observedGeneration); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "err", err)
		}
	}
	cancel()
	return newGetResponse(GetStatusNotModified, nil, meta), nil
}

func (c *DaramjweeCache) topTierCloseCallback(requestCtx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc, meta *Metadata, isStale bool, observedGeneration *topWriteGeneration) func() {
	if !isStale {
		return cancel
	}

	c.debugLog("msg", "top tier is stale, scheduling refresh", "key", key)
	return newStaleRefreshCallback(c, requestCtx, key, fetcher, cancel, meta, nil, observedGeneration).handle
}

// lowerTierHitParams groups the parameters for handleLowerTierHit.
type lowerTierHitParams struct {
	requestCtx         context.Context
	setupCtx           context.Context
	key                string
	tierIndex          int
	req                GetRequest
	fetcher            Fetcher
	src                io.ReadCloser
	meta               *Metadata
	cancel             context.CancelFunc
	expectedGeneration *topWriteGeneration
	higherTiersClean   bool
}

// handleLowerTierHit processes the logic when an object is found in a lower tier.
func (c *DaramjweeCache) handleLowerTierHit(p lowerTierHitParams) (*GetResponse, error) {
	c.debugLog("msg", "lower tier hit, promoting to top tier", "key", p.key, "tier_index", p.tierIndex)

	metaToPromote := cloneMetadata(p.meta)
	if metaToPromote == nil {
		metaToPromote = &Metadata{}
	}

	isStale := c.isTierCachedStale(p.meta, p.tierIndex)

	plan := c.planLowerTierHit(p, p.meta, isStale)
	return c.executeLowerTierPlan(p, plan, metaToPromote)
}

// buildLowerTierObservation creates an Observation from the current lookup state.
// This is used by the Planner for policy decisions.
func (c *DaramjweeCache) buildLowerTierObservation(p lowerTierHitParams, meta *Metadata, isStale bool) (Observation, generationValidity) {
	obs := Observation{
		Source:      SourceLower,
		SourceTier:  p.tierIndex,
		Freshness:   FreshnessFresh,
		HasTopStore: hasRealStore(c.topWriteStore()),
	}
	if isStale {
		obs.Freshness = FreshnessStale
	}
	if meta != nil {
		obs.EntryNegative = meta.IsNegative
	}
	if p.higherTiersClean {
		obs.UpperTiersHealth = UpperTiersClean
	} else {
		obs.UpperTiersHealth = UpperTiersDirty
	}
	if c.isConditionalRequestSatisfied(p.req, meta) {
		obs.ConditionalMatched = true
	}
	if c.probation != nil && p.higherTiersClean && !isStale && !obs.ConditionalMatched && obs.HasTopStore && !c.probation.observe(p.key) {
		obs.Admission = AdmissionDeferred
	}
	validity := generationInvalid
	if p.higherTiersClean && c.canServeConditionalLowerHit(p.key, p.expectedGeneration) {
		validity = generationValid
	}
	return obs, validity
}

func (c *DaramjweeCache) planLowerTierHit(p lowerTierHitParams, meta *Metadata, isStale bool) ReadPlan {
	obs, validity := c.buildLowerTierObservation(p, meta, isStale)
	plan := (&Planner{}).plan(obs, validity)
	c.debugLog("msg", "planner observation", "key", p.key, "tier_index", p.tierIndex,
		"source", obs.Source, "freshness", obs.Freshness, "admission", obs.Admission,
		"generation_validity", validity, "reply", plan.Reply, "body", plan.Body,
		"publish", plan.Publish, "refresh", plan.Refresh)
	return plan
}

// serveLowerTierWithoutPromotion serves data from a lower tier when higher tiers
// are dirty and promotion is not safe.
func (c *DaramjweeCache) serveLowerTierWithoutPromotion(p lowerTierHitParams, _ bool) (*GetResponse, error) {
	if c.isConditionalRequestSatisfied(p.req, p.meta) {
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(p.src, p.cancel), p.meta), nil
	}
	if p.meta.IsNegative {
		if err := p.src.Close(); err != nil {
			p.cancel()
			return nil, err
		}
		p.cancel()
		return newGetResponse(GetStatusNotFound, nil, p.meta), nil
	}
	return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(p.src, p.cancel), p.meta), nil
}

func (c *DaramjweeCache) handleConditionalLowerTierHit(requestCtx, _ context.Context, key string, tierIndex int, fetcher Fetcher, src io.ReadCloser, meta, _ *Metadata, cancel context.CancelFunc, isStale bool, expectedGeneration *topWriteGeneration) (*GetResponse, error) {
	if !c.canServeConditionalLowerHit(key, expectedGeneration) {
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(src, cancel), meta), nil
	}
	if err := src.Close(); err != nil {
		cancel()
		return nil, err
	}

	if isStale {
		source := tierDestination{tierIndex: tierIndex, store: c.tiers[tierIndex]}
		if err := c.scheduleRefreshWithMetadata(detachedValueContext(requestCtx), key, fetcher, cloneMetadata(meta), &source, expectedGeneration); err != nil {
			c.warnLog("msg", "failed to schedule stale refresh", "key", key, "source_tier", tierIndex, "err", err)
		}
	}
	cancel()
	return newGetResponse(GetStatusNotModified, nil, meta), nil
}

func (c *DaramjweeCache) canServeConditionalLowerHit(key string, expectedGeneration *topWriteGeneration) bool {
	return c.canAttemptExpectedTopWrite(key, expectedGeneration)
}

func (c *DaramjweeCache) canAttemptExpectedTopWrite(key string, expectedGeneration *topWriteGeneration) bool {
	return expectedGeneration != nil &&
		expectedGeneration.coord.manager == &c.topWrites &&
		expectedGeneration.coord.key == key &&
		expectedGeneration.coord.canAttemptExpectedTopWrite(expectedGeneration.generation)
}

func (c *DaramjweeCache) handleStaleLowerTierHit(requestCtx context.Context, key string, tierIndex int, fetcher Fetcher, src io.ReadCloser, meta *Metadata, cancel context.CancelFunc, expectedGeneration *topWriteGeneration) (*GetResponse, error) {
	c.debugLog("msg", "lower tier is stale, serving stale and scheduling refresh", "key", key, "tier_index", tierIndex)
	source := tierDestination{tierIndex: tierIndex, store: c.tiers[tierIndex]}
	streamCloser := newSafeCloser(src, c.lowerTierRefreshOnCloseCallback(requestCtx, key, fetcher, cancel, meta, source, expectedGeneration))
	if meta.IsNegative {
		if err := streamCloser.Close(); err != nil {
			return nil, err
		}
		return newGetResponse(GetStatusNotFound, nil, meta), nil
	}
	return newGetResponse(GetStatusOK, streamCloser, meta), nil
}

func (c *DaramjweeCache) promoteNegativeLowerTierHit(requestCtx, setupCtx context.Context, key string, tierIndex int, src io.ReadCloser, meta, metaToPromote *Metadata, cancel context.CancelFunc, expectedGeneration *topWriteGeneration) (*GetResponse, error) {
	target := c.topWriteStore()
	writer, err := c.setStreamToTopStoreBestEffortWithGeneration(c.beginSetContextForStore(requestCtx, setupCtx, target), key, metaToPromote, expectedGeneration)
	if err != nil {
		closeErr := src.Close()
		if closeErr != nil {
			cancel()
			return nil, errors.Join(err, closeErr)
		}
		if errors.Is(err, ErrTopWriteInvalidated) {
			cancel()
			return newGetResponse(GetStatusNotFound, nil, meta), nil
		}
		c.warnLog("msg", "failed to acquire top-tier sink for negative promotion", "key", key, "err", err)
		cancel()
		return newGetResponse(GetStatusNotFound, nil, meta), nil
	}

	if closeErr := src.Close(); closeErr != nil {
		abortErr := writer.Abort()
		cancel()
		return nil, errors.Join(closeErr, abortErr)
	}
	closeErr := writer.Close()
	if closeErr == nil {
		if destinations := c.regularFanoutDestinations(tierIndex); len(destinations) > 0 {
			c.schedulePersistFromCurrentTop(requestCtx, key, destinations...)
		}
	}
	cancel()
	if closeErr != nil {
		if errors.Is(closeErr, ErrTopWriteInvalidated) {
			c.infoLog("msg", "skipping negative promotion because top-tier state changed", "key", key)
		} else {
			c.warnLog("msg", "failed to publish negative entry to top tier", "key", key, "err", closeErr)
		}
	}
	return newGetResponse(GetStatusNotFound, nil, meta), nil
}

func (c *DaramjweeCache) promotePositiveLowerTierHit(requestCtx, setupCtx context.Context, key string, tierIndex int, src io.ReadCloser, meta, metaToPromote *Metadata, cancel context.CancelFunc, expectedGeneration *topWriteGeneration) *GetResponse {
	target := c.topWriteStore()
	writer, err := c.setStreamToTopStoreForFill(c.beginSetContextForStore(requestCtx, setupCtx, target), key, metaToPromote, expectedGeneration)
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
	return newGetResponse(GetStatusOK, streamThroughWithTrace(src, writer, cancel, onPublish, func(event string, keyvals ...any) {
		c.diagnosticLog(event, key, expectedGeneration.generation, keyvals...)
	}), meta)
}

// handleMiss processes the logic when an object is not found in any tier.
//
// Concurrent misses for the same key are coalesced: the first caller becomes
// the miss leader and fetches from the origin while filling the top tier.
// Waiters wait (bounded by missWaitCap or their request deadline) for the
// leader's fill to become visible and then re-serve from the top tier,
// falling back to their own origin fetch when the leader is too slow or
// fails to publish. Misses that cannot fill the top tier (higherTiersClean
// false) skip coalescing entirely.
func (c *DaramjweeCache) handleMiss(requestCtx, setupCtx context.Context, key string, req GetRequest, fetcher Fetcher, cancel context.CancelFunc, expectedGeneration *topWriteGeneration, higherTiersClean bool) (*GetResponse, error) {
	c.debugLog("msg", "full cache miss, fetching from origin", "key", key)

	if !higherTiersClean {
		// No top-tier fill can happen; coalescing would only delay waiters.
		return c.handleMissAsLeader(requestCtx, setupCtx, key, req, fetcher, cancel, expectedGeneration, false, nil)
	}

	for {
		lead, ok := c.missLeads.current(key)
		if !ok {
			become, becameLeader := c.missLeads.tryLead(key)
			if !becameLeader {
				continue
			}
			return c.handleMissAsLeader(requestCtx, setupCtx, key, req, fetcher, cancel, expectedGeneration, true, become)
		}

		if lead.wait(setupCtx) {
			if err := setupCtx.Err(); err != nil {
				return nil, err
			}
			// Leader is too slow or abandoned: fall back to an unregistered
			// leader run. Its fill attempt is rejected while the leader still
			// owns the fill lease, so it degrades to a direct serve.
			return c.handleMissAsLeader(requestCtx, setupCtx, key, req, fetcher, cancel, expectedGeneration, true, nil)
		}

		if resp, err, retry := c.serveMissWaiterFromTop(requestCtx, setupCtx, key, req, cancel); !retry {
			return resp, err
		}
	}
}

// serveMissWaiterFromTop re-serves a miss waiter from the top tier after the
// leader's fill became visible. retry is true when the top tier is still
// missing the key and the waiter should attempt to become the leader itself.
func (c *DaramjweeCache) serveMissWaiterFromTop(requestCtx, setupCtx context.Context, key string, req GetRequest, cancel context.CancelFunc) (*GetResponse, error, bool) {
	stream, meta, err := c.getStreamFromStore(c.getStreamContextForStore(requestCtx, setupCtx, c.topWriteStore()), c.topWriteStore(), key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil, true
		}
		return nil, err, false
	}
	if meta == nil {
		if closeErr := stream.Close(); closeErr != nil {
			cancel()
			return nil, errors.Join(err, closeErr), false
		}
		cancel()
		return nil, ErrNilMetadata, false
	}
	if meta.IsNegative {
		if err := stream.Close(); err != nil {
			cancel()
			return nil, err, false
		}
		cancel()
		return newGetResponse(GetStatusNotFound, nil, meta), nil, false
	}
	if c.isConditionalRequestSatisfied(req, meta) {
		if err := stream.Close(); err != nil {
			cancel()
			return nil, err, false
		}
		cancel()
		return newGetResponse(GetStatusNotModified, nil, meta), nil, false
	}
	return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(stream, cancel), meta), nil, false
}

// handleMissAsLeader runs the full miss path as the miss leader. When lead is
// non-nil the returned response signals lead completion: streaming responses
// signal when the body is closed (the moment the top-tier fill becomes
// visible or aborts) and release the leader registration at the same point;
// non-body responses and errors signal and release immediately.
func (c *DaramjweeCache) handleMissAsLeader(requestCtx, setupCtx context.Context, key string, req GetRequest, fetcher Fetcher, cancel context.CancelFunc, expectedGeneration *topWriteGeneration, higherTiersClean bool, lead *missLead) (*GetResponse, error) {
	resp, err := c.handleMissInner(requestCtx, setupCtx, key, req, fetcher, cancel, expectedGeneration, higherTiersClean)
	if lead == nil {
		return resp, err
	}
	if err != nil || resp == nil || resp.Body == nil {
		lead.signal()
		c.missLeads.release(key, lead)
		return resp, err
	}
	resp.Body = newCancelOnCloseReadCloser(resp.Body, func() {
		lead.signal()
		c.missLeads.release(key, lead)
	})
	return resp, nil
}

func (c *DaramjweeCache) handleMissInner(requestCtx, setupCtx context.Context, key string, req GetRequest, fetcher Fetcher, cancel context.CancelFunc, expectedGeneration *topWriteGeneration, higherTiersClean bool) (*GetResponse, error) {
	var oldMetadata *Metadata
	if meta, err := c.statFromStore(setupCtx, c.topWriteStore(), key); err == nil {
		oldMetadata = meta
	}

	result, err := c.fetchFromOrigin(c.fetchContextForFetcher(requestCtx, setupCtx, fetcher), fetcher, oldMetadata)
	if err != nil {
		return c.handleMissFetchError(requestCtx, setupCtx, key, req, cancel, fetcher, err, expectedGeneration, higherTiersClean)
	}

	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata = cloneMetadata(result.Metadata)
	result.Metadata.CachedAt = time.Now()

	if !higherTiersClean {
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(result.Body, cancel), result.Metadata), nil
	}
	return c.publishMissResult(requestCtx, setupCtx, key, result, cancel, expectedGeneration), nil
}

func (c *DaramjweeCache) handleMissFetchError(requestCtx, setupCtx context.Context, key string, req GetRequest, cancel context.CancelFunc, _ Fetcher, fetchErr error, expectedGeneration *topWriteGeneration, higherTiersClean bool) (*GetResponse, error) {
	if errors.Is(fetchErr, ErrCacheableNotFound) {
		if !higherTiersClean {
			cancel()
			return newGetResponse(GetStatusNotFound, nil, &Metadata{IsNegative: true, CachedAt: time.Now()}), nil
		}
		return c.handleNegativeCacheWithGeneration(requestCtx, setupCtx, key, cancel, expectedGeneration)
	}
	if errors.Is(fetchErr, ErrNotModified) {
		if !higherTiersClean {
			cancel()
			return nil, errors.New("daramjwee: origin returned not modified after unreadable cache tier")
		}
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
		if !errors.Is(err, ErrNotFound) {
			cancel()
			return nil, err
		}
		cancel()
		return nil, errors.New("daramjwee: origin returned not modified but cached body is unavailable")
	}
	if meta.IsNegative {
		if err := stream.Close(); err != nil {
			cancel()
			return nil, err
		}
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

func (c *DaramjweeCache) publishMissResult(requestCtx, setupCtx context.Context, key string, result *FetchResult, cancel context.CancelFunc, expectedGeneration *topWriteGeneration) *GetResponse {
	target := c.topWriteStore()
	writer, err := c.setStreamToTopStoreForFill(c.beginSetContextForStore(requestCtx, setupCtx, target), key, result.Metadata, expectedGeneration)
	if err != nil {
		if errors.Is(err, ErrTopWriteInvalidated) {
			return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(result.Body, cancel), result.Metadata)
		}
		c.warnLog("msg", "failed to acquire top sink on miss", "key", key, "err", err)
		return newGetResponse(GetStatusOK, newCancelOnCloseReadCloser(result.Body, cancel), result.Metadata)
	}

	return newGetResponse(GetStatusOK, streamThroughWithTrace(result.Body, writer, cancel, func() {
		c.schedulePersistFromCurrentTop(requestCtx, key, c.persistDestinationsAfterTop()...)
	}, func(event string, keyvals ...any) {
		c.diagnosticLog(event, key, expectedGeneration.generation, keyvals...)
	}), result.Metadata)
}

func (c *DaramjweeCache) isConditionalRequestSatisfied(req GetRequest, meta *Metadata) bool {
	return meta != nil && !meta.IsNegative && ifNoneMatchMatchesCacheTag(req.IfNoneMatch, meta.CacheTag)
}
