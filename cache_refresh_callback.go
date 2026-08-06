package daramjwee

import "context"

// staleRefreshCallback holds the state for a stale cache entry refresh on close.
// A single struct serves both the top-tier and lower-tier refresh paths; when
// source is non-nil the refresh may promote the lower-tier fallback entry back
// to the top tier on a not-modified response.
type staleRefreshCallback struct {
	cache              *DaramjweeCache
	requestCtx         context.Context
	key                string
	fetcher            Fetcher
	cancel             context.CancelFunc
	meta               *Metadata
	source             *tierDestination
	observedGeneration *topWriteGeneration
}

func newStaleRefreshCallback(cache *DaramjweeCache, requestCtx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc, meta *Metadata, source *tierDestination, observedGeneration *topWriteGeneration) *staleRefreshCallback {
	ownedGeneration := observedGeneration.retain()
	return &staleRefreshCallback{
		cache:              cache,
		requestCtx:         requestCtx,
		key:                key,
		fetcher:            fetcher,
		cancel:             cancel,
		meta:               meta,
		source:             source,
		observedGeneration: ownedGeneration,
	}
}

func (s *staleRefreshCallback) handle() {
	defer s.cancel()
	defer s.observedGeneration.release()
	if err := s.cache.scheduleRefreshWithMetadata(
		detachedValueContext(s.requestCtx), s.key, s.fetcher,
		cloneMetadata(s.meta), s.source, s.observedGeneration,
	); err != nil {
		if s.source != nil {
			s.cache.warnLog("msg", "failed to schedule stale refresh", "key", s.key, "source_tier", s.source.tierIndex, "err", err)
		} else {
			s.cache.warnLog("msg", "failed to schedule stale refresh", "key", s.key, "err", err)
		}
	}
}
