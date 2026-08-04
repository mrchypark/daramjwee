package daramjwee

import "context"

// staleRefreshCallback holds the state for a stale cache entry refresh on close.
// Using a struct instead of a closure reduces allocations on the hot path.
type staleRefreshCallback struct {
	cache              *DaramjweeCache
	requestCtx         context.Context
	key                string
	fetcher            Fetcher
	cancel             context.CancelFunc
	meta               *Metadata
	observedGeneration uint64
}

func newStaleRefreshCallback(cache *DaramjweeCache, requestCtx context.Context, key string, fetcher Fetcher, cancel context.CancelFunc, meta *Metadata, observedGeneration uint64) *staleRefreshCallback {
	return &staleRefreshCallback{
		cache:              cache,
		requestCtx:         requestCtx,
		key:                key,
		fetcher:            fetcher,
		cancel:             cancel,
		meta:               meta,
		observedGeneration: observedGeneration,
	}
}

func (s *staleRefreshCallback) handle() {
	defer s.cancel()
	if err := s.cache.scheduleRefreshWithMetadata(
		detachedValueContext(s.requestCtx), s.key, s.fetcher,
		cloneMetadata(s.meta), nil, &s.observedGeneration,
	); err != nil {
		s.cache.warnLog("msg", "failed to schedule stale refresh", "key", s.key, "err", err)
	}
}

// lowerTierRefreshCallback holds the state for a lower-tier stale refresh on close.
type lowerTierRefreshCallback struct {
	cache              *DaramjweeCache
	requestCtx         context.Context
	key                string
	fetcher            Fetcher
	cancel             context.CancelFunc
	meta               *Metadata
	source             tierDestination
	observedGeneration uint64
}

func (s *lowerTierRefreshCallback) handle() {
	defer s.cancel()
	if err := s.cache.scheduleRefreshWithMetadata(
		detachedValueContext(s.requestCtx), s.key, s.fetcher,
		cloneMetadata(s.meta), &s.source, &s.observedGeneration,
	); err != nil {
		s.cache.warnLog("msg", "failed to schedule stale refresh", "key", s.key, "source_tier", s.source.tierIndex, "err", err)
	}
}
