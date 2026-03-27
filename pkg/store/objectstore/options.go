package objectstore

import "time"

type Option func(*config)

type config struct {
	prefix               string
	defaultGCGrace       time.Duration
	wholeThreshold       int64
	pageSize             int64
	memoryPageCacheBytes int64
}

// WithPrefix scopes all objectstore paths under the provided prefix.
func WithPrefix(prefix string) Option {
	return func(cfg *config) {
		cfg.prefix = prefix
	}
}

// WithDefaultGCGrace configures the default grace period for conservative GC sweeps.
func WithDefaultGCGrace(grace time.Duration) Option {
	return func(cfg *config) {
		cfg.defaultGCGrace = grace
	}
}

// WithWholeObjectThreshold chooses paged layout when the logical object size exceeds the threshold.
// A non-positive threshold keeps whole layout for all objects.
func WithWholeObjectThreshold(threshold int64) Option {
	return func(cfg *config) {
		cfg.wholeThreshold = threshold
	}
}

// WithPageSize configures the page size used for paged layout reads.
func WithPageSize(size int64) Option {
	return func(cfg *config) {
		cfg.pageSize = size
	}
}

// WithMemoryPageCache enables in-memory page caching with the provided byte capacity.
// A non-positive capacity disables the page cache.
func WithMemoryPageCache(capacityBytes int64) Option {
	return func(cfg *config) {
		cfg.memoryPageCacheBytes = capacityBytes
	}
}
