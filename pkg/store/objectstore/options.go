package objectstore

import "time"

type Option func(*config)

type config struct {
	dir                  string
	prefix               string
	gcGrace              time.Duration
	packThreshold        int64
	pagedThreshold       int64
	pageSize             int64
	pageCacheBytes       int64
	blockCacheBytes      int64
	checkpointCacheBytes int64
	checkpointTTL        time.Duration
}

// WithDir configures the local objectstore working directory used for
// ingest segments and catalog state. When omitted, a temporary directory is used.
func WithDir(dir string) Option {
	return func(cfg *config) {
		cfg.dir = dir
	}
}

// WithPrefix scopes all objectstore paths under the provided prefix.
func WithPrefix(prefix string) Option {
	return func(cfg *config) {
		cfg.prefix = prefix
	}
}

// WithGCGrace configures the default grace period for conservative GC sweeps.
func WithGCGrace(grace time.Duration) Option {
	return func(cfg *config) {
		cfg.gcGrace = grace
	}
}

// WithPackThreshold configures the size threshold for packing flushed
// records into shard segments. Records larger than the threshold are uploaded
// as direct remote blobs instead. A non-positive threshold keeps all records
// on the packed-segment path.
func WithPackThreshold(threshold int64) Option {
	return func(cfg *config) {
		cfg.packThreshold = threshold
	}
}

// WithPagedThreshold chooses paged layout when the logical object size exceeds the threshold.
// A non-positive threshold keeps whole layout for all objects.
func WithPagedThreshold(threshold int64) Option {
	return func(cfg *config) {
		cfg.pagedThreshold = threshold
	}
}

// WithPageSize configures the page size used for paged layout reads.
func WithPageSize(size int64) Option {
	return func(cfg *config) {
		cfg.pageSize = size
	}
}

// WithPageCache enables in-memory page caching with the provided byte capacity.
// A non-positive capacity disables the page cache.
func WithPageCache(capacityBytes int64) Option {
	return func(cfg *config) {
		cfg.pageCacheBytes = capacityBytes
	}
}

// WithBlockCache enables in-memory block caching with the provided byte capacity.
// A non-positive capacity disables the block cache.
func WithBlockCache(capacityBytes int64) Option {
	return func(cfg *config) {
		cfg.blockCacheBytes = capacityBytes
	}
}

// WithCheckpointCache enables in-memory shard checkpoint caching with the
// provided byte capacity. A non-positive capacity disables the cache.
func WithCheckpointCache(capacityBytes int64) Option {
	return func(cfg *config) {
		cfg.checkpointCacheBytes = capacityBytes
	}
}

// WithCheckpointTTL sets how long decoded shard checkpoints stay valid in
// the in-memory checkpoint cache before being reloaded from remote storage.
// A non-positive TTL falls back to the backend default.
func WithCheckpointTTL(ttl time.Duration) Option {
	return func(cfg *config) {
		cfg.checkpointTTL = ttl
	}
}
