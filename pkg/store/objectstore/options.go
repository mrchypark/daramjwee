package objectstore

import "time"

type Option func(*config)

type config struct {
	dataDir               string
	prefix                string
	defaultGCGrace        time.Duration
	packedObjectThreshold int64
	wholeThreshold        int64
	pageSize              int64
	memoryPageCacheBytes  int64
	memoryBlockCacheBytes int64
	memoryCheckpointBytes int64
	checkpointCacheTTL    time.Duration
}

// WithDataDir configures the local objectstore working directory used for
// ingest segments and catalog state. When omitted, a temporary directory is used.
func WithDataDir(dir string) Option {
	return func(cfg *config) {
		cfg.dataDir = dir
	}
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

// WithPackedObjectThreshold configures the size threshold for packing flushed
// records into shard segments. Records larger than the threshold are uploaded
// as direct remote blobs instead. A non-positive threshold keeps all records
// on the packed-segment path.
func WithPackedObjectThreshold(threshold int64) Option {
	return func(cfg *config) {
		cfg.packedObjectThreshold = threshold
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

// WithMemoryBlockCache enables in-memory block caching with the provided byte capacity.
// A non-positive capacity disables the block cache.
func WithMemoryBlockCache(capacityBytes int64) Option {
	return func(cfg *config) {
		cfg.memoryBlockCacheBytes = capacityBytes
	}
}

// WithMemoryCheckpointCache enables in-memory shard checkpoint caching with the
// provided byte capacity. A non-positive capacity disables the cache.
func WithMemoryCheckpointCache(capacityBytes int64) Option {
	return func(cfg *config) {
		cfg.memoryCheckpointBytes = capacityBytes
	}
}

// WithCheckpointCacheTTL sets how long decoded shard checkpoints stay valid in
// the in-memory checkpoint cache before being reloaded from remote storage.
// A non-positive TTL falls back to the backend default.
func WithCheckpointCacheTTL(ttl time.Duration) Option {
	return func(cfg *config) {
		cfg.checkpointCacheTTL = ttl
	}
}
