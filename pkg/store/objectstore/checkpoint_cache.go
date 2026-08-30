package objectstore

import "time"

const defaultCheckpointCacheTTL = 2 * time.Second

type checkpointCache struct{ *ttlCache[*checkpoint] }

func newCheckpointCache(maxBytes int64, ttl time.Duration, now func() time.Time) *checkpointCache {
	if ttl <= 0 {
		ttl = defaultCheckpointCacheTTL
	}
	cache := newTTLCache(maxBytes, ttl, now, cloneCheckpoint)
	if cache == nil {
		return nil
	}
	return &checkpointCache{cache}
}

func (c *checkpointCache) Get(key string) (*checkpoint, bool) {
	if c == nil {
		return nil, false
	}
	return c.ttlCache.Get(key)
}

func (c *checkpointCache) Set(key string, value *checkpoint, sizeBytes int64) {
	if c == nil || value == nil {
		return
	}
	c.ttlCache.Set(key, value, sizeBytes)
}

func cloneCheckpoint(value *checkpoint) *checkpoint {
	if value == nil {
		return nil
	}
	cloned := &checkpoint{UpdatedAt: value.UpdatedAt, Entries: make(map[string]checkpointEntry, len(value.Entries))}
	for key, entry := range value.Entries {
		cloned.Entries[key] = entry
	}
	return cloned
}
