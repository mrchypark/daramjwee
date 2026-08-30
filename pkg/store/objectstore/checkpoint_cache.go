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
	return c.ttlCache.Get("checkpoint:" + key)
}

func (c *checkpointCache) Set(key string, value *checkpoint, sizeBytes int64) {
	if c == nil || value == nil {
		return
	}
	c.ttlCache.Set("checkpoint:"+key, value, sizeBytes)
}

func (c *checkpointCache) GetEntry(key string) (checkpointEntry, bool, bool) {
	if c == nil {
		return checkpointEntry{}, false, false
	}
	value, cached := c.ttlCache.Get("entry:" + key)
	if !cached {
		return checkpointEntry{}, false, false
	}
	entry, exists := value.Entries[key]
	return entry, exists, true
}

func (c *checkpointCache) SetEntry(key string, entry *checkpointEntry, sizeBytes int64) {
	if c == nil {
		return
	}
	entries := make(map[string]checkpointEntry, 1)
	if entry != nil {
		entries[key] = *entry
	}
	c.ttlCache.Set("entry:"+key, &checkpoint{Entries: entries}, sizeBytes)
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
