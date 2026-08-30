package objectstore

import (
	"time"

	"github.com/goccy/go-json"

	"github.com/mrchypark/daramjwee"
)

const defaultManifestCacheTTL = 2 * time.Second

type manifestCache struct{ *ttlCache[*manifest] }

func newManifestCache(maxBytes int64, ttl time.Duration, now func() time.Time) *manifestCache {
	if ttl <= 0 {
		ttl = defaultManifestCacheTTL
	}
	cache := newTTLCache(maxBytes, ttl, now, cloneManifest)
	if cache == nil {
		return nil
	}
	return &manifestCache{cache}
}

func (c *manifestCache) Get(key string) (*manifest, bool) {
	if c == nil {
		return nil, false
	}
	return c.ttlCache.Get(key)
}

func (c *manifestCache) Set(key string, value *manifest) {
	if c == nil || value == nil {
		return
	}
	sizeBytes := int64(1)
	if encoded, err := json.Marshal(value); err == nil {
		sizeBytes = int64(len(encoded))
	}
	c.ttlCache.Set(key, value, sizeBytes)
}

func cloneManifest(value *manifest) *manifest {
	if value == nil {
		return nil
	}
	cloned := *value
	cloned.Metadata = *daramjwee.CloneMetadata(&value.Metadata)
	return &cloned
}
