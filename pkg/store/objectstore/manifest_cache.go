package objectstore

import (
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
)

const defaultManifestCacheTTL = 2 * time.Second

type manifestCacheEntry struct {
	manifest  *manifest
	sizeBytes int64
	expiresAt time.Time
}

// manifestCache is a bounded, TTL-based in-memory cache for decoded manifests.
// Manifests live at fixed per-key paths, so a freshly published version may be
// served for up to the configured TTL before the cache expires it.
type manifestCache struct {
	mu       sync.Mutex
	entries  map[string]manifestCacheEntry
	policy   daramjwee.EvictionPolicy
	maxBytes int64
	current  int64
	ttl      time.Duration
	now      func() time.Time
}

func newManifestCache(maxBytes int64, ttl time.Duration, now func() time.Time) *manifestCache {
	if maxBytes <= 0 {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultManifestCacheTTL
	}
	if now == nil {
		now = time.Now
	}
	return &manifestCache{
		entries:  make(map[string]manifestCacheEntry),
		policy:   policy.NewLRU(),
		maxBytes: maxBytes,
		ttl:      ttl,
		now:      now,
	}
}

func (c *manifestCache) Get(key string) (*manifest, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && c.now().After(entry.expiresAt) {
		c.removeLocked(key)
		return nil, false
	}
	c.policy.Touch(key)
	return cloneManifest(entry.manifest), true
}

func (c *manifestCache) Set(key string, m *manifest) {
	if c == nil || m == nil {
		return
	}
	sizeBytes := int64(0)
	if encoded, err := json.Marshal(m); err == nil {
		sizeBytes = int64(len(encoded))
	}
	if sizeBytes <= 0 {
		sizeBytes = 1
	}
	if sizeBytes > c.maxBytes {
		c.mu.Lock()
		c.removeLocked(key)
		c.mu.Unlock()
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[key]; ok {
		c.current -= old.sizeBytes
		c.policy.Remove(key)
		delete(c.entries, key)
	}

	c.entries[key] = manifestCacheEntry{
		manifest:  cloneManifest(m),
		sizeBytes: sizeBytes,
		expiresAt: c.now().Add(c.ttl),
	}
	c.current += sizeBytes
	c.policy.Add(key, sizeBytes)

	for c.current > c.maxBytes {
		evicted := c.policy.Evict()
		if len(evicted) == 0 {
			break
		}
		for _, key := range evicted {
			c.removeLocked(key)
		}
	}
}

func (c *manifestCache) removeLocked(key string) {
	entry, ok := c.entries[key]
	if !ok {
		return
	}
	delete(c.entries, key)
	c.policy.Remove(key)
	c.current -= entry.sizeBytes
	if c.current < 0 {
		c.current = 0
	}
}

func cloneManifest(m *manifest) *manifest {
	if m == nil {
		return nil
	}
	cloned := *m
	cloned.Metadata = *daramjwee.CloneMetadata(&m.Metadata)
	return &cloned
}
