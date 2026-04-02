package objectstore

import (
	"sync"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
)

const defaultCheckpointCacheTTL = 2 * time.Second

type checkpointCacheEntry struct {
	checkpoint *checkpoint
	sizeBytes  int64
	expiresAt  time.Time
}

type checkpointCache struct {
	mu       sync.Mutex
	entries  map[string]checkpointCacheEntry
	policy   daramjwee.EvictionPolicy
	maxBytes int64
	current  int64
	ttl      time.Duration
	now      func() time.Time
}

func newCheckpointCache(maxBytes int64, ttl time.Duration, now func() time.Time) *checkpointCache {
	if maxBytes <= 0 {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultCheckpointCacheTTL
	}
	if now == nil {
		now = time.Now
	}
	return &checkpointCache{
		entries:  make(map[string]checkpointCacheEntry),
		policy:   policy.NewLRU(),
		maxBytes: maxBytes,
		ttl:      ttl,
		now:      now,
	}
}

func (c *checkpointCache) Get(shardID string) (*checkpoint, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[shardID]
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && c.now().After(entry.expiresAt) {
		c.removeLocked(shardID)
		return nil, false
	}
	c.policy.Touch(shardID)
	return cloneCheckpoint(entry.checkpoint), true
}

func (c *checkpointCache) Set(shardID string, cp *checkpoint, sizeBytes int64) {
	if c == nil || cp == nil {
		return
	}
	if sizeBytes <= 0 {
		sizeBytes = 1
	}
	if sizeBytes > c.maxBytes {
		c.mu.Lock()
		c.removeLocked(shardID)
		c.mu.Unlock()
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.entries[shardID]; ok {
		c.current -= old.sizeBytes
		c.policy.Remove(shardID)
		delete(c.entries, shardID)
	}

	c.entries[shardID] = checkpointCacheEntry{
		checkpoint: cloneCheckpoint(cp),
		sizeBytes:  sizeBytes,
		expiresAt:  c.now().Add(c.ttl),
	}
	c.current += sizeBytes
	c.policy.Add(shardID, sizeBytes)

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

func (c *checkpointCache) removeLocked(shardID string) {
	entry, ok := c.entries[shardID]
	if !ok {
		return
	}
	delete(c.entries, shardID)
	c.policy.Remove(shardID)
	c.current -= entry.sizeBytes
	if c.current < 0 {
		c.current = 0
	}
}

func cloneCheckpoint(cp *checkpoint) *checkpoint {
	if cp == nil {
		return nil
	}
	cloned := &checkpoint{
		UpdatedAt: cp.UpdatedAt,
		Entries:   make(map[string]checkpointEntry, len(cp.Entries)),
	}
	for key, entry := range cp.Entries {
		cloned.Entries[key] = entry
	}
	return cloned
}
