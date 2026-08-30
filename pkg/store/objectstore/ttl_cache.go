package objectstore

import (
	"sync"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
)

type ttlCacheEntry[T any] struct {
	value     T
	sizeBytes int64
	expiresAt time.Time
}

type ttlCache[T any] struct {
	mu       sync.Mutex
	entries  map[string]ttlCacheEntry[T]
	policy   daramjwee.EvictionPolicy
	maxBytes int64
	current  int64
	ttl      time.Duration
	now      func() time.Time
	clone    func(T) T
}

func newTTLCache[T any](maxBytes int64, ttl time.Duration, now func() time.Time, clone func(T) T) *ttlCache[T] {
	if maxBytes <= 0 {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &ttlCache[T]{
		entries:  make(map[string]ttlCacheEntry[T]),
		policy:   policy.NewLRU(),
		maxBytes: maxBytes,
		ttl:      ttl,
		now:      now,
		clone:    clone,
	}
}

func (c *ttlCache[T]) Get(key string) (T, bool) {
	var zero T
	if c == nil {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return zero, false
	}
	if !entry.expiresAt.IsZero() && c.now().After(entry.expiresAt) {
		c.removeLocked(key)
		return zero, false
	}
	c.policy.Touch(key)
	return c.clone(entry.value), true
}

func (c *ttlCache[T]) Set(key string, value T, sizeBytes int64) {
	if sizeBytes <= 0 {
		sizeBytes = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if sizeBytes > c.maxBytes {
		c.removeLocked(key)
		return
	}
	if old, ok := c.entries[key]; ok {
		c.current -= old.sizeBytes
		c.policy.Remove(key)
	}
	c.entries[key] = ttlCacheEntry[T]{value: c.clone(value), sizeBytes: sizeBytes, expiresAt: c.now().Add(c.ttl)}
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

func (c *ttlCache[T]) removeLocked(key string) {
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
