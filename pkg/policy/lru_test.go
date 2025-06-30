package policy

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// TestLRU_AddAndEvict tests the basic Add and Evict functionality.
func TestLRU_AddAndEvict(t *testing.T) {
	p := NewLRUPolicy().(*LRUPolicy)

	p.Add("key1", 10)
	p.Add("key2", 20)
	p.Add("key3", 30)

	// Current state (most recent -> oldest): key3, key2, key1
	// Evict should remove the oldest item (key1)
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key1" {
		t.Fatalf("Expected to evict key1, but got %v", evictedKeys)
	}
	if _, ok := p.cache["key1"]; ok {
		t.Fatal("key1 should not be in cache after eviction")
	}

	// Should remove the next oldest item (key2)
	evictedKeys = p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key2" {
		t.Fatalf("Expected to evict key2, but got %v", evictedKeys)
	}

	// Should remove the last remaining item (key3)
	evictedKeys = p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key3" {
		t.Fatalf("Expected to evict key3, but got %v", evictedKeys)
	}

	// Verify cache is empty
	if p.ll.Len() != 0 || len(p.cache) != 0 {
		t.Error("Cache should be empty after evicting all items")
	}
}

// TestLRU_Touch tests if Touch correctly moves an item to the front.
func TestLRU_Touch(t *testing.T) {
	p := NewLRUPolicy().(*LRUPolicy)

	p.Add("key1", 10)
	p.Add("key2", 20)
	p.Add("key3", 30)

	// Current state (most recent -> oldest): key3, key2, key1
	// Touching key1 should make it the most recent.
	p.Touch("key1")

	// Evict should now remove the oldest, which is key2.
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key2" {
		t.Fatalf("Expected key2 to be evicted after key1 was touched, but got %v", evictedKeys)
	}
}

// TestLRU_Remove tests explicit removal of an item.
func TestLRU_Remove(t *testing.T) {
	p := NewLRUPolicy().(*LRUPolicy)

	p.Add("key1", 10)
	p.Add("key2", 20)
	p.Add("key3", 30)

	// Remove key2 from the middle.
	p.Remove("key2")
	if _, ok := p.cache["key2"]; ok {
		t.Fatal("key2 should not be in cache after Remove")
	}
	if p.ll.Len() != 2 {
		t.Fatalf("List length should be 2 after removing one item, but got %d", p.ll.Len())
	}

	// Evict should now remove the oldest, which is key1.
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key1" {
		t.Fatalf("Expected key1 to be evicted, but got %v", evictedKeys)
	}
}

// TestLRU_AddExisting updates an existing item.
func TestLRU_AddExisting(t *testing.T) {
	p := NewLRUPolicy().(*LRUPolicy)

	p.Add("key1", 10)
	p.Add("key2", 20)
	// Update key1's size and re-add it. This should have the same effect as Touch.
	p.Add("key1", 100)

	// Since key1 is now the most recent, Evict should remove key2.
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key2" {
		t.Fatalf("Expected key2 to be evicted, but got %v", evictedKeys)
	}

	// Verify key1's size was updated correctly.
	if elem, ok := p.cache["key1"]; ok {
		if entry := elem.Value.(*lruEntry); entry.size != 100 {
			t.Errorf("Expected size of key1 to be 100, but got %d", entry.size)
		}
	} else {
		t.Fatal("key1 not found in cache")
	}
}

// TestLRU_EdgeCases tests edge cases like operating on an empty cache.
func TestLRU_EdgeCases(t *testing.T) {
	p := NewLRUPolicy().(*LRUPolicy)

	// 1. Call Evict on an empty cache (should not panic).
	evictedKeys := p.Evict()
	if evictedKeys != nil {
		t.Errorf("Evict on an empty policy should return nil, but got %v", evictedKeys)
	}

	// 2. Call Touch and Remove on non-existent keys (should not panic).
	p.Touch("non-existent")
	p.Remove("non-existent")

	// 3. Add an item, then remove it, then call Evict.
	p.Add("key1", 10)
	p.Remove("key1")
	evictedKeys = p.Evict()
	if evictedKeys != nil {
		t.Errorf("Evict after removing the only item should return nil, but got %v", evictedKeys)
	}
}

// TestLRU_Churn is a randomized load test that verifies the internal state consistency
// of the LRU policy during frequent Add/Remove/Touch operations.
func TestLRU_Churn(t *testing.T) {
	p := NewLRUPolicy().(*LRUPolicy)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const cacheSize = 100    // Max number of items in cache
	const iterations = 10000 // Total number of operations

	keys := make([]string, 0, cacheSize)

	for i := 0; i < iterations; i++ {
		// 30% chance to add a new item
		if rng.Intn(10) < 3 || p.ll.Len() < cacheSize {
			// If cache is full, evict the oldest item
			if p.ll.Len() >= cacheSize {
				evicted := p.Evict()
				// Remove evicted key from tracking list
				for i, k := range keys {
					if k == evicted[0] {
						keys = append(keys[:i], keys[i+1:]...)
						break
					}
				}
			}

			// Add new key
			newKey := "key" + strconv.Itoa(i)
			p.Add(newKey, 1)
			keys = append(keys, newKey)
		} else if len(keys) > 0 {
			// 70% chance to perform an operation on an existing item

			// Select a random key
			randomKey := keys[rng.Intn(len(keys))]

			// 50% chance for Touch, 50% chance for Remove
			if rng.Intn(2) == 0 {
				p.Touch(randomKey)
			} else {
				p.Remove(randomKey)
				// Remove key from tracking list
				for i, k := range keys {
					if k == randomKey {
						keys = append(keys[:i], keys[i+1:]...)
						break
					}
				}
			}
		}

		// Verify internal state (map and list lengths) are consistent after each operation.
		if p.ll.Len() != len(p.cache) {
			t.Fatalf("inconsistent state: list length (%d) != cache map length (%d)", p.ll.Len(), len(p.cache))
		}
	}

	t.Logf("Churn test completed with final cache size: %d", p.ll.Len())
}

// BenchmarkLRU_Churn measures the overall performance of the LRU policy
// under frequent Add/Remove/Touch operations.
func BenchmarkLRU_Churn(b *testing.B) {
	p := NewLRUPolicy().(*LRUPolicy)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const cacheSize = 1000

	// Pre-populate the cache before the benchmark loop.
	for i := 0; i < cacheSize; i++ {
		p.Add("key"+strconv.Itoa(i), 1)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// 50% chance to touch an existing item.
		if rng.Intn(2) == 0 {
			keyIndex := rng.Intn(cacheSize)
			p.Touch("key" + strconv.Itoa(keyIndex))
		} else {
			// 50% chance to evict an item and add a new one.
			p.Evict()
			newKey := "key" + strconv.Itoa(i+cacheSize)
			p.Add(newKey, 1)
		}
	}
}