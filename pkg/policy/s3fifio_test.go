package policy

import (
	"testing"
)

// helper function to check if a key exists in the policy's internal cache
func isInCache(p *S2FIFOPolicy, key string) bool {
	_, ok := p.cache[key]
	return ok
}

// TestS2FIFO_AddAndPromotion tests basic adding and promotion from small to main queue.
func TestS2FIFO_AddAndPromotion(t *testing.T) {
	p := NewS2FIFOPolicy(100, 0.5).(*S2FIFOPolicy) // 100 bytes total, 50 for small queue

	// 1. Add a new key. It should be in the small queue.
	p.Add("key1", 10)
	if !isInCache(p, "key1") {
		t.Fatal("key1 should be in cache after Add")
	}
	if p.cache["key1"].Value.(*s2fifoEntry).isMain {
		t.Error("key1 should be in small queue, not main, after initial Add")
	}
	if p.smallSize != 10 || p.mainSize != 0 {
		t.Errorf("Expected sizes to be small:10, main:0, but got small:%d, main:%d", p.smallSize, p.mainSize)
	}

	// 2. Touch the key. It should be promoted to the main queue.
	p.Touch("key1")
	if !p.cache["key1"].Value.(*s2fifoEntry).isMain {
		t.Error("key1 should be promoted to main queue after Touch")
	}
	if p.smallSize != 0 || p.mainSize != 10 {
		t.Errorf("Expected sizes to be small:0, main:10, but got small:%d, main:%d", p.smallSize, p.mainSize)
	}
}

// TestS2FIFO_SecondChance tests the "second chance" mechanism in the main queue.
func TestS2FIFO_SecondChance(t *testing.T) {
	p := NewS2FIFOPolicy(100, 0.5).(*S2FIFOPolicy)

	// Add and promote key1 to main queue
	p.Add("key1", 10)
	p.Touch("key1")

	// At this point, wasHit should be false
	if p.cache["key1"].Value.(*s2fifoEntry).wasHit {
		t.Fatal("wasHit should be false after promotion")
	}

	// 2. Touch it again. Now wasHit should be true.
	p.Touch("key1")
	if !p.cache["key1"].Value.(*s2fifoEntry).wasHit {
		t.Error("wasHit should become true after a second touch in main queue")
	}
}

// TestS2FIFO_EvictFromSmallQueue tests eviction from the small queue when it exceeds its capacity.
func TestS2FIFO_EvictFromSmallQueue(t *testing.T) {
	// Small queue capacity is 20 bytes.
	p := NewS2FIFOPolicy(100, 0.2).(*S2FIFOPolicy)

	p.Add("key1", 10) // Oldest
	p.Add("key2", 10)
	p.Add("key3", 10) // Newest. Now smallSize is 30, which is > 20.

	// Evict should target the oldest item in the small queue ("key1").
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key1" {
		t.Fatalf("Expected to evict key1, but got %v", evictedKeys)
	}

	if isInCache(p, "key1") {
		t.Error("key1 should not be in cache after eviction")
	}
	if p.smallSize != 20 {
		t.Errorf("Expected smallSize to be 20 after eviction, but got %d", p.smallSize)
	}
}

// TestS2FIFO_EvictFromMainQueue tests eviction logic from the main queue.
func TestS2FIFO_EvictFromMainQueue(t *testing.T) {
	// Total capacity 100, small capacity 20.
	p := NewS2FIFOPolicy(100, 0.2).(*S2FIFOPolicy)

	// 1. Add items and promote them to main queue
	p.Add("main1", 30) // Oldest in main
	p.Touch("main1")
	p.Add("main2", 30)
	p.Touch("main2")

	// 2. Add an item to small queue, but keep smallSize within its capacity.
	p.Add("small1", 10) // Now smallSize (10) <= smallCapacity (20).

	// 3. Eviction should now target the main queue.
	// "main1" is the oldest and its wasHit is false. It should be evicted.
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "main1" {
		t.Fatalf("Expected to evict main1, but got %v", evictedKeys)
	}
	if isInCache(p, "main1") {
		t.Error("main1 should not be in cache after eviction")
	}
	if p.mainSize != 30 {
		t.Errorf("Expected mainSize to be 30 after eviction, but got %d", p.mainSize)
	}
}

// TestS2FIFO_EvictFromMainWithSecondChance tests that wasHit items are spared from eviction.
func TestS2FIFO_EvictFromMainWithSecondChance(t *testing.T) {
	p := NewS2FIFOPolicy(100, 0.2).(*S2FIFOPolicy)

	// 1. Add items and promote them.
	p.Add("main1", 30) // Oldest
	p.Touch("main1")
	p.Add("main2", 30) // Newest
	p.Touch("main2")

	// 2. Give "main1" a second chance.
	p.Touch("main1") // This sets main1.wasHit = true

	// 3. Add an item to small queue to trigger main queue eviction check.
	p.Add("small1", 10)

	// 4. Call Evict.
	// It should check "main1", see wasHit=true, set it to false, and spare it.
	// Then it should check "main2", see wasHit=false, and evict it.
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "main2" {
		t.Fatalf("Expected to evict main2, but got %v", evictedKeys)
	}

	// 5. Verify state after eviction.
	if !isInCache(p, "main1") {
		t.Error("main1 should still be in cache")
	}
	if p.cache["main1"].Value.(*s2fifoEntry).wasHit {
		t.Error("main1's wasHit flag should be reset to false after being spared")
	}
}

// TestS2FIFO_Remove tests explicit removal of items.
func TestS2FIFO_Remove(t *testing.T) {
	p := NewS2FIFOPolicy(100, 0.5).(*S2FIFOPolicy)
	p.Add("key1", 10) // In small
	p.Add("key2", 10) // In small, will be promoted
	p.Touch("key2")   // Now in main

	// Remove from small queue
	p.Remove("key1")
	if isInCache(p, "key1") {
		t.Error("key1 should be removed")
	}
	if p.smallSize != 0 {
		t.Errorf("smallSize should be 0 after removing key1, got %d", p.smallSize)
	}

	// Remove from main queue
	p.Remove("key2")
	if isInCache(p, "key2") {
		t.Error("key2 should be removed")
	}
	if p.mainSize != 0 {
		t.Errorf("mainSize should be 0 after removing key2, got %d", p.mainSize)
	}

	// Remove a non-existent key (should not panic)
	p.Remove("non-existent-key")
}

// TestS2FIFO_EdgeCases tests various edge cases.
func TestS2FIFO_EdgeCases(t *testing.T) {
	// 1. Evict from an empty policy
	p := NewS2FIFOPolicy(100, 0.5).(*S2FIFOPolicy)
	evicted := p.Evict()
	if evicted != nil {
		t.Errorf("Evict on empty policy should return nil, got %v", evicted)
	}

	// 2. Update an existing item's size
	p.Add("key1", 10)
	p.Add("key1", 50) // Update size
	if p.smallSize != 50 {
		t.Errorf("Expected smallSize to be 50 after update, got %d", p.smallSize)
	}
	p.Touch("key1")
	p.Add("key1", 20) // Update size while in main
	if p.mainSize != 20 {
		t.Errorf("Expected mainSize to be 20 after update, got %d", p.mainSize)
	}
}
