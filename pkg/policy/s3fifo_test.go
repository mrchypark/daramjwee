package policy

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// helper function to check if a key exists in the policy's internal cache
func isInCache(p *S3FIFOPolicy, key string) bool {
	_, ok := p.cache[key]
	return ok
}

// TestS3FIFO_AddAndPromotion tests basic adding and promotion from small to main queue.
func TestS3FIFO_AddAndPromotion(t *testing.T) {
	p := NewS3FIFOPolicy(100, 0.5).(*S3FIFOPolicy) // 100 bytes total, 50 for small queue

	// 1. Add a new key. It should be in the small queue.
	p.Add("key1", 10)
	if !isInCache(p, "key1") {
		t.Fatal("key1 should be in cache after Add")
	}
	if p.cache["key1"].Value.(*s3fifoEntry).isMain {
		t.Error("key1 should be in small queue, not main, after initial Add")
	}
	if p.smallSize != 10 || p.mainSize != 0 {
		t.Errorf("Expected sizes to be small:10, main:0, but got small:%d, main:%d", p.smallSize, p.mainSize)
	}

	// 2. Touch the key. It should be promoted to the main queue.
	p.Touch("key1")
	if !p.cache["key1"].Value.(*s3fifoEntry).isMain {
		t.Error("key1 should be promoted to main queue after Touch")
	}
	if p.smallSize != 0 || p.mainSize != 10 {
		t.Errorf("Expected sizes to be small:0, main:10, but got small:%d, main:%d", p.smallSize, p.mainSize)
	}
}

// TestS3FIFO_SecondChance tests the "second chance" mechanism in the main queue.
func TestS3FIFO_SecondChance(t *testing.T) {
	p := NewS3FIFOPolicy(100, 0.5).(*S3FIFOPolicy)

	// Add and promote key1 to main queue
	p.Add("key1", 10)
	p.Touch("key1")

	// At this point, wasHit should be false
	if p.cache["key1"].Value.(*s3fifoEntry).wasHit {
		t.Fatal("wasHit should be false after promotion")
	}

	// 2. Touch it again. Now wasHit should be true.
	p.Touch("key1")
	if !p.cache["key1"].Value.(*s3fifoEntry).wasHit {
		t.Error("wasHit should become true after a second touch in main queue")
	}
}

// TestS3FIFO_EvictFromSmallQueue tests eviction from the small queue when it exceeds its capacity.
func TestS3FIFO_EvictFromSmallQueue(t *testing.T) {
	// Small queue capacity is 20 bytes.
	p := NewS3FIFOPolicy(100, 0.2).(*S3FIFOPolicy)

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

// TestS3FIFO_EvictFromMainQueue tests eviction logic from the main queue.
func TestS3FIFO_EvictFromMainQueue(t *testing.T) {
	// Total capacity 100, small capacity 20.
	p := NewS3FIFOPolicy(100, 0.2).(*S3FIFOPolicy)

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

// TestS3FIFO_EvictFromMainWithSecondChance tests that wasHit items are spared from eviction.
func TestS3FIFO_EvictFromMainWithSecondChance(t *testing.T) {
	p := NewS3FIFOPolicy(100, 0.2).(*S3FIFOPolicy)

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
	if p.cache["main1"].Value.(*s3fifoEntry).wasHit {
		t.Error("main1's wasHit flag should be reset to false after being spared")
	}
}

// TestS3FIFO_Remove tests explicit removal of items.
func TestS3FIFO_Remove(t *testing.T) {
	p := NewS3FIFOPolicy(100, 0.5).(*S3FIFOPolicy)
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

// TestS3FIFO_EdgeCases tests various edge cases.
func TestS3FIFO_EdgeCases(t *testing.T) {
	// 1. Evict from an empty policy
	p := NewS3FIFOPolicy(100, 0.5).(*S3FIFOPolicy)
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

// TestS3FIFO_Churn은 잦은 추가/삭제/접근 상황에서 S3-FIFO 정책의
// 내부 상태가 일관성을 유지하는지 검증하는 무작위 부하 테스트입니다.
func TestS3FIFO_Churn(t *testing.T) {
	// S3-FIFO 정책 생성 (총 용량 100, small 큐 비율 20%)
	p := NewS3FIFOPolicy(100, 0.2).(*S3FIFOPolicy)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const cacheSize = 100    // 캐시의 최대 아이템 수 (용량과 무관하게 아이템 개수로 제한)
	const iterations = 10000 // 총 연산 횟수

	keys := make([]string, 0, cacheSize)

	for i := 0; i < iterations; i++ {
		// 30% 확률로 새 아이템 추가
		if rng.Intn(10) < 3 || (p.smallQueue.Len()+p.mainQueue.Len()) < cacheSize {
			// 캐시가 꽉 찼으면, 아이템을 축출
			if (p.smallQueue.Len() + p.mainQueue.Len()) >= cacheSize {
				evicted := p.Evict()
				if evicted != nil {
					// 추적 리스트에서 축출된 키를 제거
					for i, k := range keys {
						if k == evicted[0] {
							keys = append(keys[:i], keys[i+1:]...)
							break
						}
					}
				}
			}

			// 새 키 추가
			newKey := "key" + strconv.Itoa(i)
			p.Add(newKey, 1) // 모든 아이템의 사이즈는 1로 가정
			keys = append(keys, newKey)

		} else if len(keys) > 0 {
			// 70% 확률로 기존 아이템에 대한 연산 수행

			// 무작위로 키를 선택
			randomKey := keys[rng.Intn(len(keys))]

			// 50% 확률로 Touch, 50% 확률로 Remove
			if rng.Intn(2) == 0 {
				p.Touch(randomKey)
			} else {
				p.Remove(randomKey)
				// 추적 리스트에서 키 제거
				for i, k := range keys {
					if k == randomKey {
						keys = append(keys[:i], keys[i+1:]...)
						break
					}
				}
			}
		}

		// ✅ 핵심 검증: 매 연산마다 S3-FIFO의 내부 상태 일관성을 확인합니다.
		// 두 큐의 아이템 개수의 합이 캐시 맵의 전체 아이템 개수와 일치해야 합니다.
		totalItemsInQueues := p.smallQueue.Len() + p.mainQueue.Len()
		if totalItemsInQueues != len(p.cache) {
			t.Fatalf("inconsistent state: total items in queues (%d) != cache map length (%d)", totalItemsInQueues, len(p.cache))
		}
	}

	t.Logf("S3-FIFO Churn test completed with final cache size: %d", len(p.cache))
}

// BenchmarkS3FIFO_Churn은 잦은 추가/삭제/접근 상황에서 S3-FIFO 정책의
// 전반적인 처리 성능을 측정합니다.
func BenchmarkS3FIFO_Churn(b *testing.B) {
	// 벤치마크를 위해 충분한 용량을 가진 정책을 생성합니다.
	// 용량 자체보다는 연산 속도에 초점을 맞춥니다.
	p := NewS3FIFOPolicy(100000, 0.1).(*S3FIFOPolicy)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const cacheSize = 1000

	for i := 0; i < cacheSize; i++ {
		p.Add("key"+strconv.Itoa(i), 1)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if rng.Intn(2) == 0 {
			keyIndex := rng.Intn(cacheSize)
			p.Touch("key" + strconv.Itoa(keyIndex))
		} else {
			p.Evict()
			newKey := "key" + strconv.Itoa(i+cacheSize)
			p.Add(newKey, 1)
		}
	}
}
