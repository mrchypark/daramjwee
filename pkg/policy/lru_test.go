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

	p.Add("key1", 10) // 1. key1 추가
	p.Add("key2", 20) // 2. key2 추가 (이제 key2가 가장 최신)
	p.Add("key3", 30) // 3. key3 추가 (이제 key3가 가장 최신)

	// 현재 상태 (최신 -> 오래된 순): key3, key2, key1
	// Evict는 가장 오래된 아이템(key1)을 제거해야 함
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key1" {
		t.Fatalf("Expected to evict key1, but got %v", evictedKeys)
	}
	if _, ok := p.cache["key1"]; ok {
		t.Fatal("key1 should not be in cache after eviction")
	}

	// 다음으로 가장 오래된 아이템(key2)을 제거해야 함
	evictedKeys = p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key2" {
		t.Fatalf("Expected to evict key2, but got %v", evictedKeys)
	}

	// 마지막 남은 아이템(key3)을 제거해야 함
	evictedKeys = p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key3" {
		t.Fatalf("Expected to evict key3, but got %v", evictedKeys)
	}

	// 캐시가 비었는지 확인
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

	// 현재 상태 (최신 -> 오래된 순): key3, key2, key1
	// 여기서 key1을 Touch하면, key1이 가장 최신이 되어야 함
	p.Touch("key1")

	// Evict를 호출하면 가장 오래된 key2가 제거되어야 함
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

	// 중간에 있는 key2를 제거
	p.Remove("key2")
	if _, ok := p.cache["key2"]; ok {
		t.Fatal("key2 should not be in cache after Remove")
	}
	if p.ll.Len() != 2 {
		t.Fatalf("List length should be 2 after removing one item, but got %d", p.ll.Len())
	}

	// Evict하면 가장 오래된 key1이 제거되어야 함
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
	// key1의 사이즈를 업데이트하면서 다시 Add. 이것은 Touch와 같은 효과를 내야 함
	p.Add("key1", 100)

	// key1이 가장 최신이 되었으므로, Evict는 key2를 제거해야 함
	evictedKeys := p.Evict()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key2" {
		t.Fatalf("Expected key2 to be evicted, but got %v", evictedKeys)
	}

	// key1의 사이즈가 올바르게 업데이트되었는지 확인
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

	// 1. 비어있는 캐시에서 Evict 호출 (패닉이 발생하면 안 됨)
	evictedKeys := p.Evict()
	if evictedKeys != nil {
		t.Errorf("Evict on an empty policy should return nil, but got %v", evictedKeys)
	}

	// 2. 존재하지 않는 키에 대한 Touch와 Remove 호출 (패닉이 발생하면 안 됨)
	p.Touch("non-existent")
	p.Remove("non-existent")

	// 3. 아이템을 추가했다가 바로 제거한 후 Evict
	p.Add("key1", 10)
	p.Remove("key1")
	evictedKeys = p.Evict()
	if evictedKeys != nil {
		t.Errorf("Evict after removing the only item should return nil, but got %v", evictedKeys)
	}
}

// TestLRU_Churn은 잦은 추가/삭제/접근 상황에서 LRU 정책의
// 내부 상태가 일관성을 유지하는지 검증하는 무작위 부하 테스트입니다.
func TestLRU_Churn(t *testing.T) {
	p := NewLRUPolicy().(*LRUPolicy)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const cacheSize = 100    // 캐시의 최대 아이템 수
	const iterations = 10000 // 총 연산 횟수

	keys := make([]string, 0, cacheSize)

	for i := 0; i < iterations; i++ {
		// 30% 확률로 새 아이템 추가
		if rng.Intn(10) < 3 || p.ll.Len() < cacheSize {
			// 캐시가 꽉 찼으면, 가장 오래된 아이템을 축출
			if p.ll.Len() >= cacheSize {
				evicted := p.Evict()
				// Evict는 축출된 키를 반환하며, 이 키를 추적 리스트에서 제거
				for i, k := range keys {
					if k == evicted[0] {
						keys = append(keys[:i], keys[i+1:]...)
						break
					}
				}
			}

			// 새 키 추가
			newKey := "key" + strconv.Itoa(i)
			p.Add(newKey, 1)
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

		// 매 연산마다 캐시의 내부 상태(맵과 리스트의 길이)가 일치하는지 검증
		if p.ll.Len() != len(p.cache) {
			t.Fatalf("inconsistent state: list length (%d) != cache map length (%d)", p.ll.Len(), len(p.cache))
		}
	}

	t.Logf("Churn test completed with final cache size: %d", p.ll.Len())
}

// BenchmarkLRU_Churn은 잦은 추가/삭제/접근 상황에서 LRU 정책의
// 내부 상태가 일관성을 유지하는지 검증하고, 전반적인 처리 성능을 측정합니다.
func BenchmarkLRU_Churn(b *testing.B) {
	// 벤치마크 실행 전 초기 설정을 합니다.
	p := NewLRUPolicy().(*LRUPolicy)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const cacheSize = 1000 // 실제 부하와 유사하도록 캐시 크기를 늘립니다.

	// 벤치마크 루프 전에 캐시를 미리 채워둡니다.
	for i := 0; i < cacheSize; i++ {
		p.Add("key"+strconv.Itoa(i), 1)
	}

	// 실제 측정을 시작하기 전에 타이머를 리셋합니다.
	b.ResetTimer()

	// b.N번 반복하며 성능을 측정합니다.
	for i := 0; i < b.N; i++ {
		// 50% 확률로 기존 아이템 접근 (Touch)
		if rng.Intn(2) == 0 {
			keyIndex := rng.Intn(cacheSize)
			p.Touch("key" + strconv.Itoa(keyIndex))
		} else {
			// 50% 확률로 아이템 교체 (Evict + Add)
			p.Evict()
			newKey := "key" + strconv.Itoa(i+cacheSize)
			p.Add(newKey, 1)
		}
	}
}
