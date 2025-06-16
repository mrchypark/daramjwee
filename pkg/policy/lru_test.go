package policy

import (
	"testing"
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
