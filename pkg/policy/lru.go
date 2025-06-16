// Package policy provides implementations of the daramjwee.EvictionPolicy interface.
// NOTE: golangci-lint에서 "could not import container/list (unsupported version: 2)" 와 같은
// typecheck 오류가 발생할 수 있습니다. 이는 코드 자체의 문제라기보다는
// Go 환경 또는 golangci-lint 버전과의 호환성 문제일 가능성이 있습니다.
// 관련 환경 점검 및 golangci-lint 설정 확인이 필요할 수 있습니다.
package policy

import (
	"container/list"

	"github.com/mrchypark/daramjwee"
)

// lruEntry is the type of value stored in the linked list.
type lruEntry struct {
	key  string
	size int64
}

// LRUPolicy implements a classic Least Recently Used eviction policy.
// This implementation is NOT thread-safe. Synchronization must be handled
// by the caller (e.g., the Store implementation).
type LRUPolicy struct {
	ll    *list.List
	cache map[string]*list.Element
}

// NewLRUPolicy creates a new LRU policy.
func NewLRUPolicy() daramjwee.EvictionPolicy {
	return &LRUPolicy{
		ll:    list.New(),
		cache: make(map[string]*list.Element),
	}
}

// 컴파일 타임에 LRUPolicy가 EvictionPolicy 인터페이스를 만족하는지 확인합니다.
var _ daramjwee.EvictionPolicy = (*LRUPolicy)(nil)

// Touch moves an item to the front of the list, marking it as recently used.
func (p *LRUPolicy) Touch(key string) {
	if elem, ok := p.cache[key]; ok {
		p.ll.MoveToFront(elem)
	}
}

// Add adds a new item to the front of the list. If the item already
// exists, it's moved to the front.
func (p *LRUPolicy) Add(key string, size int64) {
	if elem, ok := p.cache[key]; ok {
		// Item already exists, update its size and move to front.
		p.ll.MoveToFront(elem)
		elem.Value.(*lruEntry).size = size
		return
	}

	// Add new item.
	newElem := p.ll.PushFront(&lruEntry{key: key, size: size})
	p.cache[key] = newElem
}

// Remove removes an item from the cache.
func (p *LRUPolicy) Remove(key string) {
	if elem, ok := p.cache[key]; ok {
		p.removeElement(elem)
	}
}

// Evict removes and returns the least recently used item (from the back of the list).
func (p *LRUPolicy) Evict() []string {
	elem := p.ll.Back()
	if elem == nil {
		return nil
	}

	entry := p.removeElement(elem)
	return []string{entry.key}
}

// removeElement is an internal helper to remove a list element and its
// corresponding map entry.
func (p *LRUPolicy) removeElement(e *list.Element) *lruEntry {
	p.ll.Remove(e)
	entry := e.Value.(*lruEntry)
	delete(p.cache, entry.key)
	return entry
}
