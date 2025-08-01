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

// LRU implements a classic Least Recently Used eviction policy.
// This implementation is NOT thread-safe. Synchronization must be handled
// by the caller (e.g., the Store implementation).
type LRU struct {
	ll    *list.List
	cache map[string]*list.Element
}

// NewLRU creates a new LRU policy.
func NewLRU() daramjwee.EvictionPolicy {
	return &LRU{
		ll:    list.New(),
		cache: make(map[string]*list.Element),
	}
}

// Touch moves an item to the front of the list, marking it as recently used.
func (p *LRU) Touch(key string) {
	if elem, ok := p.cache[key]; ok {
		p.ll.MoveToFront(elem)
	}
}

// Add adds a new item to the front of the list. If the item already
// exists, it's moved to the front and its size is updated.
func (p *LRU) Add(key string, size int64) {
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
func (p *LRU) Remove(key string) {
	if elem, ok := p.cache[key]; ok {
		p.removeElement(elem)
	}
}

// Evict removes and returns the least recently used item (from the back of the list).
// It returns nil if the cache is empty.
func (p *LRU) Evict() []string {
	elem := p.ll.Back()
	if elem == nil {
		return nil
	}

	entry := p.removeElement(elem)
	return []string{entry.key}
}

// removeElement is an internal helper to remove a list element and its
// corresponding map entry.
func (p *LRU) removeElement(e *list.Element) *lruEntry {
	p.ll.Remove(e)
	entry := e.Value.(*lruEntry)
	delete(p.cache, entry.key)
	return entry
}
