// Package policy provides implementations of the daramjwee.EvictionPolicy interface,
// such as a Least Recently Used (LRU) policy.
package policy

import (
	"container/list"

	"github.com/mrchypark/daramjwee"
)

// lruEntry represents an entry in the LRU cache.
// It stores the key and size of the cached item, and is used as the value
// in the linked list elements.
type lruEntry struct {
	key  string // key is the unique identifier of the cached item.
	size int64  // size is the size of the cached item in bytes.
}

// LRUPolicy implements a classic Least Recently Used (LRU) eviction policy
// using a doubly linked list and a map.
// The linked list (`ll`) maintains the order of items by recency of use (front is most recent).
// The map (`cache`) provides quick access to list elements by their keys.
// IMPORTANT: This implementation is NOT thread-safe. External synchronization
// must be handled by the caller (e.g., the Store implementation that uses this policy).
type LRUPolicy struct {
	ll    *list.List // ll is the doubly linked list storing *lruEntry, ordered by recency.
	cache map[string]*list.Element // cache maps item keys to their corresponding *list.Element in ll.
}

// NewLRUPolicy creates and initializes a new LRUPolicy instance.
// It returns an daramjwee.EvictionPolicy interface.
func NewLRUPolicy() daramjwee.EvictionPolicy {
	return &LRUPolicy{
		ll:    list.New(), // Initialize the linked list.
		cache: make(map[string]*list.Element), // Initialize the cache map.
	}
}

// Ensures at compile time that LRUPolicy satisfies the daramjwee.EvictionPolicy interface.
var _ daramjwee.EvictionPolicy = (*LRUPolicy)(nil)

// Touch marks an item associated with the given key as recently used.
// It achieves this by moving the item's corresponding element to the front of the linked list.
// If the key is not found in the cache, this operation is a no-op.
// Parameters:
//   key: The key of the item to touch.
func (p *LRUPolicy) Touch(key string) {
	if elem, ok := p.cache[key]; ok {
		p.ll.MoveToFront(elem)
	}
}

// Add adds a new item with the given key and size to the cache, marking it as
// the most recently used item (i.e., adds it to the front of the linked list).
// If an item with the same key already exists, its size is updated, and it's
// moved to the front (marked as most recently used).
// Parameters:
//   key: The key of the item to add.
//   size: The size of the item in bytes.
func (p *LRUPolicy) Add(key string, size int64) {
	if elem, ok := p.cache[key]; ok {
		// Item already exists, update its size and move to the front.
		p.ll.MoveToFront(elem)
		elem.Value.(*lruEntry).size = size // Update the size of the existing entry.
		return
	}

	// Add new item to the front of the list and to the cache map.
	newEntry := &lruEntry{key: key, size: size}
	newElem := p.ll.PushFront(newEntry)
	p.cache[key] = newElem
}

// Remove explicitly removes an item associated with the given key from the cache.
// If the key is not found, this operation is a no-op.
// Parameters:
//   key: The key of the item to remove.
func (p *LRUPolicy) Remove(key string) {
	if elem, ok := p.cache[key]; ok {
		p.removeElement(elem)
	}
}

// Evict identifies and removes the least recently used item from the cache.
// The LRU item is the one at the back of the linked list.
// Returns:
//   A slice containing the key of the evicted item. If the cache is empty,
//   it returns nil.
func (p *LRUPolicy) Evict() []string {
	elem := p.ll.Back() // Get the least recently used element (at the back).
	if elem == nil {
		return nil // Cache is empty, nothing to evict.
	}

	entry := p.removeElement(elem)
	return []string{entry.key} // Return the key of the evicted item.
}

// removeElement is an internal helper function to remove a given list element
// from both the linked list and the cache map.
// Parameters:
//   e: The *list.Element to remove.
// Returns:
//   A pointer to the *lruEntry that was stored in the removed element.
func (p *LRUPolicy) removeElement(e *list.Element) *lruEntry {
	p.ll.Remove(e) // Remove element from the linked list.
	entry := e.Value.(*lruEntry) // Assert the type of the element's value.
	delete(p.cache, entry.key) // Delete the entry from the cache map.
	return entry
}
