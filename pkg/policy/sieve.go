package policy

import (
	"container/list"

	"github.com/mrchypark/daramjwee"
)

// sieveEntry represents an item in the cache for the Sieve policy.
type sieveEntry struct {
	key     string
	size    int64
	visited bool // Indicates if the item has been recently accessed.
}

// SievePolicy implements the SIEVE cache eviction algorithm.
// This implementation is not thread-safe; external synchronization (e.g., by the Store) is required.
//
// Key ideas:
// - All items are stored in a doubly linked list.
// - A 'hand' pointer scans the list.
// - During eviction, the 'hand' scans from its current position towards the older items.
// - If an item with `visited` flag true is encountered, its `visited` flag is set to false, and it gets another chance.
// - If an item with `visited` flag false is encountered, it is evicted.
// - Accessing an item (`Touch`) sets its `visited` flag to true.
type SievePolicy struct {
	ll    *list.List               // Doubly linked list storing items in order.
	cache map[string]*list.Element // Map for fast lookup of list elements by key.
	hand  *list.Element            // Pointer to the current position in the list for scanning.
}

// NewSievePolicy creates a new SIEVE eviction policy.
func NewSievePolicy() daramjwee.EvictionPolicy {
	return &SievePolicy{
		ll:    list.New(),
		cache: make(map[string]*list.Element),
		hand:  nil,
	}
}

// Add adds a new item to the cache. If the item already exists, its size is updated,
// it's moved to the front of the list, and its visited flag is set to true.
// If it's a new item, it's added to the front with visited set to false.
func (p *SievePolicy) Add(key string, size int64) {
	if elem, ok := p.cache[key]; ok {
		// Item already exists, update and move to front.
		p.ll.MoveToFront(elem)
		entry := elem.Value.(*sieveEntry)
		entry.size = size
		entry.visited = true // Treat as recently accessed on update.
		return
	}

	// Add new item with visited = false.
	entry := &sieveEntry{key: key, size: size, visited: false}
	elem := p.ll.PushFront(entry)
	p.cache[key] = elem
}

// Touch is called when an item is accessed. It sets the item's visited flag to true.
func (p *SievePolicy) Touch(key string) {
	if elem, ok := p.cache[key]; ok {
		elem.Value.(*sieveEntry).visited = true
	}
}

// Remove removes an item from the cache.
func (p *SievePolicy) Remove(key string) {
	if elem, ok := p.cache[key]; ok {
		p.removeElement(elem)
	}
}

// Evict determines which item(s) should be evicted and returns their keys.
// It scans the list starting from the 'hand' pointer, evicting items with
// `visited` flag false, and resetting `visited` to false for items with `visited` true.
func (p *SievePolicy) Evict() []string {
	if p.ll.Len() == 0 {
		return nil
	}

	// Start scanning from the hand position.
	victimElem := p.hand
	if victimElem == nil {
		// If hand is not initialized, start from the back (oldest item).
		victimElem = p.ll.Back()
	}

	// Scan until an item with visited = false is found.
	for {
		entry := victimElem.Value.(*sieveEntry)
		if entry.visited {
			// Give it another chance: set visited to false and move to the next candidate.
			entry.visited = false
			prev := victimElem.Prev()
			if prev == nil {
				// If at the beginning of the list, wrap around to the end.
				victimElem = p.ll.Back()
			} else {
				victimElem = prev
			}
		} else {
			// Found a victim: visited is false.
			break
		}
	}

	// Update hand position for the next Evict call.
	prev := victimElem.Prev()
	if prev == nil {
		p.hand = p.ll.Back()
	} else {
		p.hand = prev
	}

	// Remove the chosen victim.
	victimEntry := p.removeElement(victimElem)
	return []string{victimEntry.key}
}

// removeElement is an internal helper function that removes a given list.Element
// from the list and cache map, and adjusts the hand pointer if necessary.
func (p *SievePolicy) removeElement(e *list.Element) *sieveEntry {
	// If the element to be removed is the one pointed to by hand, move hand to the previous element.
	if e == p.hand {
		prev := e.Prev()
		if prev == nil {
			// If the removed element was the first and hand pointed to it, wrap hand to the end.
			p.hand = p.ll.Back()
		} else {
			p.hand = prev
		}
	}

	p.ll.Remove(e)
	entry := e.Value.(*sieveEntry)
	delete(p.cache, entry.key)
	return entry
}