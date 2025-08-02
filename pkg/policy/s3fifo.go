package policy

import (
	"container/list"

	"github.com/mrchypark/daramjwee"
)

// s3fifoEntry represents an item in the cache for the S3-FIFO policy.
type s3fifoEntry struct {
	key    string
	size   int64
	isMain bool // Indicates if the item is in the main queue or the small queue.
	wasHit bool // Indicates if the item has been accessed while in the main queue (given a second chance).
}

// S3FIFO implements the S3-FIFO (Second-Chance FIFO) eviction policy.
// This implementation is not thread-safe; external synchronization (e.g., by the Store) is required.
//
// Key ideas:
//   - Items are initially added to a 'small' queue.
//   - Items accessed in the small queue are promoted to a 'main' queue.
//   - Items in the main queue get a 'second chance': if accessed, their `wasHit` flag is set.
//   - Eviction prioritizes the small queue if it exceeds its capacity.
//   - Otherwise, eviction scans the main queue, evicting items with `wasHit` false,
//     and resetting `wasHit` to false for items with `wasHit` true.
type S3FIFO struct {
	smallQueue *list.List               // Small queue for newly added items.
	mainQueue  *list.List               // Main queue for frequently accessed items.
	cache      map[string]*list.Element // Map for fast lookup of list elements by key.

	smallSize int64 // Current size of the small queue in bytes.
	mainSize  int64 // Current size of the main queue in bytes.

	smallCapacity int64 // Maximum capacity of the small queue in bytes.
}

// NewS3FIFO creates a new S3-FIFO policy.
// totalCapacity is the total capacity of the cache in bytes.
// smallRatio is the proportion of the total capacity allocated to the small queue (e.g., 10 for 10%).
func NewS3FIFO(totalCapacity int64, smallRatio int) daramjwee.EvictionPolicy {
	if smallRatio <= 0 || smallRatio >= 100 {
		smallRatio = 10 // Default to 10% if invalid ratio is provided.
	}
	smallCap := totalCapacity * int64(smallRatio) / 100
	return &S3FIFO{
		smallQueue:    list.New(),
		mainQueue:     list.New(),
		cache:         make(map[string]*list.Element),
		smallCapacity: smallCap,
	}
}

// Add adds a new item to the cache. Items are always initially added to the smallQueue.
// If the item already exists, its size is updated.
func (p *S3FIFO) Add(key string, size int64) {
	if elem, ok := p.cache[key]; ok {
		// Item already exists, update its size.
		entry := elem.Value.(*s3fifoEntry)
		if entry.isMain {
			p.mainSize -= entry.size
			p.mainSize += size
		} else {
			p.smallSize -= entry.size
			p.smallSize += size
		}
		entry.size = size
		return
	}

	// Add new item to the front of the small queue.
	entry := &s3fifoEntry{key: key, size: size, isMain: false, wasHit: false}
	elem := p.smallQueue.PushFront(entry)
	p.cache[key] = elem
	p.smallSize += size
}

// Touch is called when an item is accessed.
// If the item is in the smallQueue, it is promoted to the mainQueue.
// If the item is already in the mainQueue, its 'wasHit' flag is set to true.
func (p *S3FIFO) Touch(key string) {
	elem, ok := p.cache[key]
	if !ok {
		return
	}

	entry := elem.Value.(*s3fifoEntry)

	if !entry.isMain {
		// Promote from Small to Main queue.
		p.smallQueue.Remove(elem)
		p.smallSize -= entry.size

		entry.isMain = true
		newElem := p.mainQueue.PushFront(entry)
		p.cache[key] = newElem
		p.mainSize += entry.size
	} else {
		// Item already in Main queue, set hit flag.
		entry.wasHit = true
	}
}

// Remove removes an item from the cache.
func (p *S3FIFO) Remove(key string) {
	if elem, ok := p.cache[key]; ok {
		p.removeElement(elem)
	}
}

// Evict determines which item(s) should be evicted and returns their keys.
// It prioritizes eviction from the small queue if it exceeds its capacity.
// Otherwise, it scans the main queue, applying the second-chance mechanism.
func (p *S3FIFO) Evict() []string {
	// 1. If the small queue exceeds its capacity, evict the oldest item from it.
	if p.smallSize > p.smallCapacity {
		elem := p.smallQueue.Back()
		if elem != nil {
			entry := p.removeElement(elem)
			return []string{entry.key}
		}
	}

	// 2. Scan the main queue for eviction candidates.
	if p.mainQueue.Len() > 0 {
		// Get the oldest element in the main queue.
		elem := p.mainQueue.Back()
		for elem != nil {
			// Store the previous element before potentially modifying `elem`.
			prevElem := elem.Prev()

			entry := elem.Value.(*s3fifoEntry)
			if entry.wasHit {
				// Give it a second chance: reset hit flag and move to front of main queue.
				entry.wasHit = false
				p.mainQueue.MoveToFront(elem)
			} else {
				// Evict candidate found.
				victim := p.removeElement(elem)
				return []string{victim.key}
			}
			// Move to the next element in the scan.
			elem = prevElem
		}
		// If the loop finishes, all items in the main queue were given a second chance.
	}

	// 3. If the main queue is empty, or if the small queue still exceeds capacity
	//    (after trying to evict from main), evict from the small queue again.
	if p.mainQueue.Len() == 0 || p.smallSize > p.smallCapacity {
		if p.smallQueue.Len() > 0 {
			elem := p.smallQueue.Back()
			entry := p.removeElement(elem)
			return []string{entry.key}
		}
	}

	return nil
}

// removeElement is an internal helper function that removes a given list.Element
// from its respective queue and the cache map, and updates queue sizes.
func (p *S3FIFO) removeElement(e *list.Element) *s3fifoEntry {
	entry := e.Value.(*s3fifoEntry)
	if entry.isMain {
		p.mainQueue.Remove(e)
		p.mainSize -= entry.size
	} else {
		p.smallQueue.Remove(e)
		p.smallSize -= entry.size
	}
	delete(p.cache, entry.key)
	return entry
}
