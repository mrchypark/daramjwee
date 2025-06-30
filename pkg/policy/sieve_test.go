package policy

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// getEntry safely extracts a sieveEntry from a list.Element for testing purposes.
func getEntry(p *SievePolicy, key string) *sieveEntry {
	if elem, ok := p.cache[key]; ok {
		return elem.Value.(*sieveEntry)
	}
	return nil
}

// TestSievePolicy_BasicAddAndTouch tests the basic Add and Touch operations.
func TestSievePolicy_BasicAddAndTouch(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)

	// Add a new item
	p.Add("A", 10)
	assert.Equal(t, 1, p.ll.Len(), "List length should be 1 after adding one item.")
	assert.Equal(t, 1, len(p.cache), "Cache map length should be 1 after adding one item.")

	entryA := getEntry(p, "A")
	assert.NotNil(t, entryA, "Should find 'A' in the cache.")
	assert.False(t, entryA.visited, "Visited flag of a newly added item should be false.")

	// Touch the item
	p.Touch("A")
	assert.True(t, entryA.visited, "Visited flag should be true after Touch.")

	// Touching a non-existent item should not panic.
	assert.NotPanics(t, func() { p.Touch("B") })
}

// TestSievePolicy_AddExisting tests adding an already existing item.
func TestSievePolicy_AddExisting(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 10)
	p.Add("B", 20)

	// Re-add existing item "A" with a different size
	p.Add("A", 100)
	assert.Equal(t, 2, p.ll.Len(), "Total item count should remain the same.")

	// Verify "A" moved to the front of the list
	assert.Equal(t, "A", p.ll.Front().Value.(*sieveEntry).key, "'A' should move to the front of the list.")

	// Verify "A"'s information is updated correctly
	entryA := getEntry(p, "A")
	assert.Equal(t, int64(100), entryA.size, "Size should be updated.")
	assert.True(t, entryA.visited, "Visited flag should be true when re-adding an existing item.")
}

// TestSievePolicy_Remove tests item removal.
func TestSievePolicy_Remove(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 10)
	p.Add("B", 20)
	p.Add("C", 30)

	// Remove middle item "B"
	p.Remove("B")
	assert.Equal(t, 2, p.ll.Len(), "List length should decrease after item removal.")
	assert.Nil(t, getEntry(p, "B"), "'B' should be removed from the cache.")
	assert.NotNil(t, getEntry(p, "A"), "'A' should still exist.")
	assert.NotNil(t, getEntry(p, "C"), "'C' should still exist.")

	// Removing a non-existent item should not panic.
	assert.NotPanics(t, func() { p.Remove("D") })
	assert.Equal(t, 2, p.ll.Len(), "Removing a non-existent item should have no effect.")
}

// TestSievePolicy_Evict_MainScenario tests the core scenario of the SIEVE algorithm.
// Popular objects should survive, while one-hit wonders should be evicted.
func TestSievePolicy_Evict_MainScenario(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	oneHitWonders := []string{"1", "2", "3", "4", "5"}
	popularObjects := []string{"6", "7", "8", "9", "10"}

	// Add all objects
	for _, v := range oneHitWonders {
		p.Add(v, 1)
	}
	for _, v := range popularObjects {
		p.Add(v, 1)
	}

	// Touch only popular objects
	for _, v := range popularObjects {
		p.Touch(v)
	}

	// Evict and verify (one-hit wonders should be evicted in order)
	for i := 0; i < len(oneHitWonders); i++ {
		evicted := p.Evict()
		expectedVictim := oneHitWonders[i]
		assert.Equal(t, []string{expectedVictim}, evicted, fmt.Sprintf("Evicted item #%d should be '%s'.", i+1, expectedVictim))
	}

	// Popular objects should still remain in the cache
	for _, v := range popularObjects {
		assert.NotNil(t, getEntry(p, v), fmt.Sprintf("Popular object '%s' should not be evicted.", v))
	}
}

// TestSievePolicy_Evict_FullRotation verifies that when all items are visited,
// the hand scans the entire list and evicts the first scanned item.
func TestSievePolicy_Evict_FullRotation(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 1)
	p.Add("B", 1)
	p.Add("C", 1)

	// Touch all items
	p.Touch("A")
	p.Touch("B")
	p.Touch("C")

	// Call Evict
	// 1. Hand points to A. A.visited = true -> false. Hand moves to B.
	// 2. Hand points to B. B.visited = true -> false. Hand moves to C.
	// 3. Hand points to C. C.visited = true -> false. Hand wraps around to A.
	// 4. Hand points to A again. A.visited is now false, so A is evicted.
	evicted := p.Evict()
	assert.Equal(t, []string{"A"}, evicted, "When all are visited, the oldest item ('A') should be evicted.")
}

// TestSievePolicy_EvictEmpty verifies that Evict returns nil for an empty policy.
func TestSievePolicy_EvictEmpty(t *testing.T) {
	p := NewSievePolicy()
	assert.Nil(t, p.Evict(), "Evict() on an empty cache should return nil.")
}

// TestSievePolicy_EvictSingleItem verifies that Evict works correctly with a single item.
func TestSievePolicy_EvictSingleItem(t *testing.T) {
	p := NewSievePolicy()
	p.Add("A", 10)

	evicted := p.Evict()
	assert.Equal(t, []string{"A"}, evicted, "When there is only one item, that item should be evicted.")
	assert.Equal(t, 0, p.(*SievePolicy).ll.Len(), "Cache should be empty after eviction.")
}

// TestSievePolicy_RemoveHandledElement verifies that the hand pointer is safely adjusted
// when the item it points to is removed.
func TestSievePolicy_RemoveHandledElement(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 1)
	p.Add("B", 1)
	p.Add("C", 1)

	// Call Evict once to set the hand (A is evicted, hand points to B)
	p.Evict()
	assert.Equal(t, "B", p.hand.Value.(*sieveEntry).key, "After first Evict, hand should point to 'B'.")

	// Remove 'B' which the hand is pointing to
	p.Remove("B")

	// Verify that the hand moved to the previous element 'C'
	assert.Equal(t, "C", p.hand.Value.(*sieveEntry).key, "If the element pointed to by hand is deleted, hand should point to the previous element.")
}

// TestSievePolicy_Churn verifies that the internal state remains consistent
// during frequent Add/Remove operations.
func TestSievePolicy_Churn(t *testing.T) {
	p := NewSievePolicy()

	// Fill the cache
	for i := 0; i < 10; i++ {
		p.Add(strconv.Itoa(i), 1)
	}

	// Frequent Add and Evict operations
	for i := 10; i < 1000; i++ {
		// Assume Evict is called to make space
		evicted := p.Evict()
		assert.NotNil(t, evicted, "Should always have an eviction candidate during churn test.")

		// Add a new item
		p.Add(strconv.Itoa(i), 1)

		// Verify internal state (no panics, list length remains constant)
		assert.Equal(t, 10, p.(*SievePolicy).ll.Len(), "List length should remain constant during churn test.")
	}
}

// TestSievePolicy_Churn_Randomized is a randomized load test that verifies
// the internal state consistency of the SIEVE policy during frequent Add/Remove/Touch operations.
func TestSievePolicy_Churn_Randomized(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
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
				if evicted != nil {
					// Remove evicted key from tracking list
					for i, k := range keys {
						if k == evicted[0] {
							keys = append(keys[:i], keys[i+1:]...)
							break
						}
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

		// Critical verification: ensure internal state (map and list lengths) are consistent after each operation
		if p.ll.Len() != len(p.cache) {
			t.Fatalf("inconsistent state: list length (%d) != cache map length (%d)", p.ll.Len(), len(p.cache))
		}
	}

	t.Logf("SIEVE Churn test completed with final cache size: %d", p.ll.Len())
}

// BenchmarkSieve_Churn measures the overall performance of the SIEVE policy
// under frequent Add/Remove/Touch operations.
func BenchmarkSieve_Churn(b *testing.B) {
	p := NewSievePolicy().(*SievePolicy)
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