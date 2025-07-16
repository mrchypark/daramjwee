//go:build race

package daramjwee

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestCache_Chaos_RaceCondition is a chaotic test designed to be run with the -race flag.
// It concurrently performs random Get, Set, and Delete operations to uncover
// potential data races under high contention.
func TestCache_Chaos_RaceCondition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// 1. Create cache for testing
	// Remove defer cache.Close() from here.
	cache, hot, _ := setupCache(t)

	ctx := context.Background()
	const numKeys = 50
	var keys []string
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("chaos-key-%d", i)
		keys = append(keys, key)
		hot.setData(key, "initial-data", &Metadata{})
	}

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			// Use a separate seeded random number generator for each goroutine.
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for j := 0; j < 100; j++ {
				key := keys[r.Intn(len(keys))]
				op := r.Intn(3)

				switch op {
				case 0: // Get
					fetcher := &mockFetcher{content: "fetched-data", etag: "v-fetch"}
					stream, err := cache.Get(ctx, key, fetcher)
					if err == nil && stream != nil {
						_, _ = io.Copy(io.Discard, stream)
						stream.Close()
					}
				case 1: // Set
					writer, err := cache.Set(ctx, key, &Metadata{ETag: "v-set"})
					if err == nil && writer != nil {
						writer.Write([]byte("set-data"))
						writer.Close()
					}
				case 2: // Delete
					cache.Delete(ctx, key)
				}
			}
		}(i)
	}

	// 2. Explicitly wait for all 'chaos' goroutines to complete their work.
	wg.Wait()

	// 3. Close the cache after 'chaos' is completely finished.
	// This prevents circular wait problems.
	cache.Close()

	// The main purpose of the test is race condition detection, so final state verification is omitted.
}
