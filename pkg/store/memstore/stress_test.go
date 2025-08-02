package memstore

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemStore_EvictionStress verifies that MemStore's memory usage remains stable
// under continuous write and eviction scenarios.
func TestMemStore_EvictionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	ctx := context.Background()
	// Set a small capacity (1MB) to ensure frequent evictions.
	capacity := int64(1 * 1024 * 1024)
	p := policy.NewLRU()
	store := New(capacity, p)

	// Measure memory stats before the test.
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	// Add 10,000 objects to trigger continuous evictions.
	iterations := 10000
	// Each object has a random size between 1KB and 10KB.
	dataChunk := make([]byte, 10*1024)
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("stress-key-%d", i)
		size := 1024 + rand.Intn(9*1024) // 1KB ~ 10KB

		writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)

		_, err = writer.Write(dataChunk[:size])
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)
	}

	// Verify that the store's current size does not exceed its capacity after the stress test.
	assert.LessOrEqual(t, store.currentSize, store.capacity, "Current size should not exceed capacity after stress test")

	// Force garbage collection to clean up unused memory.
	runtime.GC()

	// Measure and log memory stats after the test.
	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	t.Logf("Memory Stats before test: Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB",
		bToMb(mBefore.Alloc), bToMb(mBefore.TotalAlloc), bToMb(mBefore.Sys))
	t.Logf("Memory Stats after test : Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB",
		bToMb(mAfter.Alloc), bToMb(mAfter.TotalAlloc), bToMb(mAfter.Sys))
	t.Logf("Allocated memory growth: %v MiB", bToMb(mAfter.TotalAlloc-mBefore.TotalAlloc))

	// The primary goal of this test is to ensure that memory usage does not grow indefinitely
	// in proportion to the number of iterations. Specific thresholds can be set in CI environments.
}

// bToMb converts bytes to megabytes.
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
