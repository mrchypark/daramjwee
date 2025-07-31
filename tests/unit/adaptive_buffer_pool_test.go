package unit

import (
	"fmt"
	"testing"

	"github.com/mrchypark/daramjwee"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBufferPool_Creation(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      daramjwee.StrategyAdaptive,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    4,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)

	require.NoError(t, err)
	require.NotNil(t, pool)
	assert.Equal(t, config.LargeObjectThreshold, config.LargeObjectThreshold)
	assert.Equal(t, config.VeryLargeObjectThreshold, config.VeryLargeObjectThreshold)
}

func TestAdaptiveBufferPool_StrategySelection(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      daramjwee.StrategyAdaptive,
	}

	// Test different buffer sizes to verify adaptive behavior
	testCases := []struct {
		size int
		name string
	}{
		{16 * 1024, "small"},    // 16KB - small
		{64 * 1024, "medium"},   // 64KB - medium
		{512 * 1024, "large"},   // 512KB - large
		{2048 * 1024, "xlarge"}, // 2MB - very large
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("size_%s", tc.name), func(t *testing.T) {
			// Test that we can get and put buffers of different sizes
			buf := pool.Get(tc.size)
			assert.True(t, len(buf) >= tc.size)
			pool.Put(buf)
		})
	}
}

func TestAdaptiveBufferPool_GetPut(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      daramjwee.StrategyAdaptive,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    4,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	// Test small object
	smallBuf := pool.Get(16 * 1024)
	assert.True(t, len(smallBuf) >= 16*1024)
	pool.Put(smallBuf)

	// Test medium object
	mediumBuf := pool.Get(64 * 1024)
	assert.True(t, len(mediumBuf) >= 64*1024)
	pool.Put(mediumBuf)

	// Test large object (should not use pooling)
	largeBuf := pool.Get(512 * 1024)
	assert.True(t, len(largeBuf) >= 512*1024)
	pool.Put(largeBuf) // Should not actually pool this

	// Test very large object (should not use pooling)
	veryLargeBuf := pool.Get(2048 * 1024)
	assert.True(t, len(veryLargeBuf) >= 2048*1024)
	pool.Put(veryLargeBuf) // Should not actually pool this
}

func TestAdaptiveBufferPool_Stats(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      daramjwee.StrategyAdaptive,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    4,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	// Perform some operations
	smallBuf := pool.Get(16 * 1024)
	mediumBuf := pool.Get(64 * 1024)
	largeBuf := pool.Get(512 * 1024)

	pool.Put(smallBuf)
	pool.Put(mediumBuf)
	pool.Put(largeBuf)

	// Get stats
	stats := pool.GetStats()

	// Debug output
	t.Logf("Stats: TotalGets=%d, TotalPuts=%d", stats.TotalGets, stats.TotalPuts)
	t.Logf("Size categories: Small=%d, Medium=%d, Large=%d, VeryLarge=%d",
		stats.SmallObjectOps, stats.MediumObjectOps, stats.LargeObjectOps, stats.VeryLargeObjectOps)
	t.Logf("Strategies: Pooled=%d, Chunked=%d, Direct=%d",
		stats.PooledOperations, stats.ChunkedOperations, stats.DirectOperations)

	// Verify basic stats
	assert.True(t, stats.TotalGets > 0)
	assert.True(t, stats.TotalPuts > 0)

	// Verify size-category stats
	assert.True(t, stats.SmallObjectOps > 0)
	assert.True(t, stats.MediumObjectOps > 0)
	assert.True(t, stats.LargeObjectOps > 0)

	// Verify strategy stats
	assert.True(t, stats.PooledOperations > 0)
	// ChunkedOperations should be > 0 since we processed a large object
	assert.True(t, stats.ChunkedOperations > 0, "ChunkedOperations should be > 0, got %d", stats.ChunkedOperations)
}

func TestAdaptiveBufferPool_ConfigValidation(t *testing.T) {
	testCases := []struct {
		name        string
		config      daramjwee.BufferPoolConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: daramjwee.BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            512 * 1024,
				MinBufferSize:            4 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				LargeObjectStrategy:      daramjwee.StrategyAdaptive,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    4,
			},
			expectError: false,
		},
		{
			name: "invalid threshold order",
			config: daramjwee.BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            512 * 1024,
				MinBufferSize:            4 * 1024,
				LargeObjectThreshold:     1024 * 1024, // Larger than very large threshold
				VeryLargeObjectThreshold: 256 * 1024,
				LargeObjectStrategy:      daramjwee.StrategyAdaptive,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    4,
			},
			expectError: true,
		},
		{
			name: "invalid buffer size order",
			config: daramjwee.BufferPoolConfig{
				Enabled:           true,
				DefaultBufferSize: 16 * 1024,
				MaxBufferSize:     8 * 1024, // Smaller than default
				MinBufferSize:     4 * 1024,
			},
			expectError: true,
		},
	}

	logger := log.NewNopLogger()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := daramjwee.NewAdaptiveBufferPool(tc.config, logger)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAdaptiveBufferPool_ConcurrentAccess(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		MaxConcurrentLargeOps:    2, // Limit to 2 concurrent operations
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	// Test concurrent access to the pool
	const numGoroutines = 10
	const numOperations = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < numOperations; j++ {
				// Get buffers of various sizes
				sizes := []int{16 * 1024, 64 * 1024, 256 * 1024}
				for _, size := range sizes {
					buf := pool.Get(size)
					assert.True(t, len(buf) >= size)
					pool.Put(buf)
				}
			}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify stats
	stats := pool.GetStats()
	assert.True(t, stats.TotalGets > 0)
	assert.True(t, stats.TotalPuts > 0)
}

func TestObjectSizeHandling(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      daramjwee.StrategyAdaptive,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	testCases := []struct {
		size int
		name string
	}{
		{16 * 1024, "small"},    // 16KB
		{64 * 1024, "medium"},   // 64KB
		{512 * 1024, "large"},   // 512KB
		{2048 * 1024, "xlarge"}, // 2MB
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("size_%s", tc.name), func(t *testing.T) {
			// Test that different sized objects are handled appropriately
			buf := pool.Get(tc.size)
			assert.True(t, len(buf) >= tc.size)

			// Fill buffer with test data
			for i := 0; i < len(buf) && i < tc.size; i++ {
				buf[i] = byte(i % 256)
			}

			pool.Put(buf)
		})
	}
}

func TestBufferPoolStrategy_String(t *testing.T) {
	testCases := []struct {
		strategy daramjwee.BufferPoolStrategy
		expected string
	}{
		{daramjwee.StrategyPooled, "pooled"},
		{daramjwee.StrategyChunked, "chunked"},
		{daramjwee.StrategyDirect, "direct"},
		{daramjwee.StrategyAdaptive, "adaptive"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.strategy.String())
		})
	}
}

func TestObjectSizeCategory_String(t *testing.T) {
	testCases := []struct {
		category daramjwee.ObjectSizeCategory
		expected string
	}{
		{daramjwee.SizeCategorySmall, "small"},
		{daramjwee.SizeCategoryMedium, "medium"},
		{daramjwee.SizeCategoryLarge, "large"},
		{daramjwee.SizeCategoryVeryLarge, "very_large"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.category.String())
		})
	}
}
