package daramjwee

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBufferPool_Creation(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      StrategyAdaptive,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    4,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := NewAdaptiveBufferPool(config, logger)

	require.NoError(t, err)
	require.NotNil(t, pool)
	assert.Equal(t, config.LargeObjectThreshold, pool.config.LargeObjectThreshold)
	assert.Equal(t, config.VeryLargeObjectThreshold, pool.config.VeryLargeObjectThreshold)
}

func TestAdaptiveBufferPool_StrategySelection(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      StrategyAdaptive,
	}

	testCases := []struct {
		size             int
		expectedStrategy BufferPoolStrategy
		expectedCategory ObjectSizeCategory
	}{
		{16 * 1024, StrategyPooled, SizeCategorySmall},       // 16KB - small
		{64 * 1024, StrategyPooled, SizeCategoryMedium},      // 64KB - medium
		{512 * 1024, StrategyChunked, SizeCategoryLarge},     // 512KB - large
		{2048 * 1024, StrategyDirect, SizeCategoryVeryLarge}, // 2MB - very large
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("size_%d", tc.size), func(t *testing.T) {
			strategy := selectStrategy(tc.size, config)
			category := classifyObjectSize(tc.size, config)

			assert.Equal(t, tc.expectedStrategy, strategy)
			assert.Equal(t, tc.expectedCategory, category)
		})
	}
}

func TestAdaptiveBufferPool_GetPut(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      StrategyAdaptive,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    4,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := NewAdaptiveBufferPool(config, logger)
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
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		LargeObjectStrategy:      StrategyAdaptive,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    4,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := NewAdaptiveBufferPool(config, logger)
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
		config      BufferPoolConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            512 * 1024,
				MinBufferSize:            4 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				LargeObjectStrategy:      StrategyAdaptive,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    4,
			},
			expectError: false,
		},
		{
			name: "invalid threshold order",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            512 * 1024,
				MinBufferSize:            4 * 1024,
				LargeObjectThreshold:     1024 * 1024, // Larger than very large threshold
				VeryLargeObjectThreshold: 256 * 1024,
				LargeObjectStrategy:      StrategyAdaptive,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    4,
			},
			expectError: true,
		},
		{
			name: "invalid buffer size order",
			config: BufferPoolConfig{
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
			_, err := NewAdaptiveBufferPool(tc.config, logger)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAdaptiveBufferPool_LargeOpsSemaphore(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		MaxConcurrentLargeOps:    2, // Limit to 2 concurrent operations
	}

	logger := log.NewNopLogger()
	pool, err := NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	ctx := context.Background()

	// Acquire first slot
	err = pool.acquireLargeOpSlot(ctx)
	assert.NoError(t, err)

	// Acquire second slot
	err = pool.acquireLargeOpSlot(ctx)
	assert.NoError(t, err)

	// Third acquisition should block, so test with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = pool.acquireLargeOpSlot(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	// Release one slot
	pool.releaseLargeOpSlot()

	// Now acquisition should succeed
	ctx = context.Background()
	err = pool.acquireLargeOpSlot(ctx)
	assert.NoError(t, err)

	// Clean up
	pool.releaseLargeOpSlot()
	pool.releaseLargeOpSlot()
}

func TestObjectSizeClassification(t *testing.T) {
	config := BufferPoolConfig{
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
	}

	testCases := []struct {
		size     int
		expected ObjectSizeCategory
	}{
		{16 * 1024, SizeCategorySmall},       // 16KB
		{64 * 1024, SizeCategoryMedium},      // 64KB
		{512 * 1024, SizeCategoryLarge},      // 512KB
		{2048 * 1024, SizeCategoryVeryLarge}, // 2MB
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("size_%d", tc.size), func(t *testing.T) {
			category := classifyObjectSize(tc.size, config)
			assert.Equal(t, tc.expected, category)
		})
	}
}

func TestBufferPoolStrategy_String(t *testing.T) {
	testCases := []struct {
		strategy BufferPoolStrategy
		expected string
	}{
		{StrategyPooled, "pooled"},
		{StrategyChunked, "chunked"},
		{StrategyDirect, "direct"},
		{StrategyAdaptive, "adaptive"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.strategy.String())
		})
	}
}

func TestObjectSizeCategory_String(t *testing.T) {
	testCases := []struct {
		category ObjectSizeCategory
		expected string
	}{
		{SizeCategorySmall, "small"},
		{SizeCategoryMedium, "medium"},
		{SizeCategoryLarge, "large"},
		{SizeCategoryVeryLarge, "very_large"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.category.String())
		})
	}
}
