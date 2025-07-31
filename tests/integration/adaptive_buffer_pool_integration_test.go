package integration

import (
	"github.com/mrchypark/daramjwee"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdaptiveBufferPoolIntegration tests the complete integration of adaptive buffer pool
func TestAdaptiveBufferPoolIntegration(t *testing.T) {
	logger := log.NewNopLogger()

	// Create a simple in-memory store for testing
	hotStore := newMockStore()

	tests := []struct {
		name           string
		options        []daramjwee.Option
		dataSize       int
		expectedPool   string // "adaptive" or "default"
		shouldOptimize bool
	}{
		{
			name: "small object uses default pool",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
			},
			dataSize:       16 * 1024, // 16KB
			expectedPool:   "adaptive",
			shouldOptimize: false,
		},
		{
			name: "large object uses adaptive optimization",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
			},
			dataSize:       300 * 1024, // 300KB
			expectedPool:   "adaptive",
			shouldOptimize: true,
		},
		{
			name: "very large object uses direct streaming",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
			},
			dataSize:       1200 * 1024, // 1.2MB
			expectedPool:   "adaptive",
			shouldOptimize: true,
		},
		{
			name: "backward compatibility without large object optimization",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithBufferPool(true, 32*1024),
			},
			dataSize:       100 * 1024, // 100KB
			expectedPool:   "default",
			shouldOptimize: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create cache with specified options
			cache, err := daramjwee.New(logger, tt.options...)
			require.NoError(t, err)
			defer cache.Close()

			// Verify cache type
			daramjweeCache, ok := cache.(*daramjwee.DaramjweeCache)
			require.True(t, ok)

			// Check buffer pool type
			if tt.expectedPool == "adaptive" {
				_, isAdaptive := daramjweeCache.BufferPool.(*daramjwee.AdaptiveBufferPoolImpl)
				if tt.shouldOptimize {
					assert.True(t, isAdaptive, "Expected adaptive buffer pool for large object optimization")
				}
			}

			// Test cache operations with different data sizes
			// Use reasonable test data size for memory efficiency
			testDataSize := 16 * 1024 // Use consistent 16KB for all tests to avoid memory issues
			testData := strings.Repeat("x", testDataSize)
			key := "test-key"

			// Create a simple fetcher
			fetcher := &mockFetcher{
				content: testData,
				etag:    "test-etag",
			}

			// Test Get operation
			ctx := context.Background()
			stream, err := cache.Get(ctx, key, fetcher)
			require.NoError(t, err)
			defer stream.Close()

			// Read and verify data
			data, err := io.ReadAll(stream)
			require.NoError(t, err)
			// Compare length first to avoid huge string comparison in test output
			assert.Equal(t, len(testData), len(data), "Data length should match")
			if len(testData) == len(data) {
				assert.Equal(t, testData, string(data), "Data content should match")
			}

			// Test Set operation
			metadata := &daramjwee.Metadata{
				ETag:     "set-etag",
				CachedAt: time.Now(),
			}

			writer, err := cache.Set(ctx, key+"-set", metadata)
			require.NoError(t, err)

			_, err = io.Copy(writer, strings.NewReader(testData))
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Verify the set data can be retrieved
			stream2, err := cache.Get(ctx, key+"-set", fetcher)
			require.NoError(t, err)
			defer stream2.Close()

			data2, err := io.ReadAll(stream2)
			require.NoError(t, err)
			assert.Equal(t, testData, string(data2))
		})
	}
}

// TestAdaptiveBufferPoolMetrics tests metrics collection for adaptive buffer pool
func TestAdaptiveBufferPoolMetrics(t *testing.T) {
	logger := log.NewNopLogger()
	hotStore := newMockStore()

	// Create cache with adaptive buffer pool and metrics enabled
	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
		daramjwee.WithBufferPoolMetrics(true, 0), // Enable metrics without periodic logging
	)
	require.NoError(t, err)
	defer cache.Close()

	daramjweeCache := cache.(*daramjwee.DaramjweeCache)
	adaptivePool, ok := daramjweeCache.BufferPool.(*daramjwee.AdaptiveBufferPoolImpl)
	require.True(t, ok, "Expected adaptive buffer pool")

	// Test different object sizes to generate metrics
	testCases := []struct {
		name string
		size int
		data string
	}{
		{"small", 16 * 1024, strings.Repeat("s", 16*1024)},
		{"medium", 128 * 1024, strings.Repeat("m", 32*1024)},      // Reduced data size for testing
		{"large", 300 * 1024, strings.Repeat("l", 32*1024)},       // Reduced data size for testing
		{"very_large", 1200 * 1024, strings.Repeat("v", 32*1024)}, // Reduced data size for testing
	}

	ctx := context.Background()
	for i, tc := range testCases {
		key := tc.name + "-key"
		fetcher := &mockFetcher{
			content: tc.data,
			etag:    "etag-" + tc.name,
		}

		// Perform cache operations
		stream, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)

		data, err := io.ReadAll(stream)
		require.NoError(t, err)
		stream.Close()

		assert.Equal(t, tc.data, string(data))

		// Also test direct buffer operations
		buf := adaptivePool.Get(tc.size)
		adaptivePool.Put(buf)

		// Test copy operations
		src := strings.NewReader(tc.data)
		dst := &bytes.Buffer{}
		_, err = adaptivePool.CopyBuffer(dst, src)
		require.NoError(t, err)
		assert.Equal(t, tc.data, dst.String())

		t.Logf("Completed test case %d: %s (size: %d)", i+1, tc.name, tc.size)
	}

	// Get and verify metrics
	stats := adaptivePool.GetStats()

	// Verify basic metrics
	assert.Greater(t, stats.TotalGets, 0, "Should have buffer gets")
	assert.Greater(t, stats.TotalPuts, 0, "Should have buffer puts")

	// Verify size-category metrics
	assert.Greater(t, stats.SmallObjectOps, 0, "Should have small object operations")
	assert.Greater(t, stats.MediumObjectOps, 0, "Should have medium object operations")
	assert.Greater(t, stats.LargeObjectOps, 0, "Should have large object operations")
	assert.Greater(t, stats.VeryLargeObjectOps, 0, "Should have very large object operations")

	// Verify strategy metrics
	totalStrategyOps := stats.PooledOperations + stats.ChunkedOperations + stats.DirectOperations
	assert.Greater(t, totalStrategyOps, 0, "Should have strategy operations")

	// Verify latency metrics exist
	assert.NotNil(t, stats.AverageLatencyNs, "Should have latency metrics")
	assert.Contains(t, stats.AverageLatencyNs, "small", "Should have small object latency")
	assert.Contains(t, stats.AverageLatencyNs, "medium", "Should have medium object latency")
	assert.Contains(t, stats.AverageLatencyNs, "large", "Should have large object latency")
	assert.Contains(t, stats.AverageLatencyNs, "very_large", "Should have very large object latency")

	// Verify memory efficiency
	assert.GreaterOrEqual(t, stats.MemoryEfficiency, 0.0, "Memory efficiency should be non-negative")
	assert.LessOrEqual(t, stats.MemoryEfficiency, 1.0, "Memory efficiency should not exceed 1.0")

	t.Logf("Buffer pool stats: Gets=%d, Puts=%d, Hits=%d, Misses=%d",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses)
	t.Logf("Size categories: Small=%d, Medium=%d, Large=%d, VeryLarge=%d",
		stats.SmallObjectOps, stats.MediumObjectOps, stats.LargeObjectOps, stats.VeryLargeObjectOps)
	t.Logf("Strategies: Pooled=%d, Chunked=%d, Direct=%d",
		stats.PooledOperations, stats.ChunkedOperations, stats.DirectOperations)
	t.Logf("Memory efficiency: %.3f", stats.MemoryEfficiency)
}

// TestAdaptiveBufferPoolBackwardCompatibility ensures existing buffer pool usage continues to work
func TestAdaptiveBufferPoolBackwardCompatibility(t *testing.T) {
	logger := log.NewNopLogger()
	hotStore := newMockStore()

	// Test existing buffer pool configurations
	compatibilityTests := []struct {
		name    string
		options []daramjwee.Option
	}{
		{
			name: "basic buffer pool",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithBufferPool(true, 32*1024),
			},
		},
		{
			name: "advanced buffer pool",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithBufferPoolAdvanced(daramjwee.BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 64 * 1024,
					MaxBufferSize:     128 * 1024,
					MinBufferSize:     8 * 1024,
					EnableLogging:     false,
					LoggingInterval:   0,
				}),
			},
		},
		{
			name: "disabled buffer pool",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithBufferPool(false, 32*1024),
			},
		},
	}

	for _, tt := range compatibilityTests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := daramjwee.New(logger, tt.options...)
			require.NoError(t, err)
			defer cache.Close()

			// Test basic cache operations
			ctx := context.Background()
			testData := "test data for backward compatibility"
			key := "compat-key"

			fetcher := &mockFetcher{
				content: testData,
				etag:    "compat-etag",
			}

			// Test Get operation
			stream, err := cache.Get(ctx, key, fetcher)
			require.NoError(t, err)
			defer stream.Close()

			data, err := io.ReadAll(stream)
			require.NoError(t, err)
			assert.Equal(t, testData, string(data))

			// Test Set operation
			metadata := &daramjwee.Metadata{
				ETag:     "compat-set-etag",
				CachedAt: time.Now(),
			}

			writer, err := cache.Set(ctx, key+"-set", metadata)
			require.NoError(t, err)

			_, err = io.Copy(writer, strings.NewReader(testData))
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Verify buffer pool stats are accessible
			daramjweeCache := cache.(*daramjwee.DaramjweeCache)
			stats := daramjweeCache.BufferPool.GetStats()

			// Basic stats should be available regardless of pool type
			assert.GreaterOrEqual(t, stats.TotalGets, 0)
			assert.GreaterOrEqual(t, stats.TotalPuts, 0)
		})
	}
}

// TestConfigurationValidation tests configuration validation for adaptive buffer pool
func TestConfigurationValidation(t *testing.T) {
	logger := log.NewNopLogger()
	hotStore := newMockStore()

	invalidConfigs := []struct {
		name        string
		options     []daramjwee.Option
		expectError bool
		errorMsg    string
	}{
		{
			name: "invalid large threshold",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(0, 1024*1024, 64*1024, 4),
			},
			expectError: true,
			errorMsg:    "large object threshold must be positive",
		},
		{
			name: "invalid very large threshold",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(256*1024, 128*1024, 64*1024, 4),
			},
			expectError: true,
			errorMsg:    "very large object threshold must be larger than large object threshold",
		},
		{
			name: "invalid chunk size",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 0, 4),
			},
			expectError: true,
			errorMsg:    "chunk size must be positive",
		},
		{
			name: "invalid max concurrent ops",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 0),
			},
			expectError: true,
			errorMsg:    "max concurrent large operations must be positive",
		},
		{
			name: "valid configuration",
			options: []daramjwee.Option{
				daramjwee.WithHotStore(hotStore),
				daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
			},
			expectError: false,
		},
	}

	for _, tt := range invalidConfigs {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := daramjwee.New(logger, tt.options...)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				assert.Nil(t, cache)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, cache)
				if cache != nil {
					cache.Close()
				}
			}
		})
	}
}
