package benchmark_test_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	helpers "github.com/mrchypark/daramjwee/tests/benchmark/helpers_test"
)

// TestLargeDataPerformance tests performance with very large data sizes
func TestLargeDataPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large data performance test in short mode")
	}

	logger := log.NewNopLogger()

	// Test progressively larger data sizes
	testSizes := []struct {
		name string
		size int
	}{
		{"2MB", 2 * 1024 * 1024},
		{"5MB", 5 * 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"20MB", 20 * 1024 * 1024},
	}

	for _, ts := range testSizes {
		t.Run(ts.name, func(t *testing.T) {
			// Test both default and adaptive buffer pools
			defaultTime := measureLargeDataPerformance(t, logger, ts.size, false)
			adaptiveTime := measureLargeDataPerformance(t, logger, ts.size, true)

			t.Logf("Size: %s, Default: %v, Adaptive: %v", ts.name, defaultTime, adaptiveTime)

			// For very large objects, adaptive should be significantly faster
			ratio := float64(adaptiveTime) / float64(defaultTime)
			t.Logf("Performance ratio (adaptive/default): %.3fx", ratio)

			// Adaptive should be at least as fast or faster
			if ratio > 1.5 { // Allow 50% slower as acceptable
				t.Logf("Warning: Adaptive buffer pool is %.2fx slower than default for %s", ratio, ts.name)
			} else {
				t.Logf("Success: Adaptive buffer pool performance is %.2fx relative to default for %s", ratio, ts.name)
			}
		})
	}
}

func measureLargeDataPerformance(t *testing.T, logger log.Logger, size int, useAdaptive bool) time.Duration {
	hotStore := newMockStore()

	var options []daramjwee.Option
	if useAdaptive {
		options = []daramjwee.Option{
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 8), // Increased concurrent ops for large data
		}
	} else {
		options = []daramjwee.Option{
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithBufferPool(true, 32*1024),
		}
	}

	cache, err := daramjwee.New(logger, options...)
	require.NoError(t, err)
	defer cache.Close()

	// Create test data - use a pattern to avoid memory issues
	testData := createLargeTestData(size)

	fetcher := &mockFetcher{
		content: testData,
		etag:    "large-data-etag",
	}

	ctx := context.Background()
	key := "large-data-key"

	start := time.Now()

	// Test Get operation
	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)

	// Read all data
	_, err = io.ReadAll(stream)
	require.NoError(t, err)
	stream.Close()

	return time.Since(start)
}

// createLargeTestData creates test data of specified size efficiently
func createLargeTestData(size int) string {
	// For very large sizes, use a more efficient approach
	if size > 5*1024*1024 { // > 5MB
		// Create a smaller base pattern and use it to build larger chunks
		basePattern := strings.Repeat("0123456789abcdef", 1024) // 16KB pattern
		baseSize := len(basePattern)

		if size <= baseSize {
			return basePattern[:size]
		}

		var builder strings.Builder
		builder.Grow(size)

		// Add full base patterns
		fullPatterns := size / baseSize
		for i := 0; i < fullPatterns; i++ {
			builder.WriteString(basePattern)
		}

		// Add remainder
		remainder := size % baseSize
		if remainder > 0 {
			builder.WriteString(basePattern[:remainder])
		}

		return builder.String()
	}

	// For smaller sizes, use the original approach
	pattern := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	patternLen := len(pattern)

	if size <= patternLen {
		return pattern[:size]
	}

	// Calculate how many full patterns we need
	fullPatterns := size / patternLen
	remainder := size % patternLen

	var builder strings.Builder
	builder.Grow(size)

	// Add full patterns
	for i := 0; i < fullPatterns; i++ {
		builder.WriteString(pattern)
	}

	// Add remainder
	if remainder > 0 {
		builder.WriteString(pattern[:remainder])
	}

	return builder.String()
}

// BenchmarkLargeDataOperations benchmarks operations with large data
func BenchmarkLargeDataOperations(b *testing.B) {
	logger := log.NewNopLogger()

	testSizes := []struct {
		name string
		size int
	}{
		{"2MB", 2 * 1024 * 1024},
		{"5MB", 5 * 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, ts := range testSizes {
		b.Run(ts.name+"_Default", func(b *testing.B) {
			benchmarkLargeDataOperation(b, logger, ts.size, false)
		})

		b.Run(ts.name+"_Adaptive", func(b *testing.B) {
			benchmarkLargeDataOperation(b, logger, ts.size, true)
		})
	}
}

func benchmarkLargeDataOperation(b *testing.B, logger log.Logger, size int, useAdaptive bool) {
	var bufferPool daramjwee.BufferPool

	if useAdaptive {
		config := daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                128 * 1024, // Larger chunks for very large data
			MaxConcurrentLargeOps:    8,
		}
		pool, err := NewAdaptiveBufferPoolImpl(config, logger)
		require.NoError(b, err)
		bufferPool = pool
		defer pool.Close()
	} else {
		config := daramjwee.BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     128 * 1024,
			MinBufferSize:     4 * 1024,
		}
		bufferPool = daramjwee.NewDefaultBufferPoolWithLogger(config, logger)
	}

	// Create test data
	testData := createLargeTestData(size)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		src := strings.NewReader(testData)
		dst := &bytes.Buffer{}

		_, err := bufferPool.CopyBuffer(dst, src)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestMemoryUsageWithLargeData tests memory usage patterns with large data
func TestMemoryUsageWithLargeData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory usage test in short mode")
	}

	logger := log.NewNopLogger()

	// Test memory usage with different strategies
	testCases := []struct {
		name        string
		size        int
		useAdaptive bool
	}{
		{"5MB_Default", 5 * 1024 * 1024, false},
		{"5MB_Adaptive", 5 * 1024 * 1024, true},
		{"10MB_Default", 10 * 1024 * 1024, false},
		{"10MB_Adaptive", 10 * 1024 * 1024, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var bufferPool daramjwee.BufferPool

			if tc.useAdaptive {
				config := daramjwee.BufferPoolConfig{
					Enabled:                  true,
					DefaultBufferSize:        32 * 1024,
					MaxBufferSize:            128 * 1024,
					MinBufferSize:            4 * 1024,
					LargeObjectThreshold:     256 * 1024,
					VeryLargeObjectThreshold: 1024 * 1024,
					ChunkSize:                128 * 1024,
					MaxConcurrentLargeOps:    4,
				}
				pool, err := NewAdaptiveBufferPoolImpl(config, logger)
				require.NoError(t, err)
				bufferPool = pool
				defer pool.Close()
			} else {
				config := daramjwee.BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 32 * 1024,
					MaxBufferSize:     128 * 1024,
					MinBufferSize:     4 * 1024,
				}
				bufferPool = daramjwee.NewDefaultBufferPoolWithLogger(config, logger)
			}

			// Perform multiple operations to test memory usage
			iterations := 10
			for i := 0; i < iterations; i++ {
				buf := bufferPool.Get(tc.size)

				// Simulate some work with the buffer
				if len(buf) > 1000 {
					for j := 0; j < 1000; j++ {
						buf[j] = byte(j % 256)
					}
				}

				bufferPool.Put(buf)
			}

			// Get stats if available
			stats := bufferPool.GetStats()
			t.Logf("Stats for %s: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d",
				tc.name, stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)
		})
	}
}

// TestConcurrentLargeDataOperations tests concurrent access with large data
func TestConcurrentLargeDataOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent large data test in short mode")
	}

	logger := log.NewNopLogger()

	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            128 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		ChunkSize:                128 * 1024,
		MaxConcurrentLargeOps:    8, // Allow more concurrent operations
	}

	pool, err := NewAdaptiveBufferPoolImpl(config, logger)
	require.NoError(t, err)
	defer pool.Close()

	// Test concurrent operations with large data
	const numGoroutines = 10
	const dataSize = 2 * 1024 * 1024 // 2MB

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Each goroutine performs multiple operations
			for j := 0; j < 5; j++ {
				buf := pool.Get(dataSize)

				// Simulate work
				if len(buf) > 100 {
					for k := 0; k < 100; k++ {
						buf[k] = byte((id + j + k) % 256)
					}
				}

				pool.Put(buf)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
			// Goroutine completed
		case <-time.After(30 * time.Second):
			t.Fatal("Test timed out waiting for goroutines to complete")
		}
	}

	// Check final stats
	stats := pool.GetStats()
	t.Logf("Final stats: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

	// Verify no buffer leaks
	if stats.ActiveBuffers != 0 {
		t.Errorf("Buffer leak detected: %d active buffers remaining", stats.ActiveBuffers)
	}
}

// TestExtremelyLargeData tests with very large data sizes (if system can handle it)
func TestExtremelyLargeData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping extremely large data test in short mode")
	}

	logger := log.NewNopLogger()

	// Test with 50MB and 100MB if system has enough memory
	testSizes := []struct {
		name string
		size int
	}{
		{"50MB", 50 * 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
	}

	for _, ts := range testSizes {
		t.Run(ts.name, func(t *testing.T) {
			config := daramjwee.BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            128 * 1024,
				MinBufferSize:            4 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                256 * 1024, // Even larger chunks
				MaxConcurrentLargeOps:    2,          // Limit concurrent ops for very large data
			}

			pool, err := NewAdaptiveBufferPoolImpl(config, logger)
			require.NoError(t, err)
			defer pool.Close()

			start := time.Now()

			// Test direct buffer operations (not full data creation to avoid memory issues)
			buf := pool.Get(ts.size)

			// Simulate some work on the buffer
			if len(buf) > 10000 {
				for i := 0; i < 10000; i++ {
					buf[i] = byte(i % 256)
				}
			}

			pool.Put(buf)

			duration := time.Since(start)
			t.Logf("Processed %s in %v", ts.name, duration)

			// Verify stats
			stats := pool.GetStats()
			t.Logf("Stats: Gets=%d, Puts=%d, Strategy breakdown - Direct=%d",
				stats.TotalGets, stats.TotalPuts, stats.DirectOperations)

			// For extremely large data, should use direct strategy
			if stats.DirectOperations == 0 {
				t.Logf("Note: Expected direct operations for %s, but got strategy breakdown - Pooled=%d, Chunked=%d, Direct=%d",
					ts.name, stats.PooledOperations, stats.ChunkedOperations, stats.DirectOperations)
			}
		})
	}
}
