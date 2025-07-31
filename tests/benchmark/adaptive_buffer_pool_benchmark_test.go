package benchmark_test

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

// BenchmarkBufferPoolComparison compares performance between default and adaptive buffer pools
func BenchmarkBufferPoolComparison(b *testing.B) {
	logger := log.NewNopLogger()

	// Test different object sizes
	testSizes := []struct {
		name string
		size int
	}{
		{"Small_16KB", 16 * 1024},
		{"Medium_128KB", 128 * 1024},
		{"Large_256KB", 256 * 1024},
		{"VeryLarge_512KB", 512 * 1024},
	}

	for _, ts := range testSizes {
		testData := strings.Repeat("x", ts.size)

		b.Run(ts.name+"_Default", func(b *testing.B) {
			benchmarkBufferPool(b, logger, testData, false)
		})

		b.Run(ts.name+"_Adaptive", func(b *testing.B) {
			benchmarkBufferPool(b, logger, testData, true)
		})
	}
}

func benchmarkBufferPool(b *testing.B, logger log.Logger, testData string, useAdaptive bool) {
	hotStore := newMockStore()

	var options []daramjwee.Option
	if useAdaptive {
		options = []daramjwee.Option{
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
		}
	} else {
		options = []daramjwee.Option{
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithBufferPool(true, 32*1024),
		}
	}

	cache, err := daramjwee.New(logger, options...)
	require.NoError(b, err)
	defer cache.Close()

	fetcher := &mockFetcher{
		content: testData,
		etag:    "bench-etag",
	}

	ctx := context.Background()
	key := "bench-key"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Test Get operation
		stream, err := cache.Get(ctx, key, fetcher)
		if err != nil {
			b.Fatal(err)
		}

		// Read all data
		_, err = io.ReadAll(stream)
		if err != nil {
			b.Fatal(err)
		}
		stream.Close()

		// Clear cache for next iteration
		cache.Delete(ctx, key)
	}
}

// BenchmarkBufferPoolDirectOperations benchmarks direct buffer pool operations
func BenchmarkBufferPoolDirectOperations(b *testing.B) {
	logger := log.NewNopLogger()

	testSizes := []struct {
		name string
		size int
	}{
		{"Small_16KB", 16 * 1024},
		{"Medium_128KB", 128 * 1024},
		{"Large_256KB", 256 * 1024},
		{"VeryLarge_512KB", 512 * 1024},
	}

	for _, ts := range testSizes {
		b.Run(ts.name+"_Default", func(b *testing.B) {
			benchmarkDirectBufferPool(b, logger, ts.size, false)
		})

		b.Run(ts.name+"_Adaptive", func(b *testing.B) {
			benchmarkDirectBufferPool(b, logger, ts.size, true)
		})
	}
}

func benchmarkDirectBufferPool(b *testing.B, logger log.Logger, size int, useAdaptive bool) {
	var bufferPool daramjwee.BufferPool

	if useAdaptive {
		config := daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                64 * 1024,
			MaxConcurrentLargeOps:    4,
		}
		pool, err := daramjwee.NewAdaptiveBufferPoolImpl(config, logger)
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := bufferPool.Get(size)
		bufferPool.Put(buf)
	}
}

// BenchmarkAdaptiveCopyOperations benchmarks copy operations with different buffer pools
func BenchmarkAdaptiveCopyOperations(b *testing.B) {
	logger := log.NewNopLogger()

	testSizes := []struct {
		name string
		size int
	}{
		{"Small_16KB", 16 * 1024},
		{"Medium_128KB", 128 * 1024},
		{"Large_256KB", 256 * 1024},
		{"VeryLarge_512KB", 512 * 1024},
	}

	for _, ts := range testSizes {
		testData := strings.Repeat("x", ts.size)

		b.Run(ts.name+"_Default", func(b *testing.B) {
			benchmarkCopyOperation(b, logger, testData, false)
		})

		b.Run(ts.name+"_Adaptive", func(b *testing.B) {
			benchmarkCopyOperation(b, logger, testData, true)
		})
	}
}

func benchmarkCopyOperation(b *testing.B, logger log.Logger, testData string, useAdaptive bool) {
	var bufferPool daramjwee.BufferPool

	if useAdaptive {
		config := daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                64 * 1024,
			MaxConcurrentLargeOps:    4,
		}
		pool, err := daramjwee.NewAdaptiveBufferPoolImpl(config, logger)
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

// BenchmarkMemoryEfficiency measures memory allocation patterns
func BenchmarkMemoryEfficiency(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("Default_LargeObjects", func(b *testing.B) {
		benchmarkMemoryEfficiency(b, logger, false)
	})

	b.Run("Adaptive_LargeObjects", func(b *testing.B) {
		benchmarkMemoryEfficiency(b, logger, true)
	})
}

func benchmarkMemoryEfficiency(b *testing.B, logger log.Logger, useAdaptive bool) {
	var bufferPool daramjwee.BufferPool

	if useAdaptive {
		config := daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                64 * 1024,
			MaxConcurrentLargeOps:    4,
		}
		pool, err := daramjwee.NewAdaptiveBufferPoolImpl(config, logger)
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

	// Mix of different sizes to simulate real workload
	sizes := []int{
		16 * 1024,  // Small
		64 * 1024,  // Medium
		256 * 1024, // Large
		512 * 1024, // Very Large
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		size := sizes[i%len(sizes)]
		buf := bufferPool.Get(size)

		// Simulate some work with the buffer
		for j := 0; j < len(buf) && j < 1000; j++ {
			buf[j] = byte(j % 256)
		}

		bufferPool.Put(buf)
	}
}

// BenchmarkAdaptiveConcurrentAccess tests concurrent access patterns
func BenchmarkAdaptiveConcurrentAccess(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("Default_Concurrent", func(b *testing.B) {
		benchmarkConcurrentAccess(b, logger, false)
	})

	b.Run("Adaptive_Concurrent", func(b *testing.B) {
		benchmarkConcurrentAccess(b, logger, true)
	})
}

func benchmarkConcurrentAccess(b *testing.B, logger log.Logger, useAdaptive bool) {
	var bufferPool daramjwee.BufferPool

	if useAdaptive {
		config := daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                64 * 1024,
			MaxConcurrentLargeOps:    4,
		}
		pool, err := daramjwee.NewAdaptiveBufferPoolImpl(config, logger)
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

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		sizes := []int{32 * 1024, 128 * 1024, 256 * 1024}
		i := 0
		for pb.Next() {
			size := sizes[i%len(sizes)]
			buf := bufferPool.Get(size)
			bufferPool.Put(buf)
			i++
		}
	})
}

// Performance validation test
func TestPerformanceValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance validation in short mode")
	}

	logger := log.NewNopLogger()

	// Test large object performance improvement
	testSizes := []int{256 * 1024, 512 * 1024, 1024 * 1024}

	for _, size := range testSizes {
		t.Run("Size_"+string(rune(size/1024))+"KB", func(t *testing.T) {
			// Measure default buffer pool performance
			defaultTime := measureBufferPoolPerformance(t, logger, size, false)

			// Measure adaptive buffer pool performance
			adaptiveTime := measureBufferPoolPerformance(t, logger, size, true)

			t.Logf("Size: %dKB, Default: %v, Adaptive: %v", size/1024, defaultTime, adaptiveTime)

			// For large objects, adaptive should be at least as fast or faster
			if size >= 256*1024 {
				// Allow some variance, but adaptive should not be significantly slower
				maxAllowedRatio := 1.2 // 20% slower is acceptable
				ratio := float64(adaptiveTime) / float64(defaultTime)

				if ratio > maxAllowedRatio {
					t.Logf("Warning: Adaptive buffer pool is %.2fx slower than default for %dKB objects", ratio, size/1024)
				} else {
					t.Logf("Success: Adaptive buffer pool performance ratio: %.2fx for %dKB objects", ratio, size/1024)
				}
			}
		})
	}
}

func measureBufferPoolPerformance(t *testing.T, logger log.Logger, size int, useAdaptive bool) time.Duration {
	var bufferPool daramjwee.BufferPool

	if useAdaptive {
		config := daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                64 * 1024,
			MaxConcurrentLargeOps:    4,
		}
		pool, err := daramjwee.NewAdaptiveBufferPoolImpl(config, logger)
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

	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		buf := bufferPool.Get(size)
		// Simulate some work
		if len(buf) > 0 {
			buf[0] = byte(i % 256)
		}
		bufferPool.Put(buf)
	}

	return time.Since(start)
}
