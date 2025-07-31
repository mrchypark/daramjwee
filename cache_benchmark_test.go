package daramjwee

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkBufferPoolOptimization_ColdHitPromotion benchmarks cold hit promotion
// performance with and without buffer pool optimization.
func BenchmarkBufferPoolOptimization_ColdHitPromotion(b *testing.B) {
	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
		{"XLarge_1MB", 1024 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			b.Run("WithoutBufferPool", func(b *testing.B) {
				benchmarkColdHitPromotion(b, ds.size, false)
			})
			b.Run("WithBufferPool", func(b *testing.B) {
				benchmarkColdHitPromotion(b, ds.size, true)
			})
		})
	}
}

// BenchmarkBufferPoolOptimization_CacheMiss benchmarks cache miss performance
// with and without buffer pool optimization.
func BenchmarkBufferPoolOptimization_CacheMiss(b *testing.B) {
	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
		{"XLarge_1MB", 1024 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			b.Run("WithoutBufferPool", func(b *testing.B) {
				benchmarkCacheMiss(b, ds.size, false)
			})
			b.Run("WithBufferPool", func(b *testing.B) {
				benchmarkCacheMiss(b, ds.size, true)
			})
		})
	}
}

// BenchmarkBufferPoolOptimization_BackgroundRefresh benchmarks background refresh
// performance with and without buffer pool optimization.
func BenchmarkBufferPoolOptimization_BackgroundRefresh(b *testing.B) {
	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
		{"XLarge_1MB", 1024 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			b.Run("WithoutBufferPool", func(b *testing.B) {
				benchmarkBackgroundRefresh(b, ds.size, false)
			})
			b.Run("WithBufferPool", func(b *testing.B) {
				benchmarkBackgroundRefresh(b, ds.size, true)
			})
		})
	}
}

// BenchmarkBufferPoolOptimization_ConcurrentOperations benchmarks concurrent
// cache operations with and without buffer pool optimization.
func BenchmarkBufferPoolOptimization_ConcurrentOperations(b *testing.B) {
	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			b.Run("WithoutBufferPool", func(b *testing.B) {
				benchmarkConcurrentOperations(b, ds.size, false)
			})
			b.Run("WithBufferPool", func(b *testing.B) {
				benchmarkConcurrentOperations(b, ds.size, true)
			})
		})
	}
}

// benchmarkColdHitPromotion benchmarks the performance of cold hit promotion operations.
func benchmarkColdHitPromotion(b *testing.B, dataSize int, useBufferPool bool) {
	// Generate test data
	testData := make([]byte, dataSize)
	_, err := rand.Read(testData)
	require.NoError(b, err)

	// Setup cache
	var opts []Option
	if useBufferPool {
		opts = append(opts, WithBufferPool(true, 32*1024))
	}
	cache, hot, cold := setupBenchmarkCache(b, opts...)

	// Pre-populate cold store
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("cold-key-%d", i)
		keys[i] = key
		cold.setData(key, string(testData), &Metadata{ETag: "test-etag"})
	}

	// Measure memory before benchmark
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%len(keys)]
			stream, err := cache.Get(context.Background(), key, &mockFetcher{})
			if err != nil {
				b.Errorf("Get failed: %v", err)
				continue
			}
			io.Copy(io.Discard, stream)
			stream.Close()
			i++
		}
	})

	b.StopTimer()

	// Measure memory after benchmark
	var memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	// Report memory statistics
	b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
	b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
	b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")

	// Wait for all background operations to complete
	time.Sleep(100 * time.Millisecond)
	for len(hot.writeCompleted) > 0 {
		<-hot.writeCompleted
	}
}

// benchmarkCacheMiss benchmarks the performance of cache miss operations.
func benchmarkCacheMiss(b *testing.B, dataSize int, useBufferPool bool) {
	// Generate test data
	testData := make([]byte, dataSize)
	_, err := rand.Read(testData)
	require.NoError(b, err)

	// Setup cache
	var opts []Option
	if useBufferPool {
		opts = append(opts, WithBufferPool(true, 32*1024))
	}
	cache, hot, cold := setupBenchmarkCache(b, opts...)

	// Measure memory before benchmark
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("miss-key-%d", i)
			fetcher := &mockFetcher{content: string(testData), etag: "test-etag"}
			stream, err := cache.Get(context.Background(), key, fetcher)
			if err != nil {
				b.Errorf("Get failed: %v", err)
				continue
			}
			io.Copy(io.Discard, stream)
			stream.Close()
			i++
		}
	})

	b.StopTimer()

	// Measure memory after benchmark
	var memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	// Report memory statistics
	b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
	b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
	b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")

	// Wait for all background operations to complete
	time.Sleep(100 * time.Millisecond)
	for len(hot.writeCompleted) > 0 {
		<-hot.writeCompleted
	}
	for len(cold.writeCompleted) > 0 {
		<-cold.writeCompleted
	}
}

// benchmarkBackgroundRefresh benchmarks the performance of background refresh operations.
func benchmarkBackgroundRefresh(b *testing.B, dataSize int, useBufferPool bool) {
	// Generate test data
	testData := make([]byte, dataSize)
	_, err := rand.Read(testData)
	require.NoError(b, err)

	// Setup cache
	var opts []Option
	if useBufferPool {
		opts = append(opts, WithBufferPool(true, 32*1024))
	}
	cache, hot, _ := setupBenchmarkCache(b, opts...)

	// Pre-populate hot store with stale data
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("refresh-key-%d", i)
		keys[i] = key
		hot.setData(key, string(testData), &Metadata{
			ETag:     "old-etag",
			CachedAt: time.Now().Add(-2 * time.Hour), // Make it stale
		})
	}

	// Measure memory before benchmark
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%len(keys)]
			fetcher := &mockFetcher{content: string(testData), etag: "new-etag"}
			err := cache.ScheduleRefresh(context.Background(), key, fetcher)
			if err != nil {
				b.Errorf("ScheduleRefresh failed: %v", err)
			}
			i++
		}
	})

	b.StopTimer()

	// Wait for all refresh operations to complete
	time.Sleep(200 * time.Millisecond)

	// Measure memory after benchmark
	var memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	// Report memory statistics
	b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
	b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
	b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
}

// benchmarkConcurrentOperations benchmarks concurrent cache operations.
func benchmarkConcurrentOperations(b *testing.B, dataSize int, useBufferPool bool) {
	// Generate test data
	testData := make([]byte, dataSize)
	_, err := rand.Read(testData)
	require.NoError(b, err)

	// Setup cache
	var opts []Option
	if useBufferPool {
		opts = append(opts, WithBufferPool(true, 32*1024))
	}
	cache, hot, cold := setupBenchmarkCache(b, opts...)

	// Pre-populate stores with mixed data
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("concurrent-key-%d", i)
		if i%3 == 0 {
			// Hot cache hit
			hot.setData(key, string(testData), &Metadata{ETag: "hot-etag"})
		} else if i%3 == 1 {
			// Cold cache hit
			cold.setData(key, string(testData), &Metadata{ETag: "cold-etag"})
		}
		// i%3 == 2 will be cache miss
	}

	// Measure memory before benchmark
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark with mixed operations
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent-key-%d", i%numKeys)

			// Mix of operations
			switch i % 4 {
			case 0, 1: // Get operations (75% of traffic)
				fetcher := &mockFetcher{content: string(testData), etag: "fetcher-etag"}
				stream, err := cache.Get(context.Background(), key, fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, stream)
				stream.Close()
			case 2: // Set operation (12.5% of traffic)
				writer, err := cache.Set(context.Background(), key, &Metadata{ETag: "set-etag"})
				if err != nil {
					b.Errorf("Set failed: %v", err)
					continue
				}
				writer.Write(testData)
				writer.Close()
			case 3: // Delete operation (12.5% of traffic)
				err := cache.Delete(context.Background(), key)
				if err != nil {
					b.Errorf("Delete failed: %v", err)
				}
			}
			i++
		}
	})

	b.StopTimer()

	// Measure memory after benchmark
	var memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	// Report memory statistics
	b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
	b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
	b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")

	// Wait for all background operations to complete
	time.Sleep(100 * time.Millisecond)
}

// BenchmarkBufferPoolOptimization_MemoryAllocation specifically measures
// memory allocation patterns with different buffer pool configurations.
func BenchmarkBufferPoolOptimization_MemoryAllocation(b *testing.B) {
	testCases := []struct {
		name     string
		dataSize int
		poolSize int
	}{
		{"Small_1KB_Pool4KB", 1024, 4 * 1024},
		{"Medium_32KB_Pool32KB", 32 * 1024, 32 * 1024},
		{"Large_256KB_Pool256KB", 256 * 1024, 256 * 1024},
		{"Mismatch_1KB_Pool64KB", 1024, 64 * 1024},
		{"Mismatch_128KB_Pool32KB", 128 * 1024, 32 * 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("WithoutBufferPool", func(b *testing.B) {
				benchmarkMemoryAllocation(b, tc.dataSize, 0, false)
			})
			b.Run("WithBufferPool", func(b *testing.B) {
				benchmarkMemoryAllocation(b, tc.dataSize, tc.poolSize, true)
			})
		})
	}
}

// benchmarkMemoryAllocation focuses on measuring memory allocation patterns.
func benchmarkMemoryAllocation(b *testing.B, dataSize, poolSize int, useBufferPool bool) {
	// Generate test data
	testData := make([]byte, dataSize)
	_, err := rand.Read(testData)
	require.NoError(b, err)

	// Setup cache
	var opts []Option
	if useBufferPool {
		opts = append(opts, WithBufferPool(true, poolSize))
	}
	cache, _, cold := setupBenchmarkCache(b, opts...)

	// Pre-populate cold store to trigger promotions
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("alloc-key-%d", i)
		cold.setData(key, string(testData), &Metadata{ETag: "test-etag"})
	}

	// Force GC and measure baseline
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()
	b.ReportAllocs()

	// Run operations that trigger buffer allocations
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("alloc-key-%d", i)
		stream, err := cache.Get(context.Background(), key, &mockFetcher{})
		if err != nil {
			b.Errorf("Get failed: %v", err)
			continue
		}

		// Read data to trigger buffer usage
		buffer := make([]byte, 1024)
		for {
			_, err := stream.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Errorf("Read failed: %v", err)
				break
			}
		}
		stream.Close()
	}

	b.StopTimer()

	// Force GC and measure final memory
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate and report detailed memory metrics
	totalAlloc := memAfter.TotalAlloc - memBefore.TotalAlloc
	mallocs := memAfter.Mallocs - memBefore.Mallocs
	gcCycles := memAfter.NumGC - memBefore.NumGC

	b.ReportMetric(float64(totalAlloc), "total-alloc-bytes")
	b.ReportMetric(float64(totalAlloc)/float64(b.N), "alloc-per-op-bytes")
	b.ReportMetric(float64(mallocs), "mallocs")
	b.ReportMetric(float64(mallocs)/float64(b.N), "mallocs-per-op")
	b.ReportMetric(float64(gcCycles), "gc-cycles")

	// If buffer pool is enabled, report buffer pool statistics
	if useBufferPool {
		if dc, ok := cache.(*DaramjweeCache); ok && dc.BufferPool != nil {
			if bp, ok := dc.BufferPool.(*DefaultBufferPool); ok {
				stats := bp.GetStats()
				b.ReportMetric(float64(stats.PoolHits), "pool-hits")
				b.ReportMetric(float64(stats.PoolMisses), "pool-misses")
				b.ReportMetric(float64(stats.TotalGets), "pool-gets")
				b.ReportMetric(float64(stats.TotalPuts), "pool-puts")
				b.ReportMetric(float64(stats.ActiveBuffers), "active-buffers")

				// Calculate hit ratio
				if stats.TotalGets > 0 {
					hitRatio := float64(stats.PoolHits) / float64(stats.TotalGets) * 100
					b.ReportMetric(hitRatio, "pool-hit-ratio-percent")
				}
			}
		}
	}
}

// BenchmarkBufferPoolOptimization_GCPressure measures GC pressure
// with and without buffer pool optimization.
func BenchmarkBufferPoolOptimization_GCPressure(b *testing.B) {
	dataSizes := []int{1024, 32 * 1024, 256 * 1024}

	for _, dataSize := range dataSizes {
		sizeName := fmt.Sprintf("%dKB", dataSize/1024)
		b.Run(sizeName, func(b *testing.B) {
			b.Run("WithoutBufferPool", func(b *testing.B) {
				benchmarkGCPressure(b, dataSize, false)
			})
			b.Run("WithBufferPool", func(b *testing.B) {
				benchmarkGCPressure(b, dataSize, true)
			})
		})
	}
}

// benchmarkGCPressure specifically measures garbage collection pressure.
func benchmarkGCPressure(b *testing.B, dataSize int, useBufferPool bool) {
	// Generate test data
	testData := make([]byte, dataSize)
	_, err := rand.Read(testData)
	require.NoError(b, err)

	// Setup cache
	var opts []Option
	if useBufferPool {
		opts = append(opts, WithBufferPool(true, 32*1024))
	}
	cache, _, cold := setupBenchmarkCache(b, opts...)

	// Pre-populate cold store
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("gc-key-%d", i)
		cold.setData(key, string(testData), &Metadata{ETag: "test-etag"})
	}

	// Force initial GC and measure baseline
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()

	// Run intensive operations to trigger GC
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("gc-key-%d", i%numKeys)
		stream, err := cache.Get(context.Background(), key, &mockFetcher{})
		if err != nil {
			b.Errorf("Get failed: %v", err)
			continue
		}

		// Create additional allocation pressure
		buffer := bytes.NewBuffer(make([]byte, 0, dataSize))
		io.Copy(buffer, stream)
		stream.Close()

		// Force some allocations to increase GC pressure
		_ = make([]byte, 1024)
	}

	b.StopTimer()

	// Measure final GC statistics
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Report GC-specific metrics
	gcCycles := memAfter.NumGC - memBefore.NumGC
	totalPauseNs := memAfter.PauseTotalNs - memBefore.PauseTotalNs

	b.ReportMetric(float64(gcCycles), "gc-cycles")
	b.ReportMetric(float64(totalPauseNs)/1e6, "gc-pause-total-ms")

	if gcCycles > 0 {
		avgPauseNs := float64(totalPauseNs) / float64(gcCycles)
		b.ReportMetric(avgPauseNs/1e6, "gc-pause-avg-ms")
	}

	b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
	b.ReportMetric(float64(memAfter.Sys-memBefore.Sys), "sys-bytes")
}
