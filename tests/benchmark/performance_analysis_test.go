package benchmark

import (
	"github.com/mrchypark/daramjwee"
	"bytes"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

// TestPerformanceAnalysis analyzes why performance improvements are not linear
func TestPerformanceAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance analysis in short mode")
	}

	logger := log.NewNopLogger()

	testSizes := []int{
		256 * 1024,       // 256KB - Large threshold
		512 * 1024,       // 512KB
		1024 * 1024,      // 1MB - Very large threshold
		2 * 1024 * 1024,  // 2MB
		5 * 1024 * 1024,  // 5MB
		10 * 1024 * 1024, // 10MB
		20 * 1024 * 1024, // 20MB
	}

	t.Log("=== Performance Analysis ===")
	t.Log("Size\t\tDefault(ms)\tAdaptive(ms)\tRatio\tStrategy")

	for _, size := range testSizes {
		// Analyze strategy selection
		strategy := analyzeStrategy(size)

		// Measure performance
		defaultTime := measureDirectBufferPerformance(t, logger, size, false)
		adaptiveTime := measureDirectBufferPerformance(t, logger, size, true)

		ratio := float64(adaptiveTime) / float64(defaultTime)

		t.Logf("%dKB\t\t%.2f\t\t%.2f\t\t%.3f\t%s",
			size/1024,
			float64(defaultTime.Nanoseconds())/1e6,
			float64(adaptiveTime.Nanoseconds())/1e6,
			ratio,
			strategy)
	}

	// Analyze memory allocation patterns
	t.Log("\n=== Memory Allocation Analysis ===")
	analyzeMemoryAllocation(t, logger)

	// Analyze GC impact
	t.Log("\n=== GC Impact Analysis ===")
	analyzeGCImpact(t, logger)

	// Analyze chunk size impact
	t.Log("\n=== Chunk Size Impact Analysis ===")
	analyzeChunkSizeImpact(t, logger)
}

func analyzeStrategy(size int) string {
	if size < 32*1024 {
		return "Small(Pooled)"
	} else if size < 256*1024 {
		return "Medium(Pooled)"
	} else if size < 1024*1024 {
		return "Large(Chunked)"
	} else {
		return "VeryLarge(Direct)"
	}
}

func measureDirectBufferPerformance(t *testing.T, logger log.Logger, size int, useAdaptive bool) time.Duration {
	var bufferPool BufferPool

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
		bufferPool = NewDefaultBufferPooldaramjwee.WithLogger(config, logger)
	}

	// Warm up
	for i := 0; i < 3; i++ {
		buf := bufferPool.Get(size)
		bufferPool.Put(buf)
	}

	// Measure multiple iterations for accuracy
	iterations := 100
	start := time.Now()

	for i := 0; i < iterations; i++ {
		buf := bufferPool.Get(size)
		// Simulate minimal work to avoid skewing results
		if len(buf) > 0 {
			buf[0] = byte(i % 256)
		}
		bufferPool.Put(buf)
	}

	return time.Since(start) / time.Duration(iterations)
}

func analyzeMemoryAllocation(t *testing.T, logger log.Logger) {
	testSizes := []int{256 * 1024, 1024 * 1024, 5 * 1024 * 1024}

	for _, size := range testSizes {
		t.Logf("\n--- Memory Analysis for %dKB ---", size/1024)

		// Test default buffer pool
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		defaultPool := NewDefaultBufferPooldaramjwee.WithLogger(daramjwee.BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     128 * 1024,
			MinBufferSize:     4 * 1024,
		}, logger)

		for i := 0; i < 10; i++ {
			buf := defaultPool.Get(size)
			defaultPool.Put(buf)
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)
		defaultAlloc := m2.TotalAlloc - m1.TotalAlloc

		// Test adaptive buffer pool
		runtime.GC()
		runtime.ReadMemStats(&m1)

		adaptivePool, err := NewAdaptiveBufferPoolImpl(daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                64 * 1024,
			MaxConcurrentLargeOps:    4,
		}, logger)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			buf := adaptivePool.Get(size)
			adaptivePool.Put(buf)
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)
		adaptiveAlloc := m2.TotalAlloc - m1.TotalAlloc

		adaptivePool.Close()

		t.Logf("Default allocation: %d bytes", defaultAlloc)
		t.Logf("Adaptive allocation: %d bytes", adaptiveAlloc)
		t.Logf("Allocation ratio: %.3f", float64(adaptiveAlloc)/float64(defaultAlloc))
	}
}

func analyzeGCImpact(t *testing.T, logger log.Logger) {
	testSizes := []int{1024 * 1024, 5 * 1024 * 1024, 10 * 1024 * 1024}

	for _, size := range testSizes {
		t.Logf("\n--- GC Analysis for %dKB ---", size/1024)

		// Test default buffer pool GC impact
		var gcStats1, gcStats2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&gcStats1)

		defaultPool := NewDefaultBufferPooldaramjwee.WithLogger(daramjwee.BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     128 * 1024,
			MinBufferSize:     4 * 1024,
		}, logger)

		start := time.Now()
		for i := 0; i < 50; i++ {
			buf := defaultPool.Get(size)
			// Simulate some work
			if len(buf) > 1000 {
				for j := 0; j < 1000; j++ {
					buf[j] = byte(j % 256)
				}
			}
			defaultPool.Put(buf)
		}
		defaultTime := time.Since(start)

		runtime.GC()
		runtime.ReadMemStats(&gcStats2)
		defaultGCs := gcStats2.NumGC - gcStats1.NumGC

		// Test adaptive buffer pool GC impact
		runtime.GC()
		runtime.ReadMemStats(&gcStats1)

		adaptivePool, err := NewAdaptiveBufferPoolImpl(daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                64 * 1024,
			MaxConcurrentLargeOps:    4,
		}, logger)
		require.NoError(t, err)

		start = time.Now()
		for i := 0; i < 50; i++ {
			buf := adaptivePool.Get(size)
			// Simulate some work
			if len(buf) > 1000 {
				for j := 0; j < 1000; j++ {
					buf[j] = byte(j % 256)
				}
			}
			adaptivePool.Put(buf)
		}
		adaptiveTime := time.Since(start)

		runtime.GC()
		runtime.ReadMemStats(&gcStats2)
		adaptiveGCs := gcStats2.NumGC - gcStats1.NumGC

		adaptivePool.Close()

		t.Logf("Default: %v, GCs: %d", defaultTime, defaultGCs)
		t.Logf("Adaptive: %v, GCs: %d", adaptiveTime, adaptiveGCs)
		t.Logf("Time ratio: %.3f, GC ratio: %.3f",
			float64(adaptiveTime)/float64(defaultTime),
			float64(adaptiveGCs)/float64(maxUint32(defaultGCs, 1)))
	}
}

func analyzeChunkSizeImpact(t *testing.T, logger log.Logger) {
	dataSize := 5 * 1024 * 1024 // 5MB test data
	chunkSizes := []int{32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024}

	testData := createLargeTestData(dataSize)

	t.Logf("\n--- Chunk Size Impact Analysis (5MB data) ---")
	t.Logf("ChunkSize\tTime(ms)\tThroughput(MB/s)")

	for _, chunkSize := range chunkSizes {
		config := daramjwee.BufferPoolConfig{
			Enabled:                  true,
			DefaultBufferSize:        32 * 1024,
			MaxBufferSize:            128 * 1024,
			MinBufferSize:            4 * 1024,
			LargeObjectThreshold:     256 * 1024,
			VeryLargeObjectThreshold: 1024 * 1024,
			ChunkSize:                chunkSize,
			MaxConcurrentLargeOps:    4,
		}

		pool, err := NewAdaptiveBufferPoolImpl(config, logger)
		require.NoError(t, err)

		// Measure copy performance
		start := time.Now()
		for i := 0; i < 10; i++ {
			src := strings.NewReader(testData)
			dst := &bytes.Buffer{}
			_, err := pool.CopyBuffer(dst, src)
			require.NoError(t, err)
		}
		elapsed := time.Since(start) / 10 // Average per operation

		pool.Close()

		throughput := float64(dataSize) / (1024 * 1024) / elapsed.Seconds() // MB/s
		t.Logf("%dKB\t\t%.2f\t\t%.2f",
			chunkSize/1024,
			float64(elapsed.Nanoseconds())/1e6,
			throughput)
	}
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TestStrategyTransitionPoints tests performance at strategy transition points
func TestStrategyTransitionPoints(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping strategy transition test in short mode")
	}

	logger := log.NewNopLogger()

	// Test sizes around transition points
	transitionSizes := []struct {
		name string
		size int
	}{
		{"Just_Below_Large", 255 * 1024},      // Just below large threshold
		{"Just_Above_Large", 257 * 1024},      // Just above large threshold
		{"Just_Below_VeryLarge", 1023 * 1024}, // Just below very large threshold
		{"Just_Above_VeryLarge", 1025 * 1024}, // Just above very large threshold
	}

	t.Log("=== Strategy Transition Analysis ===")
	t.Log("Size\t\tStrategy\tTime(ns)\tAllocs")

	for _, ts := range transitionSizes {
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

		pool, err := NewAdaptiveBufferPoolImpl(config, logger)
		require.NoError(t, err)

		// Measure performance
		start := time.Now()
		for i := 0; i < 1000; i++ {
			buf := pool.Get(ts.size)
			pool.Put(buf)
		}
		elapsed := time.Since(start) / 1000

		// Get stats to see strategy usage
		stats := pool.GetStats()

		strategy := "Unknown"
		if stats.PooledOperations > 0 {
			strategy = "Pooled"
		} else if stats.ChunkedOperations > 0 {
			strategy = "Chunked"
		} else if stats.DirectOperations > 0 {
			strategy = "Direct"
		}

		pool.Close()

		t.Logf("%s\t%s\t\t%d\t\t%d",
			ts.name, strategy, elapsed.Nanoseconds(), stats.PoolMisses)
	}
}
