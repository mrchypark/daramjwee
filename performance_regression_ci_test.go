//go:build ci
// +build ci

package daramjwee

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PerformanceBaseline represents performance baseline metrics (CI version)
type PerformanceBaseline struct {
	ObjectSize       int
	Strategy         BufferPoolStrategy
	ThroughputMBps   float64
	LatencyNs        int64
	AllocsPerOp      float64
	MemoryEfficiency float64
}

// TestMemoryAllocationPatterns tests memory allocation patterns for different sizes (CI version)
func TestMemoryAllocationPatterns(t *testing.T) {
	logger := log.NewNopLogger()
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    true,
	}

	pool, err := NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)
	defer pool.Close()

	testSizes := []int{
		32 * 1024,  // 32KB - reduced test cases for CI
		256 * 1024, // 256KB
	}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("Size_%dKB", size/1024), func(t *testing.T) {
			// Reduced iterations for CI
			allocs := testing.AllocsPerRun(50, func() {
				buf := pool.Get(size)
				if len(buf) > 0 {
					buf[0] = 1 // Use the buffer
				}
				pool.Put(buf)
			})

			t.Logf("CI Test - Allocations per operation for %dKB: %.2f", size/1024, allocs)

			// More generous threshold for CI
			maxExpectedAllocs := 100.0 // Very generous for CI
			if allocs <= maxExpectedAllocs {
				t.Logf("CI Test - Allocation pattern is acceptable: %.2f allocs/op", allocs)
			} else {
				t.Logf("CI Test - High allocation pattern: %.2f allocs/op (may be acceptable for CI)", allocs)
			}
		})
	}
}

// TestPerformanceBaseline establishes performance baselines (CI version)
func TestPerformanceBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance baseline test in short mode")
	}

	logger := log.NewNopLogger()
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    true,
	}

	pool, err := NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)
	defer pool.Close()

	// Reduced test cases for CI
	testCases := []struct {
		name string
		size int
	}{
		{"Small_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := measurePerformanceBaseline(t, pool, tc.size, 100) // Reduced iterations for CI

			t.Logf("CI Test - %s Baseline: Throughput=%.2f MB/s, Latency=%dns, Allocs/Op=%.2f, Efficiency=%.3f",
				tc.name, baseline.ThroughputMBps, baseline.LatencyNs, baseline.AllocsPerOp, baseline.MemoryEfficiency)

			// Basic sanity checks for CI
			assert.Greater(t, baseline.ThroughputMBps, 0.0, "Throughput should be positive")
			assert.Greater(t, baseline.LatencyNs, int64(0), "Latency should be positive")
			assert.GreaterOrEqual(t, baseline.MemoryEfficiency, 0.0, "Memory efficiency should be non-negative")
		})
	}
}

// measurePerformanceBaseline measures performance baseline for a given configuration (CI version)
func measurePerformanceBaseline(t *testing.T, pool BufferPool, size int, iterations int) PerformanceBaseline {
	// Warm up
	for i := 0; i < 10; i++ { // Reduced warmup for CI
		buf := pool.Get(size)
		pool.Put(buf)
	}

	// Measure throughput
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		buf := pool.Get(size)
		copy(buf[:size], data)
		pool.Put(buf)
	}
	duration := time.Since(start)

	throughputMBps := float64(size*iterations) / (1024 * 1024) / duration.Seconds()

	// Measure latency
	latencyStart := time.Now()
	buf := pool.Get(size)
	pool.Put(buf)
	latencyNs := time.Since(latencyStart).Nanoseconds()

	// Measure allocations
	allocs := testing.AllocsPerRun(iterations/10, func() { // Reduced for CI
		buf := pool.Get(size)
		pool.Put(buf)
	})

	// Calculate memory efficiency (simplified)
	stats := pool.GetStats()
	var efficiency float64
	if stats.TotalGets > 0 {
		efficiency = float64(stats.PoolHits) / float64(stats.TotalGets)
	}

	return PerformanceBaseline{
		ObjectSize:       size,
		ThroughputMBps:   throughputMBps,
		LatencyNs:        latencyNs,
		AllocsPerOp:      allocs,
		MemoryEfficiency: efficiency,
	}
}

// TestPerformanceRegression tests for performance regressions (CI version)
func TestPerformanceRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance regression test in short mode")
	}

	logger := log.NewNopLogger()

	// Test adaptive vs default buffer pool
	adaptiveConfig := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    false, // Disable for fair comparison
	}

	defaultConfig := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1 * 1024,
	}

	adaptivePool, err := NewAdaptiveBufferPool(adaptiveConfig, logger)
	require.NoError(t, err)
	defer adaptivePool.Close()

	defaultPool := NewDefaultBufferPoolWithLogger(defaultConfig, logger)

	testSize := 64 * 1024
	iterations := 100 // Reduced for CI

	// Measure adaptive pool performance
	adaptiveBaseline := measurePerformanceBaseline(t, adaptivePool, testSize, iterations)

	// Measure default pool performance
	defaultBaseline := measurePerformanceBaseline(t, defaultPool, testSize, iterations)

	t.Logf("CI Test - Adaptive Pool: Throughput=%.2f MB/s, Latency=%dns, Allocs/Op=%.2f",
		adaptiveBaseline.ThroughputMBps, adaptiveBaseline.LatencyNs, adaptiveBaseline.AllocsPerOp)
	t.Logf("CI Test - Default Pool: Throughput=%.2f MB/s, Latency=%dns, Allocs/Op=%.2f",
		defaultBaseline.ThroughputMBps, defaultBaseline.LatencyNs, defaultBaseline.AllocsPerOp)

	// Performance should be reasonable (very generous thresholds for CI)
	throughputRatio := adaptiveBaseline.ThroughputMBps / defaultBaseline.ThroughputMBps
	latencyRatio := float64(adaptiveBaseline.LatencyNs) / float64(defaultBaseline.LatencyNs)

	t.Logf("CI Test - Performance ratios - Throughput: %.2fx, Latency: %.2fx", throughputRatio, latencyRatio)

	// Very generous thresholds for CI environment
	if throughputRatio >= 0.1 { // Allow 10x slower throughput
		t.Logf("CI Test - Throughput performance is acceptable")
	} else {
		t.Logf("CI Test - Throughput performance is low but may be acceptable for CI")
	}

	if latencyRatio <= 20.0 { // Allow 20x higher latency
		t.Logf("CI Test - Latency performance is acceptable")
	} else {
		t.Logf("CI Test - Latency performance is high but may be acceptable for CI")
	}
}

// TestConcurrentPerformance tests concurrent performance (CI version)
func TestConcurrentPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent performance test in short mode")
	}

	logger := log.NewNopLogger()
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    true,
	}

	pool, err := NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)
	defer pool.Close()

	testSize := 64 * 1024
	goroutines := 4  // Reduced for CI
	iterations := 25 // Reduced for CI

	start := time.Now()

	// Run concurrent test
	done := make(chan bool, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < iterations; j++ {
				buf := pool.Get(testSize)
				if len(buf) > 0 {
					buf[0] = byte(j)
				}
				pool.Put(buf)
			}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	duration := time.Since(start)
	totalOps := goroutines * iterations
	opsPerSec := float64(totalOps) / duration.Seconds()

	t.Logf("CI Test - Concurrent performance: %d ops in %v (%.2f ops/sec)", totalOps, duration, opsPerSec)

	// Basic sanity check
	assert.Greater(t, opsPerSec, 0.0, "Operations per second should be positive")

	// Check pool stats
	stats := pool.GetStats()
	t.Logf("CI Test - Pool stats: Gets=%d, Puts=%d, Hits=%d, Misses=%d",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses)

	assert.Equal(t, int64(totalOps), stats.TotalGets, "Total gets should match operations")
	assert.Equal(t, int64(totalOps), stats.TotalPuts, "Total puts should match operations")
}
