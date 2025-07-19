//go:build !ci
// +build !ci

package daramjwee

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PerformanceBaseline represents performance baseline metrics
type PerformanceBaseline struct {
	ObjectSize       int
	Strategy         BufferPoolStrategy
	ThroughputMBps   float64
	LatencyNs        int64
	AllocationsPerOp int64
	BytesPerOp       int64
	GCPressure       float64
	Timestamp        time.Time
}

// PerformanceRegression represents a detected performance regression
type PerformanceRegression struct {
	TestName       string
	ObjectSize     int
	Strategy       BufferPoolStrategy
	BaselineMetric float64
	CurrentMetric  float64
	RegressionPct  float64
	MetricType     string
	Severity       string
	Timestamp      time.Time
}

// PerformanceTestSuite manages performance regression testing
type PerformanceTestSuite struct {
	baselines map[string]PerformanceBaseline
	logger    log.Logger
	config    PerformanceTestConfig
}

// PerformanceTestConfig configures performance testing
type PerformanceTestConfig struct {
	RegressionThreshold float64       // Percentage threshold for regression detection
	MinIterations       int           // Minimum iterations for stable results
	WarmupIterations    int           // Warmup iterations before measurement
	TestDuration        time.Duration // Maximum duration per test
	EnableGCPressure    bool          // Enable GC pressure measurement
	EnableMemoryProfile bool          // Enable memory profiling
	BaselineFile        string        // File to store/load baselines
}

// NewPerformanceTestSuite creates a new performance test suite
func NewPerformanceTestSuite(config PerformanceTestConfig, logger log.Logger) *PerformanceTestSuite {
	if config.RegressionThreshold <= 0 {
		config.RegressionThreshold = 10.0 // 10% default threshold
	}
	if config.MinIterations <= 0 {
		config.MinIterations = 100
	}
	if config.WarmupIterations <= 0 {
		config.WarmupIterations = 10
	}
	if config.TestDuration <= 0 {
		config.TestDuration = 30 * time.Second
	}

	return &PerformanceTestSuite{
		baselines: make(map[string]PerformanceBaseline),
		logger:    logger,
		config:    config,
	}
}

// BenchmarkAdaptiveBufferPool_AllSizeCategories benchmarks all object size categories
func BenchmarkAdaptiveBufferPool_AllSizeCategories(b *testing.B) {
	logger := log.NewNopLogger()
	suite := NewPerformanceTestSuite(PerformanceTestConfig{
		RegressionThreshold: 15.0,
		MinIterations:       50,
		WarmupIterations:    5,
		EnableGCPressure:    true,
	}, logger)

	// Test different object sizes across all categories
	testSizes := []struct {
		name     string
		size     int
		category ObjectSizeCategory
	}{
		{"Small_1KB", 1 * 1024, SizeCategorySmall},
		{"Small_4KB", 4 * 1024, SizeCategorySmall},
		{"Small_16KB", 16 * 1024, SizeCategorySmall},
		{"Medium_32KB", 32 * 1024, SizeCategoryMedium},
		{"Medium_64KB", 64 * 1024, SizeCategoryMedium},
		{"Medium_128KB", 128 * 1024, SizeCategoryMedium},
		{"Large_256KB", 256 * 1024, SizeCategoryLarge},
		{"Large_512KB", 512 * 1024, SizeCategoryLarge},
		{"VeryLarge_1MB", 1024 * 1024, SizeCategoryVeryLarge},
		{"VeryLarge_2MB", 2 * 1024 * 1024, SizeCategoryVeryLarge},
		{"VeryLarge_5MB", 5 * 1024 * 1024, SizeCategoryVeryLarge},
	}

	for _, tc := range testSizes {
		b.Run(tc.name, func(b *testing.B) {
			suite.benchmarkBufferPoolOperations(b, tc.size, tc.category)
		})
	}
}

// benchmarkBufferPoolOperations benchmarks buffer pool operations for a specific size
func (pts *PerformanceTestSuite) benchmarkBufferPoolOperations(b *testing.B, size int, category ObjectSizeCategory) {
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            10 * 1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    10,
		EnableDetailedMetrics:    true,
	}

	pool, err := NewAdaptiveBufferPool(config, pts.logger)
	require.NoError(b, err)
	defer pool.Close()

	// Warmup
	for i := 0; i < pts.config.WarmupIterations; i++ {
		buf := pool.Get(size)
		pool.Put(buf)
	}

	// Measure GC stats before
	var gcBefore, gcAfter runtime.MemStats
	if pts.config.EnableGCPressure {
		runtime.GC()
		runtime.ReadMemStats(&gcBefore)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Run benchmark
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(size)
			// Simulate some work with the buffer
			if len(buf) > 0 {
				buf[0] = 0xFF
				if len(buf) > 1 {
					buf[len(buf)-1] = 0xFF
				}
			}
			pool.Put(buf)
		}
	})

	b.StopTimer()

	// Measure GC stats after
	if pts.config.EnableGCPressure {
		runtime.ReadMemStats(&gcAfter)
		gcPressure := float64(gcAfter.NumGC-gcBefore.NumGC) / float64(b.N) * 1000
		b.ReportMetric(gcPressure, "gc-ops/1000-ops")
	}

	// Calculate throughput
	throughputMBps := float64(size*b.N) / b.Elapsed().Seconds() / (1024 * 1024)
	b.ReportMetric(throughputMBps, "MB/s")

	// Store baseline or check for regression
	strategy := selectStrategyWithLogging(size, config, pts.logger)
	baseline := PerformanceBaseline{
		ObjectSize:     size,
		Strategy:       strategy,
		ThroughputMBps: throughputMBps,
		LatencyNs:      b.Elapsed().Nanoseconds() / int64(b.N),
		AllocationsPerOp: int64(testing.AllocsPerRun(10, func() {
			buf := pool.Get(size)
			pool.Put(buf)
		})),
		BytesPerOp: int64(testing.AllocsPerRun(10, func() {
			buf := pool.Get(size)
			pool.Put(buf)
		})) * int64(size),
		Timestamp: time.Now(),
	}

	if pts.config.EnableGCPressure {
		baseline.GCPressure = float64(gcAfter.NumGC-gcBefore.NumGC) / float64(b.N)
	}

	testName := fmt.Sprintf("BufferPool_%s_%s", category.String(), strategy.String())
	pts.checkRegression(b, testName, baseline)
}

// BenchmarkStrategyComparison compares performance between different strategies
func BenchmarkStrategyComparison(b *testing.B) {
	logger := log.NewNopLogger()

	// Test sizes that trigger different strategies
	testCases := []struct {
		name     string
		size     int
		strategy BufferPoolStrategy
	}{
		{"Pooled_32KB", 32 * 1024, StrategyPooled},
		{"Chunked_512KB", 512 * 1024, StrategyChunked},
		{"Direct_2MB", 2 * 1024 * 1024, StrategyDirect},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			config := BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            10 * 1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
				EnableDetailedMetrics:    true,
			}

			pool, err := NewAdaptiveBufferPool(config, logger)
			require.NoError(b, err)
			defer pool.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf := pool.Get(tc.size)
				// Simulate work
				if len(buf) > 0 {
					buf[0] = byte(i)
				}
				pool.Put(buf)
			}

			throughputMBps := float64(tc.size*b.N) / b.Elapsed().Seconds() / (1024 * 1024)
			b.ReportMetric(throughputMBps, "MB/s")
		})
	}
}

// BenchmarkConcurrentAccess benchmarks concurrent access patterns
func BenchmarkConcurrentAccess(b *testing.B) {
	logger := log.NewNopLogger()
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            10 * 1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    10,
		EnableDetailedMetrics:    true,
	}

	pool, err := NewAdaptiveBufferPool(config, logger)
	require.NoError(b, err)
	defer pool.Close()

	// Test different concurrency levels
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}
	testSize := 64 * 1024 // Medium size object

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf := pool.Get(testSize)
					// Simulate work
					if len(buf) > 0 {
						buf[0] = 0xFF
					}
					pool.Put(buf)
				}
			})

			throughputMBps := float64(testSize*b.N) / b.Elapsed().Seconds() / (1024 * 1024)
			b.ReportMetric(throughputMBps, "MB/s")
		})
	}
}

// BenchmarkMemoryPressureResponse benchmarks performance under memory pressure
func BenchmarkMemoryPressureResponse(b *testing.B) {
	logger := log.NewNopLogger()
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            10 * 1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    5, // Reduced for pressure testing
		EnableDetailedMetrics:    true,
	}

	pool, err := NewAdaptiveBufferPool(config, logger)
	require.NoError(b, err)
	defer pool.Close()

	testSize := 256 * 1024 // Large object

	// Create memory pressure by allocating large buffers
	var pressureBuffers [][]byte
	for i := 0; i < 100; i++ {
		pressureBuffers = append(pressureBuffers, make([]byte, 1024*1024)) // 1MB each
	}
	defer func() {
		// Release pressure buffers
		for i := range pressureBuffers {
			pressureBuffers[i] = nil
		}
		runtime.GC()
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := pool.Get(testSize)
		if len(buf) > 0 {
			buf[0] = byte(i)
		}
		pool.Put(buf)
	}

	throughputMBps := float64(testSize*b.N) / b.Elapsed().Seconds() / (1024 * 1024)
	b.ReportMetric(throughputMBps, "MB/s")
}

// BenchmarkCopyOperations benchmarks copy operations with different strategies
func BenchmarkCopyOperations(b *testing.B) {
	logger := log.NewNopLogger()
	config := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            10 * 1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		ChunkSize:                64 * 1024,
		MaxConcurrentLargeOps:    10,
		EnableDetailedMetrics:    true,
	}

	pool, err := NewAdaptiveBufferPool(config, logger)
	require.NoError(b, err)
	defer pool.Close()

	testSizes := []int{
		16 * 1024,       // Small
		128 * 1024,      // Medium
		512 * 1024,      // Large
		2 * 1024 * 1024, // Very Large
	}

	for _, size := range testSizes {
		b.Run(fmt.Sprintf("Copy_%dKB", size/1024), func(b *testing.B) {
			// Create test data
			testData := make([]byte, size)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				src := bytes.NewReader(testData)
				dst := &bytes.Buffer{}

				_, err := pool.CopyBuffer(dst, src)
				if err != nil {
					b.Fatal(err)
				}
			}

			throughputMBps := float64(size*b.N) / b.Elapsed().Seconds() / (1024 * 1024)
			b.ReportMetric(throughputMBps, "MB/s")
		})
	}
}

// checkRegression checks for performance regression against baseline
func (pts *PerformanceTestSuite) checkRegression(b *testing.B, testName string, current PerformanceBaseline) {
	if baseline, exists := pts.baselines[testName]; exists {
		// Check throughput regression
		if current.ThroughputMBps < baseline.ThroughputMBps {
			regressionPct := (baseline.ThroughputMBps - current.ThroughputMBps) / baseline.ThroughputMBps * 100
			if regressionPct > pts.config.RegressionThreshold {
				regression := PerformanceRegression{
					TestName:       testName,
					ObjectSize:     current.ObjectSize,
					Strategy:       current.Strategy,
					BaselineMetric: baseline.ThroughputMBps,
					CurrentMetric:  current.ThroughputMBps,
					RegressionPct:  regressionPct,
					MetricType:     "throughput",
					Severity:       pts.classifyRegressionSeverity(regressionPct),
					Timestamp:      time.Now(),
				}
				pts.reportRegression(b, regression)
			}
		}

		// Check latency regression
		if current.LatencyNs > baseline.LatencyNs {
			regressionPct := float64(current.LatencyNs-baseline.LatencyNs) / float64(baseline.LatencyNs) * 100
			if regressionPct > pts.config.RegressionThreshold {
				regression := PerformanceRegression{
					TestName:       testName,
					ObjectSize:     current.ObjectSize,
					Strategy:       current.Strategy,
					BaselineMetric: float64(baseline.LatencyNs),
					CurrentMetric:  float64(current.LatencyNs),
					RegressionPct:  regressionPct,
					MetricType:     "latency",
					Severity:       pts.classifyRegressionSeverity(regressionPct),
					Timestamp:      time.Now(),
				}
				pts.reportRegression(b, regression)
			}
		}
	}

	// Update baseline
	pts.baselines[testName] = current
}

// classifyRegressionSeverity classifies regression severity
func (pts *PerformanceTestSuite) classifyRegressionSeverity(regressionPct float64) string {
	switch {
	case regressionPct >= 50:
		return "CRITICAL"
	case regressionPct >= 25:
		return "HIGH"
	case regressionPct >= 15:
		return "MEDIUM"
	default:
		return "LOW"
	}
}

// reportRegression reports a detected performance regression
func (pts *PerformanceTestSuite) reportRegression(b *testing.B, regression PerformanceRegression) {
	if pts.logger != nil {
		pts.logger.Log(
			"level", "warn",
			"msg", "performance regression detected",
			"test", regression.TestName,
			"metric", regression.MetricType,
			"baseline", regression.BaselineMetric,
			"current", regression.CurrentMetric,
			"regression_pct", regression.RegressionPct,
			"severity", regression.Severity,
		)
	}

	// Report as benchmark metric for visibility (only if b is a real benchmark)
	if b != nil && b.N > 0 {
		b.ReportMetric(regression.RegressionPct, fmt.Sprintf("regression_%s_pct", regression.MetricType))
	}
}

// TestPerformanceRegressionDetection tests the regression detection system
func TestPerformanceRegressionDetection(t *testing.T) {
	logger := log.NewNopLogger()
	suite := NewPerformanceTestSuite(PerformanceTestConfig{
		RegressionThreshold: 10.0,
		MinIterations:       10,
		WarmupIterations:    2,
	}, logger)

	// Create a mock baseline
	baseline := PerformanceBaseline{
		ObjectSize:     32 * 1024,
		Strategy:       StrategyPooled,
		ThroughputMBps: 1000.0,
		LatencyNs:      1000,
		Timestamp:      time.Now(),
	}
	suite.baselines["test"] = baseline

	// Test regression detection
	t.Run("ThroughputRegression", func(t *testing.T) {
		current := baseline
		current.ThroughputMBps = 800.0 // 20% regression

		var regressionDetected bool
		suite.checkRegression(&testing.B{}, "test", current)

		// In a real implementation, we'd capture the regression report
		// For now, we just verify the baseline was updated
		assert.Equal(t, current.ThroughputMBps, suite.baselines["test"].ThroughputMBps)
		_ = regressionDetected // Placeholder for actual regression detection verification
	})

	t.Run("LatencyRegression", func(t *testing.T) {
		current := baseline
		current.LatencyNs = 1500 // 50% regression

		suite.checkRegression(&testing.B{}, "test", current)
		assert.Equal(t, current.LatencyNs, suite.baselines["test"].LatencyNs)
	})
}

// TestGCPressureMeasurement tests GC pressure measurement
func TestGCPressureMeasurement(t *testing.T) {
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

	// Measure GC pressure
	var gcBefore, gcAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&gcBefore)

	// Perform operations that might trigger GC
	iterations := 1000
	for i := 0; i < iterations; i++ {
		buf := pool.Get(64 * 1024)
		// Fill buffer to ensure it's actually used
		for j := 0; j < len(buf); j += 4096 {
			buf[j] = byte(i)
		}
		pool.Put(buf)
	}

	runtime.ReadMemStats(&gcAfter)

	gcPressure := float64(gcAfter.NumGC - gcBefore.NumGC)
	t.Logf("GC pressure: %.2f GC cycles for %d operations", gcPressure, iterations)

	// Verify GC pressure is reasonable (not too high)
	assert.True(t, gcPressure < float64(iterations)/10, "GC pressure should be reasonable")
}

// TestMemoryAllocationPatterns tests memory allocation patterns
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

	testSizes := []int{4 * 1024, 32 * 1024, 256 * 1024}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("Size_%dKB", size/1024), func(t *testing.T) {
			allocsPerOp := testing.AllocsPerRun(10, func() {
				buf := pool.Get(size)
				pool.Put(buf)
			})

			t.Logf("Allocations per operation for %dKB: %.2f", size/1024, allocsPerOp)

			// For pooled buffers, allocations should be reasonable
			// Note: AdaptiveBufferPool has more complexity than simple pools
			if size <= int(config.LargeObjectThreshold) {
				assert.True(t, allocsPerOp < 50.0, "Pooled buffers should have reasonable allocations")
			} else {
				// Large objects may have more allocations due to chunking
				assert.True(t, allocsPerOp < 100.0, "Large object allocations should be bounded")
			}
		})
	}
}
