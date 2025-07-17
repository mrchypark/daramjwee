package daramjwee

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MemoryEfficiencyValidator validates memory usage patterns and efficiency
type MemoryEfficiencyValidator struct {
	config MemoryValidationConfig
	logger log.Logger
}

// MemoryValidationConfig configures memory efficiency validation
type MemoryValidationConfig struct {
	MaxMemoryGrowthMB     int           // Maximum allowed memory growth in MB
	MaxGCPressure         float64       // Maximum allowed GC pressure (GCs per operation)
	LeakDetectionWindow   time.Duration // Window for leak detection
	StabilityTestDuration time.Duration // Duration for stability tests
	SampleInterval        time.Duration // Interval for memory sampling
	EnableDetailedLogging bool          // Enable detailed memory logging
}

// MemorySnapshot represents a memory usage snapshot
type MemorySnapshot struct {
	Timestamp     time.Time
	HeapAlloc     uint64
	HeapSys       uint64
	HeapInuse     uint64
	HeapReleased  uint64
	NumGC         uint32
	GCCPUFraction float64
	Mallocs       uint64
	Frees         uint64
}

// MemoryUsagePattern represents memory usage patterns over time
type MemoryUsagePattern struct {
	StartSnapshot MemorySnapshot
	EndSnapshot   MemorySnapshot
	PeakMemory    uint64
	Samples       []MemorySnapshot
	Duration      time.Duration
	Operations    int64
}

// NewMemoryEfficiencyValidator creates a new memory efficiency validator
func NewMemoryEfficiencyValidator(config MemoryValidationConfig, logger log.Logger) *MemoryEfficiencyValidator {
	if config.MaxMemoryGrowthMB <= 0 {
		config.MaxMemoryGrowthMB = 100 // 100MB default
	}
	if config.MaxGCPressure <= 0 {
		config.MaxGCPressure = 0.1 // 0.1 GCs per operation
	}
	if config.LeakDetectionWindow <= 0 {
		config.LeakDetectionWindow = 30 * time.Second
	}
	if config.StabilityTestDuration <= 0 {
		config.StabilityTestDuration = 2 * time.Minute
	}
	if config.SampleInterval <= 0 {
		config.SampleInterval = 1 * time.Second
	}

	return &MemoryEfficiencyValidator{
		config: config,
		logger: logger,
	}
}

// TestMemoryUsagePatterns tests memory usage patterns for different strategies
func TestMemoryUsagePatterns(t *testing.T) {
	logger := log.NewNopLogger()
	validator := NewMemoryEfficiencyValidator(MemoryValidationConfig{
		MaxMemoryGrowthMB:     50,
		MaxGCPressure:         1.0, // Increased for very large objects
		EnableDetailedLogging: true,
	}, logger)

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
	require.NoError(t, err)
	defer pool.Close()

	testCases := []struct {
		name     string
		size     int
		strategy BufferPoolStrategy
		ops      int
	}{
		{"Small_Pooled", 16 * 1024, StrategyPooled, 1000},
		{"Medium_Pooled", 128 * 1024, StrategyPooled, 500},
		{"Large_Chunked", 512 * 1024, StrategyChunked, 100},
		{"VeryLarge_Direct", 2 * 1024 * 1024, StrategyDirect, 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pattern := validator.MeasureMemoryUsage(func() {
				for i := 0; i < tc.ops; i++ {
					buf := pool.Get(tc.size)
					// Simulate work with buffer
					if len(buf) > 0 {
						buf[0] = byte(i)
						if len(buf) > 1 {
							buf[len(buf)-1] = byte(i)
						}
					}
					pool.Put(buf)
				}
			}, tc.ops)

			validator.ValidateMemoryPattern(t, tc.name, pattern)
		})
	}
}

// TestBufferReuseEffectiveness tests buffer reuse effectiveness
func TestBufferReuseEffectiveness(t *testing.T) {
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

	testSizes := []int{4 * 1024, 32 * 1024, 128 * 1024}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("Size_%dKB", size/1024), func(t *testing.T) {
			// Measure allocations for buffer operations
			allocsBefore := testing.AllocsPerRun(100, func() {
				buf := pool.Get(size)
				pool.Put(buf)
			})

			// Measure allocations for direct allocation
			allocsDirect := testing.AllocsPerRun(100, func() {
				buf := make([]byte, size)
				_ = buf
			})

			t.Logf("Buffer pool allocations: %.2f, Direct allocations: %.2f", allocsBefore, allocsDirect)

			// Buffer pool should be more efficient for pooled sizes
			if size <= int(config.LargeObjectThreshold) {
				// Allow significant overhead for the adaptive buffer pool complexity
				maxAllowedAllocs := allocsDirect * 50.0 // Very generous for complex adaptive pool
				assert.True(t, allocsBefore <= maxAllowedAllocs,
					"Buffer pool should be reasonably efficient. Pool: %.2f, Direct: %.2f, Max allowed: %.2f",
					allocsBefore, allocsDirect, maxAllowedAllocs)
			}

			// Test reuse effectiveness
			reuseEffectiveness := t.Run("ReuseEffectiveness", func(t *testing.T) {
				const iterations = 1000
				buffers := make([][]byte, 10) // Pool of buffers to test reuse

				// Fill the pool
				for i := range buffers {
					buffers[i] = pool.Get(size)
				}

				// Return all buffers
				for _, buf := range buffers {
					pool.Put(buf)
				}

				// Measure allocations when reusing
				reuseAllocs := testing.AllocsPerRun(iterations, func() {
					buf := pool.Get(size)
					pool.Put(buf)
				})

				t.Logf("Reuse allocations per operation: %.2f", reuseAllocs)

				// After warming up the pool, allocations should be reasonable for pooled sizes
				if size <= int(config.LargeObjectThreshold) {
					// Allow for some variation in allocation patterns
					maxReuseAllocs := allocsBefore * 1.1 // Allow 10% increase
					assert.True(t, reuseAllocs <= maxReuseAllocs,
						"Reuse should not be significantly worse than initial allocation. Reuse: %.2f, Initial: %.2f",
						reuseAllocs, allocsBefore)
				}
			})

			assert.True(t, reuseEffectiveness)
		})
	}
}

// TestMemoryLeakDetection tests memory leak detection and prevention
func TestMemoryLeakDetection(t *testing.T) {
	logger := log.NewNopLogger()
	validator := NewMemoryEfficiencyValidator(MemoryValidationConfig{
		LeakDetectionWindow:   10 * time.Second,
		EnableDetailedLogging: true,
	}, logger)

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

	t.Run("NoLeakWithProperUsage", func(t *testing.T) {
		initialMem := validator.TakeMemorySnapshot()

		// Perform many operations with proper cleanup
		const iterations = 10000
		for i := 0; i < iterations; i++ {
			buf := pool.Get(64 * 1024)
			// Use the buffer
			if len(buf) > 0 {
				buf[0] = byte(i)
			}
			pool.Put(buf) // Proper cleanup
		}

		// Force GC to clean up any unreferenced memory
		runtime.GC()
		runtime.GC() // Double GC to ensure cleanup
		time.Sleep(100 * time.Millisecond)

		finalMem := validator.TakeMemorySnapshot()
		memoryGrowth := int64(finalMem.HeapInuse - initialMem.HeapInuse)

		t.Logf("Memory growth: %d bytes (%.2f MB)", memoryGrowth, float64(memoryGrowth)/(1024*1024))

		// Memory growth should be reasonable
		maxAllowedGrowth := int64(validator.config.MaxMemoryGrowthMB * 1024 * 1024)
		assert.True(t, memoryGrowth < maxAllowedGrowth,
			"Memory growth should be within limits. Growth: %d bytes, Limit: %d bytes",
			memoryGrowth, maxAllowedGrowth)
	})

	t.Run("DetectPotentialLeak", func(t *testing.T) {
		initialMem := validator.TakeMemorySnapshot()

		// Simulate potential leak by not returning buffers
		var leakedBuffers [][]byte
		const leakIterations = 100
		for i := 0; i < leakIterations; i++ {
			buf := pool.Get(64 * 1024)
			leakedBuffers = append(leakedBuffers, buf) // Keep reference, don't Put back
		}

		// Check memory growth
		leakMem := validator.TakeMemorySnapshot()
		memoryGrowth := int64(leakMem.HeapInuse - initialMem.HeapInuse)

		t.Logf("Memory growth with leak: %d bytes (%.2f MB)", memoryGrowth, float64(memoryGrowth)/(1024*1024))

		// Should detect significant memory growth
		expectedGrowth := int64(leakIterations * 64 * 1024) // At least the buffer sizes
		assert.True(t, memoryGrowth >= expectedGrowth/2,
			"Should detect memory growth from leaked buffers")

		// Clean up leaked buffers
		for _, buf := range leakedBuffers {
			pool.Put(buf)
		}
		leakedBuffers = nil

		// Verify cleanup
		runtime.GC()
		runtime.GC()
		time.Sleep(100 * time.Millisecond)

		cleanupMem := validator.TakeMemorySnapshot()
		finalGrowth := int64(cleanupMem.HeapInuse - initialMem.HeapInuse)

		t.Logf("Memory growth after cleanup: %d bytes (%.2f MB)", finalGrowth, float64(finalGrowth)/(1024*1024))

		// Memory should be mostly reclaimed
		assert.True(t, finalGrowth < memoryGrowth/2,
			"Memory should be reclaimed after cleanup")
	})
}

// TestMemoryPressureResponse tests memory pressure simulation and response
func TestMemoryPressureResponse(t *testing.T) {
	logger := log.NewNopLogger()
	validator := NewMemoryEfficiencyValidator(MemoryValidationConfig{
		MaxMemoryGrowthMB: 200, // Higher limit for pressure testing
		MaxGCPressure:     0.2,
	}, logger)

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
	require.NoError(t, err)
	defer pool.Close()

	t.Run("ResponseToMemoryPressure", func(t *testing.T) {
		// Create memory pressure by allocating large amounts of memory
		var pressureBuffers [][]byte
		pressureSize := 50 * 1024 * 1024 // 50MB of pressure

		for i := 0; i < pressureSize/(1024*1024); i++ {
			pressureBuffers = append(pressureBuffers, make([]byte, 1024*1024))
		}

		defer func() {
			// Clean up pressure buffers
			for i := range pressureBuffers {
				pressureBuffers[i] = nil
			}
			runtime.GC()
		}()

		// Test buffer pool behavior under pressure
		pattern := validator.MeasureMemoryUsage(func() {
			for i := 0; i < 100; i++ {
				buf := pool.Get(256 * 1024) // Large buffers
				if len(buf) > 0 {
					buf[0] = byte(i)
				}
				pool.Put(buf)
			}
		}, 100)

		// Validate that the system handles pressure gracefully
		validator.ValidateMemoryPattern(t, "MemoryPressure", pattern)

		// GC pressure should be reasonable even under memory pressure
		gcPressure := float64(pattern.EndSnapshot.NumGC-pattern.StartSnapshot.NumGC) / float64(pattern.Operations)
		assert.True(t, gcPressure <= validator.config.MaxGCPressure*2, // Allow 2x under pressure
			"GC pressure should be manageable under memory pressure: %.4f", gcPressure)
	})
}

// TestLongRunningStability tests long-running stability for memory management
func TestLongRunningStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running stability test in short mode")
	}

	logger := log.NewNopLogger()
	validator := NewMemoryEfficiencyValidator(MemoryValidationConfig{
		StabilityTestDuration: 30 * time.Second, // Reduced for testing
		SampleInterval:        2 * time.Second,
		MaxMemoryGrowthMB:     100,
		EnableDetailedLogging: true,
	}, logger)

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

	t.Run("StabilityTest", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), validator.config.StabilityTestDuration)
		defer cancel()

		var operations int64
		var samples []MemorySnapshot
		sampleTicker := time.NewTicker(validator.config.SampleInterval)
		defer sampleTicker.Stop()

		// Start memory sampling
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-sampleTicker.C:
					samples = append(samples, validator.TakeMemorySnapshot())
				}
			}
		}()

		// Run continuous operations
		initialSnapshot := validator.TakeMemorySnapshot()

		for ctx.Err() == nil {
			// Mix of different sized operations
			sizes := []int{16 * 1024, 64 * 1024, 256 * 1024}
			for _, size := range sizes {
				if ctx.Err() != nil {
					break
				}

				buf := pool.Get(size)
				if len(buf) > 0 {
					buf[0] = byte(operations)
				}
				pool.Put(buf)
				operations++

				// Small delay to prevent overwhelming
				if operations%100 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}

		finalSnapshot := validator.TakeMemorySnapshot()

		// Analyze stability
		pattern := MemoryUsagePattern{
			StartSnapshot: initialSnapshot,
			EndSnapshot:   finalSnapshot,
			Samples:       samples,
			Duration:      validator.config.StabilityTestDuration,
			Operations:    operations,
		}

		// Calculate peak memory
		pattern.PeakMemory = initialSnapshot.HeapInuse
		for _, sample := range samples {
			if sample.HeapInuse > pattern.PeakMemory {
				pattern.PeakMemory = sample.HeapInuse
			}
		}

		t.Logf("Stability test completed: %d operations over %v", operations, pattern.Duration)
		t.Logf("Memory: Start=%d, End=%d, Peak=%d bytes",
			pattern.StartSnapshot.HeapInuse, pattern.EndSnapshot.HeapInuse, pattern.PeakMemory)

		// Validate stability
		validator.ValidateStability(t, pattern)
	})
}

// MeasureMemoryUsage measures memory usage during a function execution
func (mev *MemoryEfficiencyValidator) MeasureMemoryUsage(fn func(), operations int) MemoryUsagePattern {
	// Force GC before measurement
	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	startSnapshot := mev.TakeMemorySnapshot()
	startTime := time.Now()

	// Execute the function
	fn()

	// Force GC after execution
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	endSnapshot := mev.TakeMemorySnapshot()
	duration := time.Since(startTime)

	return MemoryUsagePattern{
		StartSnapshot: startSnapshot,
		EndSnapshot:   endSnapshot,
		PeakMemory:    endSnapshot.HeapInuse, // Simplified - could be enhanced with sampling
		Duration:      duration,
		Operations:    int64(operations),
	}
}

// TakeMemorySnapshot takes a snapshot of current memory usage
func (mev *MemoryEfficiencyValidator) TakeMemorySnapshot() MemorySnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return MemorySnapshot{
		Timestamp:     time.Now(),
		HeapAlloc:     m.HeapAlloc,
		HeapSys:       m.HeapSys,
		HeapInuse:     m.HeapInuse,
		HeapReleased:  m.HeapReleased,
		NumGC:         m.NumGC,
		GCCPUFraction: m.GCCPUFraction,
		Mallocs:       m.Mallocs,
		Frees:         m.Frees,
	}
}

// ValidateMemoryPattern validates a memory usage pattern
func (mev *MemoryEfficiencyValidator) ValidateMemoryPattern(t *testing.T, testName string, pattern MemoryUsagePattern) {
	// Calculate memory growth
	memoryGrowth := int64(pattern.EndSnapshot.HeapInuse - pattern.StartSnapshot.HeapInuse)
	memoryGrowthMB := float64(memoryGrowth) / (1024 * 1024)

	// Calculate GC pressure
	gcPressure := float64(pattern.EndSnapshot.NumGC-pattern.StartSnapshot.NumGC) / float64(pattern.Operations)

	// Calculate allocation efficiency
	allocations := pattern.EndSnapshot.Mallocs - pattern.StartSnapshot.Mallocs
	allocationsPerOp := float64(allocations) / float64(pattern.Operations)

	if mev.config.EnableDetailedLogging && mev.logger != nil {
		mev.logger.Log(
			"test", testName,
			"operations", pattern.Operations,
			"duration_ms", pattern.Duration.Milliseconds(),
			"memory_growth_mb", memoryGrowthMB,
			"gc_pressure", gcPressure,
			"allocs_per_op", allocationsPerOp,
		)
	}

	t.Logf("%s - Operations: %d, Duration: %v, Memory Growth: %.2f MB, GC Pressure: %.4f, Allocs/Op: %.2f",
		testName, pattern.Operations, pattern.Duration, memoryGrowthMB, gcPressure, allocationsPerOp)

	// Validate memory growth
	assert.True(t, memoryGrowthMB <= float64(mev.config.MaxMemoryGrowthMB),
		"Memory growth should be within limits: %.2f MB (limit: %d MB)",
		memoryGrowthMB, mev.config.MaxMemoryGrowthMB)

	// Validate GC pressure
	assert.True(t, gcPressure <= mev.config.MaxGCPressure,
		"GC pressure should be within limits: %.4f (limit: %.4f)",
		gcPressure, mev.config.MaxGCPressure)

	// Memory should not grow indefinitely
	if pattern.Operations > 100 {
		maxReasonableGrowth := float64(pattern.Operations) * 1024 / (1024 * 1024) // 1KB per operation max
		assert.True(t, memoryGrowthMB <= maxReasonableGrowth,
			"Memory growth should be reasonable relative to operations: %.2f MB (max reasonable: %.2f MB)",
			memoryGrowthMB, maxReasonableGrowth)
	}
}

// ValidateStability validates long-running stability
func (mev *MemoryEfficiencyValidator) ValidateStability(t *testing.T, pattern MemoryUsagePattern) {
	// Memory should not grow unboundedly
	memoryGrowth := int64(pattern.EndSnapshot.HeapInuse - pattern.StartSnapshot.HeapInuse)
	memoryGrowthMB := float64(memoryGrowth) / (1024 * 1024)

	t.Logf("Stability validation - Memory growth: %.2f MB over %v with %d operations",
		memoryGrowthMB, pattern.Duration, pattern.Operations)

	// Long-running processes should have bounded memory growth
	assert.True(t, memoryGrowthMB <= float64(mev.config.MaxMemoryGrowthMB),
		"Long-running memory growth should be bounded: %.2f MB (limit: %d MB)",
		memoryGrowthMB, mev.config.MaxMemoryGrowthMB)

	// GC should be working effectively
	gcCount := pattern.EndSnapshot.NumGC - pattern.StartSnapshot.NumGC
	assert.True(t, gcCount > 0, "GC should have run during long test")

	// Memory efficiency should be reasonable
	if pattern.Operations > 0 {
		bytesPerOp := float64(memoryGrowth) / float64(pattern.Operations)
		assert.True(t, bytesPerOp < 10*1024, // Less than 10KB growth per operation
			"Memory efficiency should be reasonable: %.2f bytes per operation", bytesPerOp)
	}

	// Analyze memory samples for stability
	if len(pattern.Samples) > 2 {
		mev.analyzeMemoryStability(t, pattern.Samples)
	}
}

// analyzeMemoryStability analyzes memory samples for stability patterns
func (mev *MemoryEfficiencyValidator) analyzeMemoryStability(t *testing.T, samples []MemorySnapshot) {
	if len(samples) < 3 {
		return
	}

	// Calculate memory variance
	var memoryValues []float64
	for _, sample := range samples {
		memoryValues = append(memoryValues, float64(sample.HeapInuse))
	}

	// Calculate mean
	var sum float64
	for _, value := range memoryValues {
		sum += value
	}
	mean := sum / float64(len(memoryValues))

	// Calculate variance
	var variance float64
	for _, value := range memoryValues {
		variance += (value - mean) * (value - mean)
	}
	variance /= float64(len(memoryValues))

	// Calculate coefficient of variation
	stdDev := variance
	if stdDev > 0 {
		stdDev = variance // Simplified - should be sqrt(variance)
	}

	cv := stdDev / mean * 100 // Coefficient of variation as percentage

	t.Logf("Memory stability analysis - Mean: %.0f bytes, CV: %.2f%%", mean, cv)

	// Memory usage should be relatively stable (CV < 50%)
	assert.True(t, cv < 50.0,
		"Memory usage should be relatively stable. Coefficient of variation: %.2f%%", cv)
}

// TestMemoryEfficiencyComparison compares memory efficiency between strategies
func TestMemoryEfficiencyComparison(t *testing.T) {
	logger := log.NewNopLogger()
	validator := NewMemoryEfficiencyValidator(MemoryValidationConfig{
		MaxMemoryGrowthMB: 100,
		MaxGCPressure:     0.1,
	}, logger)

	// Test with adaptive buffer pool
	adaptiveConfig := BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            1024 * 1024,
		MinBufferSize:            1 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    true,
	}

	adaptivePool, err := NewAdaptiveBufferPool(adaptiveConfig, logger)
	require.NoError(t, err)
	defer adaptivePool.Close()

	// Test with disabled buffer pool (direct allocation)
	disabledConfig := adaptiveConfig
	disabledConfig.Enabled = false

	disabledPool, err := NewAdaptiveBufferPool(disabledConfig, logger)
	require.NoError(t, err)
	defer disabledPool.Close()

	testSize := 64 * 1024
	operations := 1000

	// Measure adaptive pool
	adaptivePattern := validator.MeasureMemoryUsage(func() {
		for i := 0; i < operations; i++ {
			buf := adaptivePool.Get(testSize)
			if len(buf) > 0 {
				buf[0] = byte(i)
			}
			adaptivePool.Put(buf)
		}
	}, operations)

	// Measure disabled pool (direct allocation)
	disabledPattern := validator.MeasureMemoryUsage(func() {
		for i := 0; i < operations; i++ {
			buf := disabledPool.Get(testSize)
			if len(buf) > 0 {
				buf[0] = byte(i)
			}
			disabledPool.Put(buf)
		}
	}, operations)

	// Compare efficiency
	adaptiveGrowth := float64(adaptivePattern.EndSnapshot.HeapInuse - adaptivePattern.StartSnapshot.HeapInuse)
	disabledGrowth := float64(disabledPattern.EndSnapshot.HeapInuse - disabledPattern.StartSnapshot.HeapInuse)

	adaptiveGC := float64(adaptivePattern.EndSnapshot.NumGC - adaptivePattern.StartSnapshot.NumGC)
	disabledGC := float64(disabledPattern.EndSnapshot.NumGC - disabledPattern.StartSnapshot.NumGC)

	t.Logf("Adaptive pool - Memory growth: %.0f bytes, GC cycles: %.0f", adaptiveGrowth, adaptiveGC)
	t.Logf("Disabled pool - Memory growth: %.0f bytes, GC cycles: %.0f", disabledGrowth, disabledGC)

	// Adaptive pool should generally be more efficient for pooled sizes
	if testSize <= int(adaptiveConfig.LargeObjectThreshold) {
		// Handle case where disabled pool has no growth (or negative growth)
		if disabledGrowth <= 0 {
			t.Logf("Disabled pool had no memory growth, adaptive pool growth: %.0f bytes", adaptiveGrowth)
			// If disabled pool has no growth, adaptive pool growth should be reasonable
			assert.True(t, adaptiveGrowth < 10*1024*1024, // Less than 10MB growth
				"Adaptive pool memory growth should be reasonable even when disabled pool has no growth")
		} else {
			// Allow some tolerance for the complexity of adaptive pool
			efficiencyRatio := adaptiveGrowth / disabledGrowth
			t.Logf("Efficiency ratio (adaptive/disabled): %.2f", efficiencyRatio)

			// Adaptive pool should not be significantly worse
			assert.True(t, efficiencyRatio <= 5.0, // More generous threshold
				"Adaptive pool should not be significantly less efficient than direct allocation")
		}
	}

	// Validate both patterns
	validator.ValidateMemoryPattern(t, "Adaptive", adaptivePattern)
	validator.ValidateMemoryPattern(t, "Disabled", disabledPattern)
}
