package unit

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

func TestChunkedTeeReader_BasicFunctionality(t *testing.T) {
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

	// Test data
	testData := strings.Repeat("Hello, World! ", 1000) // ~13KB
	src := strings.NewReader(testData)
	var dst bytes.Buffer

	// Create ChunkedTeeReader
	teeReader := NewChunkedTeeReader(src, &dst, pool, 1024)

	// Read all data
	result, err := io.ReadAll(teeReader)
	require.NoError(t, err)

	// Verify data integrity
	assert.Equal(t, testData, string(result))
	assert.Equal(t, testData, dst.String())

	// Check stats
	stats := teeReader.GetStats()
	assert.True(t, stats.TotalBytesRead > 0)
	assert.Equal(t, int64(len(testData)), stats.TotalBytesRead)
}

func TestChunkedTeeReader_LargeData(t *testing.T) {
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

	// Large test data (500KB)
	testData := strings.Repeat("Large data chunk for testing chunked streaming optimization. ", 8000)
	src := strings.NewReader(testData)
	var dst bytes.Buffer

	// Create daramjwee.ChunkedTeeReader with smaller chunk size to force chunking
	teeReader := NewChunkedTeeReader(src, &dst, pool, 16*1024)

	// Read all data
	result, err := io.ReadAll(teeReader)
	require.NoError(t, err)

	// Verify data integrity
	assert.Equal(t, testData, string(result))
	assert.Equal(t, testData, dst.String())

	// Check stats
	stats := teeReader.GetStats()
	assert.True(t, stats.TotalBytesRead > 0)
	assert.True(t, stats.TotalChunksRead > 0)
	assert.True(t, stats.ChunkReuseCount > 0)

	t.Logf("Stats: Bytes=%d, Chunks=%d, Reuse=%d",
		stats.TotalBytesRead, stats.TotalChunksRead, stats.ChunkReuseCount)
}

func TestChunkedTeeReaderWithMetrics_DetailedTracking(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		ChunkSize:                8 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	// Test data
	testData := strings.Repeat("Metrics test data. ", 1000)
	src := strings.NewReader(testData)
	var dst bytes.Buffer

	// Create daramjwee.ChunkedTeeReader with metrics
	teeReader := NewChunkedTeeReaderWithMetrics(src, &dst, pool, 4*1024, true)

	// Read data in chunks
	buffer := make([]byte, 2048)
	for {
		n, err := teeReader.Read(buffer)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		assert.True(t, n > 0)
	}

	// Get detailed stats
	detailedStats := teeReader.GetDetailedStats()
	assert.True(t, detailedStats.TotalBytesRead > 0)
	assert.True(t, detailedStats.AverageReadLatency > 0)
	assert.True(t, detailedStats.MetricSampleCount > 0)

	t.Logf("Detailed Stats: AvgLatency=%v, Samples=%d, Utilization=%.2f",
		detailedStats.AverageReadLatency, detailedStats.MetricSampleCount, detailedStats.AverageChunkUtilization)
}

func TestChunkedCopyBuffer_BasicFunctionality(t *testing.T) {
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

	testCases := []struct {
		name         string
		dataSize     int
		expectedSize int
	}{
		{"Small data", 10 * 1024, 10 * 1024},          // 10KB
		{"Medium data", 100 * 1024, 100 * 1024},       // 100KB
		{"Large data", 500 * 1024, 500 * 1024},        // 500KB
		{"Very large data", 2048 * 1024, 2048 * 1024}, // 2MB
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test data
			testData := strings.Repeat("X", tc.dataSize)
			src := strings.NewReader(testData)
			var dst bytes.Buffer

			// Perform chunked copy
			copied, err := pool.ChunkedCopyBuffer(&dst, src, tc.expectedSize)
			require.NoError(t, err)

			// Verify results
			assert.Equal(t, int64(tc.expectedSize), copied)
			assert.Equal(t, tc.expectedSize, dst.Len())
			assert.Equal(t, testData, dst.String())
		})
	}
}

func TestChunkedCopyBuffer_WithCallback(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		ChunkSize:                16 * 1024,
		LargeObjectThreshold:     64 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	// Large test data
	testData := strings.Repeat("Progress test data. ", 5000) // ~100KB
	src := strings.NewReader(testData)
	var dst bytes.Buffer

	// Track progress
	var progressUpdates []int64
	progressCallback := func(copied, total int64) {
		progressUpdates = append(progressUpdates, copied)
	}

	// Perform copy with progress tracking
	copied, err := pool.ChunkedCopyBufferWithCallback(&dst, src, len(testData), progressCallback)
	require.NoError(t, err)

	// Verify results
	assert.Equal(t, int64(len(testData)), copied)
	assert.Equal(t, testData, dst.String())
	assert.True(t, len(progressUpdates) > 0)

	// Verify progress updates are increasing
	for i := 1; i < len(progressUpdates); i++ {
		assert.True(t, progressUpdates[i] >= progressUpdates[i-1])
	}

	t.Logf("Progress updates: %v", progressUpdates)
}

func TestOptimizedCopyBuffer_StrategySelection(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		ChunkSize:                64 * 1024,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	testCases := []struct {
		name         string
		dataSize     int64
		expectedHint string
	}{
		{"Small object", 16 * 1024, "regular buffered"},
		{"Medium object", 128 * 1024, "regular buffered"},
		{"Large object", 512 * 1024, "chunked copying"},
		{"Very large object", 2048 * 1024, "direct streaming"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testData := strings.Repeat("S", int(tc.dataSize))
			src := strings.NewReader(testData)
			var dst bytes.Buffer

			hint := CopyHint{
				EstimatedSize: tc.dataSize,
				IsStreaming:   true,
				Priority:      CopyPriorityNormal,
			}

			copied, err := pool.OptimizedCopyBuffer(&dst, src, hint)
			require.NoError(t, err)

			assert.Equal(t, tc.dataSize, copied)
			assert.Equal(t, int(tc.dataSize), dst.Len())

			t.Logf("Copied %d bytes using %s strategy", copied, tc.expectedHint)
		})
	}
}

func TestCopyBufferWithTimeout_Success(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		ChunkSize:                32 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
		EnableDetailedMetrics:    true,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	testData := strings.Repeat("Timeout test. ", 1000)
	src := strings.NewReader(testData)
	var dst bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	copied, err := pool.CopyBufferWithTimeout(ctx, &dst, src, len(testData))
	require.NoError(t, err)

	assert.Equal(t, int64(len(testData)), copied)
	assert.Equal(t, testData, dst.String())
}

func TestCopyBufferWithTimeout_Timeout(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:                  true,
		DefaultBufferSize:        32 * 1024,
		MaxBufferSize:            512 * 1024,
		MinBufferSize:            4 * 1024,
		ChunkSize:                32 * 1024,
		LargeObjectThreshold:     256 * 1024,
		VeryLargeObjectThreshold: 1024 * 1024,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)

	// Create a slow reader that will cause timeout
	slowReader := &slowReader{delay: 100 * time.Millisecond}
	var dst bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = pool.CopyBufferWithTimeout(ctx, &dst, slowReader, 1000)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestChunkPoolManager_BasicOperations(t *testing.T) {
	config := ChunkPoolConfig{
		ChunkSizes:              []int{16 * 1024, 32 * 1024, 64 * 1024},
		MaxChunksPerPool:        50,
		MemoryPressureThreshold: 10 * 1024 * 1024, // 10MB
		CleanupInterval:         1 * time.Second,
		MaxChunkAge:             5 * time.Second,
		EnableMetrics:           true,
	}

	logger := log.NewNopLogger()
	manager := NewChunkPoolManager(config, logger)
	defer manager.Close()

	// Test chunk allocation and return
	chunk1 := manager.GetChunk(16 * 1024)
	assert.True(t, len(chunk1) >= 16*1024)

	chunk2 := manager.GetChunk(32 * 1024)
	assert.True(t, len(chunk2) >= 32*1024)

	chunk3 := manager.GetChunk(48 * 1024) // Should get 64KB chunk
	assert.True(t, len(chunk3) >= 48*1024)

	// Return chunks
	manager.PutChunk(chunk1)
	manager.PutChunk(chunk2)
	manager.PutChunk(chunk3)

	// Get stats
	stats := manager.GetStats()
	assert.True(t, stats.TotalGets > 0)
	assert.True(t, stats.TotalPuts > 0)
	assert.True(t, len(stats.PoolStats) > 0)

	t.Logf("Manager Stats: Pools=%d, Gets=%d, Puts=%d, Memory=%d",
		stats.TotalPools, stats.TotalGets, stats.TotalPuts, stats.TotalMemoryUsage)
}

func TestChunkPoolManager_MemoryPressure(t *testing.T) {
	config := ChunkPoolConfig{
		ChunkSizes:              []int{64 * 1024},
		MaxChunksPerPool:        10,
		MemoryPressureThreshold: 128 * 1024, // Very low threshold
		EnableMetrics:           true,
	}

	logger := log.NewNopLogger()
	manager := NewChunkPoolManager(config, logger)
	defer manager.Close()

	// Allocate chunks to trigger memory pressure
	var chunks [][]byte
	for i := 0; i < 5; i++ {
		chunk := manager.GetChunk(64 * 1024)
		chunks = append(chunks, chunk)
	}

	// Return chunks - some should be dropped due to memory pressure
	for _, chunk := range chunks {
		manager.PutChunk(chunk)
	}

	stats := manager.GetStats()
	assert.True(t, stats.MemoryPressureCount > 0)

	t.Logf("Memory pressure events: %d", stats.MemoryPressureCount)
}

// slowReader is a helper for testing timeout scenarios
type slowReader struct {
	delay time.Duration
	count int
}

func (sr *slowReader) Read(p []byte) (n int, err error) {
	if sr.count > 0 {
		return 0, io.EOF
	}

	time.Sleep(sr.delay)
	sr.count++

	// Return some data
	data := "slow data"
	copy(p, data)
	return len(data), nil
}
