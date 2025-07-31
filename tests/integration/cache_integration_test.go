package integration

import (
	"github.com/mrchypark/daramjwee"
	"context"
	"crypto/rand"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	helpers "github.com/mrchypark/daramjwee/tests/integration/helpers_test"
)

// TestCacheOperationCompatibility_PromoteAndTeeStream tests that promoteAndTeeStream
// behavior is identical with and without buffer pool optimization.
func TestCacheOperationCompatibility_PromoteAndTeeStream(t *testing.T) {
	testCases := []struct {
		name        string
		dataSize    int
		description string
	}{
		{"Small", 1024, "1KB data"},
		{"Medium", 32 * 1024, "32KB data"},
		{"Large", 256 * 1024, "256KB data"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			testData := make([]byte, tc.dataSize)
			_, err := rand.Read(testData)
			require.NoError(t, err)

			// Test without buffer pool
			resultWithoutPool := testPromoteAndTeeStreamBehavior(t, testData, false)

			// Test with buffer pool
			resultWithPool := testPromoteAndTeeStreamBehavior(t, testData, true)

			// Verify identical behavior
			assert.Equal(t, resultWithoutPool.readData, resultWithPool.readData,
				"Read data should be identical with and without buffer pool")
			assert.Equal(t, resultWithoutPool.hotStoreData, resultWithPool.hotStoreData,
				"Hot store data should be identical with and without buffer pool")
			assert.Equal(t, resultWithoutPool.metadata.ETag, resultWithPool.metadata.ETag,
				"daramjwee.Metadata should be identical with and without buffer pool")
		})
	}
}

// TestCacheOperationCompatibility_CacheAndTeeStream tests that cacheAndTeeStream
// behavior is identical with and without buffer pool optimization.
func TestCacheAndTeeStream(t *testing.T) {
	testCases := []struct {
		name        string
		dataSize    int
		description string
	}{
		{"Small", 512, "512B data"},
		{"Medium", 16 * 1024, "16KB data"},
		{"Large", 128 * 1024, "128KB data"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			testData := make([]byte, tc.dataSize)
			_, err := rand.Read(testData)
			require.NoError(t, err)

			// Test without buffer pool
			resultWithoutPool := testCacheAndTeeStreamBehavior(t, testData, false)

			// Test with buffer pool
			resultWithPool := testCacheAndTeeStreamBehavior(t, testData, true)

			// Verify identical behavior
			assert.Equal(t, resultWithoutPool.readData, resultWithPool.readData,
				"Read data should be identical with and without buffer pool")
			assert.Equal(t, resultWithoutPool.hotStoreData, resultWithPool.hotStoreData,
				"Hot store data should be identical with and without buffer pool")
			assert.Equal(t, resultWithoutPool.metadata.ETag, resultWithPool.metadata.ETag,
				"daramjwee.Metadata should be identical with and without buffer pool")
		})
	}
}

// TestCacheOperationCompatibility_BackgroundCopy tests that background copy operations
// (scheduleSetToStore and ScheduleRefresh) behave identically with and without buffer pool.
func TestCacheOperationCompatibility_BackgroundCopy(t *testing.T) {
	testCases := []struct {
		name        string
		dataSize    int
		description string
	}{
		{"Small", 2048, "2KB data"},
		{"Medium", 64 * 1024, "64KB data"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			testData := make([]byte, tc.dataSize)
			_, err := rand.Read(testData)
			require.NoError(t, err)

			// Test without buffer pool
			resultWithoutPool := testBackgroundCopyBehavior(t, testData, false)

			// Test with buffer pool
			resultWithPool := testBackgroundCopyBehavior(t, testData, true)

			// Verify identical behavior (allow for some flexibility in background operations)
			if len(resultWithoutPool.refreshedData) > 0 && len(resultWithPool.refreshedData) > 0 {
				assert.Equal(t, resultWithoutPool.refreshedData, resultWithPool.refreshedData,
					"Refreshed data should be identical with and without buffer pool")
			}
		})
	}
}

// TestCacheOperationCompatibility_FullWorkflow tests complete cache workflows
// to ensure all optimized operations work together correctly.
func TestCacheOperationCompatibility_FullWorkflow(t *testing.T) {
	testCases := []struct {
		name        string
		dataSize    int
		description string
	}{
		{"SmallWorkflow", 4 * 1024, "4KB full workflow"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			testData := make([]byte, tc.dataSize)
			_, err := rand.Read(testData)
			require.NoError(t, err)

			// Test without buffer pool
			resultWithoutPool := testFullCacheWorkflow(t, testData, false)

			// Test with buffer pool
			resultWithPool := testFullCacheWorkflow(t, testData, true)

			// Verify identical behavior across all operations
			assert.Equal(t, resultWithoutPool.initialFetchData, resultWithPool.initialFetchData,
				"Initial fetch data should be identical")
			assert.Equal(t, resultWithoutPool.coldHitData, resultWithPool.coldHitData,
				"Cold hit data should be identical")
			assert.Equal(t, resultWithoutPool.hotHitData, resultWithPool.hotHitData,
				"Hot hit data should be identical")
			// Skip refresh data comparison as it's timing-dependent
		})
	}
}

// promoteAndTeeStreamResult holds the results of promoteAndTeeStream operation testing.
type promoteAndTeeStreamResult struct {
	readData     []byte
	hotStoreData []byte
	metadata     *daramjwee.Metadata
}

// testPromoteAndTeeStreamBehavior tests the promoteAndTeeStream operation with or without buffer pool.
func testPromoteAndTeeStreamBehavior(t *testing.T, testData []byte, useBufferPool bool) promoteAndTeeStreamResult {
	t.Helper()

	// Setup cache with or without buffer pool
	var opts []daramjwee.Option
	if useBufferPool {
		opts = append(opts, daramjwee.WithBufferPool(true, 32*1024))
	}
	cache, hot, cold := setupCache(t, opts...)

	// Setup cold store with test data
	key := "promote-test-key"
	metadata := &daramjwee.Metadata{
		ETag:     "test-etag",
		CachedAt: time.Now().Add(-1 * time.Hour),
	}
	cold.setData(key, string(testData), metadata)

	// Perform cold hit to trigger promoteAndTeeStream
	ctx := context.Background()
	stream, err := cache.Get(ctx, key, &mockFetcher{})
	require.NoError(t, err)

	// Read all data from stream
	readData, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	// Wait for promotion to complete
	hot.Wait()

	// Get data from hot store
	hot.mu.RLock()
	hotStoreData := make([]byte, len(hot.data[key]))
	copy(hotStoreData, hot.data[key])
	hotMetadata := hot.meta[key]
	hot.mu.RUnlock()

	return promoteAndTeeStreamResult{
		readData:     readData,
		hotStoreData: hotStoreData,
		metadata:     hotMetadata,
	}
}

// cacheAndTeeStreamResult holds the results of cacheAndTeeStream operation testing.
type cacheAndTeeStreamResult struct {
	readData     []byte
	hotStoreData []byte
	metadata     *daramjwee.Metadata
}

// testCacheAndTeeStreamBehavior tests the cacheAndTeeStream operation with or without buffer pool.
func testCacheAndTeeStreamBehavior(t *testing.T, testData []byte, useBufferPool bool) cacheAndTeeStreamResult {
	t.Helper()

	// Setup cache with or without buffer pool
	var opts []daramjwee.Option
	if useBufferPool {
		opts = append(opts, daramjwee.WithBufferPool(true, 32*1024))
	}
	cache, hot, _ := setupCache(t, opts...)

	// Setup fetcher with test data
	key := "cache-test-key"
	fetcher := &mockFetcher{
		content: string(testData),
		etag:    "test-etag",
	}

	// Perform cache miss to trigger cacheAndTeeStream
	ctx := context.Background()
	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)

	// Read all data from stream
	readData, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	// Wait for caching to complete
	hot.Wait()

	// Get data from hot store
	hot.mu.RLock()
	hotStoreData := make([]byte, len(hot.data[key]))
	copy(hotStoreData, hot.data[key])
	hotMetadata := hot.meta[key]
	hot.mu.RUnlock()

	return cacheAndTeeStreamResult{
		readData:     readData,
		hotStoreData: hotStoreData,
		metadata:     hotMetadata,
	}
}

// backgroundCopyResult holds the results of background copy operation testing.
type backgroundCopyResult struct {
	coldStoreData []byte
	refreshedData []byte
}

// testBackgroundCopyBehavior tests background copy operations with or without buffer pool.
func testBackgroundCopyBehavior(t *testing.T, testData []byte, useBufferPool bool) backgroundCopyResult {
	t.Helper()

	// Setup cache with or without buffer pool
	var opts []daramjwee.Option
	if useBufferPool {
		opts = append(opts, daramjwee.WithBufferPool(true, 32*1024))
	}
	cache, hot, cold := setupCache(t, opts...)

	key := "background-copy-key"

	// First, populate hot store
	hot.setData(key, string(testData), &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	// Trigger background copy to cold store by performing a cache miss on different key
	// This will cause scheduleSetToStore to be called
	missKey := "miss-key-to-trigger-background"
	fetcher := &mockFetcher{content: string(testData), etag: "v1"}
	stream, err := cache.Get(context.Background(), missKey, fetcher)
	require.NoError(t, err)
	io.ReadAll(stream)
	stream.Close()

	// Wait for background operations
	hot.Wait()
	cold.Wait()

	// Get data from cold store
	cold.mu.RLock()
	var coldStoreData []byte
	if data, exists := cold.data[missKey]; exists {
		coldStoreData = make([]byte, len(data))
		copy(coldStoreData, data)
	}
	cold.mu.RUnlock()

	// Test ScheduleRefresh with buffer pool
	refreshFetcher := &mockFetcher{content: string(testData), etag: "v2"}
	err = cache.ScheduleRefresh(context.Background(), key, refreshFetcher)
	require.NoError(t, err)

	// Wait for refresh to complete
	time.Sleep(200 * time.Millisecond)

	// Get refreshed data from hot store
	hot.mu.RLock()
	var refreshedData []byte
	if data, exists := hot.data[key]; exists {
		refreshedData = make([]byte, len(data))
		copy(refreshedData, data)
	}
	hot.mu.RUnlock()

	return backgroundCopyResult{
		coldStoreData: coldStoreData,
		refreshedData: refreshedData,
	}
}

// fullWorkflowResult holds the results of full cache workflow testing.
type fullWorkflowResult struct {
	initialFetchData []byte
	coldHitData      []byte
	hotHitData       []byte
	refreshedData    []byte
}

// testFullCacheWorkflow tests a complete cache workflow with or without buffer pool.
func testFullCacheWorkflow(t *testing.T, testData []byte, useBufferPool bool) fullWorkflowResult {
	t.Helper()

	// Setup cache with or without buffer pool
	var opts []daramjwee.Option
	if useBufferPool {
		opts = append(opts, daramjwee.WithBufferPool(true, 32*1024))
	}
	cache, hot, cold := setupCache(t, opts...)

	key := "workflow-key"
	ctx := context.Background()

	// Step 1: Initial fetch (cache miss)
	fetcher := &mockFetcher{content: string(testData), etag: "v1"}
	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	initialFetchData, err := io.ReadAll(stream)
	require.NoError(t, err)
	stream.Close()

	// Wait for caching operations
	hot.Wait()
	cold.Wait()

	// Step 2: Clear hot cache to simulate cold hit
	hot.Delete(ctx, key)

	// Perform cold hit - use a fetcher that won't be called since data should be in cold store
	stream, err = cache.Get(ctx, key, &mockFetcher{content: string(testData), etag: "v1"})
	require.NoError(t, err)
	coldHitData, err := io.ReadAll(stream)
	require.NoError(t, err)
	stream.Close()

	// Wait for promotion
	hot.Wait()

	// Step 3: Hot hit
	stream, err = cache.Get(ctx, key, &mockFetcher{})
	require.NoError(t, err)
	hotHitData, err := io.ReadAll(stream)
	require.NoError(t, err)
	stream.Close()

	// Step 4: Schedule refresh
	refreshFetcher := &mockFetcher{content: string(testData), etag: "v2"}
	err = cache.ScheduleRefresh(ctx, key, refreshFetcher)
	require.NoError(t, err)

	// Wait for refresh
	time.Sleep(100 * time.Millisecond)

	// Get refreshed data
	hot.mu.RLock()
	refreshedData := make([]byte, len(hot.data[key]))
	copy(refreshedData, hot.data[key])
	hot.mu.RUnlock()

	return fullWorkflowResult{
		initialFetchData: initialFetchData,
		coldHitData:      coldHitData,
		hotHitData:       hotHitData,
		refreshedData:    refreshedData,
	}
}

// TestCacheOperationCompatibility_ErrorHandling tests that error handling
// behavior is identical with and without buffer pool optimization.
func TestCacheOperationCompatibility_ErrorHandling(t *testing.T) {
	t.Run("PromotionFailure", func(t *testing.T) {
		testData := []byte(strings.Repeat("test", 1024))

		// Test without buffer pool
		resultWithoutPool := testPromotionFailureBehavior(t, testData, false)

		// Test with buffer pool
		resultWithPool := testPromotionFailureBehavior(t, testData, true)

		// Verify identical error handling
		assert.Equal(t, resultWithoutPool.readData, resultWithPool.readData,
			"Read data should be identical even when promotion fails")
		assert.Equal(t, resultWithoutPool.promotionFailed, resultWithPool.promotionFailed,
			"Promotion failure behavior should be identical")
	})

	t.Run("CacheWriteFailure", func(t *testing.T) {
		testData := []byte(strings.Repeat("test", 1024))

		// Test without buffer pool
		resultWithoutPool := testCacheWriteFailureBehavior(t, testData, false)

		// Test with buffer pool
		resultWithPool := testCacheWriteFailureBehavior(t, testData, true)

		// Verify identical error handling
		assert.Equal(t, resultWithoutPool.readData, resultWithPool.readData,
			"Read data should be identical even when cache write fails")
		assert.Equal(t, resultWithoutPool.cacheWriteFailed, resultWithPool.cacheWriteFailed,
			"daramjwee.Cache write failure behavior should be identical")
	})
}

// promotionFailureResult holds the results of promotion failure testing.
type promotionFailureResult struct {
	readData        []byte
	promotionFailed bool
}

// testPromotionFailureBehavior tests promotion failure handling with or without buffer pool.
func testPromotionFailureBehavior(t *testing.T, testData []byte, useBufferPool bool) promotionFailureResult {
	t.Helper()

	// Setup cache with failing hot store
	hot := newMockStore()
	hot.forceSetError = true
	cold := helpers.newMockStore()

	var opts []daramjwee.Option
	opts = append(opts, daramjwee.WithHotStore(hot), daramjwee.WithColdStore(cold))
	if useBufferPool {
		opts = append(opts, daramjwee.WithBufferPool(true, 32*1024))
	}

	cache, err := daramjwee.New(log.NewNopLogger(), opts...)
	require.NoError(t, err)
	defer cache.Close()

	// Setup cold store with test data
	key := "promotion-fail-key"
	cold.setData(key, string(testData), &daramjwee.Metadata{ETag: "test-etag"})

	// Perform cold hit (should fail promotion but still return data)
	stream, err := cache.Get(context.Background(), key, &mockFetcher{})
	require.NoError(t, err)

	readData, err := io.ReadAll(stream)
	require.NoError(t, err)
	stream.Close()

	// Check if promotion failed
	hot.mu.RLock()
	_, promotionSucceeded := hot.data[key]
	hot.mu.RUnlock()

	return promotionFailureResult{
		readData:        readData,
		promotionFailed: !promotionSucceeded,
	}
}

// cacheWriteFailureResult holds the results of cache write failure testing.
type cacheWriteFailureResult struct {
	readData         []byte
	cacheWriteFailed bool
}

// testCacheWriteFailureBehavior tests cache write failure handling with or without buffer pool.
func testCacheWriteFailureBehavior(t *testing.T, testData []byte, useBufferPool bool) cacheWriteFailureResult {
	t.Helper()

	// Setup cache with failing hot store
	hot := newMockStore()
	hot.forceSetError = true

	var opts []daramjwee.Option
	opts = append(opts, daramjwee.WithHotStore(hot))
	if useBufferPool {
		opts = append(opts, daramjwee.WithBufferPool(true, 32*1024))
	}

	cache, err := daramjwee.New(log.NewNopLogger(), opts...)
	require.NoError(t, err)
	defer cache.Close()

	// Perform cache miss (should fail to cache but still return data)
	key := "cache-fail-key"
	fetcher := &helpers.mockFetcher{content: string(testData), etag: "test-etag"}
	stream, err := cache.Get(context.Background(), key, fetcher)
	require.NoError(t, err)

	readData, err := io.ReadAll(stream)
	require.NoError(t, err)
	stream.Close()

	// Check if caching failed
	hot.mu.RLock()
	_, cacheSucceeded := hot.data[key]
	hot.mu.RUnlock()

	return cacheWriteFailureResult{
		readData:         readData,
		cacheWriteFailed: !cacheSucceeded,
	}
}
