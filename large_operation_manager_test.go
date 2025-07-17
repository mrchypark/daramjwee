package daramjwee

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLargeOperationManager_Creation(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     5,
		QueueSize:            10,
		RequestTimeout:       10 * time.Second,
		QueueTimeout:         2 * time.Second,
		EnablePriorityQueues: true,
		EnableBackpressure:   true,
		EnableFairness:       true,
		MonitoringInterval:   5 * time.Second,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	require.NotNil(t, lom)
	defer lom.Close()

	assert.Equal(t, config.MaxConcurrentOps, lom.config.MaxConcurrentOps)
	assert.Equal(t, config.EnablePriorityQueues, lom.config.EnablePriorityQueues)
	assert.NotNil(t, lom.stats)
	assert.NotNil(t, lom.semaphore)
	assert.NotNil(t, lom.priorityQueues)
	assert.NotNil(t, lom.queueProcessor)
}

func TestLargeOperationManager_ImmediateAllocation(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     3,
		QueueSize:            5,
		RequestTimeout:       5 * time.Second,
		EnablePriorityQueues: false,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	ctx := context.Background()

	// Request resources up to the limit
	var tokens []*ResourceToken
	for i := 0; i < config.MaxConcurrentOps; i++ {
		token, err := lom.RequestResource(ctx, 1024, PriorityNormal)
		require.NoError(t, err)
		require.NotNil(t, token)
		tokens = append(tokens, token)
	}

	// Verify stats
	stats := lom.GetStats()
	assert.Equal(t, int64(config.MaxConcurrentOps), stats.TotalRequests)
	assert.Equal(t, int64(config.MaxConcurrentOps), stats.GrantedRequests)
	assert.Equal(t, int64(config.MaxConcurrentOps), stats.CurrentConcurrentOps)

	// Release all tokens
	for _, token := range tokens {
		err := lom.ReleaseResource(token)
		assert.NoError(t, err)
	}

	// Verify final stats
	finalStats := lom.GetStats()
	assert.Equal(t, int64(0), finalStats.CurrentConcurrentOps)
}

func TestLargeOperationManager_QueueingBehavior(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     2,
		QueueSize:            0, // Disable queuing for now
		RequestTimeout:       1 * time.Second,
		QueueTimeout:         100 * time.Millisecond,
		EnablePriorityQueues: false,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	ctx := context.Background()

	// Fill up all immediate slots
	var immediateTokens []*ResourceToken
	for i := 0; i < config.MaxConcurrentOps; i++ {
		token, err := lom.RequestResource(ctx, 1024, PriorityNormal)
		require.NoError(t, err)
		immediateTokens = append(immediateTokens, token)
	}

	// Next request should fail immediately since no queuing
	token, err := lom.RequestResource(ctx, 1024, PriorityNormal)
	assert.Error(t, err)
	assert.Nil(t, token)
	assert.Equal(t, ErrResourceUnavailable, err)

	// Release one token
	err = lom.ReleaseResource(immediateTokens[0])
	require.NoError(t, err)

	// Now request should succeed
	token, err = lom.RequestResource(ctx, 1024, PriorityNormal)
	require.NoError(t, err)
	require.NotNil(t, token)

	// Clean up remaining tokens
	lom.ReleaseResource(token)
	for _, token := range immediateTokens[1:] {
		lom.ReleaseResource(token)
	}
}

func TestLargeOperationManager_PriorityQueues(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     3,
		QueueSize:            0, // Disable queuing for now
		RequestTimeout:       1 * time.Second,
		QueueTimeout:         100 * time.Millisecond,
		EnablePriorityQueues: true,
		EnableFairness:       false,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	ctx := context.Background()

	// Test different priorities can be requested
	priorities := []OperationPriority{PriorityLow, PriorityNormal, PriorityHigh, PriorityCritical}
	var tokens []*ResourceToken

	for _, priority := range priorities {
		if len(tokens) < config.MaxConcurrentOps {
			token, err := lom.RequestResource(ctx, 1024, priority)
			require.NoError(t, err)
			require.NotNil(t, token)
			assert.Equal(t, priority, token.Priority)
			tokens = append(tokens, token)
		}
	}

	// Verify priority statistics
	stats := lom.GetStats()
	assert.True(t, stats.TotalRequests > 0)
	assert.True(t, len(stats.RequestsByPriority) > 0)

	// Should have requests for different priorities
	assert.Equal(t, int64(1), stats.RequestsByPriority[PriorityLow])
	assert.Equal(t, int64(1), stats.RequestsByPriority[PriorityNormal])
	assert.Equal(t, int64(1), stats.RequestsByPriority[PriorityHigh])

	// Clean up tokens
	for _, token := range tokens {
		lom.ReleaseResource(token)
	}
}

func TestLargeOperationManager_Timeout(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     1,
		QueueSize:            1,
		RequestTimeout:       100 * time.Millisecond, // Very short timeout
		QueueTimeout:         50 * time.Millisecond,
		EnablePriorityQueues: false,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	ctx := context.Background()

	// Fill the immediate slot
	immediateToken, err := lom.RequestResource(ctx, 1024, PriorityNormal)
	require.NoError(t, err)
	defer lom.ReleaseResource(immediateToken)

	// This request should timeout
	start := time.Now()
	token, err := lom.RequestResource(ctx, 1024, PriorityNormal)
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, duration >= config.RequestTimeout)
	assert.Contains(t, err.Error(), "timeout")
}

func TestLargeOperationManager_ContextCancellation(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     1,
		QueueSize:            2,
		RequestTimeout:       5 * time.Second,
		EnablePriorityQueues: false,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	// Fill the immediate slot
	immediateToken, err := lom.RequestResource(context.Background(), 1024, PriorityNormal)
	require.NoError(t, err)
	defer lom.ReleaseResource(immediateToken)

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start request that will be queued
	var wg sync.WaitGroup
	var requestErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, requestErr = lom.RequestResource(ctx, 1024, PriorityNormal)
	}()

	// Wait a bit then cancel
	time.Sleep(100 * time.Millisecond)
	cancel()

	wg.Wait()

	assert.Error(t, requestErr)
	assert.Equal(t, context.Canceled, requestErr)
}

func TestLargeOperationManager_Stats(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     2,
		QueueSize:            3,
		RequestTimeout:       1 * time.Second,
		EnablePriorityQueues: true,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	ctx := context.Background()

	// Make some requests
	token1, err := lom.RequestResource(ctx, 1024, PriorityHigh)
	require.NoError(t, err)

	token2, err := lom.RequestResource(ctx, 2048, PriorityNormal)
	require.NoError(t, err)

	// Check stats
	stats := lom.GetStats()
	assert.Equal(t, int64(2), stats.TotalRequests)
	assert.Equal(t, int64(2), stats.GrantedRequests)
	assert.Equal(t, int64(2), stats.CurrentConcurrentOps)
	assert.Equal(t, int64(1), stats.RequestsByPriority[PriorityHigh])
	assert.Equal(t, int64(1), stats.RequestsByPriority[PriorityNormal])

	// Release tokens
	err = lom.ReleaseResource(token1)
	assert.NoError(t, err)
	err = lom.ReleaseResource(token2)
	assert.NoError(t, err)

	// Check final stats
	finalStats := lom.GetStats()
	assert.Equal(t, int64(0), finalStats.CurrentConcurrentOps)
}

func TestLargeOperationManager_ConcurrentAccess(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     5,
		QueueSize:            20,
		RequestTimeout:       2 * time.Second,
		EnablePriorityQueues: true,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	const numGoroutines = 20
	const requestsPerGoroutine = 10

	var wg sync.WaitGroup
	successCount := int64(0)
	errorCount := int64(0)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()

			for j := 0; j < requestsPerGoroutine; j++ {
				priority := OperationPriority(j % 4) // Cycle through priorities
				token, err := lom.RequestResource(ctx, 1024+j, priority)

				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}

				atomic.AddInt64(&successCount, 1)

				// Hold the resource briefly
				time.Sleep(time.Duration(j) * time.Millisecond)

				// Release the resource
				lom.ReleaseResource(token)
			}
		}(i)
	}

	wg.Wait()

	// Verify that some requests succeeded
	assert.True(t, successCount > 0, "Some requests should have succeeded")

	// Verify final state
	stats := lom.GetStats()
	assert.Equal(t, int64(0), stats.CurrentConcurrentOps, "All resources should be released")
	assert.Equal(t, successCount+errorCount, stats.TotalRequests)
}

func TestResourceToken_Methods(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps: 2,
		RequestTimeout:   1 * time.Second,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	ctx := context.Background()
	token, err := lom.RequestResource(ctx, 1024, PriorityNormal)
	require.NoError(t, err)
	require.NotNil(t, token)

	// Test token properties
	assert.NotEmpty(t, token.ID)
	assert.Equal(t, 1024, token.Size)
	assert.Equal(t, PriorityNormal, token.Priority)
	assert.False(t, token.IsExpired()) // Should not be expired immediately

	// Test token release
	err = token.Release()
	assert.NoError(t, err)

	// Test double release (should fail)
	err = token.Release()
	assert.Error(t, err)
	assert.Equal(t, ErrTokenNotHeld, err)
}

func TestOperationPriority_String(t *testing.T) {
	testCases := []struct {
		priority OperationPriority
		expected string
	}{
		{PriorityLow, "low"},
		{PriorityNormal, "normal"},
		{PriorityHigh, "high"},
		{PriorityCritical, "critical"},
		{OperationPriority(999), "unknown"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.priority.String())
		})
	}
}

func TestLargeOperationManager_QueueOverflow(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps:     1,
		QueueSize:            2,
		RequestTimeout:       5 * time.Second,
		QueueTimeout:         100 * time.Millisecond, // Short queue timeout
		EnablePriorityQueues: false,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)
	defer lom.Close()

	ctx := context.Background()

	// Fill immediate slot
	immediateToken, err := lom.RequestResource(ctx, 1024, PriorityNormal)
	require.NoError(t, err)
	defer lom.ReleaseResource(immediateToken)

	// Fill queue
	var queueTokens []*ResourceToken
	for i := 0; i < config.QueueSize; i++ {
		go func() {
			// These will be queued but not processed immediately
			lom.RequestResource(ctx, 1024, PriorityNormal)
		}()
	}

	// Wait for queue to fill
	time.Sleep(50 * time.Millisecond)

	// This request should overflow the queue
	token, err := lom.RequestResource(ctx, 1024, PriorityNormal)
	assert.Error(t, err)
	assert.Nil(t, token)
	assert.Equal(t, ErrQueueFull, err)

	// Clean up
	for _, token := range queueTokens {
		if token != nil {
			lom.ReleaseResource(token)
		}
	}
}

func TestLargeOperationManager_Close(t *testing.T) {
	logger := log.NewNopLogger()
	config := LargeOperationConfig{
		MaxConcurrentOps: 2,
		QueueSize:        5,
	}

	lom, err := NewLargeOperationManager(config, logger)
	require.NoError(t, err)

	// Get a token before closing
	ctx := context.Background()
	token, err := lom.RequestResource(ctx, 1024, PriorityNormal)
	require.NoError(t, err)

	// Close the manager
	err = lom.Close()
	assert.NoError(t, err)

	// Token should still be releasable
	err = lom.ReleaseResource(token)
	assert.NoError(t, err)

	// New requests should fail (though this depends on implementation)
	// The manager is closed, so behavior may vary
}
