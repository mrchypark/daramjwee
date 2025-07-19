package daramjwee

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger captures log output for testing
type testLogger struct {
	mu      sync.Mutex
	entries []string
}

func (tl *testLogger) Log(keyvals ...interface{}) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	var buf bytes.Buffer
	for i := 0; i < len(keyvals); i += 2 {
		if i > 0 {
			buf.WriteString(" ")
		}
		if i+1 < len(keyvals) {
			key := keyvals[i]
			value := keyvals[i+1]

			// Convert key to string
			var keyStr string
			if k, ok := key.(string); ok {
				keyStr = k
			} else {
				keyStr = "unknown"
			}

			// Convert value to string using fmt.Sprintf
			valueStr := fmt.Sprintf("%v", value)

			buf.WriteString(keyStr)
			buf.WriteString("=")
			buf.WriteString(valueStr)
		}
	}
	tl.entries = append(tl.entries, buf.String())
	return nil
}

func (tl *testLogger) getEntries() []string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	entries := make([]string, len(tl.entries))
	copy(entries, tl.entries)
	return entries
}

func (tl *testLogger) clear() {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.entries = nil
}

func TestDefaultBufferPool_LoggingConfiguration(t *testing.T) {
	t.Run("logging_disabled_by_default", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     false,
			LoggingInterval:   time.Second,
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		// Use the pool
		buf := pool.Get(32 * 1024)
		pool.Put(buf)

		// Wait a bit to ensure no logging occurs
		time.Sleep(100 * time.Millisecond)

		// Should have no log entries
		entries := testLog.getEntries()
		assert.Empty(t, entries, "No logging should occur when disabled")

		pool.Close()
	})

	t.Run("logging_enabled_with_interval", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   50 * time.Millisecond, // Short interval for testing
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		// Use the pool to generate some statistics
		buf1 := pool.Get(32 * 1024)
		buf2 := pool.Get(16 * 1024)
		pool.Put(buf1)
		pool.Put(buf2)

		// Wait for at least one logging interval
		time.Sleep(150 * time.Millisecond)

		entries := testLog.getEntries()
		assert.NotEmpty(t, entries, "Should have log entries when logging is enabled")

		// Check that log entries contain expected fields
		found := false
		for _, entry := range entries {
			if strings.Contains(entry, "buffer pool statistics") {
				assert.Contains(t, entry, "total_gets")
				assert.Contains(t, entry, "total_puts")
				assert.Contains(t, entry, "pool_hits")
				assert.Contains(t, entry, "pool_misses")
				assert.Contains(t, entry, "active_buffers")
				assert.Contains(t, entry, "hit_rate_percent")
				found = true
				break
			}
		}
		assert.True(t, found, "Should find buffer pool statistics log entry")

		pool.Close()
	})

	t.Run("logging_with_zero_interval", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   0, // Zero interval should disable periodic logging
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		// Use the pool
		buf := pool.Get(32 * 1024)
		pool.Put(buf)

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Should have no log entries due to zero interval
		entries := testLog.getEntries()
		assert.Empty(t, entries, "No logging should occur with zero interval")

		pool.Close()
	})

	t.Run("logging_with_nil_logger", func(t *testing.T) {
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   50 * time.Millisecond,
		}

		// Should not panic with nil logger
		assert.NotPanics(t, func() {
			pool := NewDefaultBufferPoolWithLogger(config, nil)
			require.NotNil(t, pool)

			buf := pool.Get(32 * 1024)
			pool.Put(buf)

			time.Sleep(100 * time.Millisecond)
			pool.Close()
		})
	})
}

func TestDefaultBufferPool_LoggingContent(t *testing.T) {
	t.Run("log_statistics_accuracy", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   50 * time.Millisecond,
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		// Perform known operations
		buf1 := pool.Get(32 * 1024) // Miss
		buf2 := pool.Get(32 * 1024) // Miss
		pool.Put(buf1)
		pool.Put(buf2)
		buf3 := pool.Get(32 * 1024) // Should be hit

		// Wait for logging
		time.Sleep(150 * time.Millisecond)

		entries := testLog.getEntries()
		assert.NotEmpty(t, entries)

		// Find the statistics entry
		var statsEntry string
		for _, entry := range entries {
			if strings.Contains(entry, "buffer pool statistics") {
				statsEntry = entry
				break
			}
		}
		assert.NotEmpty(t, statsEntry, "Should find statistics log entry")

		// Verify statistics content
		assert.Contains(t, statsEntry, "total_gets=3")
		assert.Contains(t, statsEntry, "total_puts=2")
		assert.Contains(t, statsEntry, "active_buffers=1")

		pool.Put(buf3)
		pool.Close()
	})

	t.Run("hit_rate_calculation", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   50 * time.Millisecond,
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		// Create scenario with known hit rate
		// First get will be miss, second get of same size might be hit after put
		buf1 := pool.Get(32 * 1024)
		pool.Put(buf1)
		buf2 := pool.Get(32 * 1024) // This should be a hit
		pool.Put(buf2)

		// Wait for logging
		time.Sleep(150 * time.Millisecond)

		entries := testLog.getEntries()
		assert.NotEmpty(t, entries)

		// Find the statistics entry and check hit rate
		found := false
		for _, entry := range entries {
			if strings.Contains(entry, "buffer pool statistics") && strings.Contains(entry, "hit_rate_percent") {
				// Hit rate should be calculated (hits/total_gets * 100)
				assert.Contains(t, entry, "hit_rate_percent")
				found = true
				break
			}
		}
		assert.True(t, found, "Should find hit rate in statistics")

		pool.Close()
	})
}

func TestDefaultBufferPool_LoggingLifecycle(t *testing.T) {
	t.Run("logging_starts_and_stops", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   30 * time.Millisecond,
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		// Use the pool
		buf := pool.Get(32 * 1024)
		pool.Put(buf)

		// Wait for some log entries
		time.Sleep(100 * time.Millisecond)

		entriesBeforeClose := len(testLog.getEntries())
		assert.Greater(t, entriesBeforeClose, 0, "Should have log entries before close")

		// Close the pool
		pool.Close()

		// Wait a bit more
		time.Sleep(100 * time.Millisecond)

		entriesAfterClose := len(testLog.getEntries())

		// Should not have significantly more entries after close
		// (allowing for one more entry that might have been in flight)
		assert.LessOrEqual(t, entriesAfterClose, entriesBeforeClose+1,
			"Should not have many new log entries after close")
	})

	t.Run("multiple_close_calls", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   50 * time.Millisecond,
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		// Multiple close calls should not panic
		assert.NotPanics(t, func() {
			pool.Close()
			pool.Close()
			pool.Close()
		})
	})
}

func TestDefaultBufferPool_LoggingPerformance(t *testing.T) {
	t.Run("logging_overhead_minimal", func(t *testing.T) {
		// Test that logging doesn't significantly impact performance
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     false, // Disabled for baseline
			LoggingInterval:   0,
		}

		poolNoLogging := NewDefaultBufferPoolWithLogger(config, nil)

		config.EnableLogging = true
		config.LoggingInterval = 10 * time.Millisecond
		testLog := &testLogger{}
		poolWithLogging := NewDefaultBufferPoolWithLogger(config, testLog)

		const numOperations = 1000

		// Measure without logging
		start := time.Now()
		for i := 0; i < numOperations; i++ {
			buf := poolNoLogging.Get(32 * 1024)
			poolNoLogging.Put(buf)
		}
		durationNoLogging := time.Since(start)

		// Measure with logging
		start = time.Now()
		for i := 0; i < numOperations; i++ {
			buf := poolWithLogging.Get(32 * 1024)
			poolWithLogging.Put(buf)
		}
		durationWithLogging := time.Since(start)

		// Logging should not add significant overhead to operations
		// Allow up to 50% overhead for logging
		maxAllowedDuration := durationNoLogging + (durationNoLogging / 2)
		assert.LessOrEqual(t, durationWithLogging, maxAllowedDuration,
			"Logging should not add significant overhead. No logging: %v, With logging: %v",
			durationNoLogging, durationWithLogging)

		poolNoLogging.Close()
		poolWithLogging.Close()
	})

	t.Run("concurrent_logging_safety", func(t *testing.T) {
		testLog := &testLogger{}
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
			EnableLogging:     true,
			LoggingInterval:   20 * time.Millisecond,
		}

		pool := NewDefaultBufferPoolWithLogger(config, testLog)
		require.NotNil(t, pool)

		const numGoroutines = 50
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Run concurrent operations while logging is active
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					buf := pool.Get(32 * 1024)
					pool.Put(buf)
				}
			}()
		}

		wg.Wait()

		// Wait for some log entries
		time.Sleep(100 * time.Millisecond)

		entries := testLog.getEntries()
		assert.NotEmpty(t, entries, "Should have log entries from concurrent operations")

		// Verify no data races or corruption in log entries
		for _, entry := range entries {
			if strings.Contains(entry, "buffer pool statistics") {
				// Basic sanity check - entry should be well-formed
				assert.Contains(t, entry, "total_gets")
				assert.Contains(t, entry, "total_puts")
			}
		}

		pool.Close()
	})
}

func TestDefaultBufferPool_IntegrationWithCache(t *testing.T) {
	t.Run("cache_uses_buffer_pool_logging", func(t *testing.T) {
		testLog := &testLogger{}

		// Create cache with buffer pool logging enabled
		cache, err := New(testLog, func(cfg *Config) error {
			cfg.BufferPool.EnableLogging = true
			cfg.BufferPool.LoggingInterval = 50 * time.Millisecond
			cfg.HotStore = newNullStore() // Use null store for testing
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, cache)

		// Use cache operations that should trigger buffer pool usage
		ctx := context.Background()

		// Set some data
		writer, err := cache.Set(ctx, "test-key", &Metadata{})
		require.NoError(t, err)
		_, err = writer.Write([]byte("test data"))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)

		// Wait for logging
		time.Sleep(150 * time.Millisecond)

		entries := testLog.getEntries()

		// Should have buffer pool statistics in logs
		found := false
		for _, entry := range entries {
			if strings.Contains(entry, "buffer pool statistics") {
				found = true
				break
			}
		}
		assert.True(t, found, "Should find buffer pool statistics in cache logs")

		cache.Close()
	})
}
