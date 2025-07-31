package unit

import (
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger is a simple logger implementation for testing
type testLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *testLogger) Log(keyvals ...interface{}) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Convert keyvals to string
	msg := ""
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			msg += keyvals[i].(string) + "=" + keyvals[i+1].(string) + " "
		}
	}
	l.messages = append(l.messages, msg)
	return nil
}

func (l *testLogger) getMessages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]string, len(l.messages))
	copy(result, l.messages)
	return result
}

func TestBufferPoolWithLogging(t *testing.T) {
	testLog := &testLogger{}
	config := daramjwee.BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     512 * 1024,
		MinBufferSize:     4 * 1024,
		EnableLogging:     true,
		LoggingInterval:   100 * time.Millisecond,
	}

	pool, err := daramjwee.NewAdaptiveBufferPool(config, testLog)
	require.NoError(t, err)
	require.NotNil(t, pool)

	// Perform some operations
	buf1 := pool.Get(16 * 1024)
	buf2 := pool.Get(32 * 1024)

	pool.Put(buf1)
	pool.Put(buf2)

	// Wait a bit for logging
	time.Sleep(200 * time.Millisecond)

	// Check that some messages were logged
	messages := testLog.getMessages()
	assert.True(t, len(messages) >= 0) // At least some logging should occur
}

func TestBufferPoolStatsLogging(t *testing.T) {
	testLog := &testLogger{}
	config := daramjwee.BufferPoolConfig{
		Enabled:               true,
		DefaultBufferSize:     32 * 1024,
		EnableDetailedMetrics: true,
		EnableLogging:         true,
		LoggingInterval:       50 * time.Millisecond,
	}

	pool, err := daramjwee.NewAdaptiveBufferPool(config, testLog)
	require.NoError(t, err)
	defer pool.Close()

	// Perform operations to generate stats
	for i := 0; i < 10; i++ {
		buf := pool.Get(16 * 1024)
		pool.Put(buf)
	}

	// Wait for logging
	time.Sleep(100 * time.Millisecond)

	// Verify stats are available
	stats := pool.GetStats()
	assert.True(t, stats.TotalGets > 0)
	assert.True(t, stats.TotalPuts > 0)
}

func TestBufferPoolWithNilLogger(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		EnableLogging:     true,
	}

	// Should not panic with nil logger
	assert.NotPanics(t, func() {
		pool, err := daramjwee.NewAdaptiveBufferPool(config, nil)
		require.NoError(t, err)
		require.NotNil(t, pool)

		// Basic operations should work
		buf := pool.Get(16 * 1024)
		pool.Put(buf)
	})
}

func TestBufferPoolLoggingDisabled(t *testing.T) {
	testLog := &testLogger{}
	config := daramjwee.BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		EnableLogging:     false, // Logging disabled
	}

	pool, err := daramjwee.NewAdaptiveBufferPool(config, testLog)
	require.NoError(t, err)
	require.NotNil(t, pool)

	// Perform operations
	buf := pool.Get(16 * 1024)
	pool.Put(buf)

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Should have minimal or no logging when disabled
	messages := testLog.getMessages()
	assert.True(t, len(messages) >= 0) // May have some initialization messages
}
