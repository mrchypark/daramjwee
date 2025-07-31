package unit

import (
	"github.com/mrchypark/daramjwee"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogger captures log messages for testing
type captureLogger struct {
	mu       sync.Mutex
	messages []string
}

func (cl *captureLogger) Log(keyvals ...interface{}) error {
	var parts []string
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key := keyvals[i]
			val := keyvals[i+1]
			parts = append(parts, key.(string)+"="+toString(val))
		}
	}

	cl.mu.Lock()
	cl.messages = append(cl.messages, strings.Join(parts, " "))
	cl.mu.Unlock()
	return nil
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return val.Format(time.RFC3339)
	case time.Duration:
		return val.String()
	case error:
		return val.Error()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (cl *captureLogger) hasMessage(substring string) bool {
	for _, msg := range cl.messages {
		if strings.Contains(msg, substring) {
			return true
		}
	}
	return false
}

func (cl *captureLogger) getMessages() []string {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	// Return a copy to avoid race conditions
	result := make([]string, len(cl.messages))
	copy(result, cl.messages)
	return result
}

func TestComprehensiveLogging(t *testing.T) {
	t.Run("Primary tier hit logging", func(t *testing.T) {
		captureLog := &captureLogger{}
		logger := log.With(captureLog, "component", "test")

		hot := newMockStore()
		cold := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(hot, cold))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "test-key"
		testData := "test data"

		// Set data first
		hot.data[key] = []byte(testData)
		hot.meta[key] = &daramjwee.Metadata{CachedAt: time.Now()}

		// Get data (should hit primary tier)
		fetcher := &mockFetcher{content: testData}
		stream, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		defer stream.Close()

		// Debug: Print all captured messages
		t.Logf("Captured messages: %v", captureLog.getMessages())

		// Verify logging
		assert.True(t, captureLog.hasMessage("cache hit in primary tier"))
		assert.True(t, captureLog.hasMessage("tier=0"))
	})

	t.Run("Lower tier hit and promotion logging", func(t *testing.T) {
		captureLog := &captureLogger{}
		logger := log.With(captureLog, "component", "test")

		hot := newMockStore()
		cold := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(hot, cold))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "test-key"
		testData := "test data"

		// Set data only in cold tier
		cold.data[key] = []byte(testData)
		cold.meta[key] = &daramjwee.Metadata{CachedAt: time.Now()}

		// Get data (should hit cold tier and promote)
		fetcher := &mockFetcher{content: testData}
		stream, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		defer stream.Close()

		// Verify logging
		assert.True(t, captureLog.hasMessage("cache hit in lower tier"))
		assert.True(t, captureLog.hasMessage("source_tier=1"))
		assert.True(t, captureLog.hasMessage("starting promotion"))
	})

	t.Run("daramjwee.Cache miss logging", func(t *testing.T) {
		captureLog := &captureLogger{}
		logger := log.With(captureLog, "component", "test")

		hot := newMockStore()
		cold := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(hot, cold))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "missing-key"
		testData := "test data"

		// Get data (should miss all tiers)
		fetcher := &mockFetcher{content: testData}
		stream, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		defer stream.Close()

		// Verify logging
		assert.True(t, captureLog.hasMessage("cache miss in all tiers"))
		assert.True(t, captureLog.hasMessage("total_tiers=2"))
	})

	t.Run("Delete operation logging", func(t *testing.T) {
		captureLog := &captureLogger{}
		logger := log.With(captureLog, "component", "test")

		hot := newMockStore()
		cold := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(hot, cold))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "delete-key"

		// Set data in both tiers
		hot.data[key] = []byte("data")
		hot.meta[key] = &daramjwee.Metadata{}
		cold.data[key] = []byte("data")
		cold.meta[key] = &daramjwee.Metadata{}

		// Delete data
		err = cache.Delete(ctx, key)
		require.NoError(t, err)

		// Verify logging
		assert.True(t, captureLog.hasMessage("initiating delete operation"))
		assert.True(t, captureLog.hasMessage("total_tiers=2"))
		assert.True(t, captureLog.hasMessage("delete operation completed successfully"))
	})

	t.Run("Error logging with tier information", func(t *testing.T) {
		captureLog := &captureLogger{}
		logger := log.With(captureLog, "component", "test")

		hot := newMockStore()
		cold := newMockStore()
		cold.err = errors.daramjwee.New("simulated tier error") // Force error in tier 1

		cache, err := daramjwee.New(logger, WithStores(hot, cold))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "error-key"

		// Get data (should fail in cold tier)
		fetcher := &mockFetcher{content: "data"}
		stream, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err) // Should still succeed by fetching from origin
		defer stream.Close()

		// Verify error logging
		assert.True(t, captureLog.hasMessage("tier lookup failed with error"))
		assert.True(t, captureLog.hasMessage("tier=1"))
	})

	t.Run("Negative cache logging", func(t *testing.T) {
		captureLog := &captureLogger{}
		logger := log.With(captureLog, "component", "test")

		hot := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(hot), WithNegativeCache(5*time.Minute))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "not-found-key"

		// Get data that doesn't exist
		fetcher := &mockFetcher{err: ErrCacheableNotFound}
		_, err = cache.Get(ctx, key, fetcher)
		require.Error(t, err)
		assert.True(t, errors.Is(err, daramjwee.ErrNotFound))

		// Verify negative cache logging
		assert.True(t, captureLog.hasMessage("storing negative cache entry"))
		assert.True(t, captureLog.hasMessage("tier=0"))
	})
}

func TestTierSpecificLogging(t *testing.T) {
	t.Run("Sequential lookup logging", func(t *testing.T) {
		captureLog := &captureLogger{}
		logger := log.With(captureLog, "component", "test")

		// Create 3-tier setup
		tier0 := newMockStore()
		tier1 := newMockStore()
		tier2 := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(tier0, tier1, tier2))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "multi-tier-key"
		testData := "test data"

		// Set data only in tier 2
		tier2.data[key] = []byte(testData)
		tier2.meta[key] = &daramjwee.Metadata{CachedAt: time.Now()}

		// Get data (should check all tiers)
		fetcher := &mockFetcher{content: testData}
		stream, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		defer stream.Close()

		// Verify sequential lookup logging
		assert.True(t, captureLog.hasMessage("starting sequential lookup"))
		assert.True(t, captureLog.hasMessage("total_tiers=3"))
		assert.True(t, captureLog.hasMessage("checking tier for cache hit"))
		assert.True(t, captureLog.hasMessage("cache miss in tier"))
		assert.True(t, captureLog.hasMessage("tier=0"))
		assert.True(t, captureLog.hasMessage("tier=1"))
		assert.True(t, captureLog.hasMessage("source_tier=2"))
	})
}
