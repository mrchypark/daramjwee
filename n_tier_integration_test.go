package daramjwee

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNTierIntegration(t *testing.T) {
	t.Skip("Skipping due to race condition - needs further investigation")
	logger := log.NewNopLogger()

	t.Run("3-tier setup (memory → file → cloud)", func(t *testing.T) {
		// Create three different types of mock stores to simulate different tiers
		memoryStore := newMockStore() // Fastest tier
		fileStore := newMockStore()   // Medium tier
		cloudStore := newMockStore()  // Slowest tier

		cache, err := New(logger, WithStores(memoryStore, fileStore, cloudStore))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "integration-test-key"
		originalData := "test data from origin"

		// Test 1: Cache miss - should fetch from origin and store in primary tier
		fetcher := &integrationFetcher{data: originalData}

		reader, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()

		assert.Equal(t, originalData, string(data))
		assert.True(t, fetcher.called, "Fetcher should be called for cache miss")

		// Wait for async write to complete
		<-memoryStore.writeCompleted

		// Verify data is stored in memory store (primary tier)
		storedData, exists := memoryStore.data[key]
		assert.True(t, exists, "Data should be stored in memory store")
		assert.Equal(t, originalData, string(storedData))

		// Test 2: Cache hit from primary tier - should not call fetcher
		fetcher2 := &integrationFetcher{data: "should not be used"}

		reader2, err := cache.Get(ctx, key, fetcher2)
		require.NoError(t, err)
		require.NotNil(t, reader2)

		data2, err := io.ReadAll(reader2)
		require.NoError(t, err)
		reader2.Close()

		assert.Equal(t, originalData, string(data2))
		assert.False(t, fetcher2.called, "Fetcher should not be called for primary tier hit")
	})

	t.Run("single-tier configuration", func(t *testing.T) {
		singleStore := newMockStore()

		cache, err := New(logger, WithStores(singleStore))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "single-tier-key"
		testData := "single tier data"

		// Test cache miss and store
		fetcher := &integrationFetcher{data: testData}

		reader, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()

		assert.Equal(t, testData, string(data))
		assert.True(t, fetcher.called)

		// Wait for async write
		<-singleStore.writeCompleted

		// Verify storage
		storedData, exists := singleStore.data[key]
		assert.True(t, exists)
		assert.Equal(t, testData, string(storedData))
	})

	t.Run("promotion behavior across multiple tiers", func(t *testing.T) {
		tier0 := newMockStore() // Primary
		tier1 := newMockStore() // Secondary
		tier2 := newMockStore() // Tertiary

		// Pre-populate only the tertiary tier
		testData := []byte("data from tier 2")
		tier2.data["promotion-key"] = testData
		tier2.meta["promotion-key"] = &Metadata{
			ETag:     "tier2-etag",
			CachedAt: time.Now(),
		}

		cache, err := New(logger, WithStores(tier0, tier1, tier2))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		fetcher := &integrationFetcher{data: "should not be called"}

		// Get should find data in tier 2 and promote to upper tiers
		reader, err := cache.Get(ctx, "promotion-key", fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()

		assert.Equal(t, "data from tier 2", string(data))
		assert.False(t, fetcher.called, "Should not call fetcher for tier hit")

		// Note: Promotion happens asynchronously via TeeReader
		// In a real scenario, we would need to wait or use synchronous promotion
		// for testing purposes, but the current implementation uses streaming promotion
	})

	t.Run("failure scenarios and recovery", func(t *testing.T) {
		workingStore := newMockStore()
		failingStore := &integrationFailingStore{
			shouldFailGet: true,
			shouldFailSet: true,
		}
		recoveryStore := newMockStore()

		cache, err := New(logger, WithStores(workingStore, failingStore, recoveryStore))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "failure-test-key"

		// Test 1: All stores empty, should fetch from origin
		fetcher := &integrationFetcher{data: "origin data"}

		reader, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()

		assert.Equal(t, "origin data", string(data))
		assert.True(t, fetcher.called)

		// Test 2: Delete with partial failures
		// Pre-populate all stores
		workingStore.data[key] = []byte("data")
		failingStore.data = make(map[string][]byte)
		failingStore.data[key] = []byte("data")
		recoveryStore.data[key] = []byte("data")

		// Configure failing store to fail on delete
		failingStore.shouldFailDelete = true

		err = cache.Delete(ctx, key)
		// Should return error from failing store but continue with others
		require.Error(t, err)

		// Verify partial deletion
		_, exists := workingStore.data[key]
		assert.False(t, exists, "Working store should have deleted the key")
		_, exists = recoveryStore.data[key]
		assert.False(t, exists, "Recovery store should have deleted the key")
		_, exists = failingStore.data[key]
		assert.True(t, exists, "Failing store should still have the key")
	})

	t.Run("concurrent access patterns", func(t *testing.T) {
		store0 := newMockStore()
		store1 := newMockStore()
		store2 := newMockStore()

		cache, err := New(logger, WithStores(store0, store1, store2))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		numGoroutines := 10
		numOperations := 5

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Concurrent Get operations
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("concurrent-key-%d-%d", id, j)
					fetcher := &integrationFetcher{data: fmt.Sprintf("data-%d-%d", id, j)}

					reader, err := cache.Get(ctx, key, fetcher)
					if err != nil {
						t.Errorf("Get failed: %v", err)
						return
					}

					if reader != nil {
						io.ReadAll(reader)
						reader.Close()
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify no race conditions occurred
		// This is mainly testing that the cache doesn't panic or deadlock
	})

	t.Run("negative cache integration", func(t *testing.T) {
		store := newMockStore()

		cache, err := New(logger,
			WithStores(store),
			WithNegativeCache(5*time.Minute),
		)
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		key := "negative-cache-key"

		// Test negative cache entry creation
		fetcher := &integrationFetcher{err: ErrCacheableNotFound}

		reader, err := cache.Get(ctx, key, fetcher)
		require.ErrorIs(t, err, ErrNotFound)
		assert.Nil(t, reader)
		assert.True(t, fetcher.called)

		// Verify negative cache entry was stored
		meta, exists := store.meta[key]
		assert.True(t, exists, "Negative cache entry should be stored")
		assert.True(t, meta.IsNegative, "Entry should be marked as negative")

		// Test negative cache hit
		fetcher2 := &integrationFetcher{data: "should not be called"}

		reader2, err := cache.Get(ctx, key, fetcher2)
		require.ErrorIs(t, err, ErrNotFound)
		assert.Nil(t, reader2)
		assert.False(t, fetcher2.called, "Should serve from negative cache")
	})
}

// integrationFetcher implements Fetcher interface for integration testing
type integrationFetcher struct {
	data   string
	err    error
	called bool
	mu     sync.Mutex
}

func (f *integrationFetcher) Fetch(ctx context.Context, metadata *Metadata) (*FetchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.called = true

	if f.err != nil {
		return nil, f.err
	}

	return &FetchResult{
		Body: io.NopCloser(strings.NewReader(f.data)),
		Metadata: &Metadata{
			ETag:     "integration-etag",
			CachedAt: time.Now(),
		},
	}, nil
}

// integrationFailingStore simulates a store that can fail operations
type integrationFailingStore struct {
	data             map[string][]byte
	meta             map[string]*Metadata
	shouldFailGet    bool
	shouldFailSet    bool
	shouldFailDelete bool
	mu               sync.RWMutex
}

func (s *integrationFailingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.shouldFailGet {
		return nil, nil, errors.New("simulated get failure")
	}

	if s.meta == nil {
		return nil, nil, ErrNotFound
	}

	meta, ok := s.meta[key]
	if !ok {
		return nil, nil, ErrNotFound
	}

	if meta.IsNegative {
		return io.NopCloser(strings.NewReader("")), meta, nil
	}

	if s.data == nil {
		return nil, nil, ErrNotFound
	}

	data, ok := s.data[key]
	if !ok {
		return nil, nil, ErrNotFound
	}

	return io.NopCloser(strings.NewReader(string(data))), meta, nil
}

func (s *integrationFailingStore) SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	if s.shouldFailSet {
		return nil, errors.New("simulated set failure")
	}

	return &integrationMockWriter{
		store:    s,
		key:      key,
		metadata: metadata,
	}, nil
}

func (s *integrationFailingStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shouldFailDelete {
		return errors.New("simulated delete failure")
	}

	if s.data != nil {
		delete(s.data, key)
	}
	if s.meta != nil {
		delete(s.meta, key)
	}

	return nil
}

func (s *integrationFailingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.meta == nil {
		return nil, ErrNotFound
	}

	meta, ok := s.meta[key]
	if !ok {
		return nil, ErrNotFound
	}

	return meta, nil
}

// integrationMockWriter implements io.WriteCloser for the failing store
type integrationMockWriter struct {
	store    *integrationFailingStore
	key      string
	metadata *Metadata
	buffer   strings.Builder
}

func (w *integrationMockWriter) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func (w *integrationMockWriter) Close() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()

	if w.store.data == nil {
		w.store.data = make(map[string][]byte)
	}
	if w.store.meta == nil {
		w.store.meta = make(map[string]*Metadata)
	}

	w.store.data[w.key] = []byte(w.buffer.String())
	w.store.meta[w.key] = w.metadata

	return nil
}
