package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFetcher is a mock implementation of the Fetcher interface for testing purposes.
type mockFetcher struct {
	mu              sync.Mutex
	fetchCount      int
	content         string
	etag            string
	err             error
	fetchDelay      time.Duration
	lastOldMetadata *Metadata
}

// Fetch simulates fetching data from an origin, incrementing a fetch counter.
func (f *mockFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	f.mu.Lock()
	f.fetchCount++
	f.lastOldMetadata = oldMetadata
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(f.fetchDelay):
	}
	if f.err != nil {
		return nil, f.err
	}
	return &FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(f.content))),
		Metadata: &Metadata{ETag: f.etag},
	}, nil
}

// getFetchCount returns the number of times Fetch was called.
func (f *mockFetcher) getFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchCount
}

// mockStore is a mock implementation of the Store interface for testing purposes.
type mockStore struct {
	mu             sync.RWMutex
	data           map[string][]byte
	meta           map[string]*Metadata
	err            error
	writeCompleted chan string
	forceSetError  bool // forceSetError, if true, makes SetWithWriter return an error.
}

// newMockStore creates a new mockStore.
func newMockStore() *mockStore {
	return &mockStore{
		data:           make(map[string][]byte),
		meta:           make(map[string]*Metadata),
		writeCompleted: make(chan string, 100),
	}
}

// GetStream simulates retrieving a stream from the store.
func (s *mockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.err != nil {
		return nil, nil, s.err
	}
	meta, ok := s.meta[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	if meta.IsNegative {
		return io.NopCloser(bytes.NewReader(nil)), meta, nil
	}
	data, ok := s.data[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

// SetWithWriter simulates writing a stream to the store.
func (s *mockStore) SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	if s.forceSetError {
		return nil, errors.New("simulated set error")
	}

	if s.err != nil {
		return nil, s.err
	}

	var buf bytes.Buffer
	return &mockWriteCloser{
		onClose: func() error {
			s.mu.Lock()
			defer s.mu.Unlock()

			dataBytes := make([]byte, buf.Len())
			copy(dataBytes, buf.Bytes())

			s.meta[key] = metadata
			if !metadata.IsNegative {
				s.data[key] = dataBytes
			}

			select {
			case s.writeCompleted <- key:
			default:
			}
			return nil
		},
		buf: &buf,
	}, nil
}

// Delete simulates deleting an entry from the store.
func (s *mockStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.meta, key)
	return nil
}

// Stat simulates retrieving metadata for an entry from the store.
func (s *mockStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.meta[key]
	if !ok {
		return nil, ErrNotFound
	}
	return meta, nil
}

// setData sets content and metadata for a given key in the mock store.
func (s *mockStore) setData(key, content string, metadata *Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = []byte(content)
	s.meta[key] = metadata
}

// setNegativeEntry sets a negative cache entry for a given key.
func (s *mockStore) setNegativeEntry(key string, metadata *Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata.IsNegative = true
	s.meta[key] = metadata
	delete(s.data, key)
}

// mockWriteCloser is a mock implementation of io.WriteCloser.
type mockWriteCloser struct {
	buf     *bytes.Buffer
	onClose func() error
}

// Write writes bytes to the underlying buffer.
func (mwc *mockWriteCloser) Write(p []byte) (n int, err error) { return mwc.buf.Write(p) }

// Close executes the onClose callback.
func (mwc *mockWriteCloser) Close() error { return mwc.onClose() }

// deterministicFetcher is a mock fetcher that allows controlling fetch start and end signals.
type deterministicFetcher struct {
	mockFetcher
	fetchStarted chan struct{}
	fetchEnd     chan struct{}
	onFetch      func()
}

// newDeterministicFetcher creates a new deterministicFetcher.
func newDeterministicFetcher(content, etag string, err error) *deterministicFetcher {
	return &deterministicFetcher{
		mockFetcher:  mockFetcher{content: content, etag: etag, err: err},
		fetchStarted: make(chan struct{}, 1),
		fetchEnd:     make(chan struct{}, 1),
	}
}

// Fetch simulates fetching data, sending signals before and after the fetch operation.
func (f *deterministicFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	f.fetchStarted <- struct{}{}
	defer func() { f.fetchEnd <- struct{}{} }()
	if f.onFetch != nil {
		f.onFetch()
	}
	return f.mockFetcher.Fetch(ctx, oldMetadata)
}

// setupCache creates a new cache instance with mock stores for testing.
func setupCache(t *testing.T, opts ...Option) (Cache, *mockStore, *mockStore) {
	hot := newMockStore()
	cold := newMockStore()
	finalOpts := []Option{
		WithHotStore(hot),
		WithColdStore(cold),
		WithDefaultTimeout(2 * time.Second),
		WithShutdownTimeout(2 * time.Second),
		WithWorker("pool", 5, 20, 1*time.Second),
	}
	finalOpts = append(finalOpts, opts...)
	cache, err := New(log.NewNopLogger(), finalOpts...)
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	return cache, hot, cold
}

// TestCache_Get_FullMiss verifies the behavior when data is not present in any cache tier.
func TestCache_Get_FullMiss(t *testing.T) {
	cache, hot, _ := setupCache(t)
	ctx := context.Background()
	key, content, etag := "miss-key", "origin content", "v-origin"
	fetcher := &mockFetcher{content: content, etag: etag}

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err, "should not get an error reading from stream")

	err = stream.Close()
	require.NoError(t, err)

	<-hot.writeCompleted

	assert.Equal(t, content, string(readBytes))
	assert.Equal(t, 1, fetcher.getFetchCount())

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	require.NotNil(t, hot.meta[key], "metadata should exist in Hot cache")
	assert.Equal(t, content, string(hot.data[key]))
	assert.Equal(t, etag, hot.meta[key].ETag)
	assert.False(t, hot.meta[key].CachedAt.IsZero())
}

// TestCache_Get_ColdHit verifies cold cache hit and promotion to hot cache.
func TestCache_Get_ColdHit(t *testing.T) {
	cache, hot, cold := setupCache(t)
	ctx := context.Background()
	key, content, etag := "cold-key", "cold content", "v-cold"

	cold.setData(key, content, &Metadata{ETag: etag, CachedAt: time.Now().Add(-1 * time.Hour)})

	stream, err := cache.Get(ctx, key, &mockFetcher{})
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)

	err = stream.Close()
	require.NoError(t, err)

	<-hot.writeCompleted

	assert.Equal(t, content, string(readBytes))

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	require.NotNil(t, hot.data[key])
	assert.Equal(t, content, string(hot.data[key]))
	require.NotNil(t, hot.meta[key])
	assert.Equal(t, etag, hot.meta[key].ETag)
	assert.True(t, hot.meta[key].CachedAt.After(time.Now().Add(-1*time.Minute)))
}

func TestCache_StaleHit_ServesStaleWhileRefreshing(t *testing.T) {
	cache, hot, _ := setupCache(t, WithCache(1*time.Minute))
	ctx := context.Background()
	key, staleContent, freshContent := "stale-key", "stale data", "fresh data"
	hot.setData(key, staleContent, &Metadata{ETag: "v1", CachedAt: time.Now().Add(-2 * time.Minute)})
	fetcher := newDeterministicFetcher(freshContent, "v2", nil)

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	readBytes, _ := io.ReadAll(stream)
	stream.Close()
	assert.Equal(t, staleContent, string(readBytes))

	<-fetcher.fetchStarted
	<-fetcher.fetchEnd
	<-hot.writeCompleted

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.Equal(t, freshContent, string(hot.data[key]))
	assert.Equal(t, "v2", hot.meta[key].ETag)
}

func TestCache_NegativeCache_On_ErrCacheableNotFound(t *testing.T) {
	cache, hot, _ := setupCache(t, WithNegativeCache(5*time.Minute))
	ctx := context.Background()
	key := "negative-key"
	fetcher := &mockFetcher{err: ErrCacheableNotFound}

	stream, err := cache.Get(ctx, key, fetcher)
	require.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, stream)
	assert.Equal(t, 1, fetcher.getFetchCount())

	hot.mu.RLock()
	meta, ok := hot.meta[key]
	hot.mu.RUnlock()
	require.True(t, ok)
	assert.True(t, meta.IsNegative)

	stream, err = cache.Get(ctx, key, fetcher)
	require.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, stream)
	assert.Equal(t, 1, fetcher.getFetchCount())
}

func TestCache_NegativeCache_StaleHit(t *testing.T) {
	cache, hot, _ := setupCache(t, WithNegativeCache(1*time.Minute))
	ctx := context.Background()
	key, freshContent := "stale-negative-key", "now it exists"
	hot.setNegativeEntry(key, &Metadata{CachedAt: time.Now().Add(-2 * time.Minute)})
	fetcher := newDeterministicFetcher(freshContent, "v1", nil)

	stream, err := cache.Get(ctx, key, fetcher)
	require.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, stream)

	<-fetcher.fetchStarted
	<-fetcher.fetchEnd
	<-hot.writeCompleted

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.False(t, hot.meta[key].IsNegative)
	assert.Equal(t, freshContent, string(hot.data[key]))
}

// TestCache_Get_FetcherError verifies that Get returns an error when the fetcher fails.
func TestCache_Get_FetcherError(t *testing.T) {
	cache, _, _ := setupCache(t)
	expectedErr := errors.New("origin server is down")
	fetcher := &mockFetcher{err: expectedErr}
	_, err := cache.Get(context.Background(), "any-key", fetcher)
	assert.ErrorIs(t, err, expectedErr)
}

// TestCache_Get_ContextCancellation verifies that Get respects context cancellation.
func TestCache_Get_ContextCancellation(t *testing.T) {
	cache, _, _ := setupCache(t)
	fetcher := &mockFetcher{content: "some data", fetchDelay: 200 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := cache.Get(ctx, "timeout-key", fetcher)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestCache_Set_Directly verifies that data can be directly set into the cache.
func TestCache_Set_Directly(t *testing.T) {
	cache, hot, _ := setupCache(t)
	ctx := context.Background()
	key, content, etag := "set-key", "direct set content", "v-set"

	writer, err := cache.Set(ctx, key, &Metadata{ETag: etag})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.Equal(t, content, string(hot.data[key]))
	assert.Equal(t, etag, hot.meta[key].ETag)
	assert.False(t, hot.meta[key].CachedAt.IsZero(), "CachedAt should be set on direct Set")
}

// TestCache_Delete verifies that Delete removes items from both hot and cold stores.
func TestCache_Delete(t *testing.T) {
	cache, hot, cold := setupCache(t)
	key := "delete-key"
	hot.setData(key, "hot", &Metadata{})
	cold.setData(key, "cold", &Metadata{})

	err := cache.Delete(context.Background(), key)
	require.NoError(t, err)

	hot.mu.RLock()
	_, hotOk := hot.data[key]
	hot.mu.RUnlock()
	assert.False(t, hotOk)

	cold.mu.RLock()
	_, coldOk := cold.data[key]
	cold.mu.RUnlock()
	assert.False(t, coldOk)
}

// TestCache_Close verifies the behavior after cache.Close() is called.
func TestCache_Close(t *testing.T) {
	cache, hot, _ := setupCache(t, WithCache(1*time.Minute))
	key := "key-after-close"
	hot.setData(key, "data", &Metadata{CachedAt: time.Now().Add(-2 * time.Minute)})
	fetcher := &mockFetcher{content: "new", etag: "v2"}

	cache.Close()

	stream, err := cache.Get(context.Background(), key, fetcher)

	require.ErrorIs(t, err, ErrCacheClosed, "Get() on a closed cache should now return ErrCacheClosed")
	assert.Nil(t, stream, "Stream should be nil when an error is returned")

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, fetcher.getFetchCount(), "Fetcher should not be called after cache is closed")
}

// TestCache_Get_NotModified_ButEvictedRace_Deterministic tests a race condition
// where an item is reported as not modified but is evicted from the hot cache
// before it can be served.
func TestCache_Get_NotModified_ButEvictedRace_Deterministic(t *testing.T) {
	cache, hot, _ := setupCache(t)
	ctx := context.Background()
	key := "race-key"
	hot.mu.Lock()
	hot.meta[key] = &Metadata{ETag: "v1", CachedAt: time.Now()}
	hot.mu.Unlock()

	fetcher := newDeterministicFetcher("", "v1", ErrNotModified)
	fetcher.onFetch = func() {
		hot.Delete(context.Background(), key)
	}

	stream, err := cache.Get(ctx, key, fetcher)
	require.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, stream)
	assert.Equal(t, 1, fetcher.getFetchCount())
}

// TestCache_Concurrent_GetAndDelete verifies concurrent Get and Delete operations.
func TestCache_Concurrent_GetAndDelete(t *testing.T) {
	cache, hot, _ := setupCache(t)
	key := "get-delete-key"
	hot.setData(key, "some content", &Metadata{})

	var wg sync.WaitGroup
	wg.Add(2)

	getStarted := make(chan struct{})

	go func() {
		defer wg.Done()
		fetcherForGet := &mockFetcher{err: ErrNotModified, etag: "v1"}

		close(getStarted)

		stream, err := cache.Get(context.Background(), key, fetcherForGet)
		if err == nil {
			stream.Close()
		}
	}()

	go func() {
		defer wg.Done()
		<-getStarted

		err := cache.Delete(context.Background(), key)
		assert.NoError(t, err)
	}()

	wg.Wait()

	_, _, err := hot.GetStream(context.Background(), key)
	assert.ErrorIs(t, err, ErrNotFound, "Item should be deleted after all operations")
}

// TestCache_ReturnsError_AfterClose verifies that subsequent calls to Get, Set, and Delete
// return ErrCacheClosed after the cache instance has been closed.
func TestCache_ReturnsError_AfterClose(t *testing.T) {
	cache, _, _ := setupCache(t)
	ctx := context.Background()
	key := "any-key-after-close"

	cache.Close()

	_, getErr := cache.Get(ctx, key, &mockFetcher{})
	assert.ErrorIs(t, getErr, ErrCacheClosed, "Get() on a closed cache should return ErrCacheClosed")

	_, setErr := cache.Set(ctx, key, &Metadata{})
	assert.ErrorIs(t, setErr, ErrCacheClosed, "Set() on a closed cache should return ErrCacheClosed")

	deleteErr := cache.Delete(ctx, key)
	assert.ErrorIs(t, deleteErr, ErrCacheClosed, "Delete() on a closed cache should return ErrCacheClosed")

	_ = cache.ScheduleRefresh(ctx, key, &mockFetcher{})
	if dc, ok := cache.(*DaramjweeCache); ok {
		if dc.isClosed.Load() {
		}
	}
}

// mockCloser is a mock implementation of io.Closer that tracks whether Close was called and can simulate errors.
type mockCloser struct {
	isClosed    bool
	shouldError bool
	closeErr    error
}

// Close sets isClosed to true and returns an error if shouldError is true.
func (mc *mockCloser) Close() error {
	mc.isClosed = true
	if mc.shouldError {
		if mc.closeErr != nil {
			return mc.closeErr
		}
		return errors.New("mock closer failed as intended")
	}
	return nil
}

// TestCache_Get_ColdHit_PromotionFails verifies the edge case where a cold cache hit occurs,
// but the promotion to the hot tier fails.
func TestCache_Get_ColdHit_PromotionFails(t *testing.T) {
	hot := newMockStore()
	hot.forceSetError = true

	cold := newMockStore()
	key, content, etag := "promotion-fail-key", "cold content only", "v-cold"
	cold.setData(key, content, &Metadata{ETag: etag})

	cache, err := New(log.NewNopLogger(), WithHotStore(hot), WithColdStore(cold))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), key, &mockFetcher{})

	require.NoError(t, err, "Get should not fail even if promotion fails")
	require.NotNil(t, stream, "A valid stream should be returned from the cold cache")

	readBytes, readErr := io.ReadAll(stream)
	require.NoError(t, readErr)
	assert.Equal(t, content, string(readBytes), "The content from the cold cache should be served")
	stream.Close()

	hot.mu.RLock()
	_, exists := hot.data[key]
	hot.mu.RUnlock()
	assert.False(t, exists, "Data should not be promoted to the hot store on failure")
}

// TestCache_FreshForZero_AlwaysTriggersRefresh verifies that when PositiveFreshFor or NegativeFreshFor
// is set to zero, a background refresh is always triggered on each Get() call.
func TestCache_FreshForZero_AlwaysTriggersRefresh(t *testing.T) {
	t.Run("PositiveCacheWithFreshForZero", func(t *testing.T) {
		cache, hot, _ := setupCache(t, WithCache(0))
		ctx := context.Background()
		key := "fresh-for-zero-key"
		initialContent := "initial data"

		fetcher := &mockFetcher{content: "new data", etag: "v2"}

		hot.setData(key, initialContent, &Metadata{ETag: "v1", CachedAt: time.Now()})

		stream, err := cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		readBytes, _ := io.ReadAll(stream)
		stream.Close()

		assert.Equal(t, initialContent, string(readBytes))
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, 1, fetcher.getFetchCount(), "Fetcher should be called once after the first Get()")

		stream, err = cache.Get(ctx, key, fetcher)
		require.NoError(t, err)
		stream.Close()

		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, 2, fetcher.getFetchCount(), "Fetcher should be called twice after the second Get()")
	})

	t.Run("NegativeCacheWithFreshForZero", func(t *testing.T) {
		cache, hot, _ := setupCache(t, WithNegativeCache(0))
		ctx := context.Background()
		key := "negative-fresh-for-zero-key"

		fetcher := &mockFetcher{err: ErrCacheableNotFound}

		hot.setNegativeEntry(key, &Metadata{CachedAt: time.Now()})

		_, err := cache.Get(ctx, key, fetcher)
		require.ErrorIs(t, err, ErrNotFound)

		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, 1, fetcher.getFetchCount(), "Fetcher should be called once after the first Get() for negative cache")

		_, err = cache.Get(ctx, key, fetcher)
		require.ErrorIs(t, err, ErrNotFound)

		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, 2, fetcher.getFetchCount(), "Fetcher should be called twice after the second Get() for negative cache")
	})
}

// setupBenchmarkCache is a helper function for benchmarks.
func setupBenchmarkCache(b *testing.B, opts ...Option) (Cache, *mockStore, *mockStore) {
	b.Helper()
	hot := newMockStore()
	cold := newMockStore()
	finalOpts := []Option{
		WithHotStore(hot),
		WithColdStore(cold),
		WithDefaultTimeout(2 * time.Second),
		WithShutdownTimeout(2 * time.Second),
		WithWorker("pool", 5, 20, 1*time.Second),
	}
	finalOpts = append(finalOpts, opts...)
	cache, err := New(log.NewNopLogger(), finalOpts...)
	if err != nil {
		b.Fatalf("Failed to create cache for benchmark: %v", err)
	}
	b.Cleanup(cache.Close)
	return cache, hot, cold
}

// BenchmarkCache_Get_HotHit measures the end-to-end Get performance when a cache hit occurs in the Hot Tier.
func BenchmarkCache_Get_HotHit(b *testing.B) {
	cache, hot, _ := setupBenchmarkCache(b)
	key, content := "hot-hit-key", "this is hot content"
	hot.setData(key, content, &Metadata{})

	fetcher := &mockFetcher{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stream, err := cache.Get(context.Background(), key, fetcher)
			if err != nil {
				b.Errorf("Get failed: %v", err)
				continue
			}
			io.Copy(io.Discard, stream)
			stream.Close()
		}
	})
}

// BenchmarkCache_Get_ColdHit measures the performance when a cache hit occurs in the Cold Tier
// and the data is promoted to the Hot Tier.
func BenchmarkCache_Get_ColdHit(b *testing.B) {
	cache, _, cold := setupBenchmarkCache(b)
	key, content := "cold-hit-key", "this is cold content"
	cold.setData(key, content, &Metadata{})
	fetcher := &mockFetcher{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stream, err := cache.Get(context.Background(), key, fetcher)
			if err != nil {
				b.Errorf("Get failed: %v", err)
				continue
			}
			io.Copy(io.Discard, stream)
			stream.Close()
		}
	})
}

// BenchmarkCache_Get_Miss measures the performance when data is not found in any cache tier
// and must be fetched from the origin. This is the most expensive operation.
func BenchmarkCache_Get_Miss(b *testing.B) {
	cache, _, _ := setupBenchmarkCache(b)
	content := "this is fresh content from origin"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := fmt.Sprintf("miss-key-%d", i)
			fetcher := &mockFetcher{content: content, etag: "v-fresh"}

			stream, err := cache.Get(context.Background(), key, fetcher)
			if err != nil {
				b.Errorf("Get failed: %v", err)
				continue
			}
			io.Copy(io.Discard, stream)
			stream.Close()
			i++
		}
	})
}
