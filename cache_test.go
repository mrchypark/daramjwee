// Filename: cache_test.go
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

// --- Mock Implementations for testing ---

// mockStore is a mock implementation of the Store interface.
type mockStore struct {
	mu           sync.RWMutex
	data         map[string][]byte
	meta         map[string]*Metadata
	getErr       error // Error to return on Get
	setErr       error // Error to return on Set
	statErr      error // Error to return on Stat
	setCallCount int
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string][]byte),
		meta: make(map[string]*Metadata),
	}
}

func (s *mockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	if s.getErr != nil {
		return nil, nil, s.getErr
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.data[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	meta := s.meta[key]
	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

func (s *mockStore) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	s.mu.Lock()
	s.setCallCount++
	s.mu.Unlock()

	if s.setErr != nil {
		return nil, s.setErr
	}
	var buf bytes.Buffer
	return &mockWriteCloser{
		onClose: func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.data[key] = buf.Bytes()
			s.meta[key] = &Metadata{ETag: etag}
			return nil
		},
		buf: &buf,
	}, nil
}

func (s *mockStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.meta, key)
	return nil
}

func (s *mockStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	if s.statErr != nil {
		return nil, s.statErr
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.meta[key]
	if !ok {
		return nil, ErrNotFound
	}
	return meta, nil
}

type mockWriteCloser struct {
	buf     *bytes.Buffer
	onClose func() error
}

func (mwc *mockWriteCloser) Write(p []byte) (n int, err error) { return mwc.buf.Write(p) }
func (mwc *mockWriteCloser) Close() error                      { return mwc.onClose() }

// mockFetcher is a mock implementation of the Fetcher interface.
type mockFetcher struct {
	mu          sync.Mutex
	fetchCount  int
	content     string
	etag        string
	err         error
	fetchDelay  time.Duration
	lastOldETag string
}

func (f *mockFetcher) Fetch(ctx context.Context, oldETag string) (*FetchResult, error) {
	f.mu.Lock()
	f.fetchCount++
	f.lastOldETag = oldETag
	f.mu.Unlock()

	// Honor context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(f.fetchDelay):
		// continue
	}

	if f.err != nil {
		return nil, f.err
	}

	if oldETag == f.etag {
		return nil, ErrNotModified
	}

	return &FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(f.content))),
		Metadata: &Metadata{ETag: f.etag},
	}, nil
}

func (f *mockFetcher) getFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchCount
}

// --- Test Cases ---

func setupCache(t *testing.T, opts ...Option) (Cache, *mockStore, *mockStore) {
	hot := newMockStore()
	cold := newMockStore()

	finalOpts := []Option{
		WithHotStore(hot),
		WithColdStore(cold),
		WithDefaultTimeout(2 * time.Second),
		WithWorker("pool", 2, 10, 1*time.Second),
	}
	finalOpts = append(finalOpts, opts...)

	cache, err := New(log.NewNopLogger(), finalOpts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	return cache, hot, cold
}

// TestCache_Get_HotHit tests a simple hot cache hit scenario.
func TestCache_Get_HotHit(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)

	key := "my-key"
	content := "hot content"
	hot.data[key] = []byte(content)
	hot.meta[key] = &Metadata{ETag: "v1"}

	fetcher := &mockFetcher{content: "new content", etag: "v2"}

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	defer stream.Close()

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, content, string(readBytes))

	assert.Equal(t, 0, fetcher.getFetchCount())

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, fetcher.getFetchCount(), "Fetcher should be called in the background on hot hit")

	hot.mu.RLock()
	assert.Equal(t, "v2", hot.meta[key].ETag, "Hot cache should be updated after background refresh")
	hot.mu.RUnlock()
}

// TestCache_Get_ColdHit tests a cold cache hit and promotion to hot.
func TestCache_Get_ColdHit(t *testing.T) {
	ctx := context.Background()
	cache, hot, cold := setupCache(t)

	key := "my-key"
	content := "cold content"
	cold.data[key] = []byte(content)
	cold.meta[key] = &Metadata{ETag: "v1-cold"}

	fetcher := &mockFetcher{}

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)

	// Read the entire stream first. This populates the hotWriter's buffer.
	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)

	// Now, explicitly close the stream. This triggers the hotWriter's onClose,
	// which commits the buffer to the mock hot store.
	err = stream.Close()
	require.NoError(t, err)

	// --- Assertions ---
	assert.Equal(t, content, string(readBytes))
	assert.Equal(t, 0, fetcher.getFetchCount(), "Fetcher should not be called on cold hit")

	// Now that the stream is closed, we can safely verify the state of the hot store.
	hot.mu.RLock()
	defer hot.mu.RUnlock()
	require.NotNil(t, hot.data[key], "Data should be promoted to hot cache")
	assert.Equal(t, content, string(hot.data[key]))
	require.NotNil(t, hot.meta[key], "Metadata should be promoted to hot cache")
	assert.Equal(t, "v1-cold", hot.meta[key].ETag)
}

// TestCache_Get_FullMiss tests a full cache miss, fetching from origin.
func TestCache_Get_FullMiss(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)

	key := "miss-key"
	fetcher := &mockFetcher{content: "origin content", etag: "v-origin"}

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)

	assert.Equal(t, "origin content", string(readBytes))
	assert.Equal(t, 1, fetcher.getFetchCount())

	stream.Close()

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.Equal(t, "origin content", string(hot.data[key]))
	assert.Equal(t, "v-origin", hot.meta[key].ETag)
}

// TestCache_Get_NotModified tests the case where the fetcher returns ErrNotModified.
func TestCache_Get_NotModified(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)

	key := "not-modified-key"
	content := "original content"
	etag := "v1"

	hot.data[key] = []byte(content)
	hot.meta[key] = &Metadata{ETag: etag}

	fetcher := &mockFetcher{err: ErrNotModified, etag: etag}

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	stream.Close()

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, fetcher.getFetchCount())
	assert.Equal(t, etag, fetcher.lastOldETag)
}

// TestCache_Get_StoreError tests how the cache handles errors from the underlying store.
func TestCache_Get_StoreError(t *testing.T) {
	ctx := context.Background()

	hot := newMockStore()
	hot.getErr = errors.New("disk is full")

	cache, _, _ := setupCache(t, WithHotStore(hot))
	fetcher := &mockFetcher{content: "some content", etag: "v1"}

	stream, err := cache.Get(ctx, "any-key", fetcher)
	require.NoError(t, err, "Error from a store tier should be logged, not returned to user if next tier succeeds")

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "some content", string(readBytes))
	assert.Equal(t, 1, fetcher.getFetchCount())
}

// TestCache_Delete tests deletion from all tiers.
func TestCache_Delete(t *testing.T) {
	ctx := context.Background()
	cache, hot, cold := setupCache(t)
	key := "delete-key"

	hot.data[key] = []byte("hot")
	cold.data[key] = []byte("cold")

	err := cache.Delete(ctx, key)
	require.NoError(t, err)

	_, ok := hot.data[key]
	assert.False(t, ok, "Key should be deleted from hot store")
	_, ok = cold.data[key]
	assert.False(t, ok, "Key should be deleted from cold store")
}

// --- Additional Advanced Test Cases ---

// TestCache_Get_FetcherError tests that an error from the fetcher is propagated.
func TestCache_Get_FetcherError(t *testing.T) {
	ctx := context.Background()
	cache, _, _ := setupCache(t)
	expectedErr := errors.New("origin server is down")
	fetcher := &mockFetcher{err: expectedErr}

	_, err := cache.Get(ctx, "any-key", fetcher)

	assert.ErrorIs(t, err, expectedErr, "Error from fetcher should be returned to the caller")
}

// TestCache_Get_ContextCancellation tests that Get aborts when context is cancelled.
func TestCache_Get_ContextCancellation(t *testing.T) {
	cache, _, _ := setupCache(t)
	fetcher := &mockFetcher{content: "some data", fetchDelay: 200 * time.Millisecond}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := cache.Get(ctx, "timeout-key", fetcher)

	assert.ErrorIs(t, err, context.DeadlineExceeded, "Get should return context deadline exceeded")
}

// TestCache_Set_Directly tests the public Set method.
func TestCache_Set_Directly(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)
	key := "set-key"
	content := "direct set content"
	etag := "v-set"

	writer, err := cache.Set(ctx, key, etag)
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.Equal(t, content, string(hot.data[key]), "Content should be in hot store")
	assert.Equal(t, etag, hot.meta[key].ETag, "ETag should be in hot store")
}

// TestCache_BackgroundRefresh_SetError simulates a failure during background refresh.
func TestCache_BackgroundRefresh_SetError(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)
	key := "bg-refresh-fail"

	// 1. Prime the cache
	hot.data[key] = []byte("initial")
	hot.meta[key] = &Metadata{ETag: "v1"}

	// 2. Configure the hot store to fail on the *next* Set call.
	hot.setErr = fmt.Errorf("failed to write to hot store")
	// We need to wait for the initial set to finish, so we reset the counter after priming.
	hot.setCallCount = 0

	// 3. Setup fetcher for the refresh
	fetcher := &mockFetcher{content: "updated", etag: "v2"}

	// 4. Trigger the hot hit and background refresh
	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	stream.Close()

	// 5. Wait for the background worker
	time.Sleep(100 * time.Millisecond)

	// 6. Verify fetcher was called, but the data was not updated due to set error
	assert.Equal(t, 1, fetcher.getFetchCount())
	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.Equal(t, "v1", hot.meta[key].ETag, "ETag should not be updated on refresh failure")
	assert.Equal(t, "initial", string(hot.data[key]), "Data should not be updated on refresh failure")
}

// TestCache_Close tests that background tasks are not scheduled after closing.
func TestCache_Close(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)
	key := "key-after-close"

	hot.data[key] = []byte("data")
	hot.meta[key] = &Metadata{ETag: "v1"}

	// Close the cache
	cache.Close()

	// Try to trigger a background refresh
	fetcher := &mockFetcher{content: "new", etag: "v2"}
	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	stream.Close()

	// Wait a bit to see if any background task runs
	time.Sleep(100 * time.Millisecond)

	// The fetcher should not have been called because the worker pool is shut down
	assert.Equal(t, 0, fetcher.getFetchCount(), "Fetcher should not be called after cache is closed")
}
