package daramjwee_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore is a mock implementation of the Store interface for testing purposes.
type mockStore struct {
	mu             sync.RWMutex
	data           map[string][]byte
	meta           map[string]*daramjwee.Metadata
	err            error
	writeCompleted chan string
	forceSetError  bool // forceSetError, if true, makes SetWithWriter return an error.
}

// newMockStore creates a new mockStore.
func newMockStore() *mockStore {
	return &mockStore{
		data:           make(map[string][]byte),
		meta:           make(map[string]*daramjwee.Metadata),
		writeCompleted: make(chan string, 100),
	}
}

// GetStream simulates retrieving a stream from the store.
func (s *mockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.err != nil {
		return nil, nil, s.err
	}
	meta, ok := s.meta[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}
	if meta.IsNegative {
		return io.NopCloser(bytes.NewReader(nil)), meta, nil
	}
	data, ok := s.data[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

// SetWithWriter simulates writing a stream to the store.
func (s *mockStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
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
func (s *mockStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.meta[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}
	return meta, nil
}

// setData sets content and metadata for a given key in the mock store.
func (s *mockStore) setData(key, content string, metadata *daramjwee.Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = []byte(content)
	s.meta[key] = metadata
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

// mockFetcher is a mock implementation of the Fetcher interface for testing purposes.
type mockFetcher struct {
	mu              sync.Mutex
	fetchCount      int
	content         string
	etag            string
	err             error
	fetchDelay      time.Duration
	lastOldMetadata *daramjwee.Metadata
}

// Fetch simulates fetching data from an origin, incrementing a fetch counter.
func (f *mockFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
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
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(f.content))),
		Metadata: &daramjwee.Metadata{ETag: f.etag},
	}, nil
}

// getFetchCount returns the number of times Fetch was called.
func (f *mockFetcher) getFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchCount
}

// slowMockStore simulates a slow storage by adding an intentional delay.
type slowMockStore struct {
	mockStore
	delay time.Duration
}

// newSlowMockStore creates a new slowMockStore with the specified delay.
func newSlowMockStore(delay time.Duration) *slowMockStore {
	return &slowMockStore{
		mockStore: *newMockStore(),
		delay:     delay,
	}
}

// GetStream delays for the configured time before returning data.
func (s *slowMockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	time.Sleep(s.delay)
	return s.mockStore.GetStream(ctx, key)
}

// TestCache_WithSlowColdStore verifies that the cold hit scenario completes successfully
// within the timeout even when the Cold Store is slow.
func TestCache_WithSlowColdStore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	hot := newMockStore()
	slowCold := newSlowMockStore(100 * time.Millisecond)

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithColdStore(slowCold), daramjwee.WithDefaultTimeout(1*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	key := "slow-item"
	content := "content from slow store"
	slowCold.setData(key, content, &daramjwee.Metadata{ETag: "v-slow"})

	stream, err := cache.Get(context.Background(), key, &mockFetcher{})
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)
	err = stream.Close()
	require.NoError(t, err)

	assert.Equal(t, content, string(readBytes))

	hot.mu.RLock()
	promotedData, ok := hot.data[key]
	hot.mu.RUnlock()
	assert.True(t, ok, "Data should be promoted to hot cache")
	assert.Equal(t, content, string(promotedData))
}
