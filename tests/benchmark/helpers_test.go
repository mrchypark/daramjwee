package benchmark

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
)

// MockStore is a mock implementation of the Store interface for testing.
type mockStore struct {
	mu             sync.RWMutex
	data           map[string][]byte
	metadata       map[string]*daramjwee.Metadata
	writeCompleted chan string
}

// newMockStore creates a new mockStore.
func newMockStore() *mockStore {
	return &mockStore{
		data:           make(map[string][]byte),
		metadata:       make(map[string]*daramjwee.Metadata),
		writeCompleted: make(chan string, 1),
	}
}

// GetStream retrieves an object and its metadata as a stream.
func (s *mockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.data[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}

	meta, ok := s.metadata[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

// SetWithWriter returns a writer that streams data into the store.
func (s *mockStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	var buffer bytes.Buffer
	writer := &mockStoreWriter{
		key:      key,
		buffer:   &buffer,
		store:    s,
		metadata: metadata,
	}

	return writer, nil
}

// Delete removes an object from the store.
func (s *mockStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	delete(s.metadata, key)
	return nil
}

// Stat retrieves metadata for an object without its data.
func (s *mockStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, ok := s.metadata[key]
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
	s.metadata[key] = metadata
}

type mockStoreWriter struct {
	key      string
	buffer   *bytes.Buffer
	store    *mockStore
	metadata *daramjwee.Metadata
}

func (w *mockStoreWriter) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func (w *mockStoreWriter) Close() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()

	w.store.data[w.key] = w.buffer.Bytes()
	w.store.metadata[w.key] = w.metadata
	w.store.writeCompleted <- w.key
	return nil
}

// mockFetcher is a mock implementation of the Fetcher interface for testing.
type mockFetcher struct {
	content string
	etag    string
}

// Fetch retrieves data from the origin source.
func (f *mockFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	if oldMetadata != nil && oldMetadata.ETag == f.etag {
		return nil, daramjwee.ErrNotModified
	}

	return &daramjwee.FetchResult{
		Body: io.NopCloser(strings.NewReader(f.content)),
		Metadata: &daramjwee.Metadata{
			ETag:     f.etag,
			CachedAt: time.Now(),
		},
	}, nil
}

// setupBenchmarkCache is a helper function for benchmarks.
func setupBenchmarkCache(b *testing.B, opts ...daramjwee.Option) (daramjwee.Cache, *mockStore, *mockStore) {
	b.Helper()
	hot := newMockStore()
	cold := newMockStore()
	finalOpts := []daramjwee.Option{
		daramjwee.WithHotStore(hot),
		daramjwee.WithColdStore(cold),
		daramjwee.WithDefaultTimeout(2 * time.Second),
		daramjwee.WithShutdownTimeout(2 * time.Second),
		daramjwee.WithWorker("pool", 5, 2, 1*time.Second),
	}
	finalOpts = append(finalOpts, opts...)
	cache, err := daramjwee.New(log.NewNopLogger(), finalOpts...)
	if err != nil {
		b.Fatalf("Failed to create cache for benchmark: %v", err)
	}
	b.Cleanup(cache.Close)
	return cache, hot, cold
}
