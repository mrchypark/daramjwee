package integration

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

// MockStore is a mock implementation of the Store interface for testing.
type mockStore struct {
	mu       sync.RWMutex
	data     map[string][]byte
	metadata map[string]*daramjwee.Metadata
	forceSetError bool
	writeCompleted chan struct{}
}

// newMockStore creates a new mockStore.
func newMockStore() *mockStore {
	return &mockStore{
		data:     make(map[string][]byte),
		metadata: make(map[string]*daramjwee.Metadata),
		writeCompleted: make(chan struct{}, 1),
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
	if s.forceSetError {
		return nil, fmt.Errorf("forced set error")
	}
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

func (s *mockStore) setData(key string, data string, metadata *daramjwee.Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = []byte(data)
	s.metadata[key] = metadata
	if s.writeCompleted != nil {
		select {
		case s.writeCompleted <- struct{}{}:
		default:
		}
	}
}

func (s *mockStore) Wait() {
	if s.writeCompleted != nil {
		select {
		case <-s.writeCompleted:
		case <-time.After(5 * time.Second): // Timeout to prevent hanging tests
		}
	}
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
	if w.store.writeCompleted != nil {
		select {
		case w.store.writeCompleted <- struct{}{}:
		default:
		}
	}
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

func setupCache(t *testing.T, opts ...daramjwee.Option) (daramjwee.Cache, *mockStore, *mockStore) {
	hotStore := newMockStore()
	coldStore := newMockStore()

	defaultOpts := []daramjwee.Option{
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
	}

	// Append any additional options provided by the test
	allOpts := append(defaultOpts, opts...)

	logger := log.NewNopLogger()
	cache, err := daramjwee.New(logger, allOpts...)
	require.NoError(t, err)

	return cache, hotStore, coldStore
}