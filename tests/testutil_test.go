package daramjwee_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
)

// --- mockStore ---

type mockStore struct {
	mu             sync.RWMutex
	data           map[string][]byte
	meta           map[string]*daramjwee.Metadata
	err            error
	deleteErr      error
	writeCompleted chan string
	writeAborted   chan string
	forceSetError  bool
}

func newMockStore() *mockStore {
	return &mockStore{
		data:           make(map[string][]byte),
		meta:           make(map[string]*daramjwee.Metadata),
		writeCompleted: make(chan string, 100),
		writeAborted:   make(chan string, 100),
	}
}

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

func (s *mockStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	if s.forceSetError {
		return nil, fmt.Errorf("simulated set error")
	}
	if s.err != nil {
		return nil, s.err
	}

	var buf bytes.Buffer
	return &mockWriteSink{
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
		onAbort: func() error {
			select {
			case s.writeAborted <- key:
			default:
			}
			return nil
		},
		buf: &buf,
	}, nil
}

func (s *mockStore) Delete(ctx context.Context, key string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.meta, key)
	return nil
}

func (s *mockStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.meta[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}
	return meta, nil
}

func (s *mockStore) setData(key, content string, metadata *daramjwee.Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = []byte(content)
	s.meta[key] = metadata
}

// --- mockWriteSink ---

type mockWriteSink struct {
	buf     *bytes.Buffer
	onClose func() error
	onAbort func() error
	done    bool
}

func (mwc *mockWriteSink) Write(p []byte) (n int, err error) { return mwc.buf.Write(p) }

func (mwc *mockWriteSink) Close() error {
	if mwc.done {
		return nil
	}
	mwc.done = true
	return mwc.onClose()
}

func (mwc *mockWriteSink) Abort() error {
	if mwc.done {
		return nil
	}
	mwc.done = true
	if mwc.onAbort != nil {
		return mwc.onAbort()
	}
	return nil
}

// --- mockFetcher ---

type mockFetcher struct {
	mu              sync.Mutex
	fetchCount      int
	content         string
	etag            string
	err             error
	fetchDelay      time.Duration
	lastOldMetadata *daramjwee.Metadata
}

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
		Metadata: &daramjwee.Metadata{CacheTag: f.etag},
	}, nil
}

func (f *mockFetcher) getFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchCount
}

func (f *mockFetcher) getLastOldMetadata() *daramjwee.Metadata {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastOldMetadata == nil {
		return nil
	}
	copied := *f.lastOldMetadata
	return &copied
}

// --- blockingReadCloser ---

// silentFetcher is a fetcher that returns ErrCacheableNotFound, allowing Get
// calls that should not need the origin to proceed without a real fetcher.
type silentFetcher struct{}

func (f silentFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return nil, daramjwee.ErrCacheableNotFound
}

type blockingReadCloser struct {
	first     []byte
	second    []byte
	releaseCh chan struct{}
	stage     int
}

func newBlockingReadCloser(first, second []byte) *blockingReadCloser {
	return &blockingReadCloser{
		first:     bytes.Clone(first),
		second:    bytes.Clone(second),
		releaseCh: make(chan struct{}),
	}
}

func (r *blockingReadCloser) Read(p []byte) (int, error) {
	switch r.stage {
	case 0:
		r.stage = 1
		return copy(p, r.first), nil
	case 1:
		<-r.releaseCh
		r.stage = 2
		return copy(p, r.second), nil
	default:
		return 0, io.EOF
	}
}

func (r *blockingReadCloser) Close() error {
	r.Release()
	return nil
}

func (r *blockingReadCloser) Release() {
	select {
	case <-r.releaseCh:
	default:
		close(r.releaseCh)
	}
}

// --- blockingSourceFetcher ---

type blockingSourceFetcher struct {
	source   *blockingReadCloser
	metadata *daramjwee.Metadata
}

func (f *blockingSourceFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     f.source,
		Metadata: f.metadata,
	}, nil
}

// --- blockingSuccessFetcher ---

type blockingSuccessFetcher struct {
	started  chan struct{}
	blocker  chan struct{}
	content  string
	cacheTag string
}

func (f blockingSuccessFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	select {
	case f.started <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.blocker:
		return &daramjwee.FetchResult{
			Body:     io.NopCloser(strings.NewReader(f.content)),
			Metadata: &daramjwee.Metadata{CacheTag: f.cacheTag},
		}, nil
	}
}

// --- readCloserRefreshFetcher ---

type readCloserRefreshFetcher struct {
	body io.ReadCloser
	meta *daramjwee.Metadata
}

func (f *readCloserRefreshFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     f.body,
		Metadata: f.meta,
	}, nil
}

// --- closeTrackingReadCloser ---

type closeTrackingReadCloser struct {
	*bytes.Reader
	once     sync.Once
	onClose  func()
	closeErr error
}

func (r *closeTrackingReadCloser) Close() error {
	r.once.Do(func() {
		if r.onClose != nil {
			r.onClose()
		}
	})
	return r.closeErr
}

// --- readStoreValue ---

func readStoreValue(ctx context.Context, store daramjwee.Store, key string) (string, *daramjwee.Metadata, error) {
	reader, meta, err := store.GetStream(ctx, key)
	if err != nil {
		return "", nil, err
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return "", nil, err
	}
	return string(body), meta, nil
}

// --- writeCacheValue ---

func writeCacheValue(cache daramjwee.Cache, key, value, tag string) error {
	writer, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: tag})
	if err != nil {
		return err
	}
	if _, err := io.WriteString(writer, value); err != nil {
		abortErr := writer.Abort()
		if abortErr != nil {
			return fmt.Errorf("write: %w; abort: %w", err, abortErr)
		}
		return err
	}
	return writer.Close()
}

// --- entryExpectation ---

type entryExpectation struct {
	present  bool
	negative bool
	value    string
	cacheTag string
}

func eventuallyExpectStoreState(t *testing.T, store *mockStore, key string, want entryExpectation) {
	t.Helper()
	require.Eventually(t, func() bool {
		got, err := currentMockStoreState(store, key)
		return err == nil && got == want
	}, 2*time.Second, 10*time.Millisecond, "store state for %q did not converge to %+v", key, want)
}

func currentMockStoreState(store *mockStore, key string) (entryExpectation, error) {
	reader, meta, err := store.GetStream(context.Background(), key)
	if err != nil {
		if errors.Is(err, daramjwee.ErrNotFound) {
			return entryExpectation{}, nil
		}
		return entryExpectation{}, err
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return entryExpectation{}, err
	}
	got := entryExpectation{
		present:  true,
		negative: meta.IsNegative,
		value:    string(body),
		cacheTag: meta.CacheTag,
	}
	return got, nil
}
