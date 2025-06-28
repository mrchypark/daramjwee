package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFetcher struct {
	mu              sync.Mutex
	fetchCount      int
	content         string
	etag            string
	err             error
	fetchDelay      time.Duration
	lastOldMetadata *Metadata
}

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

func (f *mockFetcher) getFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchCount
}

type mockStore struct {
	mu             sync.RWMutex
	data           map[string][]byte
	meta           map[string]*Metadata
	err            error
	writeCompleted chan string
}

func newMockStore() *mockStore {
	return &mockStore{
		data:           make(map[string][]byte),
		meta:           make(map[string]*Metadata),
		writeCompleted: make(chan string, 100),
	}
}

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

func (s *mockStore) SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	if s.err != nil {
		return nil, s.err
	}
	var buf bytes.Buffer
	return &mockWriteCloser{
		onClose: func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.meta[key] = metadata
			if !metadata.IsNegative {
				s.data[key] = buf.Bytes()
			}
			s.writeCompleted <- key
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.meta[key]
	if !ok {
		return nil, ErrNotFound
	}
	return meta, nil
}

func (s *mockStore) setData(key, content string, metadata *Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = []byte(content)
	s.meta[key] = metadata
}

func (s *mockStore) setNegativeEntry(key string, metadata *Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	metadata.IsNegative = true
	s.meta[key] = metadata
	delete(s.data, key)
}

type mockWriteCloser struct {
	buf     *bytes.Buffer
	onClose func() error
}

func (mwc *mockWriteCloser) Write(p []byte) (n int, err error) { return mwc.buf.Write(p) }
func (mwc *mockWriteCloser) Close() error                      { return mwc.onClose() }

type deterministicFetcher struct {
	mockFetcher
	fetchStarted chan struct{}
	fetchEnd     chan struct{}
	onFetch      func()
}

func newDeterministicFetcher(content, etag string, err error) *deterministicFetcher {
	return &deterministicFetcher{
		mockFetcher:  mockFetcher{content: content, etag: etag, err: err},
		fetchStarted: make(chan struct{}, 1),
		fetchEnd:     make(chan struct{}, 1),
	}
}

func (f *deterministicFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	f.fetchStarted <- struct{}{}
	defer func() { f.fetchEnd <- struct{}{} }()
	if f.onFetch != nil {
		f.onFetch()
	}
	return f.mockFetcher.Fetch(ctx, oldMetadata)
}

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

// --- 전체 테스트 스위트 ---

// TestCache_Get_FullMiss는 캐시에 데이터가 전혀 없을 때의 동작을 검증합니다.
func TestCache_Get_FullMiss(t *testing.T) {
	cache, hot, _ := setupCache(t)
	ctx := context.Background()
	key, content, etag := "miss-key", "origin content", "v-origin"
	fetcher := &mockFetcher{content: content, etag: etag}

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	defer stream.Close()

	readBytes, _ := io.ReadAll(stream)
	assert.Equal(t, content, string(readBytes))
	assert.Equal(t, 1, fetcher.getFetchCount())

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.Equal(t, content, string(hot.data[key]))
	assert.Equal(t, etag, hot.meta[key].ETag)
	assert.False(t, hot.meta[key].CachedAt.IsZero())
}

// ✅ [복원된 테스트] TestCache_Get_ColdHit는 콜드 캐시 히트 및 핫 캐시 승격을 검증합니다.
func TestCache_Get_ColdHit(t *testing.T) {
	cache, hot, cold := setupCache(t)
	ctx := context.Background()
	key, content, etag := "cold-key", "cold content", "v-cold"

	// 1. Cold 캐시에 데이터 준비
	cold.setData(key, content, &Metadata{ETag: etag, CachedAt: time.Now().Add(-1 * time.Hour)})

	// 2. Get 호출 (Fetcher는 호출되지 않아야 함)
	stream, err := cache.Get(ctx, key, &mockFetcher{})
	require.NoError(t, err)

	readBytes, _ := io.ReadAll(stream)
	stream.Close()
	assert.Equal(t, content, string(readBytes))

	// 3. Hot 캐시로 승격되었는지 확인
	<-hot.writeCompleted // 비동기 쓰기 완료 대기
	hot.mu.RLock()
	defer hot.mu.RUnlock()
	require.NotNil(t, hot.data[key])
	assert.Equal(t, content, string(hot.data[key]))
	require.NotNil(t, hot.meta[key])
	assert.Equal(t, etag, hot.meta[key].ETag)
	// 승격 시 CachedAt이 현재 시간으로 갱신되었는지 확인
	assert.True(t, hot.meta[key].CachedAt.After(time.Now().Add(-1*time.Minute)))
}

// --- 만료 및 네거티브 캐시 테스트 (핵심 로직) ---

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

	// 첫번째 Get
	stream, err := cache.Get(ctx, key, fetcher)
	require.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, stream)
	assert.Equal(t, 1, fetcher.getFetchCount())

	hot.mu.RLock()
	meta, ok := hot.meta[key]
	hot.mu.RUnlock()
	require.True(t, ok)
	assert.True(t, meta.IsNegative)

	// 두번째 Get
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

// --- 에러 및 엣지 케이스 테스트 ---

// ✅ [복원된 테스트]
func TestCache_Get_FetcherError(t *testing.T) {
	cache, _, _ := setupCache(t)
	expectedErr := errors.New("origin server is down")
	fetcher := &mockFetcher{err: expectedErr}
	_, err := cache.Get(context.Background(), "any-key", fetcher)
	assert.ErrorIs(t, err, expectedErr)
}

// ✅ [복원된 테스트]
func TestCache_Get_ContextCancellation(t *testing.T) {
	cache, _, _ := setupCache(t)
	fetcher := &mockFetcher{content: "some data", fetchDelay: 200 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := cache.Get(ctx, "timeout-key", fetcher)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// ✅ [복원된 테스트]
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

// ✅ [복원된 테스트]
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

// ✅ [복원된 테스트]
func TestCache_Close(t *testing.T) {
	cache, hot, _ := setupCache(t, WithCache(1*time.Minute))
	key := "key-after-close"
	hot.setData(key, "data", &Metadata{CachedAt: time.Now().Add(-2 * time.Minute)})

	// Close the cache first
	cache.Close()

	// Try to trigger a background refresh
	fetcher := &mockFetcher{content: "new", etag: "v2"}
	stream, err := cache.Get(context.Background(), key, fetcher)
	require.NoError(t, err)
	stream.Close()

	// Wait a bit to see if any background task runs
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, fetcher.getFetchCount(), "Fetcher should not be called after cache is closed")
}

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

// --- 동시성(Concurrency) 테스트 ---

// ✅ [복원된 테스트]
func TestCache_Get_ThunderingHerd(t *testing.T) {
	cache, _, _ := setupCache(t)
	key := "herd-key"
	// 원본 응답이 느린 상황 시뮬레이션
	fetcher := newDeterministicFetcher("origin data", "v1", nil)
	fetcher.fetchDelay = 100 * time.Millisecond

	numRequests := 10
	var wg sync.WaitGroup
	wg.Add(numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()
			stream, err := cache.Get(context.Background(), key, fetcher)
			if assert.NoError(t, err) {
				io.Copy(io.Discard, stream) // 스트림을 소모하여 TeeReader가 동작하도록 함
				stream.Close()
			}
		}()
	}

	wg.Wait()
	// 현재 구현에서는 singleflight가 없으므로 모든 요청이 오리진으로 가야 합니다.
	assert.Equal(t, numRequests, fetcher.getFetchCount())
}

// ✅ [복원된 테스트]
func TestCache_Concurrent_GetAndDelete(t *testing.T) {
	cache, hot, _ := setupCache(t)
	key := "get-delete-key"
	hot.setData(key, "some content", &Metadata{})

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Get an object (and hold the stream)
	go func() {
		defer wg.Done()
		fetcherForGet := &mockFetcher{err: ErrNotModified, etag: "v1"}
		stream, err := cache.Get(context.Background(), key, fetcherForGet)
		if err == nil {
			time.Sleep(50 * time.Millisecond)
			stream.Close()
		}
	}()

	// Goroutine 2: Delete the same object
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		err := cache.Delete(context.Background(), key)
		assert.NoError(t, err)
	}()

	wg.Wait()
	_, err := cache.Get(context.Background(), key, &mockFetcher{err: ErrNotFound})
	assert.ErrorIs(t, err, ErrNotFound, "Item should be deleted after all operations")
}
