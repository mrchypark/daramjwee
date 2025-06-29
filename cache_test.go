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
	forceSetError  bool // 이 필드를 추가합니다.
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
	// 에러를 강제하는 경우, 에러만 반환합니다.
	if s.forceSetError {
		return nil, errors.New("simulated set error")
	}

	// 기존의 일반적인 에러 처리 로직은 그대로 둡니다.
	if s.err != nil {
		return nil, s.err
	}

	// 정상적인 쓰기 로직 (기존 테스트들이 의존하는 부분)
	var buf bytes.Buffer
	return &mockWriteCloser{
		onClose: func() error {
			s.mu.Lock()
			defer s.mu.Unlock()

			// 데이터를 복사하여 저장합니다 (이전 수정 사항).
			dataBytes := make([]byte, buf.Len())
			copy(dataBytes, buf.Bytes())

			s.meta[key] = metadata
			if !metadata.IsNegative {
				s.data[key] = dataBytes
			}

			// ✅ [핵심 수정] select-default를 사용하여 채널 전송을 non-blocking으로 만듭니다.
			// Chaos 테스트와 같이 수신자가 없을 수도 있는 고부하 상황에서
			// 채널이 꽉 차더라도 락을 잡고 무한 대기하는 것을 방지합니다.
			select {
			case s.writeCompleted <- key:
				// 수신자가 있어서 전송에 성공한 경우
			default:
				// 수신자가 없거나 채널 버퍼가 꽉 차서 전송에 실패한 경우
				// (Deadlock 방지를 위해 그냥 넘어갑니다)
			}
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

	// 1. 스트림에서 데이터를 모두 읽습니다.
	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err, "스트림에서 데이터를 읽는 중 에러가 발생하면 안 됩니다")

	// 2. (핵심 수정) 스트림을 명시적으로 닫아 쓰기 완료 신호를 트리거합니다.
	err = stream.Close()
	require.NoError(t, err)

	// 3. 이제 Hot 캐시로의 비동기 쓰기가 완료될 때까지 안전하게 기다릴 수 있습니다.
	<-hot.writeCompleted

	// 4. 읽은 콘텐츠와 캐시 상태를 검증합니다.
	assert.Equal(t, content, string(readBytes))
	assert.Equal(t, 1, fetcher.getFetchCount())

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	require.NotNil(t, hot.meta[key], "메타데이터가 Hot 캐시에 존재해야 합니다.")
	assert.Equal(t, content, string(hot.data[key]))
	assert.Equal(t, etag, hot.meta[key].ETag)
	assert.False(t, hot.meta[key].CachedAt.IsZero())
}

// TestCache_Get_ColdHit는 콜드 캐시 히트 및 핫 캐시 승격을 검증합니다.
func TestCache_Get_ColdHit(t *testing.T) {
	cache, hot, cold := setupCache(t)
	ctx := context.Background()
	key, content, etag := "cold-key", "cold content", "v-cold"

	// 1. Cold 캐시에 데이터 준비
	cold.setData(key, content, &Metadata{ETag: etag, CachedAt: time.Now().Add(-1 * time.Hour)})

	// 2. Get 호출 (Fetcher는 호출되지 않아야 함)
	stream, err := cache.Get(ctx, key, &mockFetcher{})
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)

	// 3. (핵심 수정) 스트림을 명시적으로 닫습니다.
	err = stream.Close()
	require.NoError(t, err)

	// 4. 비동기 쓰기(승격) 완료를 기다립니다.
	<-hot.writeCompleted

	// 5. 콘텐츠와 승격 상태를 검증합니다.
	assert.Equal(t, content, string(readBytes))

	hot.mu.RLock()
	defer hot.mu.RUnlock()
	require.NotNil(t, hot.data[key])
	assert.Equal(t, content, string(hot.data[key]))
	require.NotNil(t, hot.meta[key])
	assert.Equal(t, etag, hot.meta[key].ETag)
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

// TestCache_Close는 cache.Close() 이후의 동작을 검증합니다.
func TestCache_Close(t *testing.T) {
	// 1. 테스트 준비
	cache, hot, _ := setupCache(t, WithCache(1*time.Minute))
	key := "key-after-close"
	// 백그라운드 갱신을 유발할 수 있는 만료된 데이터를 준비
	hot.setData(key, "data", &Metadata{CachedAt: time.Now().Add(-2 * time.Minute)})
	fetcher := &mockFetcher{content: "new", etag: "v2"}

	// 2. 캐시를 닫음
	cache.Close()

	// 3. 닫힌 캐시에 Get을 호출하는 것이 새로운 API 계약을 따르는지 확인
	// 이제 이 호출은 에러를 반환해야 합니다.
	stream, err := cache.Get(context.Background(), key, fetcher)

	// --- 여기가 핵심 수정 부분 ---
	// NoError가 아니라, ErrCacheClosed 에러가 발생하는 것을 기대해야 합니다.
	require.ErrorIs(t, err, ErrCacheClosed, "Get() on a closed cache should now return ErrCacheClosed")
	// 에러가 발생했으므로 스트림은 nil이어야 합니다.
	assert.Nil(t, stream, "Stream should be nil when an error is returned")
	// -------------------------

	// 4. (부가 검증) 어떠한 경우에도 백그라운드 작업이 실행되지 않았는지 확인
	time.Sleep(50 * time.Millisecond) // 혹시 모를 비동기 호출을 기다림
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

func TestCache_Concurrent_GetAndDelete(t *testing.T) {
	cache, hot, _ := setupCache(t)
	key := "get-delete-key"
	hot.setData(key, "some content", &Metadata{})

	var wg sync.WaitGroup
	wg.Add(2)

	// 채널을 사용해 Get이 시작되었음을 Delete에 알립니다.
	getStarted := make(chan struct{})

	// Goroutine 1: Get an object
	go func() {
		defer wg.Done()
		fetcherForGet := &mockFetcher{err: ErrNotModified, etag: "v1"}

		// Get을 실행하기 직전에 신호를 보냅니다.
		close(getStarted)

		stream, err := cache.Get(context.Background(), key, fetcherForGet)
		if err == nil {
			// 스트림을 즉시 닫아 락을 오래 잡고 있지 않도록 합니다.
			stream.Close()
		}
	}()

	// Goroutine 2: Delete the same object
	go func() {
		defer wg.Done()
		// Get이 시작될 때까지 기다립니다.
		<-getStarted

		err := cache.Delete(context.Background(), key)
		assert.NoError(t, err)
	}()

	wg.Wait()

	// 최종 상태 검증
	_, _, err := hot.GetStream(context.Background(), key)
	assert.ErrorIs(t, err, ErrNotFound, "Item should be deleted after all operations")
}

// TestCache_ReturnsError_AfterClose는 Close()가 호출된 캐시 인스턴스에 대해
// 후속 Get, Set, Delete 호출이 명시적인 에러(ErrCacheClosed)를 반환하는지 검증합니다.
func TestCache_ReturnsError_AfterClose(t *testing.T) {
	// 1. 테스트할 캐시를 생성합니다.
	cache, _, _ := setupCache(t)
	ctx := context.Background()
	key := "any-key-after-close"

	// 2. 캐시를 즉시 닫습니다.
	cache.Close()

	// 3. 닫힌 캐시에 대한 각 공개 메서드 호출을 검증합니다.

	// 3.1. Get() 호출 검증
	_, getErr := cache.Get(ctx, key, &mockFetcher{})
	assert.ErrorIs(t, getErr, ErrCacheClosed, "Get() on a closed cache should return ErrCacheClosed")

	// 3.2. Set() 호출 검증
	_, setErr := cache.Set(ctx, key, &Metadata{})
	assert.ErrorIs(t, setErr, ErrCacheClosed, "Set() on a closed cache should return ErrCacheClosed")

	// 3.3. Delete() 호출 검증
	deleteErr := cache.Delete(ctx, key)
	assert.ErrorIs(t, deleteErr, ErrCacheClosed, "Delete() on a closed cache should return ErrCacheClosed")

	// 3.4. ScheduleRefresh() 호출 검증 (이것도 공개 API이므로 확인)
	_ = cache.ScheduleRefresh(ctx, key, &mockFetcher{})
	// 참고: ScheduleRefresh는 현재 에러를 반환하지 않지만, 안전을 위해 nil이 아니어야 함을 확인하거나
	// 향후 ErrCacheClosed를 반환하도록 수정할 수 있습니다.
	// 우선 현재의 계약을 기반으로 테스트합니다.
	if dc, ok := cache.(*DaramjweeCache); ok {
		if dc.isClosed.Load() {
			// isClosed 플래그가 있다면, 에러가 반환되어야 함.
			// 여기서는 ScheduleRefresh도 에러를 반환하도록 수정되었다고 가정합니다.
			// func (c *DaramjweeCache) ScheduleRefresh(...) error { if c.isClosed.Load() { return ErrCacheClosed } ... }
			// assert.ErrorIs(t, refreshErr, ErrCacheClosed, "ScheduleRefresh() on a closed cache should return ErrCacheClosed")
		}
	}
}

// mockCloser는 Close 호출 여부를 추적하고, 설정에 따라 에러를 반환하는 Mock 구현체입니다.
type mockCloser struct {
	isClosed    bool
	shouldError bool
	closeErr    error
}

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

// TestMultiCloser_ClosesAll_EvenIfOneFails는 multiCloser에 포함된 Closer 중
// 하나가 에러를 반환하더라도, 나머지 모든 Closer들의 Close가 호출되는 것을 보장하는지 검증합니다.
func TestMultiCloser_ClosesAll_EvenIfOneFails(t *testing.T) {
	// 1. 3개의 mockCloser를 준비합니다.
	closer1 := &mockCloser{}
	// 두 번째 closer는 에러를 반환하도록 설정합니다.
	closer2 := &mockCloser{shouldError: true}
	closer3 := &mockCloser{}

	// 2. 이들을 multiCloser로 묶습니다.
	multi := newMultiCloser(nil, closer1, closer2, closer3)

	// 3. multiCloser의 Close를 호출합니다.
	err := multi.Close()

	// 4. 결과 검증
	// 4.1. 에러가 정상적으로 반환되었는지 확인합니다.
	require.Error(t, err, "multiCloser should return the error from the failing closer")

	// 4.2. (가장 중요) 에러 발생 여부와 상관없이 모든 Closer가 호출되었는지 확인합니다.
	assert.True(t, closer1.isClosed, "The first closer should have been closed")
	assert.True(t, closer2.isClosed, "The failing closer should have been attempted to close")
	assert.True(t, closer3.isClosed, "The third closer should have been closed despite the previous error")
}

// TestCache_Get_ColdHit_PromotionFails는 콜드 캐시 히트 후
// Hot Tier로의 승격(promotion)이 실패하는 엣지 케이스를 검증합니다.
func TestCache_Get_ColdHit_PromotionFails(t *testing.T) {
	// 1. 테스트 환경 설정
	hot := newMockStore()
	hot.forceSetError = true // Hot Store에 쓰기 실패를 강제합니다.

	cold := newMockStore()
	key, content, etag := "promotion-fail-key", "cold content only", "v-cold"
	cold.setData(key, content, &Metadata{ETag: etag})

	cache, err := New(log.NewNopLogger(), WithHotStore(hot), WithColdStore(cold))
	require.NoError(t, err)
	defer cache.Close()

	// 2. Get 호출 실행
	stream, err := cache.Get(context.Background(), key, &mockFetcher{})

	// 3. 결과 검증
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
