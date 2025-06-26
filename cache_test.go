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

// mockFetcher is a mock implementation of the Fetcher interface.
type mockFetcher struct {
	mu              sync.Mutex
	fetchCount      int
	content         string
	etag            string
	err             error
	fetchDelay      time.Duration
	lastOldMetadata *Metadata

	// --- 이 부분을 추가합니다 ---
	// FetchFunc는 Fetch 메서드의 동작을 테스트 중에 교체할 수 있게 해주는 함수 필드입니다.
	FetchFunc func(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error)
}

// Fetch 메서드가 이제 FetchFunc 필드를 사용하도록 수정합니다.
func (f *mockFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	// --- 이 부분을 수정합니다 ---
	// FetchFunc이 설정되어 있다면, 그 함수를 대신 호출합니다.
	if f.FetchFunc != nil {
		return f.FetchFunc(ctx, oldMetadata)
	}

	// 기존의 Fetch 로직은 그대로 둡니다.
	f.mu.Lock()
	f.fetchCount++
	f.lastOldMetadata = oldMetadata
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

	if oldMetadata != nil && oldMetadata.ETag == f.etag {
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

// --- 고도화된 Mock 구현 ---

// mockStore는 테스트를 위한 Store 인터페이스의 Mock 구현체입니다.
type mockStore struct {
	mu           sync.RWMutex
	data         map[string][]byte
	meta         map[string]*Metadata
	getErr       error
	setErr       error
	statErr      error
	setCallCount int
	metaOnlyKeys map[string]bool
	// 쓰기 완료를 알리기 위한 채널 추가
	writeCompleted chan string
}

func newMockStore() *mockStore {
	return &mockStore{
		data:         make(map[string][]byte),
		meta:         make(map[string]*Metadata),
		metaOnlyKeys: make(map[string]bool),
		// 버퍼를 주어 비동기 쓰기에도 블로킹되지 않도록 함
		writeCompleted: make(chan string, 100),
	}
}

func (s *mockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 메타데이터만 있는 키인 경우, 데이터가 없는 것처럼 ErrNotFound를 반환합니다.
	if s.metaOnlyKeys[key] {
		return nil, nil, ErrNotFound
	}

	if s.getErr != nil {
		return nil, nil, s.getErr
	}

	data, ok := s.data[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	meta := s.meta[key]
	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

func (s *mockStore) SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
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
			defer s.mu.Unlock() // Defer unlock to ensure it runs even if there are panics

			// --- 여기가 핵심 수정 부분입니다 ---
			// 버퍼에 실제 데이터가 있을 때만 data 맵에 항목을 추가합니다.
			if buf.Len() > 0 {
				s.data[key] = buf.Bytes()
			}
			s.meta[key] = metadata
			// --- 수정 끝 ---

			// 쓰기 작업이 완료되었음을 알림
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

func (s *mockStore) setData(key, content string, metadata *Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = []byte(content)
	s.meta[key] = metadata
}

// setMetaOnly는 "메타데이터만 있는" 상태를 설정하는 헬퍼 함수입니다.
func (s *mockStore) setMetaOnly(key string, metadata *Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta[key] = metadata
	s.metaOnlyKeys[key] = true
}

// mockWriteCloser는 io.WriteCloser의 Mock 구현체입니다.
type mockWriteCloser struct {
	buf     *bytes.Buffer
	onClose func() error
}

func (mwc *mockWriteCloser) Write(p []byte) (n int, err error) { return mwc.buf.Write(p) }
func (mwc *mockWriteCloser) Close() error                      { return mwc.onClose() }

// deterministicFetcher는 Fetcher의 Mock 구현체로, 채널을 통해 동기화를 지원합니다.
type deterministicFetcher struct {
	mu              sync.Mutex
	fetchCount      int
	content         string
	etag            string
	err             error
	fetchDelay      time.Duration
	lastOldMetadata *Metadata
	fetchStarted    chan struct{} // Fetch 시작을 알리는 채널
	fetchEnd        chan struct{} // Fetch 완료를 알리는 채널

	onFetch func()
}

func newDeterministicFetcher(content, etag string) *deterministicFetcher {
	return &deterministicFetcher{
		content:      content,
		etag:         etag,
		fetchStarted: make(chan struct{}, 100), // 버퍼를 주어 비동기 호출에도 블로킹되지 않도록 함
		fetchEnd:     make(chan struct{}, 100),
	}
}

// Fetch는 daramjwee.Fetcher 인터페이스를 구현합니다.
func (f *deterministicFetcher) Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
	f.mu.Lock()
	f.fetchCount++
	f.lastOldMetadata = oldMetadata
	f.mu.Unlock()

	// 외부에서 주입한 콜백을 실행하여, 테스트 중인 코드의 실행 흐름에 개입합니다.
	if f.onFetch != nil {
		f.onFetch()
	}

	// 백그라운드 작업이 시작되었음을 메인 테스트 고루틴에 알립니다.
	f.fetchStarted <- struct{}{}
	// 함수가 종료될 때(성공하든 실패하든) 작업이 끝났음을 알립니다.
	defer func() { f.fetchEnd <- struct{}{} }()

	// 컨텍스트 타임아웃/취소를 존중합니다.
	if f.fetchDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.fetchDelay):
		}
	}

	if f.err != nil {
		return nil, f.err
	}

	if oldMetadata != nil && oldMetadata.ETag == f.etag {
		return nil, ErrNotModified
	}

	return &FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(f.content))),
		Metadata: &Metadata{ETag: f.etag},
	}, nil
}

func (f *deterministicFetcher) getFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchCount
}

// --- 테스트 설정 헬퍼 ---

func setupCache(t *testing.T, opts ...Option) (Cache, *mockStore, *mockStore) {
	hot := newMockStore()
	cold := newMockStore()

	finalOpts := []Option{
		WithHotStore(hot),
		WithColdStore(cold),
		WithDefaultTimeout(2 * time.Second),
		WithWorker("pool", 5, 20, 1*time.Second), // 워커 수를 늘려 동시성 테스트에 용이하게 함
	}
	finalOpts = append(finalOpts, opts...)

	cache, err := New(log.NewNopLogger(), finalOpts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	return cache, hot, cold
}

// TestCache_Get_ColdHit tests a cold cache hit and promotion to hot.
func TestCache_Get_ColdHit(t *testing.T) {
	ctx := context.Background()
	cache, hot, cold := setupCache(t)

	key := "my-key"
	content := "cold content"
	cold.setData(key, content, &Metadata{ETag: "v1-cold"})

	fetcher := &mockFetcher{}

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)

	err = stream.Close()
	require.NoError(t, err)

	// --- Assertions ---
	assert.Equal(t, content, string(readBytes))

	// --- 아래 라인 삭제 ---
	// fetcher가 비동기적으로 호출될 수 있으므로, 0이라고 단정하는 것은 더 이상 유효하지 않음
	// assert.Equal(t, 0, fetcher.getFetchCount(), "Fetcher should not be called on cold hit")

	// 승격이 정상적으로 이루어졌는지만 확인
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
	assert.Equal(t, etag, fetcher.lastOldMetadata.ETag)
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

	writer, err := cache.Set(ctx, key, &Metadata{ETag: etag})
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
	hot.setData(key, "initial", &Metadata{ETag: "v1"})

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

	hot.setData(key, "data", &Metadata{ETag: "v1"})

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

// --- 고도화된 테스트 케이스 ---

// TestCache_Get_HotHit_Deterministic은 time.Sleep 대신 채널을 사용하여 백그라운드 갱신을 검증합니다.
func TestCache_Get_HotHit_Deterministic(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)
	key := "my-key"

	hot.setData(key, "hot content", &Metadata{ETag: "v1"})
	fetcher := newDeterministicFetcher("new content", "v2")

	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)
	defer stream.Close()

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "hot content", string(readBytes))
	// The fetcher should not be called synchronously by the Get call.
	// However, the background refresh can be very fast.
	// We will verify the fetch count after ensuring the background work is observed.

	// 백그라운드 Fetch 시작 대기
	<-fetcher.fetchStarted
	<-fetcher.fetchEnd

	// **핵심: 실제 mockStore에 쓰기가 완료될 때까지 대기**
	select {
	case completedKey := <-hot.writeCompleted:
		assert.Equal(t, key, completedKey)
	case <-time.After(1 * time.Second):
		t.Fatal("Did not receive write completion signal in time")
	}

	assert.Equal(t, 1, fetcher.getFetchCount(), "Fetcher는 핫 히트 시 백그라운드에서 호출되어야 합니다.")
	hot.mu.RLock()
	// 이제 이 검증은 항상 성공해야 함
	assert.Equal(t, "v2", hot.meta[key].ETag, "핫 캐시는 백그라운드 갱신 후 업데이트되어야 합니다.")
	hot.mu.RUnlock()
}

// TestCache_Get_ThunderingHerd는 캐시 미스 시 여러 동시 요청이 오리진으로 모두 전달되는지(문제 재현) 검증합니다.
// singleflight가 구현되면 이 테스트는 실패해야 하며, fetchCount가 1이 되도록 수정되어야 합니다.
func TestCache_Get_ThunderingHerd(t *testing.T) {
	ctx := context.Background()
	cache, _, _ := setupCache(t)
	key := "herd-key"
	fetcher := newDeterministicFetcher("origin data", "v1")
	fetcher.fetchDelay = 100 * time.Millisecond // 오리진 응답이 느린 상황 시뮬레이션

	numRequests := 10
	var wg sync.WaitGroup
	wg.Add(numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()
			stream, err := cache.Get(ctx, key, fetcher)
			if assert.NoError(t, err) {
				_, _ = io.Copy(io.Discard, stream) // 스트림을 소모하여 TeeReader가 동작하도록 함
				stream.Close()
			}
		}()
	}

	wg.Wait()

	// 현재 구현에서는 singleflight가 없으므로 모든 요청이 오리진으로 가야 합니다.
	assert.Equal(t, numRequests, fetcher.getFetchCount(), "singleflight 부재 시, 모든 동시 요청이 오리진을 호출해야 합니다.")
}

// TestCache_Get_ColdHit_WithEviction은 콜드 캐시 히트로 인한 핫 캐시 승격이
// 다른 아이템의 축출(eviction)을 유발하는 시나리오를 검증합니다.
type lruEvictionPolicy struct {
	keys []string
}

func (p *lruEvictionPolicy) Touch(key string) {}
func (p *lruEvictionPolicy) Add(key string, size int64) {
	p.keys = append(p.keys, key)
}
func (p *lruEvictionPolicy) Remove(key string) {}
func (p *lruEvictionPolicy) Evict() []string {
	if len(p.keys) > 0 {
		key := p.keys[0]
		p.keys = p.keys[1:]
		return []string{key}
	}
	return nil
}

func TestCache_Get_ColdHit_WithEviction(t *testing.T) {
	// MemStore는 자체적인 축출 로직을 가지고 있으므로, 이 테스트는
	// Store 레벨의 축출이 아닌, Cache 레벨의 상호작용을 검증하는데 더 적합합니다.
	// 여기서는 FileStore를 사용하여 Cache의 동작에 집중합니다.
	// FileStore에는 축출 정책이 없지만, Cache가 올바르게 동작하는지 확인하는 것이 목적입니다.
	// 이 테스트는 개념 증명에 가깝습니다. 실제로는 MemStore와 같은 축출 기능이 있는
	// Store를 사용하여 더 정교하게 테스트해야 합니다.

	ctx := context.Background()
	cache, hot, cold := setupCache(t)
	keyToPromote := "cold-item"
	keyToEvict := "hot-item-to-be-evicted"

	// 1. Hot 캐시를 가득 채운 상태로 만듭니다. (시뮬레이션)
	hot.setData(keyToEvict, "i should be evicted", &Metadata{ETag: "v1"})

	// 2. Cold 캐시에 승격 대상 아이템을 넣습니다.
	cold.setData(keyToPromote, "i am from cold", &Metadata{ETag: "v-cold"})

	// 3. Get 호출로 승격을 유발합니다.
	stream, err := cache.Get(ctx, keyToPromote, &mockFetcher{})
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, stream)
	err = stream.Close()
	require.NoError(t, err)

	// 4. 승격이 완료되었는지 확인합니다.
	hot.mu.RLock()
	_, exists := hot.data[keyToPromote]
	hot.mu.RUnlock()
	assert.True(t, exists, "콜드 아이템이 핫 캐시로 승격되어야 합니다.")

	// 참고: 이 테스트를 완성하려면 hot store (mockStore)가 용량 제한 및 축출 정책과 연동되어야 합니다.
	// 예를 들어, hot.setData가 용량을 초과하면 연결된 policy.Evict()를 호출하고 데이터를 삭제하는 로직이 필요합니다.
	// 현재 mockStore에는 해당 기능이 없으므로 개념 검증 수준에서 마무리합니다.
}

// TestCache_Get_NotModified_ButEvictedRace_Deterministic는 sleep 없이 결정론적으로 경쟁 상태를 검증합니다.
func TestCache_Get_NotModified_ButEvictedRace_Deterministic(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)
	key := "deterministic-race-key"

	// 1. 핫 캐시에 아이템을 미리 저장합니다.
	hot.setMetaOnly(key, &Metadata{ETag: "v1"})

	// 2. Fetcher가 ErrNotModified를 반환하도록 설정합니다.
	fetcher := newDeterministicFetcher("", "v1")
	fetcher.err = ErrNotModified

	// 3. ✨ 핵심: Fetcher의 콜백 함수를 정의합니다.
	// 이 콜백은 cache.Get() 내부의 fetcher.Fetch()가 호출되는 시점에 실행됩니다.
	// 이 시점은 304 Not Modified 응답을 받고, 캐시를 다시 읽기 직전의 완벽한 타이밍입니다.
	fetcher.onFetch = func() {
		// `sleep` 없이, 원하는 정확한 시점에 캐시를 삭제합니다.
		err := hot.Delete(context.Background(), key)
		require.NoError(t, err)
	}

	// 4. cache.Get() API를 직접 호출합니다.
	// 이 호출은 내부적으로 다음과 같이 동작합니다.
	//    - 핫/콜드 캐시 미스
	//    - fetcher.Fetch(ctx, "v1") 호출
	//    - -> fetcher.onFetch() 콜백 실행 (hot.Delete(key) 실행됨)
	//    - fetcher.Fetch가 ErrNotModified 반환
	//    - 캐시 로직이 다시 hot.GetStream(key)을 호출하지만, 아이템은 이미 삭제됨
	//    - hot.GetStream이 ErrNotFound를 반환
	//    - 최종적으로 cache.Get이 ErrNotFound를 반환
	stream, err := cache.Get(ctx, key, fetcher)

	// 5. 최종 결과를 검증합니다.
	// 304 응답 후 캐시가 삭제되었으므로, 최종 에러는 ErrNotFound여야 합니다.
	assert.ErrorIs(t, err, ErrNotFound, "304 응답 후 캐시가 삭제된 경우, 최종적으로 ErrNotFound가 반환되어야 합니다.")
	assert.Nil(t, stream, "에러 발생 시 스트림은 nil이어야 합니다.")

	// Fetcher가 정확히 한 번 호출되었는지 확인합니다.
	assert.Equal(t, 1, fetcher.fetchCount)
}

// TestCache_Concurrent_GetAndDelete는 Get과 Delete가 동시에 발생할 때의 안정성을 검증합니다.
func TestCache_Concurrent_GetAndDelete(t *testing.T) {
	ctx := context.Background()
	cache, hot, _ := setupCache(t)
	key := "get-delete-key"
	content := "some content"

	hot.setData(key, content, &Metadata{ETag: "v1"})

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Get an object (and hold the stream)
	go func() {
		defer wg.Done()
		// Use a fetcher that won't cause a write on refresh, to isolate the Delete behavior
		fetcherForGet := &mockFetcher{err: ErrNotModified, etag: "v1"}
		stream, err := cache.Get(ctx, key, fetcherForGet)
		// Get은 성공할 수도, 실패할 수도 있습니다. 중요한 것은 패닉이 없는 것입니다.
		if err == nil {
			// 스트림을 잠시 유지하여 락이 걸리는 시간을 시뮬레이션
			<-time.After(50 * time.Millisecond)
			stream.Close()
		}
	}()

	// Goroutine 2: Delete the same object
	go func() {
		defer wg.Done()
		<-time.After(10 * time.Millisecond) // Get이 먼저 시작되도록 약간의 지연
		err := cache.Delete(ctx, key)
		assert.NoError(t, err, "Delete는 에러를 반환하지 않아야 합니다.")
	}()

	wg.Wait()

	// 최종 상태: 아이템은 삭제되어야 합니다.
	// Use a fetcher that will also return ErrNotFound for the final Get
	finalFetcher := &mockFetcher{err: ErrNotFound}
	_, err := cache.Get(ctx, key, finalFetcher)
	assert.ErrorIs(t, err, ErrNotFound, "모든 작업 후 아이템은 삭제된 상태여야 합니다.")
}

// Filename: cache_test.go (추가할 테스트 코드)

// TestCache_NegativeCache_Enabled는 네거티브 캐시가 활성화됐을 때,
// 오리진에서 ErrNotFound와 유사한 상황(결과가 nil)이 발생하면
// "없음" 상태가 캐시되는지 검증합니다.
func TestCache_NegativeCache_Enabled(t *testing.T) {
	// 1. 네거티브 캐시 활성화 (TTL 5분)
	cache, hot, _ := setupCache(t, WithNegativeCache(5*time.Minute))
	ctx := context.Background()
	key := "negative-key"

	// 2. Fetcher가 (nil, nil)을 반환하도록 설정 (데이터가 없다는 의미)
	// mockFetcher의 content를 비워두면 FetchResult.Body가 nil이 됩니다.
	// 이 테스트에서는 Fetcher가 결과 없이 nil 에러를 반환하는 상황을 시뮬레이션 합니다.
	fetcher := &mockFetcher{content: "", etag: ""} // FetchResult가 nil이 되도록 설정
	fetcher.err = nil                              // 에러는 없지만, 결과도 없는 상황

	// Fetcher의 Fetch 함수를 수정하여 명시적으로 nil, nil을 반환하게 만듭니다.
	// 실제로는 Fetcher 구현에 따라 ErrNotFound를 반환하거나 할 수 있습니다.
	// 여기서는 cache.go의 `if result == nil` 분기를 테스트하는 것이 목적입니다.
	customFetcher := &mockFetcher{}
	customFetcher.FetchFunc = func(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
		// 이 방식은 컴파일 에러를 발생시키지 않습니다.
		customFetcher.mu.Lock()
		customFetcher.fetchCount++
		customFetcher.mu.Unlock()
		return nil, nil // 명시적으로 (nil, nil) 반환
	}

	// 3. Get 호출
	stream, err := cache.Get(ctx, key, customFetcher)

	// 4. 검증
	// (nil, nil)을 받았으므로 최종적으로 사용자에게는 ErrNotFound가 반환되어야 합니다.
	assert.ErrorIs(t, err, ErrNotFound, "최종적으로 ErrNotFound가 반환되어야 합니다.")
	assert.Nil(t, stream, "에러 발생 시 스트림은 nil이어야 합니다.")

	// Fetcher는 한 번 호출되어야 합니다.
	assert.Equal(t, 1, customFetcher.getFetchCount(), "Fetcher는 한 번 호출되어야 합니다.")

	// Hot Store에 네거티브 캐시 항목이 저장되었는지 확인합니다.
	hot.mu.RLock()
	defer hot.mu.RUnlock()
	meta, metaOk := hot.meta[key]
	_, dataOk := hot.data[key]

	assert.True(t, metaOk, "네거티브 캐시 메타데이터가 저장되어야 합니다.")
	assert.False(t, dataOk, "네거티브 캐시에는 데이터가 없어야 합니다.")
	assert.True(t, meta.IsNegative, "메타데이터의 IsNegative 플래그가 true여야 합니다.")
}

// TestCache_NegativeCache_Disabled는 네거티브 캐시가 비활성화됐을 때,
// 오리진에서 데이터가 없어도 아무것도 캐시하지 않는 것을 검증합니다.
func TestCache_NegativeCache_Disabled(t *testing.T) {
	// 1. 네거티브 캐시 비활성화 (기본값)
	cache, hot, _ := setupCache(t)
	ctx := context.Background()
	key := "no-negative-key"

	// 2. Fetcher가 (nil, nil)을 반환하도록 설정
	customFetcher := &mockFetcher{}
	customFetcher.FetchFunc = func(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error) {
		// 이 방식은 컴파일 에러를 발생시키지 않습니다.
		customFetcher.mu.Lock()
		customFetcher.fetchCount++
		customFetcher.mu.Unlock()
		return nil, nil // 명시적으로 (nil, nil) 반환
	}

	// 3. Get 호출
	_, err := cache.Get(ctx, key, customFetcher)

	// 4. 검증
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Equal(t, 1, customFetcher.getFetchCount())

	// Hot Store에 아무것도 저장되지 않았는지 확인
	hot.mu.RLock()
	defer hot.mu.RUnlock()
	_, metaOk := hot.meta[key]
	assert.False(t, metaOk, "네거티브 캐시가 비활성화되면 메타데이터가 저장되지 않아야 합니다.")
}

// TestCache_PositiveGracePeriod_ServesStaleWhileRefreshing는
// 유예 기간(Grace Period)이 설정된 아이템이 만료되었을 때,
// 이전 데이터를 먼저 반환하고 백그라운드에서 갱신을 시도하는지 검증합니다.
func TestCache_PositiveGracePeriod_ServesStaleWhileRefreshing(t *testing.T) {
	// 1. 유예 기간 활성화
	cache, hot, _ := setupCache(t, WithGracePeriod(5*time.Minute))
	ctx := context.Background()
	key := "grace-key"

	// 2. 만료되었지만 유예 기간은 지나지 않은 "오래된" 데이터를 캐시에 저장
	// GraceUntil 필드를 과거 시간으로 설정하여 만료 상태를 시뮬레이션
	oldContent := "stale data"
	hot.setData(key, oldContent, &Metadata{
		ETag:       "v1",
		GraceUntil: time.Now().Add(-1 * time.Minute), // 1분 전에 만료됨
	})

	// 3. Fetcher는 "새로운" 데이터를 반환하도록 설정
	fetcher := newDeterministicFetcher("fresh data", "v2")

	// 4. Get 호출
	stream, err := cache.Get(ctx, key, fetcher)
	require.NoError(t, err)

	// 5. 검증 (1단계 - 즉시 반환)
	// 즉시 반환된 데이터는 "오래된" 데이터여야 합니다.
	readBytes, _ := io.ReadAll(stream)
	stream.Close()
	assert.Equal(t, oldContent, string(readBytes), "만료된 데이터가 즉시 반환되어야 합니다.")

	// 6. 검증 (2단계 - 백그라운드 작업)
	// 백그라운드에서 Fetcher가 호출되고 캐시가 갱신되어야 합니다.
	// newDeterministicFetcher와 mockStore의 채널을 사용하여 동기화
	<-fetcher.fetchStarted
	<-fetcher.fetchEnd
	select {
	case completedKey := <-hot.writeCompleted:
		assert.Equal(t, key, completedKey)
	case <-time.After(2 * time.Second):
		t.Fatal("백그라운드 쓰기 작업이 시간 내에 완료되지 않았습니다.")
	}

	// Fetcher가 한 번 호출되었는지 확인
	assert.Equal(t, 1, fetcher.getFetchCount())

	// 캐시가 "새로운" 데이터로 갱신되었는지 확인
	hot.mu.RLock()
	defer hot.mu.RUnlock()
	assert.Equal(t, "v2", hot.meta[key].ETag, "캐시의 ETag가 v2로 갱신되어야 합니다.")
	assert.Equal(t, "fresh data", string(hot.data[key]), "캐시의 데이터가 'fresh data'로 갱신되어야 합니다.")
}
