package daramjwee

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee/internal/runtime"
	"github.com/mrchypark/daramjwee/internal/worker"
	"github.com/stretchr/testify/require"
)

// testCloseHandler is a closeHandler for tests.
type testCloseHandler func()

func (h testCloseHandler) handle() { h() }

func TestSafeCloserReadAll(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		callback bool
	}{
		{
			name:     "normal read all",
			input:    "hello world",
			expected: "hello world",
			callback: true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
			callback: true,
		},
		{
			name:     "large text",
			input:    strings.Repeat("test data ", 100),
			expected: strings.Repeat("test data ", 100),
			callback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 콜백 실행 확인용
			callbackExecuted := false

			// strings.NewReader로 ReadCloser 생성
			reader := io.NopCloser(strings.NewReader(tt.input))

			// safeCloser 생성
			sc := newSafeCloser(reader, testCloseHandler(func() {
				callbackExecuted = true
			}))

			// ReadAll 테스트
			result, err := sc.ReadAll()

			// 결과 검증
			if err != nil {
				t.Errorf("ReadAll() error = %v", err)
				return
			}

			if string(result) != tt.expected {
				t.Errorf("ReadAll() = %q, want %q", string(result), tt.expected)
			}

			// 콜백 실행 확인
			if callbackExecuted != tt.callback {
				t.Errorf("callback executed = %v, want %v", callbackExecuted, tt.callback)
			}
		})
	}
}

func TestSafeCloserReadAllAutoClose(t *testing.T) {
	// 콜백 실행 확인
	callbackExecuted := false
	closeCount := 0

	// 커스텀 ReadCloser로 Close 호출 횟수 확인
	reader := &testReadCloser{
		Reader: strings.NewReader("test data"),
		onClose: func() {
			closeCount++
		},
	}

	sc := newSafeCloser(reader, testCloseHandler(func() {
		callbackExecuted = true
	}))

	// ReadAll 실행
	data, err := sc.ReadAll()

	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if string(data) != "test data" {
		t.Errorf("ReadAll() = %q, want %q", string(data), "test data")
	}

	// EOF 도달 시 자동으로 한 번만 닫혀야 함
	if closeCount != 1 {
		t.Errorf("close count = %d, want 1", closeCount)
	}

	// 콜백이 실행되어야 함
	if !callbackExecuted {
		t.Error("callback should be executed")
	}

	// 다시 Close 호출해도 중복 실행되지 않아야 함
	sc.Close()
	if closeCount != 1 {
		t.Errorf("close count after second Close() = %d, want 1", closeCount)
	}
}

// 테스트용 ReadCloser
type testReadCloser struct {
	io.Reader
	onClose func()
}

func (t *testReadCloser) Close() error {
	if t.onClose != nil {
		t.onClose()
	}
	return nil
}
func TestReadAllSmart(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		useSafe  bool
	}{
		{
			name:     "with safeCloser",
			input:    "test data with safeCloser",
			expected: "test data with safeCloser",
			useSafe:  true,
		},
		{
			name:     "with regular ReadCloser",
			input:    "test data with regular ReadCloser",
			expected: "test data with regular ReadCloser",
			useSafe:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rc io.ReadCloser
			callbackExecuted := false

			if tt.useSafe {
				// safeCloser 사용
				reader := io.NopCloser(strings.NewReader(tt.input))
				rc = newSafeCloser(reader, testCloseHandler(func() {
					callbackExecuted = true
				}))
			} else {
				// 일반 ReadCloser 사용
				rc = io.NopCloser(strings.NewReader(tt.input))
			}

			// ReadAllSmart 테스트
			result, err := ReadAll(rc)

			if err != nil {
				t.Errorf("ReadAllSmart() error = %v", err)
				return
			}

			if string(result) != tt.expected {
				t.Errorf("ReadAllSmart() = %q, want %q", string(result), tt.expected)
			}

			// safeCloser인 경우 콜백이 실행되어야 함
			if tt.useSafe && !callbackExecuted {
				t.Error("callback should be executed for safeCloser")
			}

			// 일반 ReadCloser인 경우 콜백이 실행되지 않아야 함
			if !tt.useSafe && callbackExecuted {
				t.Error("callback should not be executed for regular ReadCloser")
			}
		})
	}
}

func TestSetKeepsCallerMetadataUnchangedWhileStoringFreshCachedAt(t *testing.T) {
	store := newMetadataOwnershipStore()
	cache := newMetadataOwnershipCache(store)
	sentinel := time.Unix(1, 0).UTC()
	metadata := &Metadata{CacheTag: "set-v1", CachedAt: sentinel}
	want := *metadata

	sink, err := cache.Set(context.Background(), "set-key", metadata)
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	require.Equal(t, want, *metadata)
	require.True(t, store.metadata("set-key").CachedAt.After(sentinel))
}

func TestMissKeepsFetcherMetadataUnchangedWhileReturningAndStoringFreshCachedAt(t *testing.T) {
	store := newMetadataOwnershipStore()
	cache := newMetadataOwnershipCache(store)
	sentinel := time.Unix(1, 0).UTC()
	metadata := &Metadata{CacheTag: "miss-v1", CachedAt: sentinel}
	want := *metadata

	resp, err := cache.Get(context.Background(), "miss-key", GetRequest{}, metadataOwnershipFetcher{metadata: metadata})
	require.NoError(t, err)
	_, err = io.ReadAll(resp)
	require.NoError(t, err)

	require.Equal(t, want, *metadata)
	require.True(t, resp.Metadata.CachedAt.After(sentinel))
	storedMetadata := store.metadata("miss-key")
	require.NotNil(t, storedMetadata)
	require.Equal(t, resp.Metadata, *storedMetadata)
}

func TestRefreshKeepsFetcherMetadataUnchangedWhileStoringFreshCachedAt(t *testing.T) {
	store := newMetadataOwnershipStore()
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)
	cache := &DaramjweeCache{
		tiers:   []Store{store},
		logger:  log.NewNopLogger(),
		runtime: runtime.NewStandalone(manager),
		cacheID: "metadata-ownership-refresh",
		config:  cacheConfig{opTimeout: time.Second, closeTimeout: time.Second},
	}
	t.Cleanup(cache.Close)

	sentinel := time.Unix(1, 0).UTC()
	metadata := &Metadata{CacheTag: "refresh-v1", CachedAt: sentinel}
	want := *metadata

	require.NoError(t, cache.ScheduleRefresh(context.Background(), "refresh-key", metadataOwnershipFetcher{metadata: metadata}))
	select {
	case <-store.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not publish")
	}

	require.Equal(t, want, *metadata)
	require.True(t, store.metadata("refresh-key").CachedAt.After(sentinel))
}

func TestPublicSetAndMissDoNotRaceOnSharedMetadata(t *testing.T) {
	store := newMetadataOwnershipStore()
	cache := newMetadataOwnershipCache(store)
	metadata := &Metadata{CacheTag: "shared", CachedAt: time.Unix(1, 0).UTC()}
	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			sink, err := cache.Set(context.Background(), "set-race-key", metadata)
			if err == nil {
				err = sink.Abort()
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			resp, err := cache.Get(context.Background(), "miss-race-key", GetRequest{}, metadataOwnershipFetcher{metadata: metadata})
			if err == nil {
				_, err = io.ReadAll(resp)
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}

func newMetadataOwnershipCache(store Store) *DaramjweeCache {
	return &DaramjweeCache{
		tiers:  []Store{store},
		logger: log.NewNopLogger(),
		config: cacheConfig{opTimeout: time.Second, closeTimeout: time.Second},
	}
}

type metadataOwnershipFetcher struct {
	metadata *Metadata
}

func (f metadataOwnershipFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return &FetchResult{
		Body:     io.NopCloser(strings.NewReader("value")),
		Metadata: f.metadata,
	}, nil
}

type metadataOwnershipStore struct {
	mu      sync.Mutex
	entries map[string]*Metadata
	closed  chan struct{}
}

func newMetadataOwnershipStore() *metadataOwnershipStore {
	return &metadataOwnershipStore{entries: make(map[string]*Metadata), closed: make(chan struct{}, 10)}
}

func (s *metadataOwnershipStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *metadataOwnershipStore) BeginSet(_ context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &metadataOwnershipSink{store: s, key: key, metadata: cloneMetadata(metadata)}, nil
}

func (*metadataOwnershipStore) Delete(context.Context, string) error { return nil }

func (*metadataOwnershipStore) Stat(context.Context, string) (*Metadata, error) {
	return nil, ErrNotFound
}

func (s *metadataOwnershipStore) metadata(key string) *Metadata {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneMetadata(s.entries[key])
}

type metadataOwnershipSink struct {
	bytes.Buffer
	store    *metadataOwnershipStore
	key      string
	metadata *Metadata
	close    sync.Once
}

func (s *metadataOwnershipSink) Close() error {
	s.close.Do(func() {
		s.store.mu.Lock()
		s.store.entries[s.key] = cloneMetadata(s.metadata)
		s.store.mu.Unlock()
		select {
		case s.store.closed <- struct{}{}:
		default:
		}
	})
	return nil
}

func (*metadataOwnershipSink) Abort() error { return nil }
