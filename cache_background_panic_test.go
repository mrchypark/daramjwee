package daramjwee

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee/internal/runtime"
	"github.com/mrchypark/daramjwee/internal/worker"
)

type panicBackgroundReadCloser struct {
	triggered chan struct{}
	once      sync.Once
}

func (r *panicBackgroundReadCloser) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.triggered) })
	panic("background source read panic")
}

func (*panicBackgroundReadCloser) Close() error { return nil }

type panicBackgroundStore struct {
	panicWrite bool
	triggered  chan struct{}
}

func (*panicBackgroundStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *panicBackgroundStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return &panicBackgroundSink{store: s}, nil
}

func (s *panicBackgroundStore) BeginStagedSet(context.Context, string, *Metadata) (StagedWriteSink, error) {
	return &panicBackgroundSink{store: s}, nil
}

func (*panicBackgroundStore) Delete(context.Context, string) error { return nil }

func (*panicBackgroundStore) Stat(context.Context, string) (*Metadata, error) {
	return nil, ErrNotFound
}

type panicBackgroundSink struct {
	store *panicBackgroundStore
	once  sync.Once
}

func (s *panicBackgroundSink) Write(p []byte) (int, error) {
	if s.store.panicWrite {
		s.once.Do(func() { close(s.store.triggered) })
		panic("background destination write panic")
	}
	return len(p), nil
}

func (*panicBackgroundSink) Close() error                 { return nil }
func (*panicBackgroundSink) Commit(context.Context) error { return nil }
func (*panicBackgroundSink) Abort() error                 { return nil }

type panicPersistSourceStore struct {
	panicRead bool
	triggered chan struct{}
}

func (s *panicPersistSourceStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	if s.panicRead {
		return &panicBackgroundReadCloser{triggered: s.triggered}, &Metadata{CacheTag: "v1"}, nil
	}
	return io.NopCloser(bytes.NewReader([]byte("value"))), &Metadata{CacheTag: "v1"}, nil
}

func (*panicPersistSourceStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return &panicBackgroundSink{store: &panicBackgroundStore{}}, nil
}

func (*panicPersistSourceStore) Delete(context.Context, string) error { return nil }

func (*panicPersistSourceStore) Stat(context.Context, string) (*Metadata, error) {
	return &Metadata{CacheTag: "v1"}, nil
}

type panicRefreshFetcher struct {
	panicRead bool
	triggered chan struct{}
}

type panicNotModifiedFetcher struct{}

func (panicNotModifiedFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return nil, ErrNotModified
}

type panicCacheableNotFoundFetcher struct{}

func (panicCacheableNotFoundFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return nil, ErrCacheableNotFound
}

type panicMissFetcher struct{}

func (panicMissFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	panic("miss fetch panic")
}

type panicCommitStore struct {
	commitTriggered chan struct{}
	commitOnce      sync.Once
	abortCalls      atomic.Int32
}

func (*panicCommitStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *panicCommitStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return &panicCommitSink{store: s}, nil
}

func (s *panicCommitStore) BeginStagedSet(context.Context, string, *Metadata) (StagedWriteSink, error) {
	return &panicCommitSink{store: s}, nil
}

func (*panicCommitStore) Delete(context.Context, string) error { return nil }

func (*panicCommitStore) Stat(context.Context, string) (*Metadata, error) {
	return nil, ErrNotFound
}

type panicCommitSink struct {
	store *panicCommitStore
}

func (*panicCommitSink) Write(p []byte) (int, error) { return len(p), nil }

func (s *panicCommitSink) Close() error { return s.Commit(context.Background()) }

func (s *panicCommitSink) Commit(context.Context) error {
	s.store.commitOnce.Do(func() { close(s.store.commitTriggered) })
	panic("background staged commit panic")
}

func (s *panicCommitSink) Abort() error {
	s.store.abortCalls.Add(1)
	return nil
}

type panicFallbackSourceStore struct {
	meta      *Metadata
	panicRead bool
	triggered chan struct{}
}

func (s *panicFallbackSourceStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	if s.panicRead {
		return &panicBackgroundReadCloser{triggered: s.triggered}, cloneMetadata(s.meta), nil
	}
	return io.NopCloser(bytes.NewReader([]byte("fallback"))), cloneMetadata(s.meta), nil
}

func (*panicFallbackSourceStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return &panicBackgroundSink{store: &panicBackgroundStore{}}, nil
}

func (*panicFallbackSourceStore) Delete(context.Context, string) error { return nil }

func (s *panicFallbackSourceStore) Stat(context.Context, string) (*Metadata, error) {
	return cloneMetadata(s.meta), nil
}

type panicStaleTopStore struct {
	meta      *Metadata
	triggered chan struct{}
}

func (s *panicStaleTopStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return io.NopCloser(bytes.NewReader([]byte("current"))), cloneMetadata(s.meta), nil
}

func (s *panicStaleTopStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return &panicBackgroundSink{store: &panicBackgroundStore{panicWrite: true, triggered: s.triggered}}, nil
}

func (s *panicStaleTopStore) BeginStagedSet(context.Context, string, *Metadata) (StagedWriteSink, error) {
	return &panicBackgroundSink{store: &panicBackgroundStore{panicWrite: true, triggered: s.triggered}}, nil
}

func (*panicStaleTopStore) Delete(context.Context, string) error { return nil }

func (s *panicStaleTopStore) Stat(context.Context, string) (*Metadata, error) {
	return cloneMetadata(s.meta), nil
}

func (f panicRefreshFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	if f.panicRead {
		return &FetchResult{
			Body:     &panicBackgroundReadCloser{triggered: f.triggered},
			Metadata: &Metadata{CacheTag: "v2"},
		}, nil
	}
	return &FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte("value"))),
		Metadata: &Metadata{CacheTag: "v2"},
	}, nil
}

func newPanicTestRuntime(t *testing.T) backgroundRuntime {
	t.Helper()
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 4, time.Second)
	require.NoError(t, err)
	return runtime.NewStandalone(manager)
}

func waitForRuntimeRecovery(t *testing.T, rt backgroundRuntime, cacheID string, kind JobKind) {
	t.Helper()
	recovered := make(chan struct{})
	err := rt.Submit(cacheID, kind, Job{Run: func(context.Context) { close(recovered) }})
	require.NoError(t, err)
	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		t.Fatal("native runtime did not continue after background panic")
	}
}

func waitForPanicTrigger(t *testing.T, triggered <-chan struct{}) {
	t.Helper()
	select {
	case <-triggered:
	case <-time.After(2 * time.Second):
		t.Fatal("background panic path was not reached")
	}
}

func requireCoordinatorRetired(t *testing.T, cache *DaramjweeCache, key string) {
	t.Helper()
	if _, ok := cache.topWrites.coords.Load(key); ok {
		t.Fatalf("top-write coordinator for %q remained after recovered panic", key)
	}
}

func requireLaterSameKeyWrite(t *testing.T, cache *DaramjweeCache, key string) {
	t.Helper()
	writer, err := cache.Set(context.Background(), key, &Metadata{CacheTag: "later"})
	require.NoError(t, err)
	require.NoError(t, writer.Abort())
	requireCoordinatorRetired(t, cache, key)
}

func TestMissFetcherPanicReleasesLeader(t *testing.T) {
	cache, err := New(nil, WithTiers(&panicBackgroundStore{}), WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()
	impl, ok := cache.(*DaramjweeCache)
	require.True(t, ok)

	const key = "miss-fetch-panic"
	require.PanicsWithValue(t, "miss fetch panic", func() {
		_, _ = cache.Get(context.Background(), key, GetRequest{}, panicMissFetcher{})
	})

	_, active := impl.missLeads.current(key)
	require.False(t, active)
	requireCoordinatorRetired(t, impl, key)
}

func TestPersistBackgroundPanicReleasesCoordinator(t *testing.T) {
	for _, panicAt := range []string{"source", "destination"} {
		t.Run(panicAt, func(t *testing.T) {
			triggered := make(chan struct{})
			source := &panicPersistSourceStore{panicRead: panicAt == "source", triggered: triggered}
			destination := &panicBackgroundStore{panicWrite: panicAt == "destination", triggered: triggered}
			runtime := newPanicTestRuntime(t)
			cache := &DaramjweeCache{
				tiers:   []Store{source},
				runtime: runtime,
				cacheID: "persist-panic-" + panicAt,
				logger:  log.NewNopLogger(),
				config:  cacheConfig{opTimeout: time.Second, closeTimeout: time.Second},
			}
			t.Cleanup(cache.Close)

			key := "key"
			expected := cache.currentTopWriteGeneration(key)
			cache.schedulePersistFromTop(context.Background(), key, expected, tierDestination{tierIndex: 1, store: destination})
			expected.release()
			waitForPanicTrigger(t, triggered)
			waitForRuntimeRecovery(t, runtime, cache.cacheID, JobKindPersist)
			requireCoordinatorRetired(t, cache, key)
			requireLaterSameKeyWrite(t, cache, key)
		})
	}
}

func TestRefreshBackgroundPanicReleasesCoordinator(t *testing.T) {
	for _, panicAt := range []string{"source", "destination"} {
		t.Run(panicAt, func(t *testing.T) {
			triggered := make(chan struct{})
			top := &panicBackgroundStore{panicWrite: panicAt == "destination", triggered: triggered}
			runtime := newPanicTestRuntime(t)
			cache := &DaramjweeCache{
				tiers:   []Store{top},
				runtime: runtime,
				cacheID: "refresh-panic-" + panicAt,
				logger:  log.NewNopLogger(),
				config:  cacheConfig{opTimeout: time.Second, closeTimeout: time.Second},
			}
			t.Cleanup(cache.Close)

			key := "key"
			err := cache.scheduleRefreshWithMetadata(context.Background(), key, panicRefreshFetcher{
				panicRead: panicAt == "source",
				triggered: triggered,
			}, nil, nil, nil)
			require.NoError(t, err)
			waitForPanicTrigger(t, triggered)
			waitForRuntimeRecovery(t, runtime, cache.cacheID, JobKindRefresh)
			requireCoordinatorRetired(t, cache, key)
			requireLaterSameKeyWrite(t, cache, key)
		})
	}
}

func TestNotModifiedFallbackPanicReleasesCoordinator(t *testing.T) {
	for _, panicAt := range []string{"source", "destination"} {
		t.Run(panicAt, func(t *testing.T) {
			triggered := make(chan struct{})
			meta := &Metadata{CacheTag: "fallback", CachedAt: time.Now().Add(-time.Hour)}
			top := &panicBackgroundStore{panicWrite: panicAt == "destination", triggered: triggered}
			fallback := &panicFallbackSourceStore{meta: meta, panicRead: panicAt == "source", triggered: triggered}
			runtime := newPanicTestRuntime(t)
			cache := &DaramjweeCache{
				tiers:   []Store{top, fallback},
				runtime: runtime,
				cacheID: "not-modified-fallback-panic-" + panicAt,
				logger:  log.NewNopLogger(),
				config:  cacheConfig{opTimeout: time.Second, closeTimeout: time.Second},
			}
			t.Cleanup(cache.Close)

			key := "key"
			err := cache.scheduleRefreshWithMetadata(
				context.Background(),
				key,
				panicNotModifiedFetcher{},
				meta,
				&tierDestination{tierIndex: 1, store: fallback},
				nil,
			)
			require.NoError(t, err)
			waitForPanicTrigger(t, triggered)
			waitForRuntimeRecovery(t, runtime, cache.cacheID, JobKindRefresh)
			requireCoordinatorRetired(t, cache, key)
			requireLaterSameKeyWrite(t, cache, key)
		})
	}
}

func TestNotModifiedStaleTopWritePanicReleasesCoordinator(t *testing.T) {
	triggered := make(chan struct{})
	top := &panicStaleTopStore{
		meta:      &Metadata{CacheTag: "current", CachedAt: time.Now().Add(-time.Hour)},
		triggered: triggered,
	}
	runtime := newPanicTestRuntime(t)
	cache := &DaramjweeCache{
		tiers:   []Store{top},
		runtime: runtime,
		cacheID: "not-modified-stale-top-panic",
		logger:  log.NewNopLogger(),
		config:  cacheConfig{opTimeout: time.Second, closeTimeout: time.Second, positiveFreshness: time.Minute},
	}
	t.Cleanup(cache.Close)

	key := "key"
	require.NoError(t, cache.scheduleRefreshWithMetadata(
		context.Background(),
		key,
		panicNotModifiedFetcher{},
		nil,
		nil,
		nil,
	))
	waitForPanicTrigger(t, triggered)
	waitForRuntimeRecovery(t, runtime, cache.cacheID, JobKindRefresh)
	requireCoordinatorRetired(t, cache, key)
	requireLaterSameKeyWrite(t, cache, key)
}

func TestCacheableNotFoundCommitPanicAbortsAndReleasesCoordinator(t *testing.T) {
	top := &panicCommitStore{commitTriggered: make(chan struct{})}
	runtime := newPanicTestRuntime(t)
	cache := &DaramjweeCache{
		tiers:   []Store{top},
		runtime: runtime,
		cacheID: "cacheable-not-found-commit-panic",
		logger:  log.NewNopLogger(),
		config:  cacheConfig{opTimeout: time.Second, closeTimeout: time.Second},
	}
	t.Cleanup(cache.Close)

	key := "key"
	require.NoError(t, cache.scheduleRefreshWithMetadata(
		context.Background(),
		key,
		panicCacheableNotFoundFetcher{},
		nil,
		nil,
		nil,
	))
	waitForPanicTrigger(t, top.commitTriggered)
	waitForRuntimeRecovery(t, runtime, cache.cacheID, JobKindRefresh)
	require.Equal(t, int32(1), top.abortCalls.Load())
	requireCoordinatorRetired(t, cache, key)
	requireLaterSameKeyWrite(t, cache, key)
}
