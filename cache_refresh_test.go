package daramjwee

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRefreshTopEntryCachedAtStreamsBeforeSourceEOF(t *testing.T) {
	oldCachedAt := time.Now().Add(-time.Hour)
	source := newGatedRefreshSource([]byte("first-"), []byte("second"))
	store := &streamingRefreshStore{
		body:       []byte("old"),
		meta:       &Metadata{CacheTag: "v1", CachedAt: oldCachedAt},
		source:     source,
		firstWrite: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers:  []Store{store},
		config: cacheConfig{positiveFreshness: time.Minute},
	}

	// A failing assertion must not leave the source reader blocked.
	t.Cleanup(source.releaseEOF)

	done := make(chan error, 1)
	go func() {
		done <- cache.refreshTopEntryCachedAt(context.Background(), "key", cloneMetadata(store.meta), nil)
	}()

	select {
	case <-store.firstWrite:
		// The first staged write proves the source was copied incrementally.
	case <-time.After(time.Second):
		t.Fatal("staged sink did not receive the first chunk before source EOF was released")
	}

	source.releaseEOF()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("stale metadata refresh did not finish")
	}

	body, meta := store.committed()
	require.Equal(t, []byte("first-second"), body)
	require.Equal(t, "v1", meta.CacheTag)
	require.False(t, meta.IsNegative)
	require.True(t, meta.CachedAt.After(oldCachedAt))
	require.True(t, source.closed.Load(), "source must close before staged commit")
	require.True(t, store.sourceClosedBeforeCommit.Load(), "staged commit ran before source close")
}

func TestRefreshTopEntryCachedAtSkipsNonStagingStore(t *testing.T) {
	cachedAt := time.Date(2020, time.January, 2, 3, 4, 5, 6, time.UTC)
	for _, tt := range []struct {
		name     string
		metadata *Metadata
	}{
		{
			name:     "stale positive",
			metadata: &Metadata{CacheTag: "v1", CachedAt: cachedAt},
		},
		{
			name:     "stale negative",
			metadata: &Metadata{CacheTag: "v1", IsNegative: true, CachedAt: cachedAt},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &nonStagingRefreshSpy{metadata: cloneMetadata(tt.metadata)}
			cache := &DaramjweeCache{
				tiers:  []Store{store},
				config: cacheConfig{positiveFreshness: time.Minute},
			}

			require.NoError(t, cache.refreshTopEntryCachedAt(context.Background(), "key", cloneMetadata(tt.metadata), nil))
			require.Zero(t, store.statCalls)
			require.Zero(t, store.getStreamCalls)
			require.Zero(t, store.beginSetCalls)
			require.Zero(t, store.bodyCalls)
			require.Equal(t, tt.metadata, store.metadata)
		})
	}
}

func TestRefreshTopEntryCachedAtStagingStoreCancelsBeforeCommit(t *testing.T) {
	oldCachedAt := time.Now().Add(-time.Hour)
	source := newGatedRefreshSource([]byte("first-"), []byte("second"))
	store := &streamingRefreshStore{
		body:       []byte("old"),
		meta:       &Metadata{CacheTag: "v1", CachedAt: oldCachedAt},
		source:     source,
		firstWrite: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers:  []Store{store},
		config: cacheConfig{positiveFreshness: time.Minute},
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(source.releaseEOF)
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() {
		done <- cache.refreshTopEntryCachedAt(ctx, "key", cloneMetadata(store.meta), nil)
	}()

	select {
	case <-store.firstWrite:
	case <-time.After(time.Second):
		t.Fatal("staged sink did not receive the first chunk")
	}
	cancel()
	source.releaseEOF()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled stale metadata refresh did not finish")
	}

	body, meta := store.committed()
	require.Equal(t, []byte("old"), body)
	require.Equal(t, oldCachedAt, meta.CachedAt)
	require.False(t, store.published.Load(), "canceled staged refresh must not commit")
	require.Positive(t, store.abortCalls.Load(), "canceled staged refresh must abort")
}

type gatedRefreshSource struct {
	first, second []byte
	stage         int
	release       chan struct{}
	releaseOnce   sync.Once
	closed        atomic.Bool
}

func newGatedRefreshSource(first, second []byte) *gatedRefreshSource {
	return &gatedRefreshSource{first: first, second: second, release: make(chan struct{})}
}

func (r *gatedRefreshSource) Read(p []byte) (int, error) {
	switch r.stage {
	case 0:
		r.stage++
		return copy(p, r.first), nil
	case 1:
		<-r.release
		r.stage++
		return copy(p, r.second), nil
	default:
		return 0, io.EOF
	}
}

func (r *gatedRefreshSource) Close() error {
	r.closed.Store(true)
	return nil
}

func (r *gatedRefreshSource) releaseEOF() {
	r.releaseOnce.Do(func() { close(r.release) })
}

type streamingRefreshStore struct {
	mu                       sync.Mutex
	body                     []byte
	meta                     *Metadata
	source                   *gatedRefreshSource
	firstWrite               chan struct{}
	firstWriteOnce           sync.Once
	sourceClosedBeforeCommit atomic.Bool
	published                atomic.Bool
	abortCalls               atomic.Int32
}

func (s *streamingRefreshStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return s.source, cloneMetadata(s.meta), nil
}

func (s *streamingRefreshStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return nil, io.ErrClosedPipe
}

func (s *streamingRefreshStore) BeginStagedSet(_ context.Context, _ string, meta *Metadata) (StagedWriteSink, error) {
	return &streamingRefreshSink{store: s, meta: cloneMetadata(meta)}, nil
}

func (*streamingRefreshStore) Delete(context.Context, string) error { return nil }

func (s *streamingRefreshStore) Stat(context.Context, string) (*Metadata, error) {
	return cloneMetadata(s.meta), nil
}

func (s *streamingRefreshStore) committed() ([]byte, *Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.body...), cloneMetadata(s.meta)
}

type streamingRefreshSink struct {
	store *streamingRefreshStore
	meta  *Metadata
	body  []byte
}

func (s *streamingRefreshSink) Write(p []byte) (int, error) {
	s.body = append(s.body, p...)
	s.store.firstWriteOnce.Do(func() { close(s.store.firstWrite) })
	return len(p), nil
}

func (s *streamingRefreshSink) Commit(context.Context) error {
	s.store.sourceClosedBeforeCommit.Store(s.store.source.closed.Load())
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.store.body = append([]byte(nil), s.body...)
	s.store.meta = cloneMetadata(s.meta)
	s.store.published.Store(true)
	return nil
}

func (s *streamingRefreshSink) Abort() error {
	s.store.abortCalls.Add(1)
	return nil
}

type nonStagingRefreshSpy struct {
	metadata                                 *Metadata
	statCalls, getStreamCalls, beginSetCalls int
	bodyCalls                                int
}

func (s *nonStagingRefreshSpy) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	s.getStreamCalls++
	return &nonStagingRefreshBody{spy: s}, cloneMetadata(s.metadata), nil
}

func (s *nonStagingRefreshSpy) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	s.beginSetCalls++
	return &nonStagingRefreshSink{spy: s}, nil
}

func (*nonStagingRefreshSpy) Delete(context.Context, string) error { return nil }

func (s *nonStagingRefreshSpy) Stat(context.Context, string) (*Metadata, error) {
	s.statCalls++
	return cloneMetadata(s.metadata), nil
}

type nonStagingRefreshBody struct{ spy *nonStagingRefreshSpy }

func (b *nonStagingRefreshBody) Read([]byte) (int, error) {
	b.spy.bodyCalls++
	return 0, io.EOF
}

func (b *nonStagingRefreshBody) Close() error {
	b.spy.bodyCalls++
	return nil
}

type nonStagingRefreshSink struct{ spy *nonStagingRefreshSpy }

func (s *nonStagingRefreshSink) Write(p []byte) (int, error) {
	s.spy.bodyCalls++
	return len(p), nil
}

func (s *nonStagingRefreshSink) Close() error {
	s.spy.bodyCalls++
	return nil
}

func (s *nonStagingRefreshSink) Abort() error {
	s.spy.bodyCalls++
	return nil
}
