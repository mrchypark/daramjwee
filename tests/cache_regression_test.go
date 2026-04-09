package daramjwee_test

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleRefresh_ReturnsErrorWhenWorkerQueueIsFull(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(1),
		daramjwee.WithWorkerTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	blocker := make(chan struct{})
	started := make(chan struct{}, 1)
	blockingFetcher := blockingFetcher{
		started: started,
		blocker: blocker,
	}

	require.NoError(t, cache.ScheduleRefresh(ctx, "key-1", blockingFetcher))
	<-started
	require.NoError(t, cache.ScheduleRefresh(ctx, "key-2", blockingFetcher))

	err = cache.ScheduleRefresh(ctx, "key-3", blockingFetcher)
	require.Error(t, err)

	close(blocker)
}

func TestScheduleRefresh_PersistsToColdWhenColdEntryIsMissing(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFreshness(time.Hour, 0),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "refresh-cold-missing"

	wc, err := cache.Set(ctx, key, &daramjwee.Metadata{CacheTag: "v0"})
	require.NoError(t, err)
	_, err = wc.Write([]byte("old-value"))
	require.NoError(t, err)
	require.NoError(t, wc.Close())

	mf := &mockFetcher{content: "new-value", etag: "v1"}
	require.NoError(t, cache.ScheduleRefresh(ctx, key, mf))

	require.Eventually(t, func() bool {
		r, _, err := cold.GetStream(ctx, key)
		if err != nil {
			return false
		}
		_ = r.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
}

func TestScheduleRefresh_DoesNotPublishPartialDataOnCopyFailure(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "refresh-partial-copy"

	wc, err := cache.Set(ctx, key, &daramjwee.Metadata{CacheTag: "v0"})
	require.NoError(t, err)
	_, err = wc.Write([]byte("old-value"))
	require.NoError(t, err)
	require.NoError(t, wc.Close())

	fetcher := &failingRefreshFetcher{
		body: newFailingReadCloser([]byte("new-"), assert.AnError),
		meta: &daramjwee.Metadata{CacheTag: "v1"},
	}

	require.NoError(t, cache.ScheduleRefresh(ctx, key, fetcher))
	require.Eventually(t, func() bool { return fetcher.body.done() }, 2*time.Second, 10*time.Millisecond)

	reader, meta, err := hot.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "old-value", string(body))
	assert.Equal(t, "v0", meta.CacheTag)
}

func TestScheduleRefresh_WithoutColdStoreDoesNotPanicWorker(t *testing.T) {
	logBuf := &lockedBuffer{}
	logger := log.NewLogfmtLogger(logBuf)
	hot := newMockStore()
	hot.setData("stale-key", "old-value", &daramjwee.Metadata{
		CacheTag: "v0",
		CachedAt: time.Now().Add(-time.Hour),
	})

	cache, err := daramjwee.New(
		logger,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFreshness(time.Minute, 0),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)

	fetcher := &mockFetcher{content: "new-value", etag: "v1"}
	reader, err := cache.Get(context.Background(), "stale-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	_, err = io.Copy(io.Discard, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	require.Eventually(t, func() bool {
		return fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)

	cache.Close()
	assert.NotContains(t, logBuf.String(), "worker job panicked")
	assert.False(t, strings.Contains(logBuf.String(), "panic="), "unexpected panic log: %s", logBuf.String())
	assert.NotContains(t, logBuf.String(), "starting background set")
}

func TestScheduleRefresh_ContinuesAfterCallerContextCancel(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx, cancel := context.WithCancel(context.Background())
	fetcher := &mockFetcher{content: "refreshed-after-cancel", etag: "v1", fetchDelay: 50 * time.Millisecond}

	require.NoError(t, cache.ScheduleRefresh(ctx, "refresh-after-cancel", fetcher))
	cancel()

	require.Eventually(t, func() bool {
		reader, meta, err := hot.GetStream(context.Background(), "refresh-after-cancel")
		if err != nil {
			return false
		}
		defer reader.Close()
		body, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(body) == "refreshed-after-cancel" && meta.CacheTag == "v1" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

// Background refresh validation reuses the worker job context, so a fetch that
// outlives OpTimeout but stays within WorkerTimeout must still publish.
func TestScheduleRefresh_SlowFetchDoesNotInvalidateWithinWorkerTimeout(t *testing.T) {
	hotInner := newMockStore()
	hot := newContextAwareStatStore(hotInner)
	key := "refresh-slow-fetch"
	hotInner.setData(key, "old-value", &daramjwee.Metadata{
		CacheTag: "v0",
		CachedAt: time.Now().Add(-time.Hour),
	})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(20*time.Millisecond),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(300*time.Millisecond),
	)
	require.NoError(t, err)
	defer cache.Close()

	fetcher := &mockFetcher{
		content:    "fresh-value",
		etag:       "v1",
		fetchDelay: 60 * time.Millisecond,
	}

	require.NoError(t, cache.ScheduleRefresh(context.Background(), key, fetcher))

	require.Eventually(t, func() bool {
		reader, meta, err := hot.GetStream(context.Background(), key)
		if err != nil {
			return false
		}
		defer reader.Close()
		body, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(body) == "fresh-value" && meta.CacheTag == "v1"
	}, time.Second, 10*time.Millisecond)
}

// Fanout validation shares the worker job context for the same reason: a slow
// source copy should persist as long as the job itself has not timed out.
func TestScheduleRefresh_FanoutSlowCopyDoesNotInvalidateWithinWorkerTimeout(t *testing.T) {
	top := newContextAwareStatStore(newDelayedReadStore(newMockStore(), 60*time.Millisecond))
	mid := newMockStore()
	low := newMockStore()
	key := "fanout-slow-copy"
	low.setData(key, "lower-value", &daramjwee.Metadata{
		CacheTag: "v1",
		CachedAt: time.Now(),
	})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, low),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(20*time.Millisecond),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(300*time.Millisecond),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	require.Equal(t, "lower-value", string(body))
	require.NoError(t, resp.Close())

	require.Eventually(t, func() bool {
		reader, meta, err := mid.GetStream(context.Background(), key)
		if err != nil {
			return false
		}
		defer reader.Close()
		copied, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(copied) == "lower-value" && meta.CacheTag == "v1"
	}, time.Second, 10*time.Millisecond)
}

func TestScheduleRefresh_DoesNotOverwriteNewerTopEntry(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "refresh-does-not-clobber"

	wc, err := cache.Set(ctx, key, &daramjwee.Metadata{CacheTag: "v0"})
	require.NoError(t, err)
	_, err = wc.Write([]byte("old-value"))
	require.NoError(t, err)
	require.NoError(t, wc.Close())

	started := make(chan struct{}, 1)
	blocker := make(chan struct{})
	fetcher := blockingSuccessFetcher{
		started:  started,
		blocker:  blocker,
		content:  "background-value",
		cacheTag: "v1",
	}

	require.NoError(t, cache.ScheduleRefresh(ctx, key, fetcher))
	<-started

	newer, err := cache.Set(ctx, key, &daramjwee.Metadata{CacheTag: "user-v2"})
	require.NoError(t, err)
	_, err = newer.Write([]byte("user-value"))
	require.NoError(t, err)
	require.NoError(t, newer.Close())

	close(blocker)

	require.Eventually(t, func() bool {
		reader, meta, err := hot.GetStream(context.Background(), key)
		if err != nil {
			return false
		}
		defer reader.Close()
		body, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(body) == "user-value" && meta.CacheTag == "user-v2"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestScheduleRefresh_CloseWindowDoesNotOverwriteNewerSet(t *testing.T) {
	top := newNotifyingSlowSetStore(150 * time.Millisecond)

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := "refresh-close-window-no-clobber"
	top.setData(key, "old-value", &daramjwee.Metadata{
		CacheTag: "v0",
		CachedAt: time.Now().Add(-time.Hour),
	})

	started := make(chan struct{}, 1)
	blocker := make(chan struct{})
	fetcher := blockingSuccessFetcher{
		started:  started,
		blocker:  blocker,
		content:  "background-value",
		cacheTag: "v1",
	}

	require.NoError(t, cache.ScheduleRefresh(context.Background(), key, fetcher))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not start")
	}
	close(blocker)

	select {
	case <-top.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not reach top-tier close")
	}

	newer, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: "user-v2"})
	require.NoError(t, err)
	_, err = newer.Write([]byte("user-value"))
	require.NoError(t, err)
	require.NoError(t, newer.Close())

	require.Eventually(t, func() bool {
		reader, meta, err := top.GetStream(context.Background(), key)
		if err != nil {
			return false
		}
		defer reader.Close()
		current, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(current) == "user-value" && meta.CacheTag == "user-v2"
	}, 2*time.Second, 10*time.Millisecond)
}

type blockingFetcher struct {
	started chan struct{}
	blocker chan struct{}
}

type delayedReadStore struct {
	inner *mockStore
	delay time.Duration
}

func newDelayedReadStore(inner *mockStore, delay time.Duration) *delayedReadStore {
	return &delayedReadStore{inner: inner, delay: delay}
}

func (s *delayedReadStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	reader, meta, err := s.inner.GetStream(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	return &delayedReadCloser{ReadCloser: reader, delay: s.delay}, meta, nil
}

func (s *delayedReadStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return s.inner.BeginSet(ctx, key, metadata)
}

func (s *delayedReadStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *delayedReadStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.inner.Stat(ctx, key)
}

type delayedReadCloser struct {
	io.ReadCloser
	delay time.Duration
}

func (r *delayedReadCloser) Read(p []byte) (int, error) {
	time.Sleep(r.delay)
	return r.ReadCloser.Read(p)
}

type contextAwareStatStore struct {
	inner daramjwee.Store
}

func newContextAwareStatStore(inner daramjwee.Store) *contextAwareStatStore {
	return &contextAwareStatStore{inner: inner}
}

func (s *contextAwareStatStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return s.inner.GetStream(ctx, key)
}

func (s *contextAwareStatStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return s.inner.BeginSet(ctx, key, metadata)
}

func (s *contextAwareStatStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *contextAwareStatStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return s.inner.Stat(ctx, key)
}

func (f blockingFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	select {
	case f.started <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.blocker:
		return nil, daramjwee.ErrCacheableNotFound
	}
}

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

type notifyingSlowSetStore struct {
	*mockStore
	closeStarted chan struct{}
	delay        time.Duration
}

func newNotifyingSlowSetStore(delay time.Duration) *notifyingSlowSetStore {
	return &notifyingSlowSetStore{
		mockStore:    newMockStore(),
		closeStarted: make(chan struct{}, 1),
		delay:        delay,
	}
}

func (s *notifyingSlowSetStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	sink, err := s.mockStore.BeginSet(ctx, key, metadata)
	if err != nil {
		return nil, err
	}
	return &notifyingSlowSetSink{
		WriteSink:    sink,
		closeStarted: s.closeStarted,
		delay:        s.delay,
	}, nil
}

type notifyingSlowSetSink struct {
	daramjwee.WriteSink
	closeStarted chan struct{}
	delay        time.Duration
}

func (s *notifyingSlowSetSink) Close() error {
	select {
	case s.closeStarted <- struct{}{}:
	default:
	}
	time.Sleep(s.delay)
	return s.WriteSink.Close()
}

type failingRefreshFetcher struct {
	body *failingReadCloser
	meta *daramjwee.Metadata
}

func (f *failingRefreshFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     f.body,
		Metadata: f.meta,
	}, nil
}

type failingReadCloser struct {
	data     []byte
	offset   int
	err      error
	errSent  bool
	doneCh   chan struct{}
	doneOnce bool
}

func newFailingReadCloser(prefix []byte, err error) *failingReadCloser {
	return &failingReadCloser{
		data:   bytes.Clone(prefix),
		err:    err,
		doneCh: make(chan struct{}),
	}
}

func (r *failingReadCloser) Read(p []byte) (int, error) {
	if r.errSent {
		r.finish()
		return 0, r.err
	}
	if r.offset >= len(r.data) {
		r.errSent = true
		r.finish()
		return 0, r.err
	}

	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *failingReadCloser) Close() error {
	r.finish()
	return nil
}

func (r *failingReadCloser) done() bool {
	select {
	case <-r.doneCh:
		return true
	default:
		return false
	}
}

func (r *failingReadCloser) finish() {
	if r.doneOnce {
		return
	}
	r.doneOnce = true
	close(r.doneCh)
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
