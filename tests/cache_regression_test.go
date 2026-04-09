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

func TestCache_DeleteInvalidatesOpenWriter(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	sink, err := cache.Set(context.Background(), "delete-invalidates-open-writer", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("new-value"))
	require.NoError(t, err)

	require.NoError(t, cache.Delete(context.Background(), "delete-invalidates-open-writer"))

	err = sink.Close()
	require.Error(t, err)

	_, _, err = hot.GetStream(context.Background(), "delete-invalidates-open-writer")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
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

func TestCache_MissFetchDoesNotOverwriteNewerSet(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "miss-does-not-clobber"
	started := make(chan struct{}, 1)
	blocker := make(chan struct{})
	fetcher := blockingSuccessFetcher{
		started:  started,
		blocker:  blocker,
		content:  "fetched-value",
		cacheTag: "origin-v1",
	}

	type getResult struct {
		resp *daramjwee.GetResponse
		err  error
	}
	done := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(ctx, key, daramjwee.GetRequest{}, fetcher)
		done <- getResult{resp: resp, err: err}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("fetch did not start")
	}

	newer, err := cache.Set(ctx, key, &daramjwee.Metadata{CacheTag: "user-v2"})
	require.NoError(t, err)
	_, err = newer.Write([]byte("user-value"))
	require.NoError(t, err)
	require.NoError(t, newer.Close())

	close(blocker)

	result := <-done
	require.NoError(t, result.err)
	require.NotNil(t, result.resp)
	defer result.resp.Close()
	body, err := io.ReadAll(result.resp)
	require.NoError(t, err)
	require.Equal(t, "fetched-value", string(body))

	require.Eventually(t, func() bool {
		reader, meta, err := hot.GetStream(context.Background(), key)
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

func TestScheduleRefresh_QueuedJobDoesNotOverwriteNewerSet(t *testing.T) {
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

	started := make(chan struct{}, 1)
	blocker := make(chan struct{})
	blockingJob := blockingSuccessFetcher{
		started:  started,
		blocker:  blocker,
		content:  "blocker-value",
		cacheTag: "blocker-v1",
	}
	require.NoError(t, cache.ScheduleRefresh(context.Background(), "blocker-key", blockingJob))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking refresh did not start")
	}

	key := "queued-refresh-no-clobber"
	targetFetcher := &mockFetcher{content: "refresh-value", etag: "refresh-v1"}
	require.NoError(t, cache.ScheduleRefresh(context.Background(), key, targetFetcher))

	newer, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: "user-v2"})
	require.NoError(t, err)
	_, err = newer.Write([]byte("user-value"))
	require.NoError(t, err)
	require.NoError(t, newer.Close())
	drainMockStoreWrites(hot)

	close(blocker)
	require.Eventually(t, func() bool { return targetFetcher.getFetchCount() > 0 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	reader, meta, err := hot.GetStream(context.Background(), key)
	require.NoError(t, err)
	defer reader.Close()
	current, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "user-value", string(current))
	require.Equal(t, "user-v2", meta.CacheTag)
}

func TestScheduleRefresh_CloseWindowDeleteKeepsKeyDeleted(t *testing.T) {
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

	require.NoError(t, cache.Delete(context.Background(), key))

	_, _, err = top.GetStream(context.Background(), key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_LowerTierPromotionDeleteKeepsKeyDeleted(t *testing.T) {
	top := newNotifyingSlowSetStore(150 * time.Millisecond)
	lower := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFreshness(time.Hour, 0),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := "lower-tier-promotion-no-clobber"
	lower.setData(key, "lower-value", &daramjwee.Metadata{
		CacheTag: "lower-v1",
		CachedAt: time.Now(),
	})

	type getResult struct {
		body string
		err  error
	}
	done := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, &mockFetcher{})
		if err != nil {
			done <- getResult{err: err}
			return
		}
		defer resp.Close()
		body, readErr := io.ReadAll(resp)
		done <- getResult{body: string(body), err: readErr}
	}()

	select {
	case <-top.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("lower-tier promotion did not reach top-tier close")
	}

	require.NoError(t, cache.Delete(context.Background(), key))

	result := <-done
	require.NoError(t, result.err)
	require.Equal(t, "lower-value", result.body)

	_, _, err = top.GetStream(context.Background(), key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_FanoutDeleteDuringLowerTierCloseKeepsKeyDeleted(t *testing.T) {
	top := newMockStore()
	cold := newNotifyingSlowSetStore(150 * time.Millisecond)

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, cold),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := "fanout-delete-during-close"
	resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, &mockFetcher{content: "origin-value", etag: "origin-v1"})
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	require.Equal(t, "origin-value", string(body))
	require.NoError(t, resp.Close())

	select {
	case <-cold.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("fanout did not reach lower-tier close")
	}

	require.NoError(t, cache.Delete(context.Background(), key))
	time.Sleep(250 * time.Millisecond)

	_, _, topErr := top.GetStream(context.Background(), key)
	_, _, coldErr := cold.GetStream(context.Background(), key)
	require.ErrorIs(t, topErr, daramjwee.ErrNotFound)
	require.ErrorIs(t, coldErr, daramjwee.ErrNotFound)
}

func TestCache_DeleteDuringSlowCloseKeepsKeyDeleted(t *testing.T) {
	top := newNotifyingSlowSetStore(150 * time.Millisecond)

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	sink, err := cache.Set(context.Background(), "slow-close-delete", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("new-value"))
	require.NoError(t, err)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- sink.Close()
	}()

	select {
	case <-top.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not reach close")
	}

	require.NoError(t, cache.Delete(context.Background(), "slow-close-delete"))

	closeErr := <-closeDone
	require.NoError(t, closeErr)

	_, _, err = top.GetStream(context.Background(), "slow-close-delete")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

type blockingFetcher struct {
	started chan struct{}
	blocker chan struct{}
}

type blockingSuccessFetcher struct {
	started  chan struct{}
	blocker  chan struct{}
	content  string
	cacheTag string
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

func drainMockStoreWrites(store *mockStore) {
	for {
		select {
		case <-store.writeCompleted:
		default:
			return
		}
	}
}
