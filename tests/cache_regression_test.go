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
		daramjwee.WithHotStore(hot),
		daramjwee.WithColdStore(cold),
		daramjwee.WithDefaultTimeout(2*time.Second),
		daramjwee.WithWorker("pool", 1, 1, 5*time.Second),
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
		daramjwee.WithHotStore(hot),
		daramjwee.WithColdStore(cold),
		daramjwee.WithDefaultTimeout(2*time.Second),
		daramjwee.WithColdStorePositiveFreshFor(time.Hour),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "refresh-cold-missing"

	wc, err := cache.Set(ctx, key, &daramjwee.Metadata{ETag: "v0"})
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
		daramjwee.WithHotStore(hot),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "refresh-partial-copy"

	wc, err := cache.Set(ctx, key, &daramjwee.Metadata{ETag: "v0"})
	require.NoError(t, err)
	_, err = wc.Write([]byte("old-value"))
	require.NoError(t, err)
	require.NoError(t, wc.Close())

	fetcher := &failingRefreshFetcher{
		body: newFailingReadCloser([]byte("new-"), assert.AnError),
		meta: &daramjwee.Metadata{ETag: "v1"},
	}

	require.NoError(t, cache.ScheduleRefresh(ctx, key, fetcher))
	require.Eventually(t, func() bool { return fetcher.body.done() }, 2*time.Second, 10*time.Millisecond)

	reader, meta, err := hot.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "old-value", string(body))
	assert.Equal(t, "v0", meta.ETag)
}

func TestScheduleRefresh_WithoutColdStoreDoesNotPanicWorker(t *testing.T) {
	logBuf := &lockedBuffer{}
	logger := log.NewLogfmtLogger(logBuf)
	hot := newMockStore()
	hot.setData("stale-key", "old-value", &daramjwee.Metadata{
		ETag:     "v0",
		CachedAt: time.Now().Add(-time.Hour),
	})

	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(hot),
		daramjwee.WithDefaultTimeout(2*time.Second),
		daramjwee.WithCache(time.Minute),
		daramjwee.WithWorker("pool", 1, 4, 2*time.Second),
	)
	require.NoError(t, err)

	fetcher := &mockFetcher{content: "new-value", etag: "v1"}
	reader, err := cache.Get(context.Background(), "stale-key", fetcher)
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

type blockingFetcher struct {
	started chan struct{}
	blocker chan struct{}
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
