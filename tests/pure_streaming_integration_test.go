package daramjwee_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_MissReturnsStreamBeforeSourceEOF(t *testing.T) {
	hot := newMockStore()
	source := newBlockingReadCloser([]byte("hello"), []byte(" world"))
	fetcher := &blockingSourceFetcher{source: source, metadata: &daramjwee.Metadata{ETag: "v1"}}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resultCh := make(chan getResult, 1)
	go func() {
		stream, err := cache.Get(context.Background(), "miss-key", fetcher)
		resultCh <- getResult{stream: stream, err: err}
	}()

	var result getResult
	select {
	case result = <-resultCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cache.Get blocked until source EOF on miss")
	}

	require.NoError(t, result.err)
	require.NotNil(t, result.stream)
	defer result.stream.Close()

	first := make([]byte, 5)
	n, err := result.stream.Read(first)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(first[:n]))

	_, _, err = hot.GetStream(context.Background(), "miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	source.Release()
	rest, err := io.ReadAll(result.stream)
	require.NoError(t, err)
	assert.Equal(t, " world", string(rest))
	require.NoError(t, result.stream.Close())

	reader, _, err := hot.GetStream(context.Background(), "miss-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(body))
}

func TestCache_ColdHitReturnsStreamBeforeSourceEOF(t *testing.T) {
	hot := newMockStore()
	source := newBlockingReadCloser([]byte("cold"), []byte(" value"))
	cold := &blockingColdStore{
		streamFactory: func() io.ReadCloser { return source },
		metadata:      &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()},
	}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resultCh := make(chan getResult, 1)
	go func() {
		stream, err := cache.Get(context.Background(), "cold-key", &mockFetcher{})
		resultCh <- getResult{stream: stream, err: err}
	}()

	var result getResult
	select {
	case result = <-resultCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cache.Get blocked until cold source EOF on cold hit")
	}

	require.NoError(t, result.err)
	require.NotNil(t, result.stream)
	defer result.stream.Close()

	first := make([]byte, 4)
	n, err := result.stream.Read(first)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "cold", string(first[:n]))

	_, _, err = hot.GetStream(context.Background(), "cold-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	source.Release()
	rest, err := io.ReadAll(result.stream)
	require.NoError(t, err)
	assert.Equal(t, " value", string(rest))
	require.NoError(t, result.stream.Close())

	reader, _, err := hot.GetStream(context.Background(), "cold-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "cold value", string(body))
}

func TestCache_MissBeginSetFailureFallsBackToPassthrough(t *testing.T) {
	hot := newMockStore()
	hot.forceSetError = true
	fetcher := &mockFetcher{content: "passthrough", etag: "v1"}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-key", fetcher)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "passthrough", string(body))

	_, _, err = hot.GetStream(context.Background(), "miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_PartialMissReadCloseDoesNotPublishToHot(t *testing.T) {
	hot := newMockStore()
	fetcher := &mockFetcher{content: "partial-value", etag: "v1"}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "partial-miss-key", fetcher)
	require.NoError(t, err)

	buf := make([]byte, 7)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	require.NoError(t, stream.Close())

	_, _, err = hot.GetStream(context.Background(), "partial-miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_PartialColdHitReadCloseDoesNotPublishToHot(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("partial-cold-key", "partial-value", &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "partial-cold-key", &mockFetcher{})
	require.NoError(t, err)

	buf := make([]byte, 7)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	require.NoError(t, stream.Close())

	_, _, err = hot.GetStream(context.Background(), "partial-cold-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_PublishFailureDoesNotScheduleColdPersist(t *testing.T) {
	hot := &failingHotStore{}
	cold := newMockStore()
	fetcher := &mockFetcher{content: "value", etag: "v1"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "publish-fail-key", fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "value", string(body))
	require.ErrorIs(t, stream.Close(), errPublishFailed)

	require.Never(t, func() bool {
		reader, _, err := cold.GetStream(context.Background(), "publish-fail-key")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 200*time.Millisecond, 20*time.Millisecond)
}

func TestCache_NotModifiedKeepsHotStreamOpenUntilExplicitClose(t *testing.T) {
	hot := &statOnlyThenReadableStore{
		data: []byte("cached-value"),
		meta: &daramjwee.Metadata{ETag: "v1"},
	}
	fetcher := &errFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "not-modified-key", fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "cached-value", string(body))
	assert.Equal(t, 0, hot.closeCount(), "stream should remain open until caller closes it")

	require.NoError(t, stream.Close())
	assert.Equal(t, 1, hot.closeCount(), "explicit close should close the underlying hot stream exactly once")
}

func TestCache_MissStreamIgnoresOpTimeoutAfterFetchReturns(t *testing.T) {
	hot := newContextBoundStore()
	fetcher := &contextBoundFetcher{body: []byte("timeout-safe-miss"), metadata: &daramjwee.Metadata{ETag: "v1"}}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(20*time.Millisecond))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-timeout-key", fetcher)
	require.NoError(t, err)
	defer stream.Close()

	time.Sleep(50 * time.Millisecond)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "timeout-safe-miss", string(body))
	require.NoError(t, stream.Close())

	reader, meta, err := hot.GetStream(context.Background(), "miss-timeout-key")
	require.NoError(t, err)
	defer reader.Close()
	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "timeout-safe-miss", string(persisted))
	assert.Equal(t, "v1", meta.ETag)
}

func TestCache_MissUsesOpTimeoutForFetchSetup(t *testing.T) {
	hot := newMockStore()
	fetcher := &mockFetcher{content: "timeout-safe-miss", etag: "v1", fetchDelay: 100 * time.Millisecond}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(20*time.Millisecond))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-timeout-setup-key", fetcher)
	require.Error(t, err)
	require.Nil(t, stream)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCache_LowerTierStreamIgnoresOpTimeoutAfterGetReturns(t *testing.T) {
	hot := newContextBoundStore()
	lower := newContextBoundStore()
	lower.set("lower-timeout-key", []byte("lower-timeout-value"), &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "lower-timeout-key", &mockFetcher{})
	require.NoError(t, err)
	defer stream.Close()

	time.Sleep(50 * time.Millisecond)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "lower-timeout-value", string(body))
	require.NoError(t, stream.Close())

	reader, meta, err := hot.GetStream(context.Background(), "lower-timeout-key")
	require.NoError(t, err)
	defer reader.Close()
	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "lower-timeout-value", string(persisted))
	assert.Equal(t, "v1", meta.ETag)
}

func TestCache_SetSinkIgnoresOpTimeoutAfterBeginSetReturns(t *testing.T) {
	hot := newContextBoundStore()

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(20*time.Millisecond))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "set-timeout-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	_, err = writer.Write([]byte("timeout-safe-set"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, meta, err := hot.GetStream(context.Background(), "set-timeout-key")
	require.NoError(t, err)
	defer reader.Close()
	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "timeout-safe-set", string(persisted))
	assert.Equal(t, "v1", meta.ETag)
}

type getResult struct {
	stream io.ReadCloser
	err    error
}

type contextBoundFetcher struct {
	body     []byte
	metadata *daramjwee.Metadata
}

func (f *contextBoundFetcher) FetchUsesContext() bool { return true }

func (f *contextBoundFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     &contextBoundReadCloser{ctx: ctx, Reader: bytes.NewReader(bytes.Clone(f.body))},
		Metadata: f.metadata,
	}, nil
}

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

type blockingColdStore struct {
	streamFactory func() io.ReadCloser
	metadata      *daramjwee.Metadata
}

func (s *blockingColdStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return s.streamFactory(), s.metadata, nil
}

func (s *blockingColdStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &discardSink{}, nil
}

func (s *blockingColdStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingColdStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.metadata, nil
}

type discardSink struct{}

func (s *discardSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *discardSink) Close() error                { return nil }
func (s *discardSink) Abort() error                { return nil }

type contextBoundReadCloser struct {
	ctx context.Context
	*bytes.Reader
}

func (r *contextBoundReadCloser) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}
	return r.Reader.Read(p)
}

func (r *contextBoundReadCloser) Close() error {
	return nil
}

type contextBoundStore struct {
	mu   sync.Mutex
	data map[string][]byte
	meta map[string]*daramjwee.Metadata
}

func newContextBoundStore() *contextBoundStore {
	return &contextBoundStore{
		data: make(map[string][]byte),
		meta: make(map[string]*daramjwee.Metadata),
	}
}

func (s *contextBoundStore) GetStreamUsesContext() bool { return true }

func (s *contextBoundStore) BeginSetUsesContext() bool { return true }

func (s *contextBoundStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	body, ok := s.data[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}
	meta := *s.meta[key]
	return &contextBoundReadCloser{ctx: ctx, Reader: bytes.NewReader(bytes.Clone(body))}, &meta, nil
}

func (s *contextBoundStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	metaCopy := &daramjwee.Metadata{}
	if metadata != nil {
		*metaCopy = *metadata
	}
	return &contextBoundSink{ctx: ctx, store: s, key: key, meta: metaCopy}, nil
}

func (s *contextBoundStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.meta, key)
	return nil
}

func (s *contextBoundStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.meta[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}
	metaCopy := *meta
	return &metaCopy, nil
}

func (s *contextBoundStore) set(key string, body []byte, meta *daramjwee.Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = bytes.Clone(body)
	metaCopy := &daramjwee.Metadata{}
	if meta != nil {
		*metaCopy = *meta
	}
	s.meta[key] = metaCopy
}

type contextBoundSink struct {
	ctx   context.Context
	store *contextBoundStore
	key   string
	meta  *daramjwee.Metadata
	buf   bytes.Buffer
	done  atomic.Bool
}

func (s *contextBoundSink) Write(p []byte) (int, error) {
	if s.done.Load() {
		return 0, io.ErrClosedPipe
	}
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
	}
	return s.buf.Write(p)
}

func (s *contextBoundSink) Close() error {
	if !s.done.CompareAndSwap(false, true) {
		return nil
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	s.store.set(s.key, s.buf.Bytes(), s.meta)
	return nil
}

func (s *contextBoundSink) Abort() error {
	s.done.Store(true)
	return nil
}

type errFetcher struct {
	err error
}

func (f *errFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return nil, f.err
}

type closeTrackingReadCloser struct {
	*bytes.Reader
	once    sync.Once
	onClose func()
}

func (r *closeTrackingReadCloser) Close() error {
	r.once.Do(func() {
		if r.onClose != nil {
			r.onClose()
		}
	})
	return nil
}

type statOnlyThenReadableStore struct {
	data     []byte
	meta     *daramjwee.Metadata
	getCalls int32
	closed   int32
}

func (s *statOnlyThenReadableStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if atomic.AddInt32(&s.getCalls, 1) == 1 {
		return nil, nil, daramjwee.ErrNotFound
	}
	return &closeTrackingReadCloser{
		Reader: bytes.NewReader(s.data),
		onClose: func() {
			atomic.AddInt32(&s.closed, 1)
		},
	}, s.meta, nil
}

func (s *statOnlyThenReadableStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &discardSink{}, nil
}

func (s *statOnlyThenReadableStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *statOnlyThenReadableStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.meta, nil
}

func (s *statOnlyThenReadableStore) closeCount() int {
	return int(atomic.LoadInt32(&s.closed))
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

var errPublishFailed = assert.AnError

type failingHotStore struct{}

func (s *failingHotStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, daramjwee.ErrNotFound
}

func (s *failingHotStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &failingWriteSink{}, nil
}

func (s *failingHotStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *failingHotStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return nil, daramjwee.ErrNotFound
}

type failingWriteSink struct {
	bytes.Buffer
}

func (s *failingWriteSink) Close() error { return errPublishFailed }
func (s *failingWriteSink) Abort() error { return nil }
