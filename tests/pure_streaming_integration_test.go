package daramjwee_test

import (
	"bytes"
	"context"
	"io"
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

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(2*time.Second))
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
		metadata:      &daramjwee.Metadata{ETag: "v1"},
	}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithHotStore(hot),
		daramjwee.WithColdStore(cold),
		daramjwee.WithDefaultTimeout(2*time.Second),
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

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(2*time.Second))
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

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(2*time.Second))
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
	cold.setData("partial-cold-key", "partial-value", &daramjwee.Metadata{ETag: "v1"})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithHotStore(hot),
		daramjwee.WithColdStore(cold),
		daramjwee.WithDefaultTimeout(2*time.Second),
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
		daramjwee.WithHotStore(hot),
		daramjwee.WithColdStore(cold),
		daramjwee.WithDefaultTimeout(2*time.Second),
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

type getResult struct {
	stream io.ReadCloser
	err    error
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
