package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestHandleConditionalLowerTierPromotionError_CancelsOnGenericPromotionFailure(t *testing.T) {
	cache := &DaramjweeCache{
		logger: log.NewNopLogger(),
	}
	canceled := false

	resp := cache.handleConditionalLowerTierPromotionError(
		"key",
		1,
		errors.New("promotion failed"),
		io.NopCloser(&noopReader{}),
		&Metadata{CacheTag: "cache-v1"},
		func() { canceled = true },
	)

	require.True(t, canceled)
	require.Equal(t, GetStatusNotModified, resp.Status)
	require.Nil(t, resp.Body)
	require.Equal(t, "cache-v1", resp.Metadata.CacheTag)
}

func TestPromoteNegativeLowerTierHitJoinsWriterSetupAndSourceCloseErrors(t *testing.T) {
	writerSetupErr := errors.New("writer setup failed")
	sourceCloseErr := errors.New("source close failed")
	cache := &DaramjweeCache{
		tiers:  []Store{&cacheReadFailingBeginSetStore{err: writerSetupErr}},
		logger: log.NewNopLogger(),
	}
	src := &closeErrorReadCloser{err: sourceCloseErr}
	canceled := false

	resp, err := cache.promoteNegativeLowerTierHit(
		context.Background(),
		context.Background(),
		"key",
		1,
		src,
		&Metadata{IsNegative: true},
		&Metadata{IsNegative: true},
		func() { canceled = true },
		0,
	)

	require.Nil(t, resp)
	require.ErrorIs(t, err, writerSetupErr)
	require.ErrorIs(t, err, sourceCloseErr)
	require.True(t, canceled)
	require.True(t, src.closed)
}

func TestPromoteLowerTierHitToTopJoinsWriterSetupAndSourceCloseErrors(t *testing.T) {
	writerSetupErr := errors.New("writer setup failed")
	sourceCloseErr := errors.New("source close failed")
	cache := &DaramjweeCache{
		tiers:  []Store{&cacheReadFailingBeginSetStore{err: writerSetupErr}},
		logger: log.NewNopLogger(),
	}
	src := &closeErrorReadCloser{err: sourceCloseErr}

	err := cache.promoteLowerTierHitToTop(
		context.Background(),
		context.Background(),
		"key",
		1,
		src,
		&Metadata{},
		0,
	)

	require.ErrorIs(t, err, writerSetupErr)
	require.ErrorIs(t, err, sourceCloseErr)
	require.True(t, src.closed)
}

func TestPromoteRefreshFallbackToTopPreservesInvalidationOnSourceCloseSuccess(t *testing.T) {
	meta := &Metadata{CacheTag: "v1"}
	cache := &DaramjweeCache{
		tiers: []Store{
			&cacheReadFailingBeginSetStore{err: ErrTopWriteInvalidated},
			&cacheReadSourceStore{metadata: meta, body: []byte("value")},
		},
		logger: log.NewNopLogger(),
	}

	err := cache.promoteRefreshFallbackToTop(
		context.Background(),
		"key",
		tierDestination{tierIndex: 1, store: cache.tiers[1]},
		meta,
		0,
	)

	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestPromoteRefreshFallbackToTopJoinsInvalidationAndSourceCloseError(t *testing.T) {
	sourceCloseErr := errors.New("source close failed")
	meta := &Metadata{CacheTag: "v1"}
	cache := &DaramjweeCache{
		tiers: []Store{
			&cacheReadFailingBeginSetStore{err: ErrTopWriteInvalidated},
			&cacheReadSourceStore{metadata: meta, body: []byte("value"), closeErr: sourceCloseErr},
		},
		logger: log.NewNopLogger(),
	}

	err := cache.promoteRefreshFallbackToTop(
		context.Background(),
		"key",
		tierDestination{tierIndex: 1, store: cache.tiers[1]},
		meta,
		0,
	)

	require.ErrorIs(t, err, ErrTopWriteInvalidated)
	require.ErrorIs(t, err, sourceCloseErr)
}

func TestPromoteRefreshFallbackToTopPreservesNegativeInvalidation(t *testing.T) {
	meta := &Metadata{CacheTag: "v1", IsNegative: true}
	cache := &DaramjweeCache{
		tiers: []Store{
			&cacheReadFailingBeginSetStore{err: ErrTopWriteInvalidated},
			&cacheReadSourceStore{metadata: meta},
		},
		logger: log.NewNopLogger(),
	}

	err := cache.promoteRefreshFallbackToTop(
		context.Background(),
		"key",
		tierDestination{tierIndex: 1, store: cache.tiers[1]},
		meta,
		0,
	)

	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestPromoteRefreshFallbackToTopSkipsMissingNegativeSource(t *testing.T) {
	meta := &Metadata{CacheTag: "v1", IsNegative: true}
	cache := &DaramjweeCache{
		tiers: []Store{
			&cacheReadFailingBeginSetStore{err: errors.New("unexpected BeginSet")},
			&cacheReadSourceStore{metadata: meta, statErr: ErrNotFound},
		},
		logger: log.NewNopLogger(),
	}

	err := cache.promoteRefreshFallbackToTop(
		context.Background(),
		"key",
		tierDestination{tierIndex: 1, store: cache.tiers[1]},
		meta,
		0,
	)

	require.NoError(t, err)
}

func TestPromoteRefreshFallbackToTopSkipsMissingSourceStream(t *testing.T) {
	meta := &Metadata{CacheTag: "v1"}
	cache := &DaramjweeCache{
		tiers: []Store{
			&cacheReadFailingBeginSetStore{err: errors.New("unexpected BeginSet")},
			&cacheReadSourceStore{metadata: meta, getErr: ErrNotFound},
		},
		logger: log.NewNopLogger(),
	}

	err := cache.promoteRefreshFallbackToTop(
		context.Background(),
		"key",
		tierDestination{tierIndex: 1, store: cache.tiers[1]},
		meta,
		0,
	)

	require.NoError(t, err)
}

func TestFetchFromOriginRejectsNilResult(t *testing.T) {
	cache := &DaramjweeCache{}

	_, err := cache.fetchFromOrigin(context.Background(), nilResultFetcher{}, nil)

	require.ErrorContains(t, err, "fetcher returned nil result")
}

func TestFetchFromOriginRejectsNilBody(t *testing.T) {
	cache := &DaramjweeCache{}

	_, err := cache.fetchFromOrigin(context.Background(), nilBodyFetcher{}, nil)

	require.ErrorContains(t, err, "fetcher returned nil body")
}

type noopReader struct{}

func (noopReader) Read(p []byte) (int, error) { return 0, io.EOF }

type cacheReadFailingBeginSetStore struct {
	err error
}

func (s *cacheReadFailingBeginSetStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *cacheReadFailingBeginSetStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return nil, s.err
}

func (s *cacheReadFailingBeginSetStore) Delete(context.Context, string) error {
	return nil
}

func (s *cacheReadFailingBeginSetStore) Stat(context.Context, string) (*Metadata, error) {
	return nil, ErrNotFound
}

type closeErrorReadCloser struct {
	err    error
	closed bool
}

func (r *closeErrorReadCloser) Read(p []byte) (int, error) { return 0, io.EOF }

func (r *closeErrorReadCloser) Close() error {
	r.closed = true
	return r.err
}

type cacheReadSourceStore struct {
	metadata *Metadata
	body     []byte
	closeErr error
	getErr   error
	statErr  error
}

func (s *cacheReadSourceStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	if s.getErr != nil {
		return nil, nil, s.getErr
	}
	if s.closeErr != nil {
		return &closeErrorReadCloser{err: s.closeErr}, cloneMetadata(s.metadata), nil
	}
	return io.NopCloser(bytes.NewReader(s.body)), cloneMetadata(s.metadata), nil
}

func (s *cacheReadSourceStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return nil, errors.New("unexpected BeginSet")
}

func (s *cacheReadSourceStore) Delete(context.Context, string) error {
	return nil
}

func (s *cacheReadSourceStore) Stat(context.Context, string) (*Metadata, error) {
	if s.statErr != nil {
		return nil, s.statErr
	}
	return cloneMetadata(s.metadata), nil
}

type nilResultFetcher struct{}

func (nilResultFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return nil, nil
}

type nilBodyFetcher struct{}

func (nilBodyFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return &FetchResult{Metadata: &Metadata{}}, nil
}
