package daramjwee_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

type nilMetadataStore struct{}

func (s *nilMetadataStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return io.NopCloser(bytes.NewReader([]byte("value"))), nil, nil
}

func (s *nilMetadataStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return nil, assert.AnError
}

func (s *nilMetadataStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *nilMetadataStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return nil, daramjwee.ErrNotFound
}

func TestCache_SetPublishesOnClose(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "set-close-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)

	_, err = writer.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, meta, err := hot.GetStream(context.Background(), "set-close-key")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), body)
	require.NotNil(t, meta)
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestCache_SetDiscardsOnAbort(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "set-abort-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)

	_, err = writer.Write([]byte("partial"))
	require.NoError(t, err)
	require.NoError(t, writer.Abort())

	_, _, err = hot.GetStream(context.Background(), "set-abort-key")
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_ObjectStoreTierZeroPublishesOnClose(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "daramjwee-objectstore-tier-zero-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.RemoveAll(dataDir)
	})

	store := objectstore.New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		objectstore.WithDir(dataDir),
	)

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(store), daramjwee.WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "objectstore-tier-zero", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)

	_, err = writer.Write([]byte("hello objectstore"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	fetcher := &mockFetcher{content: "origin-value", etag: "origin-v1"}
	reader, err := cache.Get(context.Background(), "objectstore-tier-zero", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello objectstore"), body)
	assert.Equal(t, 0, fetcher.getFetchCount())
}

func TestCache_ObjectStoreTierZeroSinkIgnoresOpTimeoutAfterBeginSetReturns(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "daramjwee-objectstore-tier-zero-timeout-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.RemoveAll(dataDir)
	})

	store := objectstore.New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		objectstore.WithDir(dataDir),
	)

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(store), daramjwee.WithOpTimeout(20*time.Millisecond))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "objectstore-tier-zero-timeout", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	_, err = writer.Write([]byte("hello objectstore"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, meta, err := store.GetStream(context.Background(), "objectstore-tier-zero-timeout")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello objectstore"), body)
	require.NotNil(t, meta)
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestCache_GetRejectsNilFetcher(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	_, err = cache.Get(context.Background(), "missing-key", daramjwee.GetRequest{}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNilFetcher)
}

func TestCache_ScheduleRefreshRejectsNilFetcher(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	err = cache.ScheduleRefresh(context.Background(), "key", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNilFetcher)
}

func TestCache_GetReturnsErrNilMetadataForTopTierHit(t *testing.T) {
	cache, err := daramjwee.New(nil, daramjwee.WithTiers(&nilMetadataStore{}), daramjwee.WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	_, err = cache.Get(context.Background(), "key", daramjwee.GetRequest{}, &mockFetcher{})
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNilMetadata)
}

func TestCache_GetReturnsBodyAndDecisionMetadata(t *testing.T) {
	hot := newMockStore()
	hot.setData("get-response-key", "cached-value", &daramjwee.Metadata{
		CacheTag: "cache-v1",
		CachedAt: time.Now(),
	})

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "get-response-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Close()

	assert.Equal(t, daramjwee.GetStatusOK, resp.Status)
	assert.Equal(t, "cache-v1", resp.Metadata.CacheTag)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("cached-value"), body)
}
