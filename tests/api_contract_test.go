package daramjwee_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type unsupportedHotStore struct{}

func (s *unsupportedHotStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, daramjwee.ErrNotFound
}

func (s *unsupportedHotStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return nil, assert.AnError
}

func (s *unsupportedHotStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *unsupportedHotStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return nil, daramjwee.ErrNotFound
}

func (s *unsupportedHotStore) ValidateHotStore() error {
	return &daramjwee.ConfigError{Message: "unsupported hot store"}
}

func TestCache_SetPublishesOnClose(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "set-close-key", &daramjwee.Metadata{ETag: "v1"})
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
	assert.Equal(t, "v1", meta.ETag)
}

func TestCache_SetDiscardsOnAbort(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "set-abort-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)

	_, err = writer.Write([]byte("partial"))
	require.NoError(t, err)
	require.NoError(t, writer.Abort())

	_, _, err = hot.GetStream(context.Background(), "set-abort-key")
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_RejectsUnsupportedHotStore(t *testing.T) {
	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(&unsupportedHotStore{}), daramjwee.WithDefaultTimeout(time.Second))
	require.Error(t, err)
	assert.Nil(t, cache)
	assert.Contains(t, err.Error(), "unsupported hot store")
}

func TestCache_RejectsCopyAndTruncateFileStoreAsHotStore(t *testing.T) {
	dir := t.TempDir()
	hot, err := filestore.New(dir, log.NewNopLogger(), filestore.WithCopyAndTruncate())
	require.NoError(t, err)

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(time.Second))
	require.Error(t, err)
	assert.Nil(t, cache)
	assert.Contains(t, err.Error(), "unsupported hot store")
}

func TestCache_GetRejectsNilFetcher(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	_, err = cache.Get(context.Background(), "missing-key", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNilFetcher)
}

func TestCache_ScheduleRefreshRejectsNilFetcher(t *testing.T) {
	hot := newMockStore()

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	err = cache.ScheduleRefresh(context.Background(), "key", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNilFetcher)
}

func TestCache_GetReturnsErrNilMetadataForHotHit(t *testing.T) {
	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(&nilMetadataStore{}), daramjwee.WithDefaultTimeout(time.Second))
	require.NoError(t, err)
	defer cache.Close()

	_, err = cache.Get(context.Background(), "key", &mockFetcher{})
	require.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrNilMetadata)
}
