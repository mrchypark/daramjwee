package objectstore

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestStore_UsableThroughStoreInterface(t *testing.T) {
	ctx := context.Background()
	var store daramjwee.Store = New(objstore.NewInMemBucket(), log.NewNopLogger())

	writer, err := store.BeginSet(ctx, "iface-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)

	_, err = io.WriteString(writer, "hello, objectstore")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "iface-key")
	require.NoError(t, err)
	require.NotNil(t, meta)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "hello, objectstore", string(body))
	assert.Equal(t, "v1", meta.ETag)
}

func TestStore_AbortLeavesNoVisibleEntry(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())

	writer, err := store.BeginSet(ctx, "abort-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "partial")
	require.NoError(t, err)
	require.NoError(t, writer.Abort())

	_, err = store.Stat(ctx, "abort-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_OverwriteReplacesVisibleValue(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())

	writeObject := func(etag, body string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, "overwrite-key", &daramjwee.Metadata{ETag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	writeObject("v1", "old value")
	writeObject("v2", "new value")

	stream, meta, err := store.GetStream(ctx, "overwrite-key")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "new value", string(body))
	assert.Equal(t, "v2", meta.ETag)
}

func TestStore_CloseErrorDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	store := New(&errorBucket{
		Bucket: objstore.NewInMemBucket(),
		uploadFunc: func(ctx context.Context, name string, r io.Reader) error {
			_, _ = io.ReadAll(r)
			return errors.New("upload failed")
		},
	}, log.NewNopLogger())

	writer, err := store.BeginSet(ctx, "upload-error", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)

	err = writer.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload failed")

	_, err = store.Stat(ctx, "upload-error")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_DeleteRemovesVisibility(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())

	writer, err := store.BeginSet(ctx, "delete-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "data")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	require.NoError(t, store.Delete(ctx, "delete-key"))
	_, err = store.Stat(ctx, "delete-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_NegativeEntryWithNoBody(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())

	writer, err := store.BeginSet(ctx, "negative-key", &daramjwee.Metadata{
		ETag:       "neg-v1",
		IsNegative: true,
		CachedAt:   time.Now().Truncate(time.Millisecond),
	})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "negative-key")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Len(t, body, 0)
	assert.True(t, meta.IsNegative)
	assert.Equal(t, "neg-v1", meta.ETag)
}

func TestStore_MetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())
	now := time.Now().Truncate(time.Millisecond)

	writer, err := store.BeginSet(ctx, "meta-key", &daramjwee.Metadata{
		ETag:       "meta-v1",
		IsNegative: true,
		CachedAt:   now,
	})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	meta, err := store.Stat(ctx, "meta-key")
	require.NoError(t, err)
	assert.Equal(t, "meta-v1", meta.ETag)
	assert.True(t, meta.IsNegative)
	assert.True(t, meta.CachedAt.Equal(now))
}

type errorBucket struct {
	objstore.Bucket
	uploadFunc func(ctx context.Context, name string, r io.Reader) error
}

func (b *errorBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.uploadFunc != nil {
		return b.uploadFunc(ctx, name, r)
	}
	_, _ = io.ReadAll(r)
	return errors.New("simulated upload error")
}
