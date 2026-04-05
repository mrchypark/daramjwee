package objectstore

import (
	"context"
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

	writer, err := store.BeginSet(ctx, "iface-key", &daramjwee.Metadata{CacheTag: "v1"})
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
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_AbortLeavesNoVisibleEntry(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())

	writer, err := store.BeginSet(ctx, "abort-key", &daramjwee.Metadata{CacheTag: "v1"})
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
		writer, err := store.BeginSet(ctx, "overwrite-key", &daramjwee.Metadata{CacheTag: etag})
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
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestStore_DeleteRemovesVisibility(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())

	writer, err := store.BeginSet(ctx, "delete-key", &daramjwee.Metadata{CacheTag: "v1"})
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
		CacheTag:   "neg-v1",
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
	assert.Equal(t, "neg-v1", meta.CacheTag)
}

func TestStore_MetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger())
	now := time.Now().Truncate(time.Millisecond)

	writer, err := store.BeginSet(ctx, "meta-key", &daramjwee.Metadata{
		CacheTag:   "meta-v1",
		IsNegative: true,
		CachedAt:   now,
	})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	meta, err := store.Stat(ctx, "meta-key")
	require.NoError(t, err)
	assert.Equal(t, "meta-v1", meta.CacheTag)
	assert.True(t, meta.IsNegative)
	assert.True(t, meta.CachedAt.Equal(now))
}
