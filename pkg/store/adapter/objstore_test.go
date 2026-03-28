package adapter

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestObjstoreAdapter_ReadsLegacyObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	metaBytes, err := json.Marshal(&daramjwee.Metadata{ETag: "legacy"})
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, "legacy-key", strings.NewReader("legacy body")))
	require.NoError(t, bucket.Upload(ctx, legacyMetaPath("legacy-key"), strings.NewReader(string(metaBytes))))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDataDir(t.TempDir()))

	reader, meta, err := store.GetStream(ctx, "legacy-key")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "legacy body", string(body))
	assert.Equal(t, "legacy", meta.ETag)

	stat, err := store.Stat(ctx, "legacy-key")
	require.NoError(t, err)
	assert.Equal(t, "legacy", stat.ETag)
}

func TestObjstoreAdapter_PrefersModernEntriesOverLegacyFallback(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	metaBytes, err := json.Marshal(&daramjwee.Metadata{ETag: "legacy"})
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, "same-key", strings.NewReader("legacy body")))
	require.NoError(t, bucket.Upload(ctx, legacyMetaPath("same-key"), strings.NewReader(string(metaBytes))))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDataDir(t.TempDir()))
	writer, err := store.BeginSet(ctx, "same-key", &daramjwee.Metadata{ETag: "modern"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "modern body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, meta, err := store.GetStream(ctx, "same-key")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "modern body", string(body))
	assert.Equal(t, "modern", meta.ETag)
}

func TestObjstoreAdapter_DeleteRemovesLegacyObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	metaBytes, err := json.Marshal(&daramjwee.Metadata{ETag: "legacy"})
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, "legacy-delete", strings.NewReader("legacy body")))
	require.NoError(t, bucket.Upload(ctx, legacyMetaPath("legacy-delete"), strings.NewReader(string(metaBytes))))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDataDir(t.TempDir()))
	require.NoError(t, store.Delete(ctx, "legacy-delete"))

	_, _, err = store.GetStream(ctx, "legacy-delete")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestObjstoreAdapter_DeleteDoesNotRemoveRawNamesWithoutLegacyMetadata(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	key := "checkpoints/example/latest.json"
	require.NoError(t, bucket.Upload(ctx, key, strings.NewReader("internal object body")))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDataDir(t.TempDir()))
	require.NoError(t, store.Delete(ctx, key))

	reader, err := bucket.Get(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "internal object body", string(body))
}
