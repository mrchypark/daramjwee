package adapter

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func legacyMetadataJSON(cacheTag string) []byte {
	return []byte(fmt.Sprintf(`{"ETag":%q,"IsNegative":false,"CachedAt":"0001-01-01T00:00:00Z"}`, cacheTag))
}

func TestObjstoreAdapter_ReadsLegacyObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	metaBytes := legacyMetadataJSON("legacy")
	require.NoError(t, bucket.Upload(ctx, "legacy-key", strings.NewReader("legacy body")))
	require.NoError(t, bucket.Upload(ctx, legacyMetaPath("legacy-key"), strings.NewReader(string(metaBytes))))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDir(t.TempDir()))

	reader, meta, err := store.GetStream(ctx, "legacy-key")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "legacy body", string(body))
	assert.Equal(t, "legacy", meta.CacheTag)

	stat, err := store.Stat(ctx, "legacy-key")
	require.NoError(t, err)
	assert.Equal(t, "legacy", stat.CacheTag)
}

func TestObjstoreAdapter_PrefersModernEntriesOverLegacyFallback(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	metaBytes := legacyMetadataJSON("legacy")
	require.NoError(t, bucket.Upload(ctx, "same-key", strings.NewReader("legacy body")))
	require.NoError(t, bucket.Upload(ctx, legacyMetaPath("same-key"), strings.NewReader(string(metaBytes))))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDir(t.TempDir()))
	writer, err := store.BeginSet(ctx, "same-key", &daramjwee.Metadata{CacheTag: "modern"})
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
	assert.Equal(t, "modern", meta.CacheTag)
}

func TestObjstoreAdapter_DeleteRemovesLegacyObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	metaBytes := legacyMetadataJSON("legacy")
	require.NoError(t, bucket.Upload(ctx, "legacy-delete", strings.NewReader("legacy body")))
	require.NoError(t, bucket.Upload(ctx, legacyMetaPath("legacy-delete"), strings.NewReader(string(metaBytes))))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDir(t.TempDir()))
	require.NoError(t, store.Delete(ctx, "legacy-delete"))

	_, _, err := store.GetStream(ctx, "legacy-delete")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestObjstoreAdapter_DeleteDoesNotRemoveRawNamesWithoutLegacyMetadata(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	key := "checkpoints/example/latest.json"
	require.NoError(t, bucket.Upload(ctx, key, strings.NewReader("internal object body")))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDir(t.TempDir()))
	require.NoError(t, store.Delete(ctx, key))

	reader, err := bucket.Get(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "internal object body", string(body))
}

func TestObjstoreAdapter_DeleteRemovesOrphanedLegacyPayloadWithoutMetadata(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	key := "legacy-orphan"
	require.NoError(t, bucket.Upload(ctx, key, strings.NewReader("legacy body")))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDir(t.TempDir()))
	require.NoError(t, store.Delete(ctx, key))

	_, err := bucket.Get(ctx, key)
	require.True(t, bucket.IsObjNotFoundErr(err))
}

func TestObjstoreAdapter_DeleteRemovesLegacyObjectsEvenWithCorruptMetadata(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	require.NoError(t, bucket.Upload(ctx, "legacy-corrupt", strings.NewReader("legacy body")))
	require.NoError(t, bucket.Upload(ctx, legacyMetaPath("legacy-corrupt"), strings.NewReader("{invalid json")))

	store := NewObjstoreAdapter(bucket, log.NewNopLogger(), objectstore.WithDir(t.TempDir()))
	require.NoError(t, store.Delete(ctx, "legacy-corrupt"))

	_, err := bucket.Get(ctx, "legacy-corrupt")
	require.True(t, bucket.IsObjNotFoundErr(err))
	_, err = bucket.Get(ctx, legacyMetaPath("legacy-corrupt"))
	require.True(t, bucket.IsObjNotFoundErr(err))
}
