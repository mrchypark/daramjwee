package objectstore

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestStore_GetStream_LocalPublishedHitReadsFromLocalSegment(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "local-read", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "local body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "local-read")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "local body", string(body))
	assert.Equal(t, "v1", meta.ETag)
}

func TestStore_GetStream_RemoteOnlyHitResolvesThroughShardCheckpoint(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	flushed := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	flushed.autoFlush = false

	writer, err := flushed.BeginSet(ctx, "remote-only", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "remote checkpoint body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, flushed.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	remoteOnly.autoFlush = false

	stream, meta, err := remoteOnly.GetStream(ctx, "remote-only")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "remote checkpoint body", string(body))
	assert.Equal(t, "v1", meta.ETag)
}

func TestStore_GetStream_RemotePackedRecordReturnsExactLogicalObject(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	flushed := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	flushed.autoFlush = false
	keyA, keyB := sameShardKeys("packed-read")

	for _, tc := range []struct {
		key  string
		etag string
		body string
	}{
		{keyA, "v1", "alpha remote value"},
		{keyB, "v2", "beta remote value"},
	} {
		writer, err := flushed.BeginSet(ctx, tc.key, &daramjwee.Metadata{ETag: tc.etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, tc.body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.NoError(t, flushed.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	remoteOnly.autoFlush = false

	stream, meta, err := remoteOnly.GetStream(ctx, keyB)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "beta remote value", string(body))
	assert.Equal(t, "v2", meta.ETag)
}

func TestStore_GetStream_FallsBackToRemoteWhenSelectedLocalSegmentDisappears(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "local-disappears-remote-live", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "remote fallback body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	entry, ok := store.catalog.Get("local-disappears-remote-live")
	require.True(t, ok)
	remotePath := joinPath(store.prefix, "segments", "local-disappears-remote-live.seg")
	require.NoError(t, bucket.Upload(ctx, remotePath, strings.NewReader("remote fallback body")))
	require.NoError(t, store.publishCheckpoint(ctx, shardForKey("local-disappears-remote-live"), map[string]checkpointEntry{
		"local-disappears-remote-live": {
			SegmentPath: remotePath,
			Offset:      0,
			Length:      int64(len("remote fallback body")),
			Metadata:    entry.Metadata,
		},
	}))
	require.NoError(t, store.updateLocalEntry("local-disappears-remote-live", func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		require.True(t, exists)
		current.RemotePath = remotePath
		current.RemoteOffset = 0
		return current, true
	}))

	origOpen := openLocalSegmentFile
	openLocalSegmentFile = func(path string) (*os.File, error) {
		require.NoError(t, os.Remove(path))
		return origOpen(path)
	}
	t.Cleanup(func() {
		openLocalSegmentFile = origOpen
	})

	stream, meta, err := store.GetStream(ctx, "local-disappears-remote-live")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "remote fallback body", string(body))
	assert.Equal(t, "v1", meta.ETag)
}

func TestPackedRemoteReader_ReturnsUnexpectedEOFOnShortPackedBlock(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	remotePath := joinPath(store.prefix, "segments", "packed-short.seg")
	require.NoError(t, bucket.Upload(ctx, remotePath, strings.NewReader("abc")))

	reader := &packedRemoteReader{
		ctx:       ctx,
		store:     store,
		entry:     checkpointEntry{SegmentPath: remotePath, Offset: 4, Length: 2},
		blockSize: store.pageSize,
		blockIdx:  -1,
	}

	buf := make([]byte, 4)
	n, err := reader.Read(buf)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestStore_DeleteTombstoneHidesOlderPackedRecord(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "delete-tombstone", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "stale remote value")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, store.flushPending(ctx))

	require.NoError(t, store.Delete(ctx, "delete-tombstone"))

	_, statErr := store.Stat(ctx, "delete-tombstone")
	require.ErrorIs(t, statErr, daramjwee.ErrNotFound)

	_, _, getErr := store.GetStream(ctx, "delete-tombstone")
	require.ErrorIs(t, getErr, daramjwee.ErrNotFound)
}

func TestStore_DeleteRemoteOnlyKeyPreservesOtherCheckpointEntriesInSameShard(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	flushed := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	flushed.autoFlush = false
	keyA, keyB := sameShardKeys("remote-delete-restart")

	for _, tc := range []struct {
		key  string
		etag string
		body string
	}{
		{keyA, "v1", "alpha"},
		{keyB, "v2", "beta"},
	} {
		writer, err := flushed.BeginSet(ctx, tc.key, &daramjwee.Metadata{ETag: tc.etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, tc.body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.NoError(t, flushed.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	remoteOnly.autoFlush = false

	require.NoError(t, remoteOnly.Delete(ctx, keyA))

	observer := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	observer.autoFlush = false

	_, _, err := observer.GetStream(ctx, keyA)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	stream, meta, err := observer.GetStream(ctx, keyB)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "beta", string(body))
	assert.Equal(t, "v2", meta.ETag)

	checkpointObjects := listObjectNames(t, bucket, joinPath(remoteOnly.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)
	checkpoint := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.NotContains(t, checkpoint.Entries, keyA)
	require.Contains(t, checkpoint.Entries, keyB)
}

func TestStore_GetStream_FallsBackToLegacyManifestRemoteData(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	blobPath := store.blobPath("legacy-remote", "v1")
	require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader("legacy manifest body")))
	require.NoError(t, store.publishManifest(ctx, "legacy-remote", blobPath, int64(len("legacy manifest body")), &daramjwee.Metadata{ETag: "legacy"}))

	reader, meta, err := store.GetStream(ctx, "legacy-remote")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "legacy manifest body", string(body))
	assert.Equal(t, "legacy", meta.ETag)

	stat, err := store.Stat(ctx, "legacy-remote")
	require.NoError(t, err)
	assert.Equal(t, "legacy", stat.ETag)
}

func TestStore_GetStream_FallsBackToDefaultPageSizeForLegacyPagedManifest(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	body := strings.Repeat("paged-manifest-body-", 64)
	blobPath := joinPath(store.prefix, "segments", "legacy-paged.seg")
	require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader(body)))

	m := manifest{
		Version:  "legacy-paged",
		Layout:   layoutPaged,
		BlobPath: blobPath,
		Size:     int64(len(body)),
		PageSize: 0,
		Metadata: daramjwee.Metadata{ETag: "legacy-paged"},
	}
	encoded, err := json.Marshal(&m)
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, store.manifestPath("legacy-paged"), strings.NewReader(string(encoded))))

	reader, meta, err := store.GetStream(ctx, "legacy-paged")
	require.NoError(t, err)
	defer reader.Close()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, body, string(got))
	assert.Equal(t, "legacy-paged", meta.ETag)
}
