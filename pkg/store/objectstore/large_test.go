package objectstore

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestStore_PackedObjectFlushUsesPackedSegmentPath(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(32))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "packed-key", &daramjwee.Metadata{CacheTag: "small"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "small payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, store.flushPending(ctx))

	segmentObjects := listObjectNames(t, bucket, joinPath(store.prefix, "segments"))
	require.Len(t, segmentObjects, 1)
	blobObjects := listObjectNames(t, bucket, joinPath(store.prefix, "blobs"))
	require.Empty(t, blobObjects)

	checkpointObjects := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)
	checkpoint := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.Contains(t, checkpoint.Entries, "packed-key")
	assert.Equal(t, segmentObjects[0], checkpoint.Entries["packed-key"].SegmentPath)
}

func TestStore_LargeObjectFlushUsesDirectBlobPath(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(32))
	store.autoFlush = false
	body := strings.Repeat("x", 128)

	writer, err := store.BeginSet(ctx, "large-key", &daramjwee.Metadata{CacheTag: "large"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, store.flushPending(ctx))

	segmentObjects := listObjectNames(t, bucket, joinPath(store.prefix, "segments"))
	require.Empty(t, segmentObjects)
	blobObjects := listObjectNames(t, bucket, joinPath(store.prefix, "blobs"))
	require.Len(t, blobObjects, 1)

	checkpointObjects := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)
	checkpoint := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.Contains(t, checkpoint.Entries, "large-key")
	assert.Equal(t, blobObjects[0], checkpoint.Entries["large-key"].SegmentPath)

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(32))
	stream, meta, err := remoteOnly.GetStream(ctx, "large-key")
	require.NoError(t, err)
	defer stream.Close()

	readBody, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, body, string(readBody))
	assert.Equal(t, "large", meta.CacheTag)
}

func TestStore_LargeObjectAbortLeavesNoVisibleEntry(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(32))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "large-abort", &daramjwee.Metadata{CacheTag: "large"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, strings.Repeat("a", 128))
	require.NoError(t, err)
	require.NoError(t, writer.Abort())

	_, err = store.Stat(ctx, "large-abort")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	segmentObjects := listObjectNames(t, bucket, joinPath(store.prefix, "segments"))
	require.Empty(t, segmentObjects)
	blobObjects := listObjectNames(t, bucket, joinPath(store.prefix, "blobs"))
	require.Empty(t, blobObjects)
}
