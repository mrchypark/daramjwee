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

func TestStore_CompactReclaimsSupersededRemoteObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(32),
	)
	store.autoFlush = false

	writeAndFlush := func(body, etag string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, "compact-large", &daramjwee.Metadata{ETag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}

	writeAndFlush(strings.Repeat("a", 128), "v1")
	writeAndFlush(strings.Repeat("b", 128), "v2")

	before := listObjectNames(t, bucket, joinPath(store.prefix, "blobs"))
	require.Len(t, before, 2)

	stats, err := store.Compact(ctx, 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, stats.Deleted, 1)

	after := listObjectNames(t, bucket, joinPath(store.prefix, "blobs"))
	require.Len(t, after, 1)

	remoteOnly := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(32),
	)
	stream, meta, err := remoteOnly.GetStream(ctx, "compact-large")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("b", 128), string(body))
	assert.Equal(t, "v2", meta.ETag)
}

func TestStore_CompactPrunesStaleCheckpointObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "checkpoint-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "checkpoint-body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, store.flushPending(ctx))

	shardID := shardForKey("checkpoint-key")
	stalePath := joinPath(store.prefix, "checkpoints", shardID, "stale-1.json")
	require.NoError(t, bucket.Upload(ctx, stalePath, strings.NewReader(`{"entries":{}}`)))

	stats, err := store.Compact(ctx, 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, stats.Deleted, 1)

	checkpoints := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpoints, 1)
	assert.Equal(t, joinPath(store.prefix, "checkpoints", shardID, "latest.json"), checkpoints[0])
}

func TestStore_ReclaimRequeuesPublishedUnflushedLocalEntriesAfterReopen(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	dataDir := t.TempDir()

	store := New(bucket, log.NewNopLogger(), WithDataDir(dataDir))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "requeue-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "requeue-body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reopened := New(bucket, log.NewNopLogger(), WithDataDir(dataDir))
	reopened.autoFlush = false
	require.NoError(t, reopened.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	stream, meta, err := remoteOnly.GetStream(ctx, "requeue-key")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "requeue-body", string(body))
	assert.Equal(t, "v1", meta.ETag)
}
