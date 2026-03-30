package objectstore

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestStore_FlushUploadsSealedLocalSegmentAsRemoteSegmentObject(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "flush-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "flush payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	require.NoError(t, store.flushPending(ctx))

	segmentObjects := listObjectNames(t, bucket, joinPath(store.prefix, "segments"))
	require.Len(t, segmentObjects, 1)

	checkpointObjects := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)

	manifestObjects := listObjectNames(t, bucket, joinPath(store.prefix, "manifests"))
	require.Empty(t, manifestObjects)
}

func TestStore_FlushPacksMultipleKeysIntoSingleRemoteSegment(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false
	keyA, keyB := sameShardKeys("packed-key")

	writeAndClose := func(key, etag, body string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{ETag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	writeAndClose(keyA, "v1", "alpha payload")
	writeAndClose(keyB, "v2", "beta payload")

	require.NoError(t, store.flushPending(ctx))

	segmentObjects := listObjectNames(t, bucket, joinPath(store.prefix, "segments"))
	require.Len(t, segmentObjects, 1)

	checkpointObjects := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)

	checkpoint := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.Len(t, checkpoint.Entries, 2)
	assert.Equal(t, segmentObjects[0], checkpoint.Entries[keyA].SegmentPath)
	assert.Equal(t, segmentObjects[0], checkpoint.Entries[keyB].SegmentPath)
}

func TestStore_FlushWritesShardScopedCheckpointWithoutKeyManifests(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	for _, key := range []string{"checkpoint-a", "checkpoint-b"} {
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{ETag: key})
		require.NoError(t, err)
		_, err = io.WriteString(writer, key+"-body")
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	require.NoError(t, store.flushPending(ctx))

	checkpointObjects := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 2)
	for _, objectName := range checkpointObjects {
		assert.True(t, strings.Contains(objectName, "/latest.json"), objectName)
	}

	manifestObjects := listObjectNames(t, bucket, joinPath(store.prefix, "manifests"))
	require.Empty(t, manifestObjects)
}

func TestStore_FlushFailureKeepsShardPendingForRetry(t *testing.T) {
	ctx := context.Background()
	bucket := &failingUploadBucket{
		Bucket: objstore.NewInMemBucket(),
		failuresLeft: map[string]int{
			"segments/": 1,
		},
	}
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "retry-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "retry payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	err = store.flushPending(ctx)
	require.Error(t, err)

	err = store.flushPending(ctx)
	require.NoError(t, err)

	segmentObjects := listObjectNames(t, bucket, joinPath(store.prefix, "segments"))
	require.Len(t, segmentObjects, 1)
}

func TestStore_DeleteRepublishesCheckpointWithoutDeletedKey(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(t.TempDir()))
	store.autoFlush = false
	keyA, keyB := sameShardKeys("delete-checkpoint")

	for _, key := range []string{keyA, keyB} {
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{ETag: key})
		require.NoError(t, err)
		_, err = io.WriteString(writer, key+"-body")
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.NoError(t, store.flushPending(ctx))

	checkpointObjects := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)
	before := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.Contains(t, before.Entries, keyA)
	require.Contains(t, before.Entries, keyB)

	require.NoError(t, store.Delete(ctx, keyA))
	require.NoError(t, store.flushPending(ctx))

	after := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.NotContains(t, after.Entries, keyA)
	require.Contains(t, after.Entries, keyB)
}

func TestStore_FlushReclaimsLocalSegmentAfterRemoteCommit(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(dataDir))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "reclaim-after-flush", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "flush payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	require.Len(t, localSegmentPaths(t, dataDir), 1)
	require.NoError(t, store.flushPending(ctx))
	require.Empty(t, localSegmentPaths(t, dataDir))

	entry, ok := store.catalog.Get("reclaim-after-flush")
	require.True(t, ok)
	assert.Empty(t, entry.SegmentPath)
	assert.NotEmpty(t, entry.RemotePath)

	stream, meta, err := store.GetStream(ctx, "reclaim-after-flush")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "flush payload", string(body))
	assert.Equal(t, "v1", meta.ETag)
}

func TestStore_FlushDefersLocalSegmentReclaimUntilReaderCloses(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDataDir(dataDir))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "reclaim-after-reader-close", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "flush payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "reclaim-after-reader-close")
	require.NoError(t, err)
	require.Equal(t, "v1", meta.ETag)

	require.Len(t, localSegmentPaths(t, dataDir), 1)
	require.NoError(t, store.flushPending(ctx))
	require.Len(t, localSegmentPaths(t, dataDir), 1)

	require.NoError(t, stream.Close())
	require.Empty(t, localSegmentPaths(t, dataDir))

	remoteStream, remoteMeta, err := store.GetStream(ctx, "reclaim-after-reader-close")
	require.NoError(t, err)
	defer remoteStream.Close()

	body, err := io.ReadAll(remoteStream)
	require.NoError(t, err)
	assert.Equal(t, "flush payload", string(body))
	assert.Equal(t, "v1", remoteMeta.ETag)
}

func localSegmentPaths(t *testing.T, dataDir string) []string {
	t.Helper()

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	slices.Sort(segments)
	return segments
}

func listObjectNames(t *testing.T, bucket objstore.Bucket, prefix string) []string {
	t.Helper()

	var names []string
	collectObjectNames(t, bucket, prefix, &names)
	slices.Sort(names)
	return names
}

func collectObjectNames(t *testing.T, bucket objstore.Bucket, prefix string, names *[]string) {
	t.Helper()

	err := bucket.Iter(context.Background(), prefix, func(name string) error {
		if strings.HasSuffix(name, "/") {
			collectObjectNames(t, bucket, name, names)
			return nil
		}
		*names = append(*names, name)
		return nil
	})
	require.NoError(t, err)
}

func sameShardKeys(base string) (string, string) {
	shard := shardForKey(base)
	for i := 1; i < 2048; i++ {
		candidate := base + "-" + strconv.Itoa(i)
		if shardForKey(candidate) == shard {
			return base, candidate
		}
	}
	panic("failed to find same-shard key")
}

func loadCheckpoint(t *testing.T, bucket objstore.Bucket, objectName string) checkpoint {
	t.Helper()

	reader, err := bucket.Get(context.Background(), objectName)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	var cp checkpoint
	require.NoError(t, json.Unmarshal(data, &cp))
	return cp
}

type failingUploadBucket struct {
	objstore.Bucket
	mu           sync.Mutex
	failuresLeft map[string]int
}

func (b *failingUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	for prefix, remaining := range b.failuresLeft {
		if strings.HasPrefix(name, prefix) && remaining > 0 {
			b.failuresLeft[prefix] = remaining - 1
			b.mu.Unlock()
			_, _ = io.Copy(io.Discard, r)
			return errors.New("injected upload failure")
		}
	}
	b.mu.Unlock()
	return b.Bucket.Upload(ctx, name, r, opts...)
}
