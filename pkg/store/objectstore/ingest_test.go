package objectstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestStore_BeginSetIsNotVisibleBeforeClose(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "local-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "hello, local ingest")
	require.NoError(t, err)

	_, err = store.Stat(ctx, "local-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "local-key")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "hello, local ingest", string(body))
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_AbortLeavesNoVisibleLocalEntry(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "abort-local", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "partial")
	require.NoError(t, err)
	require.NoError(t, writer.Abort())

	_, err = store.Stat(ctx, "abort-local")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_ReopenRecoversPublishedLocalEntries(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()

	store := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "recover-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "recoverable payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reopened := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	reopened.autoFlush = false

	stream, meta, err := reopened.GetStream(ctx, "recover-key")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "recoverable payload", string(body))
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_MissingLocalSegmentDoesNotRemainVisible(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "missing-segment", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.NoError(t, os.Remove(segments[0]))

	_, err = store.Stat(ctx, "missing-segment")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	_, _, err = store.GetStream(ctx, "missing-segment")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_MissingLocalSegmentDoesNotFallBackToOlderRemoteGeneration(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	oldBlobPath := store.blobPath("missing-segment-remote-fallback", "remote-v1")
	require.NoError(t, bucket.Upload(ctx, oldBlobPath, strings.NewReader("remote-old")))
	require.NoError(t, store.publishManifest(ctx, "missing-segment-remote-fallback", oldBlobPath, int64(len("remote-old")), &daramjwee.Metadata{CacheTag: "remote-v1"}))

	writer, err := store.BeginSet(ctx, "missing-segment-remote-fallback", &daramjwee.Metadata{CacheTag: "local-v2"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "local-new")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.NoError(t, os.Remove(segments[0]))

	_, statErr := store.Stat(ctx, "missing-segment-remote-fallback")
	require.ErrorIs(t, statErr, daramjwee.ErrNotFound)

	_, _, getErr := store.GetStream(ctx, "missing-segment-remote-fallback")
	require.ErrorIs(t, getErr, daramjwee.ErrNotFound)

	reopened := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	reopened.autoFlush = false
	_, reopenErr := reopened.Stat(ctx, "missing-segment-remote-fallback")
	require.ErrorIs(t, reopenErr, daramjwee.ErrNotFound)
}

func TestStore_MissingLocalSegmentFallsBackToCurrentRemoteGeneration(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "missing-segment-remote-live", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "remote-current")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	entry, ok := store.catalog.Get("missing-segment-remote-live")
	require.True(t, ok)

	remotePath := store.blobPath("missing-segment-remote-live", "remote-v1")
	require.NoError(t, bucket.Upload(ctx, remotePath, strings.NewReader("remote-current")))
	require.NoError(t, store.publishCheckpoint(ctx, shardForKey("missing-segment-remote-live"), map[string]checkpointEntry{
		"missing-segment-remote-live": {
			SegmentPath: remotePath,
			Offset:      0,
			Length:      int64(len("remote-current")),
			Metadata:    entry.Metadata,
		},
	}))
	require.NoError(t, store.updateLocalEntry("missing-segment-remote-live", func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		require.True(t, exists)
		current.RemotePath = remotePath
		current.RemoteOffset = 0
		return current, true
	}))

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.NoError(t, os.Remove(segments[0]))

	stream, meta, err := store.GetStream(ctx, "missing-segment-remote-live")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "remote-current", string(body))
	assert.Equal(t, "v1", meta.CacheTag)

	reopened := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	reopened.autoFlush = false

	reopenedStream, reopenedMeta, err := reopened.GetStream(ctx, "missing-segment-remote-live")
	require.NoError(t, err)
	defer reopenedStream.Close()

	reopenedBody, err := io.ReadAll(reopenedStream)
	require.NoError(t, err)
	assert.Equal(t, "remote-current", string(reopenedBody))
	assert.Equal(t, "v1", reopenedMeta.CacheTag)
}

func TestStore_OverwriteRemovesPreviousPublishedLocalSegment(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	write := func(key, etag, body string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	write("overwrite-local", "v1", "old")
	write("overwrite-local", "v2", "new")

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
}

func TestStore_FlushUpdateSkipsKeyWhenNewerLocalEntryWasPublished(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	write := func(key, etag, body string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	write("flush-stale-key", "v1", "old")
	expectedEntries := store.catalog.Entries()
	expected := expectedEntries["flush-stale-key"]

	write("flush-stale-key", "v2", "new")

	staleUpdate := expected
	staleUpdate.RemotePath = "remote/old.seg"
	staleUpdate.RemoteOffset = 0

	require.NoError(t, store.commitFlushUpdates(expectedEntries, map[string]localCatalogEntry{
		"flush-stale-key": staleUpdate,
	}))

	current, ok := store.catalog.Get("flush-stale-key")
	require.True(t, ok)
	assert.Equal(t, "v2", current.Metadata.CacheTag)
	assert.Empty(t, current.RemotePath)

	stream, meta, err := store.GetStream(ctx, "flush-stale-key")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "new", string(body))
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestStore_DeleteRemovesPublishedLocalSegment(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "delete-local-segment", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	require.NoError(t, store.Delete(ctx, "delete-local-segment"))

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Empty(t, segments)
}

func TestStore_OverwriteDefersPreviousSegmentRemovalUntilReaderCloses(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	write := func(key, etag, body string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	write("overwrite-reader", "v1", "old")

	oldStream, oldMeta, err := store.GetStream(ctx, "overwrite-reader")
	require.NoError(t, err)
	require.Equal(t, "v1", oldMeta.CacheTag)

	segmentsBefore, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Len(t, segmentsBefore, 1)
	oldSegment := segmentsBefore[0]

	write("overwrite-reader", "v2", "new")

	_, err = os.Stat(oldSegment)
	require.NoError(t, err)

	newStream, newMeta, err := store.GetStream(ctx, "overwrite-reader")
	require.NoError(t, err)
	defer newStream.Close()
	require.Equal(t, "v2", newMeta.CacheTag)

	newBody, err := io.ReadAll(newStream)
	require.NoError(t, err)
	assert.Equal(t, "new", string(newBody))

	oldBody, err := io.ReadAll(oldStream)
	require.NoError(t, err)
	assert.Equal(t, "old", string(oldBody))
	require.NoError(t, oldStream.Close())

	_, err = os.Stat(oldSegment)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStore_DeleteDefersSegmentRemovalUntilReaderCloses(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "delete-reader", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "delete-reader")
	require.NoError(t, err)
	require.Equal(t, "v1", meta.CacheTag)

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
	segmentPath := segments[0]

	require.NoError(t, store.Delete(ctx, "delete-reader"))

	_, err = os.Stat(segmentPath)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(body))
	require.NoError(t, stream.Close())

	_, err = os.Stat(segmentPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStore_ReopenSweepsOrphanedLocalSegments(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "live-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "live payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	orphanPath := filepath.Join(dataDir, "ingest", "sealed", shardForKey("orphan-key"), "orphan.seg")
	require.NoError(t, os.MkdirAll(filepath.Dir(orphanPath), 0o755))
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan"), 0o644))

	reopened := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	reopened.autoFlush = false
	require.NoError(t, reopened.ensureReady())

	_, err = os.Stat(orphanPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	stream, meta, err := reopened.GetStream(ctx, "live-key")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "live payload", string(body))
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_ReopenFailsWhenRecoveryCannotPersistCatalogRepair(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "recover-failure", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	segments, err := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.NoError(t, os.Remove(segments[0]))

	catalogDir := filepath.Join(dataDir, "catalog")
	require.NoError(t, os.Chmod(catalogDir, 0o555))
	t.Cleanup(func() {
		_ = os.Chmod(catalogDir, 0o755)
	})

	reopened := New(
		bucket,
		log.NewNopLogger(),
		WithDir(dataDir),
	)

	_, err = reopened.Stat(ctx, "recover-failure")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to recover local objectstore state")
}

func TestStore_CloseFailureDoesNotLeaveSealedSegmentVisible(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithDir(dataDir),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "close-failure", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)

	catalogDir := filepath.Join(dataDir, "catalog")
	require.NoError(t, os.Chmod(catalogDir, 0o555))
	t.Cleanup(func() {
		_ = os.Chmod(catalogDir, 0o755)
	})

	err = writer.Close()
	require.Error(t, err)

	_, statErr := store.Stat(ctx, "close-failure")
	require.ErrorIs(t, statErr, daramjwee.ErrNotFound)

	segments, globErr := filepath.Glob(filepath.Join(dataDir, "ingest", "sealed", "*", "*.seg"))
	require.NoError(t, globErr)
	require.Empty(t, segments)
}
