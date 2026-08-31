package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/mrchypark/daramjwee"
)

func TestUncertainReadPreservesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := uncertainRead(ctx, errors.New("transport stopped"))
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, daramjwee.ErrReadStateUncertain)
}

func TestCheckpointEntryCacheRejectsReadStartedBeforeOpaqueTokenPublication(t *testing.T) {
	cache := newCheckpointCache(1<<20, time.Hour, time.Now)
	epoch := cache.entryReadEpoch("key")
	cache.SetEntry("key", &checkpointEntry{PublicationToken: "a-new-but-lexically-lower"}, 1)
	cache.setEntryIfEpoch("key", &checkpointEntry{PublicationToken: "z-old-but-lexically-higher"}, 1, epoch)

	entry, exists, ok := cache.GetEntry("key")
	require.True(t, ok)
	require.True(t, exists)
	require.Equal(t, "a-new-but-lexically-lower", entry.PublicationToken)
}

func TestCheckpointEntryCacheKeepsFirstConcurrentReadCompletion(t *testing.T) {
	cache := newCheckpointCache(1<<20, time.Hour, time.Now)
	epoch := cache.entryReadEpoch("key")
	cache.setEntryIfEpoch("key", &checkpointEntry{PublicationToken: "new"}, 1, epoch)
	cache.setEntryIfEpoch("key", &checkpointEntry{PublicationToken: "old"}, 1, epoch)

	entry, exists, ok := cache.GetEntry("key")
	require.True(t, ok)
	require.True(t, exists)
	require.Equal(t, "new", entry.PublicationToken)
}

func TestReadUpToSize_ReturnsFullBufferOnExactRead(t *testing.T) {
	got, err := readUpToSize(strings.NewReader("abcd"), 4)
	require.NoError(t, err)
	require.Equal(t, []byte("abcd"), got)
}

func TestReadUpToSize_TrimsShortRead(t *testing.T) {
	got, err := readUpToSize(strings.NewReader("ab"), 4)
	require.NoError(t, err)
	require.Equal(t, []byte("ab"), got)
}

func TestReadUpToSize_PropagatesReadError(t *testing.T) {
	boom := errors.New("boom")
	reader := io.MultiReader(strings.NewReader("ab"), errReader{err: boom})
	got, err := readUpToSize(reader, 4)
	require.ErrorIs(t, err, boom)
	require.Nil(t, got)
}

type errReader struct {
	err error
}

func (r errReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

func TestStore_GetStream_LocalPublishedHitReadsFromLocalSegment(t *testing.T) {
	ctx := context.Background()
	store := New(objstore.NewInMemBucket(), log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "local-read", &daramjwee.Metadata{CacheTag: "v1"})
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
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_GetStream_RemoteOnlyHitResolvesThroughShardCheckpoint(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	flushed := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	flushed.autoFlush = false

	writer, err := flushed.BeginSet(ctx, "remote-only", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "remote checkpoint body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, flushed.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	remoteOnly.autoFlush = false

	stream, meta, err := remoteOnly.GetStream(ctx, "remote-only")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "remote checkpoint body", string(body))
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_GetStream_RemoteEntryCacheAvoidsRepeatedFetch(t *testing.T) {
	ctx := context.Background()
	bucket := &countingCheckpointBucket{Bucket: objstore.NewInMemBucket()}
	flushed := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1),
	)
	flushed.autoFlush = false

	writer, err := flushed.BeginSet(ctx, "remote-cached", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "remote cached body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, flushed.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1),
		WithCheckpointCache(1<<20),
		WithCheckpointTTL(5*time.Second),
	)
	remoteOnly.autoFlush = false

	for range 2 {
		stream, _, err := remoteOnly.GetStream(ctx, "remote-cached")
		require.NoError(t, err)
		_, err = io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
	}

	assert.Equal(t, 1, bucket.remoteEntryCalls())
}

func TestStore_GetStream_RemoteEntryCacheReloadsAfterTTL(t *testing.T) {
	ctx := context.Background()
	bucket := &countingCheckpointBucket{Bucket: objstore.NewInMemBucket()}
	flushed := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1),
	)
	flushed.autoFlush = false

	writer, err := flushed.BeginSet(ctx, "remote-ttl", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "remote ttl body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, flushed.flushPending(ctx))

	now := time.Unix(1_000, 0)
	remoteOnly := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1),
		WithCheckpointCache(1<<20),
		WithCheckpointTTL(time.Second),
	)
	remoteOnly.autoFlush = false
	remoteOnly.now = func() time.Time { return now }

	stream, _, err := remoteOnly.GetStream(ctx, "remote-ttl")
	require.NoError(t, err)
	_, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, 1, bucket.remoteEntryCalls())

	now = now.Add(2 * time.Second)
	stream, _, err = remoteOnly.GetStream(ctx, "remote-ttl")
	require.NoError(t, err)
	_, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, 2, bucket.remoteEntryCalls())
}

func TestStore_PublishCheckpointRefreshesCheckpointCache(t *testing.T) {
	ctx := context.Background()
	bucket := &countingCheckpointBucket{Bucket: objstore.NewInMemBucket()}
	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithCheckpointCache(1<<20),
		WithCheckpointTTL(5*time.Second),
	)
	store.autoFlush = false

	shardID := shardForKey("checkpoint-cache-publish")
	oldRemotePath := joinPath(store.prefix, "blobs", "old")
	require.NoError(t, bucket.Upload(ctx, oldRemotePath, strings.NewReader("old body")))
	require.NoError(t, store.publishCheckpoint(ctx, shardID, map[string]checkpointEntry{
		"checkpoint-cache-publish": {
			SegmentPath: oldRemotePath,
			Offset:      0,
			Length:      int64(len("old body")),
			Metadata:    daramjwee.Metadata{CacheTag: "v1"},
		},
	}))

	entry, err := store.loadRemoteEntry(ctx, "checkpoint-cache-publish")
	require.NoError(t, err)
	assert.Equal(t, "v1", entry.Metadata.CacheTag)
	assert.Equal(t, 0, bucket.checkpointCalls())

	newRemotePath := joinPath(store.prefix, "blobs", "new")
	require.NoError(t, bucket.Upload(ctx, newRemotePath, strings.NewReader("new body")))
	require.NoError(t, store.publishCheckpoint(ctx, shardID, map[string]checkpointEntry{
		"checkpoint-cache-publish": {
			SegmentPath: newRemotePath,
			Offset:      0,
			Length:      int64(len("new body")),
			Metadata:    daramjwee.Metadata{CacheTag: "v2"},
		},
	}))

	entry, err = store.loadRemoteEntry(ctx, "checkpoint-cache-publish")
	require.NoError(t, err)
	assert.Equal(t, "v2", entry.Metadata.CacheTag)
	assert.Equal(t, 0, bucket.checkpointCalls())
}

func TestStore_GetStream_RemotePackedRecordReturnsExactLogicalObject(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	flushed := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
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
		writer, err := flushed.BeginSet(ctx, tc.key, &daramjwee.Metadata{CacheTag: tc.etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, tc.body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.NoError(t, flushed.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	remoteOnly.autoFlush = false

	stream, meta, err := remoteOnly.GetStream(ctx, keyB)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "beta remote value", string(body))
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestStore_GetStream_FallsBackToRemoteWhenSelectedLocalSegmentDisappears(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "local-disappears-remote-live", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "remote fallback body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	entry, ok := store.catalog.Get("local-disappears-remote-live")
	require.True(t, ok)
	remotePath := joinPath(store.prefix, "segments", "local-disappears-remote-live.seg")
	require.NoError(t, bucket.Upload(ctx, remotePath, strings.NewReader("remote fallback body")))
	remoteEntry := checkpointEntry{SegmentPath: remotePath, Length: int64(len("remote fallback body")), Metadata: entry.Metadata}
	require.NoError(t, store.publishCheckpoint(ctx, shardForKey("local-disappears-remote-live"), map[string]checkpointEntry{"local-disappears-remote-live": remoteEntry}))
	uploadRemoteEntryForTest(t, ctx, store, "local-disappears-remote-live", remoteEntry)
	_, updateErr := store.updateLocalEntry("local-disappears-remote-live", func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		require.True(t, exists)
		current.RemotePath = remotePath
		current.RemoteOffset = 0
		return current, true
	})
	require.NoError(t, updateErr)

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
	assert.Equal(t, "v1", meta.CacheTag)
}

func uploadRemoteEntryForTest(t *testing.T, ctx context.Context, store *Store, key string, entry checkpointEntry) {
	t.Helper()
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, store.bucket.Upload(ctx, store.remoteEntryPath(key), bytes.NewReader(data)))
}

func TestStore_GetStream_DoesNotServeOlderRemoteGenerationWhenLatestLocalDisappears(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()

	flushed := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	flushed.autoFlush = false
	writer, err := flushed.BeginSet(ctx, "local-disappears-stale-remote", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "old remote body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, flushed.flushPending(ctx))

	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false
	writer, err = store.BeginSet(ctx, "local-disappears-stale-remote", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "new local body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	origOpen := openLocalSegmentFile
	openLocalSegmentFile = func(path string) (*os.File, error) {
		require.NoError(t, os.Remove(path))
		return origOpen(path)
	}
	t.Cleanup(func() {
		openLocalSegmentFile = origOpen
	})

	_, _, err = store.GetStream(ctx, "local-disappears-stale-remote")
	require.ErrorIs(t, err, daramjwee.ErrReadStateUncertain)
}

func TestStore_LocalPublishedGenerationRejectsStaleCachedRemoteEntry(t *testing.T) {
	ctx := context.Background()
	for _, method := range []string{"get", "stat"} {
		t.Run(method, func(t *testing.T) {
			bucket := objstore.NewInMemBucket()
			store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithCheckpointCache(1<<20), WithCheckpointTTL(time.Hour))
			store.autoFlush = false
			key := "local-owner-stale-cache-" + method
			oldPath := store.blobPath(key, "old")
			newPath := store.blobPath(key, "new")
			require.NoError(t, bucket.Upload(ctx, oldPath, strings.NewReader("old")))
			require.NoError(t, bucket.Upload(ctx, newPath, strings.NewReader("new")))
			store.checkpointCache.SetEntry(key, &checkpointEntry{SegmentPath: oldPath, Length: 3, Generation: 1, PublicationToken: "0001"}, 1)
			current := checkpointEntry{SegmentPath: newPath, Length: 3, Generation: 2, PublicationToken: "0002", Metadata: daramjwee.Metadata{CacheTag: "new"}}
			data, err := json.Marshal(current)
			require.NoError(t, err)
			require.NoError(t, bucket.Upload(ctx, store.remoteEntryPath(key), bytes.NewReader(data)))
			require.NoError(t, store.catalog.Set(key, localCatalogEntry{RemotePath: newPath, Length: 3, Generation: 2, PublicationToken: "0002"}))

			if method == "get" {
				stream, meta, err := store.GetStream(ctx, key)
				require.NoError(t, err)
				defer stream.Close()
				body, err := io.ReadAll(stream)
				require.NoError(t, err)
				require.Equal(t, "new", string(body))
				require.Equal(t, "new", meta.CacheTag)
			} else {
				meta, err := store.Stat(ctx, key)
				require.NoError(t, err)
				require.Equal(t, "new", meta.CacheTag)
			}
		})
	}
}

func TestStore_InFlightOldEntryCannotOverwritePublishedTombstoneCache(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &staleEntryReadBucket{Bucket: base, started: make(chan struct{}), release: make(chan struct{})}
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithCheckpointCache(1<<20), WithCheckpointTTL(time.Hour))
	store.autoFlush = false
	key := "in-flight-entry-before-delete"
	writer, err := store.BeginSet(ctx, key, nil)
	require.NoError(t, err)
	_, err = io.WriteString(writer, "value")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, store.flushPending(ctx))
	for _, segmentPath := range localSegmentPaths(t, store.dataDir) {
		require.NoError(t, os.Remove(segmentPath))
	}

	firstDone := make(chan error, 1)
	go func() {
		stream, _, err := store.GetStream(ctx, key)
		if err == nil {
			err = stream.Close()
		}
		firstDone <- err
	}()
	<-bucket.started
	require.NoError(t, store.Delete(ctx, key))
	close(bucket.release)
	require.NoError(t, <-firstDone)

	cached, exists, ok := store.checkpointCache.GetEntry(key)
	require.True(t, ok)
	require.True(t, exists)
	require.True(t, cached.Missing)
	_, _, err = store.GetStream(ctx, key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	require.ErrorIs(t, err, daramjwee.ErrReadStateUncertain)
	_, err = store.Stat(ctx, key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	require.ErrorIs(t, err, daramjwee.ErrReadStateUncertain)
}

func TestStore_GetStream_RecheckUsesNewerLocalGenerationBeforeRemoteFallback(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()

	flushed := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	flushed.autoFlush = false
	writer, err := flushed.BeginSet(ctx, "local-recheck-newer-local", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "old remote body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, flushed.flushPending(ctx))

	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false
	writer, err = store.BeginSet(ctx, "local-recheck-newer-local", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "older local body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	newSegmentPath := filepath.Join(store.dataDir, "manual-newer-local.seg")
	require.NoError(t, os.WriteFile(newSegmentPath, []byte("newest local body"), 0o644))

	origOpen := openLocalSegmentFile
	first := true
	openLocalSegmentFile = func(path string) (*os.File, error) {
		if first {
			first = false
			require.NoError(t, os.Remove(path))
			_, updateErr := store.updateLocalEntry("local-recheck-newer-local", func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
				require.True(t, exists)
				current.SegmentPath = newSegmentPath
				current.Offset = 0
				current.Length = int64(len("newest local body"))
				current.Metadata.CacheTag = "v3"
				return current, true
			})
			require.NoError(t, updateErr)
		}
		return origOpen(path)
	}
	t.Cleanup(func() {
		openLocalSegmentFile = origOpen
	})

	stream, meta, err := store.GetStream(ctx, "local-recheck-newer-local")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "newest local body", string(body))
	assert.Equal(t, "v3", meta.CacheTag)
}

func TestStore_GetStream_FinalRecheckUsesNewestLocalGenerationBeforeRemoteFallback(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()

	flushed := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	flushed.autoFlush = false
	writer, err := flushed.BeginSet(ctx, "local-final-recheck-newer-local", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "old remote body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, flushed.flushPending(ctx))

	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false
	writer, err = store.BeginSet(ctx, "local-final-recheck-newer-local", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "first local body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	secondSegmentPath := filepath.Join(store.dataDir, "manual-second-local.seg")
	require.NoError(t, os.WriteFile(secondSegmentPath, []byte("second local body"), 0o644))
	thirdSegmentPath := filepath.Join(store.dataDir, "manual-third-local.seg")
	require.NoError(t, os.WriteFile(thirdSegmentPath, []byte("third local body"), 0o644))

	origOpen := openLocalSegmentFile
	openCount := 0
	openLocalSegmentFile = func(path string) (*os.File, error) {
		openCount++
		switch openCount {
		case 1:
			require.NoError(t, os.Remove(path))
			_, updateErr := store.updateLocalEntry("local-final-recheck-newer-local", func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
				require.True(t, exists)
				current.SegmentPath = secondSegmentPath
				current.Offset = 0
				current.Length = int64(len("second local body"))
				current.Metadata.CacheTag = "v3"
				return current, true
			})
			require.NoError(t, updateErr)
		case 2:
			require.NoError(t, os.Remove(path))
			_, updateErr := store.updateLocalEntry("local-final-recheck-newer-local", func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
				require.True(t, exists)
				current.SegmentPath = thirdSegmentPath
				current.Offset = 0
				current.Length = int64(len("third local body"))
				current.Metadata.CacheTag = "v4"
				return current, true
			})
			require.NoError(t, updateErr)
		}
		return origOpen(path)
	}
	t.Cleanup(func() {
		openLocalSegmentFile = origOpen
	})

	stream, meta, err := store.GetStream(ctx, "local-final-recheck-newer-local")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "third local body", string(body))
	assert.Equal(t, "v4", meta.CacheTag)
}

func TestPackedRemoteReader_ReturnsUnexpectedEOFOnShortPackedBlock(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
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

type cancelOnRemoteEntryMissBucket struct {
	objstore.Bucket
	cancel       context.CancelFunc
	manifestGets int
}

type staleEntryReadBucket struct {
	objstore.Bucket
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (b *staleEntryReadBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	blocked := false
	if strings.Contains(name, "/entries/") || strings.HasPrefix(name, "entries/") {
		b.once.Do(func() { blocked = true })
	}
	if !blocked {
		return b.Bucket.Get(ctx, name)
	}
	reader, err := b.Bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return nil, err
	}
	close(b.started)
	select {
	case <-b.release:
		return io.NopCloser(bytes.NewReader(data)), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *cancelOnRemoteEntryMissBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.Bucket.Get(ctx, name)
	if strings.Contains(name, "entries/") {
		b.cancel()
	}
	if strings.Contains(name, "manifests/") {
		b.manifestGets++
	}
	return reader, err
}

type countingCheckpointBucket struct {
	objstore.Bucket
	mu              sync.Mutex
	checkpointGets  int
	remoteEntryGets int
}

func (b *countingCheckpointBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.Bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(name, "/latest.json") {
		b.mu.Lock()
		b.checkpointGets++
		b.mu.Unlock()
	} else if strings.Contains(name, "/entries/") || strings.HasPrefix(name, "entries/") {
		b.mu.Lock()
		b.remoteEntryGets++
		b.mu.Unlock()
	}
	return reader, nil
}

func (b *countingCheckpointBucket) checkpointCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.checkpointGets
}

func (b *countingCheckpointBucket) remoteEntryCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remoteEntryGets
}

func TestStore_DeleteTombstoneHidesOlderPackedRecord(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "delete-tombstone", &daramjwee.Metadata{CacheTag: "v1"})
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

func TestStore_DeleteDoesNotRemoveManifestWhenTombstoneIsStale(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	for _, tc := range []struct {
		tag  string
		body string
	}{
		{tag: "v1", body: "first body"},
		{tag: "v2", body: "second body"},
	} {
		writer, err := store.BeginSet(ctx, "stale-delete-manifest", &daramjwee.Metadata{CacheTag: tc.tag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, tc.body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}

	entry, ok := store.catalog.Get("stale-delete-manifest")
	require.True(t, ok)
	require.NotEmpty(t, entry.RemotePath)
	require.NoError(t, store.publishManifest(ctx, "stale-delete-manifest", entry.RemotePath, entry.Length, &entry.Metadata))

	manifestPath := store.manifestPath("stale-delete-manifest")
	reader, err := bucket.Get(ctx, manifestPath)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	store.generationSeq.Store(0)
	require.NoError(t, store.Delete(ctx, "stale-delete-manifest"))

	reader, err = bucket.Get(ctx, manifestPath)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func TestStore_DeleteRemoteOnlyKeyPreservesOtherCheckpointEntriesInSameShard(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	flushed := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
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
		writer, err := flushed.BeginSet(ctx, tc.key, &daramjwee.Metadata{CacheTag: tc.etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, tc.body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.NoError(t, flushed.flushPending(ctx))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	remoteOnly.autoFlush = false

	require.NoError(t, remoteOnly.Delete(ctx, keyA))

	observer := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	observer.autoFlush = false

	_, _, err := observer.GetStream(ctx, keyA)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	stream, meta, err := observer.GetStream(ctx, keyB)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "beta", string(body))
	assert.Equal(t, "v2", meta.CacheTag)

	checkpointObjects := listObjectNames(t, bucket, joinPath(remoteOnly.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)
	checkpoint := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.NotContains(t, checkpoint.Entries, keyA)
	require.Contains(t, checkpoint.Entries, keyB)
}

func TestStore_GetStream_FallsBackToLegacyManifestRemoteData(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	blobPath := store.blobPath("legacy-remote", "v1")
	require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader("legacy manifest body")))
	require.NoError(t, store.publishManifest(ctx, "legacy-remote", blobPath, int64(len("legacy manifest body")), &daramjwee.Metadata{CacheTag: "legacy"}))

	reader, meta, err := store.GetStream(ctx, "legacy-remote")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "legacy manifest body", string(body))
	assert.Equal(t, "legacy", meta.CacheTag)

	stat, err := store.Stat(ctx, "legacy-remote")
	require.NoError(t, err)
	assert.Equal(t, "legacy", stat.CacheTag)
}

func TestStore_CanceledRemoteMissDoesNotFallBackToLegacyManifest(t *testing.T) {
	base := objstore.NewInMemBucket()
	seed := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	blobPath := seed.blobPath("canceled-legacy", "v1")
	require.NoError(t, base.Upload(context.Background(), blobPath, strings.NewReader("legacy")))
	require.NoError(t, seed.publishManifest(context.Background(), "canceled-legacy", blobPath, int64(len("legacy")), &daramjwee.Metadata{CacheTag: "legacy"}))

	for _, stat := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		bucket := &cancelOnRemoteEntryMissBucket{Bucket: base, cancel: cancel}
		store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
		if stat {
			_, err := store.Stat(ctx, "canceled-legacy")
			require.ErrorIs(t, err, context.Canceled)
		} else {
			_, _, err := store.GetStream(ctx, "canceled-legacy")
			require.ErrorIs(t, err, context.Canceled)
		}
		require.Zero(t, bucket.manifestGets)
	}
}

func TestStore_GetStream_FallsBackToDefaultPageSizeForLegacyPagedManifest(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
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
		Metadata: daramjwee.Metadata{CacheTag: "legacy-paged"},
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
	assert.Equal(t, "legacy-paged", meta.CacheTag)
}
