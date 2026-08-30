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
	"time"

	"github.com/go-kit/log"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/mrchypark/daramjwee"
)

func TestStore_FlushUsesFreshCheckpointBaseWhenMemoryCacheIsEnabled(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	ttl := time.Hour
	storeA := New(
		bucket,
		log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithCheckpointCache(1<<20),
		WithCheckpointTTL(ttl),
	)
	storeB := New(
		bucket,
		log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithCheckpointCache(1<<20),
		WithCheckpointTTL(ttl),
	)
	storeA.autoFlush = false
	storeB.autoFlush = false

	keyA, keyB, keyC := sameShardKeys3("flush-fresh-base")
	writeAndFlush := func(t *testing.T, store *Store, key, etag, body string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}

	writeAndFlush(t, storeA, keyA, "v1", "alpha")
	writeAndFlush(t, storeB, keyB, "v2", "beta")
	writeAndFlush(t, storeA, keyC, "v3", "gamma")

	checkpointObjects := listObjectNames(t, bucket, joinPath(storeA.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)
	checkpoint := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.Contains(t, checkpoint.Entries, keyA)
	require.Contains(t, checkpoint.Entries, keyB)
	require.Contains(t, checkpoint.Entries, keyC)
}

func TestStore_FlushUploadsSealedLocalSegmentAsRemoteSegmentObject(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "flush-key", &daramjwee.Metadata{CacheTag: "v1"})
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
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false
	keyA, keyB := sameShardKeys("packed-key")

	writeAndClose := func(key, etag, body string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	bodies := map[string]string{
		keyA: "alpha payload",
		keyB: "beta payload",
	}
	writeAndClose(keyA, "v1", bodies[keyA])
	writeAndClose(keyB, "v2", bodies[keyB])

	require.NoError(t, store.flushPending(ctx))

	segmentObjects := listObjectNames(t, bucket, joinPath(store.prefix, "segments"))
	require.Len(t, segmentObjects, 1)

	checkpointObjects := listObjectNames(t, bucket, joinPath(store.prefix, "checkpoints"))
	require.Len(t, checkpointObjects, 1)

	checkpoint := loadCheckpoint(t, bucket, checkpointObjects[0])
	require.Len(t, checkpoint.Entries, 2)
	assert.Equal(t, segmentObjects[0], checkpoint.Entries[keyA].SegmentPath)
	assert.Equal(t, segmentObjects[0], checkpoint.Entries[keyB].SegmentPath)
	assert.Equal(t, int64(0), checkpoint.Entries[keyA].Offset)
	assert.Equal(t, int64(len(bodies[keyA])), checkpoint.Entries[keyB].Offset)

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	for key, wantBody := range bodies {
		stream, metadata, err := remoteOnly.GetStream(ctx, key)
		require.NoError(t, err)
		body, err := io.ReadAll(stream)
		closeErr := stream.Close()
		require.NoError(t, errors.Join(err, closeErr))
		assert.Equal(t, wantBody, string(body), key)
		assert.Equal(t, map[string]string{keyA: "v1", keyB: "v2"}[key], metadata.CacheTag, key)
	}
}

func TestStore_FlushWritesShardScopedCheckpointWithoutKeyManifests(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	for _, key := range []string{"checkpoint-a", "checkpoint-b"} {
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: key})
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
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "retry-key", &daramjwee.Metadata{CacheTag: "v1"})
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

func TestStore_AutomaticFlushBacksOffAndResetsAfterSuccess(t *testing.T) {
	bucket := &failingUploadBucket{
		Bucket: objstore.NewInMemBucket(),
		failuresLeft: map[string]int{
			"segments/": 8,
		},
	}
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	scheduler := &manualFlushScheduler{}
	store.scheduleFlushAfter = scheduler.after

	writePendingObject(t, store, "automatic-retry", "retry payload")

	wantDelays := []time.Duration{
		flushDebounce,
		flushRetryMin,
		2 * flushRetryMin,
		4 * flushRetryMin,
		8 * flushRetryMin,
		16 * flushRetryMin,
		32 * flushRetryMin,
		flushRetryMax,
		flushRetryMax,
	}
	for _, wantDelay := range wantDelays {
		scheduled := scheduler.pop(t)
		assert.Equal(t, wantDelay, scheduled.delay)
		scheduled.run()
	}
	require.Zero(t, scheduler.len())

	entry, ok := store.catalog.Get("automatic-retry")
	require.True(t, ok)
	assert.NotEmpty(t, entry.RemotePath)

	writePendingObject(t, store, "automatic-after-success", "fresh payload")
	assert.Equal(t, flushDebounce, scheduler.pop(t).delay)
}

func TestStore_AutomaticFlushKeepsOneRetryWhenNewShardIsQueued(t *testing.T) {
	bucket := &failingUploadBucket{
		Bucket: objstore.NewInMemBucket(),
		failuresLeft: map[string]int{
			"segments/": 1,
		},
	}
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	scheduler := &manualFlushScheduler{}
	store.scheduleFlushAfter = scheduler.after

	firstKey := "retry-one-scheduler"
	secondKey := differentShardKey(firstKey)
	writePendingObject(t, store, firstKey, "first payload")

	initial := scheduler.pop(t)
	require.Equal(t, flushDebounce, initial.delay)
	initial.run()

	writePendingObject(t, store, secondKey, "second payload")
	require.Equal(t, 1, scheduler.len(), "enqueue during backoff must reuse the pending retry")

	retry := scheduler.pop(t)
	require.Equal(t, flushRetryMin, retry.delay)
	retry.run()
	require.Zero(t, scheduler.len())

	for _, key := range []string{firstKey, secondKey} {
		entry, ok := store.catalog.Get(key)
		require.True(t, ok)
		assert.NotEmpty(t, entry.RemotePath, key)
	}
}

func TestStore_AutomaticFlushStopsAfterClose(t *testing.T) {
	bucket := &failingUploadBucket{
		Bucket: objstore.NewInMemBucket(),
		failuresLeft: map[string]int{
			"segments/": 2,
		},
	}
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	scheduler := &manualFlushScheduler{}
	store.scheduleFlushAfter = scheduler.after

	writePendingObject(t, store, "close-stops-retry", "payload")
	scheduled := scheduler.pop(t)
	require.Error(t, store.Close())

	scheduled.run()
	require.Zero(t, scheduler.len())
	bucket.mu.Lock()
	remaining := bucket.failuresLeft["segments/"]
	bucket.mu.Unlock()
	assert.Equal(t, 1, remaining, "scheduled callback must not upload after Close")
}

func TestStore_CloseWaitsForAutomaticFlush(t *testing.T) {
	bucket := &failingUploadBucket{
		Bucket: objstore.NewInMemBucket(),
		failuresLeft: map[string]int{
			"segments/": 2,
		},
	}
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	scheduler := &manualFlushScheduler{}
	store.scheduleFlushAfter = scheduler.after
	autoFlushChecked := make(chan struct{})
	releaseAutoFlush := make(chan struct{})
	closeAcquireStarted := make(chan struct{})
	releaseCloseAcquire := make(chan struct{})
	var autoFlushCheckOnce sync.Once
	var closeAcquireOnce sync.Once
	store.afterAutoFlushCheck = func() {
		autoFlushCheckOnce.Do(func() { close(autoFlushChecked) })
		<-releaseAutoFlush
	}
	store.beforeFlushAcquire = func() {
		closeAcquireOnce.Do(func() { close(closeAcquireStarted) })
		<-releaseCloseAcquire
	}
	t.Cleanup(func() {
		select {
		case <-releaseAutoFlush:
		default:
			close(releaseAutoFlush)
		}
		select {
		case <-releaseCloseAcquire:
		default:
			close(releaseCloseAcquire)
		}
	})

	writePendingObject(t, store, "close-waits-for-flush", "payload")
	scheduled := scheduler.pop(t)
	callbackDone := make(chan struct{})
	go func() {
		defer close(callbackDone)
		scheduled.run()
	}()

	<-autoFlushChecked
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	<-closeAcquireStarted
	require.Zero(t, len(store.flushRun), "automatic callback must hold flushRun before checking autoFlush")

	close(releaseCloseAcquire)
	close(releaseAutoFlush)
	<-callbackDone
	require.Error(t, <-closeDone)
	require.Zero(t, scheduler.len())
	bucket.mu.Lock()
	remaining := bucket.failuresLeft["segments/"]
	bucket.mu.Unlock()
	assert.Zero(t, remaining)
}

func TestStore_DeleteRepublishesCheckpointWithoutDeletedKey(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false
	keyA, keyB := sameShardKeys("delete-checkpoint")

	for _, key := range []string{keyA, keyB} {
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: key})
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
	store := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "reclaim-after-flush", &daramjwee.Metadata{CacheTag: "v1"})
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
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_FlushDefersLocalSegmentReclaimUntilReaderCloses(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "reclaim-after-reader-close", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "flush payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "reclaim-after-reader-close")
	require.NoError(t, err)
	require.Equal(t, "v1", meta.CacheTag)

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
	assert.Equal(t, "v1", remoteMeta.CacheTag)
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

func sameShardKeys3(base string) (string, string, string) {
	shard := shardForKey(base)
	keys := []string{base}
	for i := 1; len(keys) < 3 && i < 4096; i++ {
		candidate := base + "-" + strconv.Itoa(i)
		if shardForKey(candidate) == shard {
			keys = append(keys, candidate)
		}
	}
	if len(keys) != 3 {
		panic("failed to find same-shard keys")
	}
	return keys[0], keys[1], keys[2]
}

func differentShardKey(base string) string {
	shard := shardForKey(base)
	for i := 1; i < 2048; i++ {
		candidate := base + "-" + strconv.Itoa(i)
		if shardForKey(candidate) != shard {
			return candidate
		}
	}
	panic("failed to find different-shard key")
}

func writePendingObject(t *testing.T, store *Store, key, body string) {
	t.Helper()

	writer, err := store.BeginSet(context.Background(), key, &daramjwee.Metadata{CacheTag: key})
	require.NoError(t, err)
	_, err = io.WriteString(writer, body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

type scheduledFlush struct {
	delay time.Duration
	run   func()
}

type manualFlushScheduler struct {
	mu        sync.Mutex
	scheduled []scheduledFlush
}

func (s *manualFlushScheduler) after(delay time.Duration, run func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled = append(s.scheduled, scheduledFlush{delay: delay, run: run})
}

func (s *manualFlushScheduler) pop(t *testing.T) scheduledFlush {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotEmpty(t, s.scheduled)
	next := s.scheduled[0]
	s.scheduled = s.scheduled[1:]
	return next
}

func (s *manualFlushScheduler) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.scheduled)
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
