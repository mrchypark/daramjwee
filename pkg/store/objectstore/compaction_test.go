package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/mrchypark/daramjwee"
)

func TestStore_CompactWaitsForFlushCheckpointPublication(t *testing.T) {
	store, bucket, baseBucket, flushDone := startCheckpointBlockedFlush(t, "compact-during-flush")
	require.Len(t, listObjectNames(t, baseBucket, joinPath(store.prefix, "segments")), 1)

	waitObserved := make(chan struct{})
	compactCtx := &waitObservedContext{Context: context.Background(), observed: waitObserved}
	type compactResult struct {
		stats SweepStats
		err   error
	}
	compactDone := make(chan compactResult, 1)
	go func() {
		stats, err := store.Compact(compactCtx, 0)
		compactDone <- compactResult{stats: stats, err: err}
	}()

	select {
	case <-waitObserved:
	case <-bucket.compactRemoteStarted:
		bucket.releaseCompactRemote()
		result := <-compactDone
		bucket.releaseCheckpoint()
		require.NoError(t, <-flushDone)
		require.NoError(t, result.err)
		t.Fatal("compaction reached and completed remote traversal before checkpoint publication")
	case <-time.After(5 * time.Second):
		t.Fatal("compaction reached neither the publication gate nor remote traversal")
	}

	select {
	case result := <-compactDone:
		t.Fatalf("compaction completed before checkpoint publication: %v", result.err)
	default:
	}
	select {
	case <-bucket.compactRemoteStarted:
		t.Fatal("compaction reached remote traversal before checkpoint publication")
	default:
	}

	bucket.releaseCheckpoint()
	require.NoError(t, <-flushDone)
	select {
	case <-bucket.compactRemoteStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not reach remote traversal after checkpoint publication")
	}
	bucket.releaseCompactRemote()

	var result compactResult
	select {
	case result = <-compactDone:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish after checkpoint publication")
	}
	require.NoError(t, result.err)

	remoteOnly := New(baseBucket, log.NewNopLogger(), WithDir(t.TempDir()))
	stream, meta, err := remoteOnly.GetStream(context.Background(), "compact-during-flush")
	require.NoError(t, err)
	defer stream.Close()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "flush body", string(body))
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestStore_CompactProtectsAnotherInstanceUpload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := objstore.NewInMemBucket()
	uploadBucket := &blockingPayloadUploadBucket{
		Bucket:   base,
		uploaded: make(chan struct{}),
		release:  make(chan struct{}),
	}
	uploader := New(uploadBucket, log.NewNopLogger(), WithDir(t.TempDir()))
	uploader.autoFlush = false
	compactBucket := &candidateScanGateBucket{
		Bucket:  base,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	compactor := New(compactBucket, log.NewNopLogger(), WithDir(t.TempDir()))
	uploadNow := time.Unix(20_000, 0)
	uploader.now = func() time.Time { return uploadNow }
	compactor.now = func() time.Time { return uploadNow.Add(2 * time.Hour) }
	compactor.gcGrace = time.Nanosecond

	writer, err := uploader.BeginSet(ctx, "cross-instance-upload", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	compactDone := make(chan error, 1)
	go func() {
		_, err := compactor.Compact(ctx, 0)
		compactDone <- err
	}()
	select {
	case <-compactBucket.started:
	case <-ctx.Done():
		t.Fatal("compaction did not reach candidate scan")
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- uploader.flushPending(ctx) }()
	select {
	case <-uploadBucket.uploaded:
	case <-ctx.Done():
		t.Fatal("payload upload did not reach the publication gate")
	}

	compactBucket.releaseScan()
	require.NoError(t, <-compactDone)
	uploadBucket.releaseUpload()
	require.NoError(t, <-flushDone)

	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	stream, _, err := remote.GetStream(ctx, "cross-instance-upload")
	require.NoError(t, err)
	defer stream.Close()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(body))
	assert.Empty(t, listObjectNames(t, base, joinPath(uploader.prefix, "uploads")))
}

func TestStore_CompactDropsStaleLocalRemotePath(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	storeA := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	storeB := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	storeA.autoFlush = false
	storeB.autoFlush = false

	write := func(store *Store, value string) {
		writer, err := store.BeginSet(ctx, "stale-local", &daramjwee.Metadata{CacheTag: value})
		require.NoError(t, err)
		_, err = io.WriteString(writer, value)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}
	write(storeA, "version-one")
	blobs := listObjectNames(t, bucket, storeA.blobRoot())
	require.Len(t, blobs, 1)
	oldPath := blobs[0]
	write(storeB, "version-two")

	_, err := storeA.Compact(ctx, 0)
	require.NoError(t, err)
	exists, err := bucket.Exists(ctx, oldPath)
	require.NoError(t, err)
	require.False(t, exists)

	remote := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	stream, _, err := remote.GetStream(ctx, "stale-local")
	require.NoError(t, err)
	defer stream.Close()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "version-two", string(body))
}

func TestStore_CompactClearsCompletedIntentAfterDeleteFailure(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &failFirstIntentDeleteBucket{Bucket: base}
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	store.autoFlush = false

	write := func(value string) {
		writer, err := store.BeginSet(ctx, "intent-cleanup", &daramjwee.Metadata{CacheTag: value})
		require.NoError(t, err)
		_, err = io.WriteString(writer, value)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}
	write("version-one")
	blobs := listObjectNames(t, base, store.blobRoot())
	require.Len(t, blobs, 1)
	oldPath := blobs[0]
	require.Len(t, listObjectNames(t, base, joinPath(store.prefix, "uploads")), 1)
	write("version-two")

	_, err := store.Compact(ctx, 0)
	require.NoError(t, err)
	exists, err := base.Exists(ctx, oldPath)
	require.NoError(t, err)
	require.False(t, exists)
	assert.Empty(t, listObjectNames(t, base, joinPath(store.prefix, "uploads")))
}

func TestStore_CompactClearsAbandonedUploadIntent(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &failFirstPayloadAfterUploadBucket{Bucket: base}
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	store.autoFlush = false
	writer, err := store.BeginSet(ctx, "abandoned-intent", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.Error(t, store.flushPending(ctx))
	blobs := listObjectNames(t, base, store.blobRoot())
	require.Len(t, blobs, 1)
	abandonedPath := blobs[0]
	require.NoError(t, store.flushPending(ctx))

	_, err = store.Compact(ctx, 0)
	require.NoError(t, err)
	exists, err := base.Exists(ctx, abandonedPath)
	require.NoError(t, err)
	require.False(t, exists)
	assert.Empty(t, listObjectNames(t, base, joinPath(store.prefix, "uploads")))
}

type failFirstIntentDeleteBucket struct {
	objstore.Bucket
	once sync.Once
}

type failFirstPayloadAfterUploadBucket struct {
	objstore.Bucket
	once sync.Once
}

func (b *failFirstPayloadAfterUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, r, opts...); err != nil {
		return err
	}
	failed := false
	if strings.Contains(name, "/blobs/") || strings.HasPrefix(name, "blobs/") {
		b.once.Do(func() { failed = true })
	}
	if failed {
		return errors.New("payload outcome unknown")
	}
	return nil
}

func (b *failFirstIntentDeleteBucket) Delete(ctx context.Context, name string) error {
	failed := false
	if strings.Contains(name, "/uploads/") || strings.HasPrefix(name, "uploads/") {
		b.once.Do(func() { failed = true })
	}
	if failed {
		return errors.New("intent delete failed")
	}
	return b.Bucket.Delete(ctx, name)
}

type candidateScanGateBucket struct {
	objstore.Bucket
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (b *candidateScanGateBucket) Iter(ctx context.Context, dir string, f func(name string) error, opts ...objstore.IterOption) error {
	blocked := false
	if strings.Contains(dir, "/segments/") || strings.HasPrefix(dir, "segments/") {
		b.once.Do(func() {
			blocked = true
			close(b.started)
		})
	}
	if blocked {
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Iter(ctx, dir, f, opts...)
}

func (b *candidateScanGateBucket) releaseScan() {
	b.releaseOnce.Do(func() { close(b.release) })
}

type blockingPayloadUploadBucket struct {
	objstore.Bucket
	uploaded    chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (b *blockingPayloadUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, r, opts...); err != nil {
		return err
	}
	blocked := false
	if strings.Contains(name, "/segments/") || strings.Contains(name, "/blobs/") || strings.HasPrefix(name, "segments/") || strings.HasPrefix(name, "blobs/") {
		b.once.Do(func() {
			blocked = true
			close(b.uploaded)
		})
	}
	if blocked {
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *blockingPayloadUploadBucket) releaseUpload() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func TestStore_RemotePublicationWaitHonorsContext(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Store) error
		want error
	}{
		{
			name: "compact deadline",
			run: func(ctx context.Context, store *Store) error {
				_, err := store.Compact(ctx, 0)
				return err
			},
			want: context.DeadlineExceeded,
		},
		{name: "flush cancellation", run: func(ctx context.Context, store *Store) error {
			return store.flushPending(ctx)
		}, want: context.Canceled},
		{name: "delete cancellation", run: func(ctx context.Context, store *Store) error {
			return store.Delete(ctx, "context-wait")
		}, want: context.Canceled},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, bucket, _, flushDone := startCheckpointBlockedFlush(t, "context-wait")
			var ctx context.Context
			var cancel context.CancelFunc
			if errors.Is(tc.want, context.DeadlineExceeded) {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			} else {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			}
			defer cancel()

			done := make(chan error, 1)
			go func() { done <- tc.run(ctx, store) }()
			select {
			case err := <-done:
				require.ErrorIs(t, err, tc.want)
			case <-time.After(5 * time.Second):
				t.Fatalf("operation did not honor context while publication was blocked")
			}

			bucket.releaseCheckpoint()
			require.NoError(t, <-flushDone)
		})
	}
}

func TestStore_CompactReclaimsSupersededRemoteObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(32),
	)
	store.autoFlush = false

	writeAndFlush := func(body, etag string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, "compact-large", &daramjwee.Metadata{CacheTag: etag})
		require.NoError(t, err)
		_, err = io.WriteString(writer, body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}

	writeAndFlush(strings.Repeat("a", 128), "v1")
	v1, ok := store.catalog.Get("compact-large")
	require.True(t, ok)
	writeAndFlush(strings.Repeat("b", 128), "v2")
	require.NoError(t, store.publishCheckpoint(ctx, shardForKey("compact-large"), map[string]checkpointEntry{
		"compact-large": {
			SegmentPath: v1.RemotePath,
			Offset:      v1.RemoteOffset,
			Length:      v1.Length,
			Generation:  v1.Generation,
			Metadata:    v1.Metadata,
		},
	}))

	before := listObjectNames(t, bucket, joinPath(store.prefix, "blobs"))
	require.Len(t, before, 2)

	stats, err := store.Compact(ctx, 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, stats.Deleted, 1)

	after := listObjectNames(t, bucket, joinPath(store.prefix, "blobs"))
	require.Len(t, after, 1)

	remoteOnly := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(32),
	)
	stream, meta, err := remoteOnly.GetStream(ctx, "compact-large")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("b", 128), string(body))
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestStore_CompactPrunesStaleCheckpointObjects(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "checkpoint-key", &daramjwee.Metadata{CacheTag: "v1"})
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

func TestStore_CompactKeepsLegacyManifestBackedBlobReachable(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))

	blobPath := store.blobPath("legacy-compact", "legacy-v1")
	require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader("legacy body")))
	require.NoError(t, store.publishManifest(ctx, "legacy-compact", blobPath, int64(len("legacy body")), &daramjwee.Metadata{CacheTag: "legacy"}))

	stats, err := store.Compact(ctx, 0)
	require.NoError(t, err)
	assert.Zero(t, stats.Deleted)

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	stream, meta, err := remoteOnly.GetStream(ctx, "legacy-compact")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "legacy body", string(body))
	assert.Equal(t, "legacy", meta.CacheTag)
}

func TestStore_CompactFailsClosedOnCorruptManifest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload func(string) string
	}{
		{name: "syntax", payload: func(string) string { return "{" }},
		{name: "null", payload: func(string) string { return "null" }},
		{name: "missing path", payload: func(string) string { return `{}` }},
		{name: "trailing garbage", payload: func(path string) string {
			return fmt.Sprintf(`{"blob_path":%q} trailing`, path)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))

			blobPath := store.blobPath("corrupt-manifest", store.nextVersion())
			require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader("payload")))
			require.NoError(t, bucket.Upload(ctx, store.manifestPath("corrupt-manifest"), strings.NewReader(tc.payload(blobPath))))

			_, err := store.Compact(ctx, 0)
			require.Error(t, err)
			exists, err := bucket.Exists(ctx, blobPath)
			require.NoError(t, err)
			require.True(t, exists)
		})
	}
}

func TestStore_CompactFailsClosedOnCorruptCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{name: "syntax", payload: "{"},
		{name: "null", payload: "null"},
		{name: "missing entries", payload: `{}`},
		{name: "missing segment path", payload: `{"entries":{"key":{}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))

			segmentPath := joinPath(store.prefix, "segments", store.nextVersion()+".seg")
			checkpointPath := joinPath(store.prefix, "checkpoints", "00", "latest.json")
			require.NoError(t, bucket.Upload(ctx, segmentPath, strings.NewReader("payload")))
			require.NoError(t, bucket.Upload(ctx, checkpointPath, strings.NewReader(tc.payload)))

			_, err := store.Compact(ctx, 0)
			require.Error(t, err)
			exists, err := bucket.Exists(ctx, segmentPath)
			require.NoError(t, err)
			require.True(t, exists)
		})
	}
}

func TestStore_ReclaimAutomaticallySchedulesFlushForPublishedUnflushedLocalEntriesAfterReopen(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	dataDir := t.TempDir()

	store := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "requeue-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "requeue-body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reopened := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, remoteOnly.Close()) })
	require.Eventually(t, func() bool {
		stream, meta, err := remoteOnly.GetStream(ctx, "requeue-key")
		if err != nil {
			return false
		}
		defer stream.Close()

		body, readErr := io.ReadAll(stream)
		return readErr == nil && string(body) == "requeue-body" && meta.CacheTag == "v1"
	}, time.Second, 20*time.Millisecond)

	reopened.flushMu.Lock()
	pending := len(reopened.pendingShards)
	reopened.flushMu.Unlock()
	assert.Zero(t, pending)
}

type blockingCheckpointUploadBucket struct {
	objstore.Bucket
	checkpointUploadStarted chan struct{}
	checkpointUploadRelease chan struct{}
	compactRemoteStarted    chan struct{}
	compactRemoteRelease    chan struct{}
	checkpointOnce          sync.Once
	checkpointReleaseOnce   sync.Once
	compactRemoteOnce       sync.Once
	compactReleaseOnce      sync.Once
}

func (b *blockingCheckpointUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	block := false
	if strings.HasSuffix(name, "/latest.json") {
		b.checkpointOnce.Do(func() {
			block = true
			close(b.checkpointUploadStarted)
		})
	}
	if block {
		select {
		case <-b.checkpointUploadRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func (b *blockingCheckpointUploadBucket) Iter(ctx context.Context, dir string, f func(name string) error, opts ...objstore.IterOption) error {
	block := false
	b.compactRemoteOnce.Do(func() {
		block = true
		close(b.compactRemoteStarted)
	})
	if block {
		select {
		case <-b.compactRemoteRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Iter(ctx, dir, f, opts...)
}

func (b *blockingCheckpointUploadBucket) releaseCheckpoint() {
	b.checkpointReleaseOnce.Do(func() { close(b.checkpointUploadRelease) })
}

func (b *blockingCheckpointUploadBucket) releaseCompactRemote() {
	b.compactReleaseOnce.Do(func() { close(b.compactRemoteRelease) })
}

func startCheckpointBlockedFlush(t *testing.T, key string) (*Store, *blockingCheckpointUploadBucket, objstore.Bucket, <-chan error) {
	t.Helper()
	ctx := context.Background()
	baseBucket := objstore.NewInMemBucket()
	bucket := &blockingCheckpointUploadBucket{
		Bucket:                  baseBucket,
		checkpointUploadStarted: make(chan struct{}),
		checkpointUploadRelease: make(chan struct{}),
		compactRemoteStarted:    make(chan struct{}),
		compactRemoteRelease:    make(chan struct{}),
	}
	t.Cleanup(func() {
		bucket.releaseCheckpoint()
		bucket.releaseCompactRemote()
	})
	store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "flush body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	flushDone := make(chan error, 1)
	go func() { flushDone <- store.flushPending(ctx) }()
	select {
	case <-bucket.checkpointUploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not reach checkpoint publication")
	}
	return store, bucket, baseBucket, flushDone
}

type waitObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *waitObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}
