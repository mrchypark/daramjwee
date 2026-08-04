package objectstore

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
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
			if tc.want == context.DeadlineExceeded {
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
	writeAndFlush(strings.Repeat("b", 128), "v2")

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

func TestStore_ReclaimAutomaticallySchedulesFlushForPublishedUnflushedLocalEntriesAfterReopen(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	dataDir := t.TempDir()

	store := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "requeue-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "requeue-body")
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reopened := New(bucket, log.NewNopLogger(), WithDir(dataDir))

	remoteOnly := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
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
