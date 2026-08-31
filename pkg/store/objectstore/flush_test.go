package objectstore

import (
	"context"
	"errors"
	"io"
	"os"
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

func TestStore_ConcurrentInstancesKeepDifferentKeysInSameShard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := objstore.NewInMemBucket()
	bucket := &checkpointRaceBucket{Bucket: base, release: make(chan struct{})}
	keyA := "concurrent-a"
	keyB := ""
	for i := 0; ; i++ {
		candidate := "concurrent-b-" + strconv.Itoa(i)
		if shardForKey(candidate) == shardForKey(keyA) {
			keyB = candidate
			break
		}
	}

	stores := []*Store{
		New(bucket, log.NewNopLogger(), WithDir(t.TempDir())),
		New(bucket, log.NewNopLogger(), WithDir(t.TempDir())),
	}
	for i, key := range []string{keyA, keyB} {
		stores[i].autoFlush = false
		writer, err := stores[i].BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: key})
		require.NoError(t, err)
		_, err = io.WriteString(writer, key)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}

	errs := make(chan error, 2)
	for _, store := range stores {
		go func() { errs <- store.flushPending(ctx) }()
	}
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	for _, key := range []string{keyA, keyB} {
		stream, meta, err := remote.GetStream(ctx, key)
		require.NoError(t, err)
		body, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		require.Equal(t, key, string(body))
		require.Equal(t, key, meta.CacheTag)
	}

	require.NoError(t, stores[0].Delete(ctx, keyA))
	_, _, err := remote.GetStream(ctx, keyA)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_DeleteTombstoneIsRetriedAfterReopen(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &failFirstEntryUploadBucket{Bucket: base}
	dataDir := t.TempDir()
	store := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	store.autoFlush = false

	key := "delete-recovery"
	blobPath := store.blobPath(key, store.nextVersion())
	require.NoError(t, base.Upload(ctx, blobPath, strings.NewReader("legacy")))
	require.NoError(t, store.publishManifest(ctx, key, blobPath, int64(len("legacy")), &daramjwee.Metadata{CacheTag: "v1"}))

	require.Error(t, store.Delete(ctx, key))
	reopened := New(base, log.NewNopLogger(), WithDir(dataDir))
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	require.Eventually(t, func() bool {
		exists, err := base.Exists(ctx, reopened.remoteEntryPath(key))
		return err == nil && exists
	}, time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		exists, err := base.Exists(ctx, reopened.manifestPath(key))
		return err == nil && !exists
	}, time.Second, 20*time.Millisecond)
	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	_, _, err := remote.GetStream(ctx, key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	_, err = reopened.Compact(ctx, 0)
	require.NoError(t, err)
	exists, err := base.Exists(ctx, blobPath)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestStore_PendingRemotePayloadResumesAfterEntryFailureAndMissingLocalSegment(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &failFirstEntryUploadBucket{Bucket: base}
	dataDir := t.TempDir()
	store := New(bucket, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1))
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, "pending-remote-recovery", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "payload")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.Error(t, store.flushPending(ctx))
	require.Len(t, listObjectNames(t, base, store.blobRoot()), 1)
	segments := localSegmentPaths(t, dataDir)
	require.Len(t, segments, 1)
	require.NoError(t, os.Remove(segments[0]))

	reopened := New(base, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1))
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	require.Eventually(t, func() bool {
		stream, _, err := remote.GetStream(ctx, "pending-remote-recovery")
		if err != nil {
			return false
		}
		defer stream.Close()
		body, err := io.ReadAll(stream)
		return err == nil && string(body) == "payload"
	}, time.Second, 20*time.Millisecond)
	require.Len(t, listObjectNames(t, base, store.blobRoot()), 1)
}

func TestStore_LegacyPartialPackedPlanDoesNotAdoptUnprotectedBody(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	dataDir := t.TempDir()
	remotePath := "segments/legacy-partial-pack"
	require.NoError(t, base.Upload(ctx, remotePath, strings.NewReader("alphabravo")))

	catalogDir := filepath.Join(dataDir, "catalog")
	require.NoError(t, os.MkdirAll(catalogDir, 0o755))
	legacy, err := json.Marshal(map[string]localCatalogEntry{
		"legacy-alpha": {
			SegmentPath:       filepath.Join(dataDir, "missing.seg"),
			Length:            5,
			Generation:        1,
			PublicationToken:  "legacy-token",
			PendingRemotePath: remotePath,
		},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(catalogDir, "snapshot.json"), legacy, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(catalogDir, "snapshot.json.state"), []byte("clean\n"), 0o644))

	store := New(base, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1<<20))
	disableAutoFlush(store)
	require.ErrorIs(t, store.flushPending(ctx), errMissingLocalEntry)

	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1<<20))
	disableAutoFlush(remote)
	_, _, err = remote.GetStream(ctx, "legacy-alpha")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_MigratedMixedPackedPlanKeepsActiveIntent(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &failFirstActiveIntentUploadBucket{Bucket: base}
	dataDir := t.TempDir()
	keyA, keyB := sameShardKeys("legacy-mixed-plan")
	remotePath := joinPath("segments", shardForKey(keyA), "00000000000000000001-legacy.seg")
	require.NoError(t, base.Upload(ctx, remotePath, strings.NewReader("alphabravo")))

	catalogDir := filepath.Join(dataDir, "catalog")
	require.NoError(t, os.MkdirAll(catalogDir, 0o755))
	entries := map[string]localCatalogEntry{
		keyA: {RemotePath: remotePath, IntentCleanupPending: true, Length: 5, Generation: 1, PublicationToken: "0001"},
		keyB: {SegmentPath: filepath.Join(dataDir, "missing.seg"), PendingRemotePath: remotePath, Length: 5, Generation: 2, PublicationToken: "assigned-during-recovery"},
	}
	plan := uploadPlan{RemotePath: remotePath, Terminal: "completed", Members: []uploadPlanMember{
		{Key: keyA, Generation: 1, PublicationToken: "0001", Length: 5},
		{Key: keyB, Generation: 2, Length: 5},
	}}
	snapshot, err := json.Marshal(map[string]any{
		"_daramjwee_catalog": "daramjwee-objectstore-catalog",
		"format_version":     2,
		"entries":            entries,
		"upload_plans":       map[string]uploadPlan{remotePath: plan},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(catalogDir, "snapshot.json"), snapshot, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(catalogDir, "snapshot.json.state"), []byte("clean\n"), 0o644))

	intentPath := joinPath("uploads", encodeKey(remotePath)+".json")
	failed := New(bucket, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1<<20))
	require.ErrorContains(t, failed.ValidateTier(0), "active intent upload failed")
	require.Equal(t, "completed", failed.catalog.UploadPlans()[remotePath].Terminal)
	exists, err := base.Exists(ctx, intentPath)
	require.NoError(t, err)
	require.False(t, exists)
	compactor := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	disableAutoFlush(compactor)
	_, err = compactor.Compact(ctx, 0)
	require.NoError(t, err)
	exists, err = base.Exists(ctx, remotePath)
	require.NoError(t, err)
	require.True(t, exists, "payloads without a terminal GC receipt must fail closed")

	store := New(bucket, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1<<20))
	disableAutoFlush(store)
	require.NoError(t, store.ValidateTier(0))
	plan = store.catalog.UploadPlans()[remotePath]
	require.Empty(t, plan.Terminal)
	exists, err = base.Exists(ctx, intentPath)
	require.NoError(t, err)
	require.True(t, exists)
	require.ErrorIs(t, store.flushPending(ctx), errMissingLocalEntry)
	_, err = compactor.Compact(ctx, 0)
	require.NoError(t, err)
	exists, err = base.Exists(ctx, remotePath)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestStore_PackedUploadPlanSurvivesEntryFailureAndMissingSegments(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &failSecondEntryUploadBucket{Bucket: base}
	dataDir := t.TempDir()
	store := New(bucket, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1<<20))
	store.autoFlush = false
	keyA, keyB := sameShardKeys("packed-plan")
	values := map[string]string{keyA: "alpha", keyB: "bravo"}
	for key, value := range values {
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: value})
		require.NoError(t, err)
		_, err = io.WriteString(writer, value)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.Error(t, store.flushPending(ctx))
	entries := store.catalog.Entries()
	require.NotEmpty(t, entries[keyA].PendingRemotePath)
	require.Equal(t, entries[keyA].PendingRemotePath, entries[keyB].PendingRemotePath)
	require.Positive(t, entries[keyA].PendingRemoteSize)
	compactor := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	_, err := compactor.Compact(ctx, 0)
	require.NoError(t, err)
	exists, err := base.Exists(ctx, entries[keyA].PendingRemotePath)
	require.NoError(t, err)
	require.True(t, exists)
	for _, segmentPath := range localSegmentPaths(t, dataDir) {
		require.NoError(t, os.Remove(segmentPath))
	}

	reopened := New(base, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1<<20))
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1<<20))
	for key, value := range values {
		require.Eventually(t, func() bool {
			stream, _, err := remote.GetStream(ctx, key)
			if err != nil {
				return false
			}
			defer stream.Close()
			body, err := io.ReadAll(stream)
			return err == nil && string(body) == value
		}, time.Second, 20*time.Millisecond)
	}
	require.Len(t, listObjectNames(t, base, ensureDir(joinPath(store.prefix, "segments"))), 1)
}

func TestStore_PackedPlanReplansAfterMemberReplacement(t *testing.T) {
	for _, deleteMember := range []bool{false, true} {
		name := "overwrite"
		if deleteMember {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base := objstore.NewInMemBucket()
			store := New(&failFirstPackedBodyBucket{Bucket: base}, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1<<20))
			store.autoFlush = false
			keyA, keyB := sameShardKeys("packed-member-replacement-" + name)
			keys := []string{keyA, keyB}
			slices.Sort(keys)
			writePendingObject(t, store, keys[0], "alpha")
			writePendingObject(t, store, keys[1], "bravo")
			require.Error(t, store.flushPending(ctx))

			if deleteMember {
				require.ErrorIs(t, store.Delete(ctx, keys[0]), errPendingUploadPlanChanged)
			} else {
				writePendingObject(t, store, keys[0], "replacement")
				require.ErrorIs(t, store.flushPending(ctx), errPendingUploadPlanChanged)
			}
			require.NoError(t, store.flushPending(ctx))
			assert.Empty(t, listObjectNames(t, base, joinPath(store.prefix, "uploads")))

			remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1<<20))
			stream, _, err := remote.GetStream(ctx, keys[1])
			require.NoError(t, err)
			body, err := io.ReadAll(stream)
			require.NoError(t, err)
			require.NoError(t, stream.Close())
			require.Equal(t, "bravo", string(body))
		})
	}
}

func TestStore_PackedPlanReplansAfterMemberSegmentLoss(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	store := New(&failFirstPackedBodyBucket{Bucket: base}, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1<<20))
	store.autoFlush = false
	keyA, keyB := sameShardKeys("packed-member-segment-loss")
	writePendingObject(t, store, keyA, "alpha")
	writePendingObject(t, store, keyB, "bravo")
	require.Error(t, store.flushPending(ctx))

	entries := store.catalog.Entries()
	require.NoError(t, os.Remove(entries[keyA].SegmentPath))
	require.ErrorIs(t, store.flushPending(ctx), errPendingUploadPlanChanged)
	missing, ok := store.catalog.Get(keyA)
	require.True(t, ok)
	require.NotEmpty(t, missing.PendingRemotePath)
	survivor, ok := store.catalog.Get(keyB)
	require.True(t, ok)
	require.Empty(t, survivor.PendingRemotePath)
	keyC := "packed-member-segment-loss-other-shard"
	for shardForKey(keyC) == shardForKey(keyA) {
		keyC += "x"
	}
	writePendingObject(t, store, keyC, "charlie")
	require.ErrorIs(t, store.flushPending(ctx), errMissingLocalEntry)

	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1<<20))
	for key, want := range map[string]string{keyB: "bravo", keyC: "charlie"} {
		stream, _, err := remote.GetStream(ctx, key)
		require.NoError(t, err)
		body, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		require.Equal(t, want, string(body))
	}
}

func TestStore_PendingRemoteWithMissingSegmentDoesNotServeOlderRemote(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	key := "pending-no-stale-fallback"
	seed := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	seed.autoFlush = false
	writer, err := seed.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "v1")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, seed.flushPending(ctx))

	dataDir := t.TempDir()
	store := New(&failFirstEntryUploadBucket{Bucket: base}, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1))
	store.autoFlush = false
	writer, err = store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "v2")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.Error(t, store.flushPending(ctx))
	segments := localSegmentPaths(t, dataDir)
	require.Len(t, segments, 1)
	require.NoError(t, os.Remove(segments[0]))

	_, _, err = store.GetStream(ctx, key)
	require.ErrorIs(t, err, daramjwee.ErrReadStateUncertain)
	_, err = store.Stat(ctx, key)
	require.ErrorIs(t, err, daramjwee.ErrReadStateUncertain)
}

func TestStore_TombstoneReplayCannotOverwriteLaterRemoteWrite(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	dataDir := t.TempDir()
	storeA := New(base, log.NewNopLogger(), WithDir(dataDir), WithPackThreshold(1))
	storeB := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	storeA.autoFlush = false
	storeB.autoFlush = false
	key := "tombstone-cas-replay"

	write := func(store *Store, value string) {
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: value})
		require.NoError(t, err)
		_, err = io.WriteString(writer, value)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}
	write(storeA, "v1")

	originalUpdate := storeA.updateCatalog
	failPublishCommit := true
	storeA.updateCatalog = func(updateKey string, fn func(localCatalogEntry, bool) (localCatalogEntry, bool)) (bool, error) {
		current, exists := storeA.catalog.Get(updateKey)
		next, keep := fn(current, exists)
		if failPublishCommit && updateKey == key && keep && !current.RemotePublished && next.RemotePublished {
			failPublishCommit = false
			return false, errors.New("published tombstone catalog commit failed")
		}
		return originalUpdate(updateKey, func(localCatalogEntry, bool) (localCatalogEntry, bool) { return next, keep })
	}
	require.Error(t, storeA.Delete(ctx, key))
	entry, ok := storeA.catalog.Get(key)
	require.True(t, ok)
	require.False(t, entry.RemotePublished)

	write(storeB, "v2")
	require.NoError(t, storeA.flushPending(ctx))
	_, ok = storeA.catalog.Get(key)
	require.False(t, ok)
	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()), WithPackThreshold(1))
	stream, _, err := remote.GetStream(ctx, key)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "v2", string(body))
}

func TestStore_CachedLegacyFallbackObservesLaterTombstone(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	seed := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	key := "cached-delete"
	blobPath := seed.blobPath(key, seed.nextVersion())
	require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader("legacy")))
	require.NoError(t, seed.publishManifest(ctx, key, blobPath, int64(len("legacy")), &daramjwee.Metadata{CacheTag: "v1"}))

	reader := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()), WithCheckpointCache(1<<20), WithCheckpointTTL(time.Hour), WithManifestCache(1<<20), WithManifestTTL(time.Hour))
	stream, _, err := reader.GetStream(ctx, key)
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	deleter := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	require.NoError(t, deleter.Delete(ctx, key))
	_, _, err = reader.GetStream(ctx, key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	_, err = reader.Stat(ctx, key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestStore_PublishedDeleteDoesNotOverrideLaterRemoteWrite(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	dataDir := t.TempDir()
	storeA := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	storeB := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	storeA.autoFlush = false
	storeB.autoFlush = false
	key, otherKey := sameShardKeys("published-delete")

	writeAndFlush := func(store *Store, key, value string) {
		t.Helper()
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: value})
		require.NoError(t, err)
		_, err = io.WriteString(writer, value)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, store.flushPending(ctx))
	}
	read := func(store *Store, key string) string {
		t.Helper()
		stream, _, err := store.GetStream(ctx, key)
		require.NoError(t, err)
		defer stream.Close()
		body, err := io.ReadAll(stream)
		require.NoError(t, err)
		return string(body)
	}

	writeAndFlush(storeA, key, "v1")
	require.NoError(t, storeA.Delete(ctx, key))
	writeAndFlush(storeB, key, "v2")
	require.Equal(t, "v2", read(storeA, key))

	remote := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	require.Equal(t, "v2", read(remote, key))
	writeAndFlush(storeA, otherKey, "other")
	require.Equal(t, "v2", read(remote, key))

	require.NoError(t, storeA.Close())
	reopened := New(bucket, log.NewNopLogger(), WithDir(dataDir))
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Equal(t, "v2", read(reopened, key))
	_, err := reopened.Compact(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "v2", read(remote, key))
}

func TestStore_DeleteCleanupRetryDoesNotRepublishTombstone(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	bucket := &failFirstManifestDeleteBucket{Bucket: base}
	storeA := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
	storeB := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	storeA.autoFlush = false
	storeB.autoFlush = false
	key := "delete-cleanup-retry"

	require.Error(t, storeA.Delete(ctx, key))
	writer, err := storeB.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = io.WriteString(writer, "v2")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, storeB.flushPending(ctx))
	require.NoError(t, storeA.flushPending(ctx))

	remote := New(base, log.NewNopLogger(), WithDir(t.TempDir()))
	stream, _, err := remote.GetStream(ctx, key)
	require.NoError(t, err)
	defer stream.Close()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "v2", string(body))
	_, err = remote.Compact(ctx, 0)
	require.NoError(t, err)
}

type failFirstEntryUploadBucket struct {
	objstore.Bucket
	once sync.Once
}

type failFirstActiveIntentUploadBucket struct {
	objstore.Bucket
	once sync.Once
}

func (b *failFirstActiveIntentUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	failed := false
	if strings.Contains(name, "/uploads/") || strings.HasPrefix(name, "uploads/") {
		b.once.Do(func() { failed = true })
	}
	if failed {
		return errors.New("active intent upload failed")
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

type failFirstPackedBodyBucket struct {
	objstore.Bucket
	once sync.Once
}

type failSecondEntryUploadBucket struct {
	objstore.Bucket
	mu      sync.Mutex
	uploads int
}

type noConditionalUploadBucket struct{ objstore.Bucket }

func (b *noConditionalUploadBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	return nil
}

func TestStore_RejectsBucketWithoutConditionalEntryUploads(t *testing.T) {
	store := New(&noConditionalUploadBucket{Bucket: objstore.NewInMemBucket()}, log.NewNopLogger(), WithDir(t.TempDir()))
	_, err := store.BeginSet(context.Background(), "unsupported-cas", nil)
	require.ErrorContains(t, err, "must support IfNotExists and IfMatch")
}

func TestStore_InvalidBucketLeavesLegacyCatalogUntouched(t *testing.T) {
	tests := []struct {
		name   string
		bucket objstore.Bucket
	}{
		{name: "unsupported", bucket: &noConditionalUploadBucket{Bucket: objstore.NewInMemBucket()}},
		{name: "nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			catalogDir := filepath.Join(dataDir, "catalog")
			require.NoError(t, os.MkdirAll(catalogDir, 0o755))
			snapshotPath := filepath.Join(catalogDir, "snapshot.json")
			legacy := []byte(`{"key":{"pending_remote_path":"segments/legacy","length":7}}`)
			require.NoError(t, os.WriteFile(snapshotPath, legacy, 0o644))

			store := New(tc.bucket, log.NewNopLogger(), WithDir(dataDir))
			require.Error(t, store.ValidateTier(0))
			got, err := os.ReadFile(snapshotPath)
			require.NoError(t, err)
			require.Equal(t, legacy, got)
			_, err = os.Stat(snapshotPath + ".state")
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = os.Stat(snapshotPath + ".tmp")
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

type failFirstManifestDeleteBucket struct {
	objstore.Bucket
	once sync.Once
}

func (b *failFirstManifestDeleteBucket) Delete(ctx context.Context, name string) error {
	failed := false
	if strings.Contains(name, "/manifests/") || strings.HasPrefix(name, "manifests/") {
		b.once.Do(func() { failed = true })
	}
	if failed {
		return errors.New("manifest delete failed")
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *failFirstEntryUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	failed := false
	if strings.Contains(name, "/entries/") || strings.HasPrefix(name, "entries/") {
		b.once.Do(func() { failed = true })
	}
	if failed {
		return errors.New("entry upload failed")
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func (b *failFirstPackedBodyBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	failed := false
	if strings.Contains(name, "/segments/") || strings.HasPrefix(name, "segments/") {
		b.once.Do(func() { failed = true })
	}
	if failed {
		return errors.New("packed body upload failed")
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func (b *failSecondEntryUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if strings.Contains(name, "/entries/") || strings.HasPrefix(name, "entries/") {
		b.mu.Lock()
		b.uploads++
		fail := b.uploads == 2
		b.mu.Unlock()
		if fail {
			return errors.New("entry upload failed")
		}
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
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

type checkpointRaceBucket struct {
	objstore.Bucket
	mu      sync.Mutex
	waiters int
	release chan struct{}
}

func (b *checkpointRaceBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/latest.json") {
		b.mu.Lock()
		b.waiters++
		if b.waiters == 2 {
			close(b.release)
		}
		release := b.release
		b.mu.Unlock()
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
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
