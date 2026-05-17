package sqlitestore

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T, opts ...Option) *SQLiteStore {
	t.Helper()
	store, err := New(filepath.Join(t.TempDir(), "cache.db"), log.NewNopLogger(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestSQLiteStore_WriteSinkConformance(t *testing.T) {
	storetest.RunWriteSinkConformance(t, func(t *testing.T) daramjwee.Store {
		t.Helper()
		return setupTestStore(t)
	})
}

func TestSQLiteStore_SetGetStatAndDelete(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t, WithChunkSize(4))
	key := "object/key"
	meta := &daramjwee.Metadata{
		CacheTag:   "v1",
		IsNegative: false,
		CachedAt:   time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC),
	}

	sink, err := store.BeginSet(ctx, key, meta)
	require.NoError(t, err)
	_, err = sink.Write([]byte("hello sqlite store"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	gotMeta, err := store.Stat(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, meta.CacheTag, gotMeta.CacheTag)
	assert.Equal(t, meta.IsNegative, gotMeta.IsNegative)
	assert.True(t, meta.CachedAt.Equal(gotMeta.CachedAt))

	reader, gotMeta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, []byte("hello sqlite store"), body)
	assert.Equal(t, meta.CacheTag, gotMeta.CacheTag)

	require.NoError(t, store.Delete(ctx, key))
	_, _, err = store.GetStream(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
	_, err = store.Stat(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestSQLiteStore_ReopenPreservesCommittedEntries(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache.db")

	store, err := New(dbPath, log.NewNopLogger(), WithChunkSize(3))
	require.NoError(t, err)
	sink, err := store.BeginSet(ctx, "persist-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("persistent payload"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())
	require.NoError(t, store.Close())

	reopened, err := New(dbPath, log.NewNopLogger(), WithChunkSize(3))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	reader, meta, err := reopened.GetStream(ctx, "persist-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("persistent payload"), body)
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestSQLiteStore_NewWithContextHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store, err := NewWithContext(ctx, filepath.Join(t.TempDir(), "cache.db"), log.NewNopLogger())
	require.Nil(t, store)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSQLiteStore_WithConnectionPoolAppliesLimits(t *testing.T) {
	store := setupTestStore(t, WithConnectionPool(3, 2))

	stats := store.db.Stats()
	assert.Equal(t, 3, stats.MaxOpenConnections)
}

func TestSQLiteStore_ReopenAdvancesGenerationPastPersistedFloor(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache.db")

	store, err := New(dbPath, log.NewNopLogger())
	require.NoError(t, err)
	futureGeneration := time.Now().Add(time.Hour).UnixNano()
	_, err = store.db.ExecContext(ctx, `
		INSERT INTO generation_floor(key, generation) VALUES(?, ?)
	`, "future-key", futureGeneration)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	reopened, err := New(dbPath, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	sink, err := reopened.BeginSet(ctx, "future-key", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("new value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	reader, meta, err := reopened.GetStream(ctx, "future-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("new value"), body)
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestSQLiteStore_OpenReaderDoesNotBlockOtherKeyMutation(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t)

	for _, key := range []string{"read-key", "delete-key", "close-key"} {
		sink, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
		require.NoError(t, err)
		_, err = sink.Write([]byte("value-" + key))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
	}

	reader, _, err := store.GetStream(ctx, "read-key")
	require.NoError(t, err)
	defer reader.Close()

	opCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	require.NoError(t, store.Delete(opCtx, "delete-key"))

	sink, err := store.BeginSet(opCtx, "close-key", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("updated"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())
}

func TestSQLiteStore_OpenReaderKeepsSnapshotAcrossSameKeyMutation(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t)

	first, err := store.BeginSet(ctx, "snapshot-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = first.Write([]byte("original"))
	require.NoError(t, err)
	require.NoError(t, first.Close())

	reader, meta, err := store.GetStream(ctx, "snapshot-key")
	require.NoError(t, err)
	assert.Equal(t, "v1", meta.CacheTag)
	defer reader.Close()

	second, err := store.BeginSet(ctx, "snapshot-key", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = second.Write([]byte("updated"))
	require.NoError(t, err)
	require.NoError(t, second.Close())

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), body)

	reopened, meta, err := store.GetStream(ctx, "snapshot-key")
	require.NoError(t, err)
	defer reopened.Close()
	body, err = io.ReadAll(reopened)
	require.NoError(t, err)
	assert.Equal(t, []byte("updated"), body)
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestSQLiteStore_OverwriteRemovesOldChunks(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t, WithChunkSize(4))

	first, err := store.BeginSet(ctx, "overwrite-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = first.Write([]byte(strings.Repeat("a", 17)))
	require.NoError(t, err)
	require.NoError(t, first.Close())

	second, err := store.BeginSet(ctx, "overwrite-key", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = second.Write([]byte("bb"))
	require.NoError(t, err)
	require.NoError(t, second.Close())

	reader, meta, err := store.GetStream(ctx, "overwrite-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("bb"), body)
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestSQLiteStore_LateCloseDoesNotOverwriteNewerValue(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t)

	oldWriter, err := store.BeginSet(ctx, "ordered-key", &daramjwee.Metadata{CacheTag: "old"})
	require.NoError(t, err)
	_, err = oldWriter.Write([]byte("old"))
	require.NoError(t, err)

	newWriter, err := store.BeginSet(ctx, "ordered-key", &daramjwee.Metadata{CacheTag: "new"})
	require.NoError(t, err)
	_, err = newWriter.Write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, newWriter.Close())
	require.ErrorIs(t, oldWriter.Close(), daramjwee.ErrTopWriteInvalidated)

	reader, meta, err := store.GetStream(ctx, "ordered-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), body)
	assert.Equal(t, "new", meta.CacheTag)
}

func TestSQLiteStore_DeleteAfterBeginSetPreventsStalePublish(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t)

	sink, err := store.BeginSet(ctx, "racy-key", &daramjwee.Metadata{CacheTag: "old"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("old value"))
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, "racy-key"))
	require.ErrorIs(t, sink.Close(), daramjwee.ErrTopWriteInvalidated)

	_, _, err = store.GetStream(ctx, "racy-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestSQLiteStore_DeleteFromOtherInstancePreventsStalePublish(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache.db")

	first, err := New(dbPath, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := New(dbPath, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	sink, err := second.BeginSet(ctx, "cross-instance-key", &daramjwee.Metadata{CacheTag: "stale"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("stale after delete"))
	require.NoError(t, err)

	require.NoError(t, first.Delete(ctx, "cross-instance-key"))
	require.ErrorIs(t, sink.Close(), daramjwee.ErrTopWriteInvalidated)

	_, _, err = first.GetStream(ctx, "cross-instance-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestSQLiteStore_ZeroLengthObjectPublishesMetadataAndEmptyBody(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t)

	sink, err := store.BeginSet(ctx, "empty-key", &daramjwee.Metadata{CacheTag: "empty", IsNegative: true})
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	meta, err := store.Stat(ctx, "empty-key")
	require.NoError(t, err)
	assert.Equal(t, "empty", meta.CacheTag)
	assert.True(t, meta.IsNegative)

	reader, meta, err := store.GetStream(ctx, "empty-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, body)
	assert.Equal(t, "empty", meta.CacheTag)
	assert.True(t, meta.IsNegative)
}

func TestSQLiteStore_CanceledContextBeforeCloseCleansStagedChunks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := setupTestStore(t, WithChunkSize(4))

	sink, err := store.BeginSet(ctx, "cancel-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("staged"))
	require.NoError(t, err)

	cancel()
	assert.ErrorIs(t, sink.Close(), context.Canceled)

	_, _, err = store.GetStream(context.Background(), "cancel-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	var stagedRows int
	err = store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM temp_chunks`).Scan(&stagedRows)
	require.NoError(t, err)
	assert.Zero(t, stagedRows)
}

func TestSQLiteStore_ContextCancellation(t *testing.T) {
	store := setupTestStore(t, WithChunkSize(4))
	key := "cancel-direct-key"
	metadata := &daramjwee.Metadata{CacheTag: "v1"}

	t.Run("BeginSet", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := store.BeginSet(ctx, key, metadata)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("GetStream", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := store.GetStream(ctx, key)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Stat", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := store.Stat(ctx, key)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Delete", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := store.Delete(ctx, key)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("ReaderRead", func(t *testing.T) {
		writeCtx := context.Background()
		sink, err := store.BeginSet(writeCtx, "cancel-read-key", metadata)
		require.NoError(t, err)
		_, err = sink.Write([]byte("read-data"))
		require.NoError(t, err)
		require.NoError(t, sink.Close())

		readCtx, cancel := context.WithCancel(context.Background())
		reader, _, err := store.GetStream(readCtx, "cancel-read-key")
		require.NoError(t, err)
		defer reader.Close()
		cancel()
		_, err = reader.Read(make([]byte, 1))
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestSQLiteStore_NewDoesNotDeleteOtherSessionStagedChunks(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache.db")

	first, err := New(dbPath, log.NewNopLogger(), WithChunkSize(4))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })

	sink, err := first.BeginSet(ctx, "shared-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("first chunk flushed and buffered"))
	require.NoError(t, err)

	second, err := New(dbPath, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, second.Close())

	require.NoError(t, sink.Close())
	reader, _, err := first.GetStream(ctx, "shared-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("first chunk flushed and buffered"), body)
}

func TestSQLiteStore_OldStagedChunkCleanupCannotPartiallyPublishLiveWriter(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cache.db")

	first, err := New(dbPath, log.NewNopLogger(), WithChunkSize(4))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })

	sink, err := first.BeginSet(ctx, "old-staged-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("first chunk remains staged"))
	require.NoError(t, err)

	_, err = first.db.ExecContext(ctx, `UPDATE temp_chunks SET created_at = ?`, time.Now().Add(-25*time.Hour).UnixNano())
	require.NoError(t, err)

	second, err := New(dbPath, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, second.Close())

	require.Error(t, sink.Close())
	_, _, err = first.GetStream(ctx, "old-staged-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestSQLiteStore_GetStreamReadsAcrossChunks(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t, WithChunkSize(5))
	want := bytes.Repeat([]byte("0123456789"), 32)

	sink, err := store.BeginSet(ctx, "chunked-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write(want)
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	reader, _, err := store.GetStream(ctx, "chunked-key")
	require.NoError(t, err)
	defer reader.Close()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
