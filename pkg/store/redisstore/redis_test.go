package redisstore

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/storetest"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMiniRedis(t *testing.T) *miniredis.Miniredis {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return mr
}

func TestRedisStore_WriteSinkConformance(t *testing.T) {
	storetest.RunWriteSinkConformance(t, func(t *testing.T) daramjwee.Store {
		t.Helper()
		mr := setupMiniRedis(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		return New(client, log.NewNopLogger())
	})
}

func TestRedisStore_SetAndGet(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger).(*RedisStore)

	ctx := context.Background()
	key := "test-key"
	testData := []byte("hello world")
	testMetadata := &daramjwee.Metadata{
		CacheTag:   "v1.0.0",
		CachedAt:   time.Now().UTC().Truncate(time.Second),
		IsNegative: false,
	}

	// 1. Set data using BeginSet
	writer, err := store.BeginSet(ctx, key, testMetadata)
	require.NoError(t, err)
	n, err := writer.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)
	require.NoError(t, writer.Close())

	// 2. Get data using GetStream
	reader, meta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	// 3. Verify metadata
	assert.Equal(t, testMetadata.CacheTag, meta.CacheTag)
	assert.Equal(t, testMetadata.CachedAt, meta.CachedAt.UTC().Truncate(time.Second))
	assert.Equal(t, testMetadata.IsNegative, meta.IsNegative)

	// 4. Verify data
	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)
}

func TestRedisStore_GetStream_Streaming(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger).(*RedisStore)

	ctx := context.Background()
	key := "test-streaming-key"
	// Create a large data slice to force multiple reads
	largeData := []byte(strings.Repeat("a", chunkSize*2))
	testMetadata := &daramjwee.Metadata{CacheTag: "v1"}

	// 1. Set data
	writer, err := store.BeginSet(ctx, key, testMetadata)
	require.NoError(t, err)
	_, err = writer.Write(largeData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// 2. Get stream
	reader, _, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	// 3. Read from the stream and verify
	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, largeData, readData)
}

func TestRedisStore_Stat(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger)

	ctx := context.Background()
	key := "test-stat-key"
	testData := []byte("hello stat")
	testMetadata := &daramjwee.Metadata{
		CacheTag:   "v1.0.1",
		CachedAt:   time.Now().UTC().Truncate(time.Second),
		IsNegative: false,
	}

	// 1. Set data first
	writer, err := store.BeginSet(ctx, key, testMetadata)
	require.NoError(t, err)
	_, err = writer.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// 2. Stat the key
	meta, err := store.Stat(ctx, key)
	require.NoError(t, err)

	// 3. Verify metadata
	assert.Equal(t, testMetadata.CacheTag, meta.CacheTag)
	assert.Equal(t, testMetadata.CachedAt, meta.CachedAt.UTC().Truncate(time.Second))
	assert.Equal(t, testMetadata.IsNegative, meta.IsNegative)
}

func TestRedisStore_Delete(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger)

	ctx := context.Background()
	key := "test-delete-key"
	testData := []byte("data to be deleted")
	testMetadata := &daramjwee.Metadata{CacheTag: "v1"}

	// 1. Set data first
	writer, err := store.BeginSet(ctx, key, testMetadata)
	require.NoError(t, err)
	_, err = writer.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// 2. Verify it exists
	_, err = store.Stat(ctx, key)
	require.NoError(t, err, "Stat should find the key before delete")

	// 3. Delete the key
	err = store.Delete(ctx, key)
	require.NoError(t, err)

	// 4. Verify it's gone
	_, _, err = store.GetStream(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "GetStream should return ErrNotFound after delete")

	_, err = store.Stat(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "Stat should return ErrNotFound after delete")
}

func TestRedisStore_GetStream_DataInconsistency(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger).(*RedisStore)

	ctx := context.Background()
	key := "test-inconsistent-key"
	testMetadata := &daramjwee.Metadata{CacheTag: "v1"}

	// Manually set metadata but not data to simulate inconsistency
	metaBytes, err := json.Marshal(testMetadata)
	require.NoError(t, err)
	require.NoError(t, client.Set(ctx, store.MetaKey(key), metaBytes, 0).Err())

	// GetStream should detect this and return ErrNotFound
	_, _, err = store.GetStream(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestRedisStore_ContextCancellation(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger)

	key := "test-cancel"
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

	t.Run("WriterClose", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		writer, err := store.BeginSet(ctx, key, metadata)
		require.NoError(t, err)

		// Cancel the context after the writer is created
		cancel()

		err = writer.Close()
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestRedisStore_AbortDeletesTempKey(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store := New(client, log.NewNopLogger()).(*RedisStore)
	ctx := context.Background()

	sink, err := store.BeginSet(ctx, "abort-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)

	writer := sink.(*redisStoreWriter)
	_, err = sink.Write([]byte("partial"))
	require.NoError(t, err)
	require.True(t, mr.Exists(writer.tempKey))

	require.NoError(t, sink.Abort())
	require.False(t, mr.Exists(writer.tempKey))
	require.False(t, mr.Exists(store.DataKey("abort-key")))
	require.False(t, mr.Exists(store.MetaKey("abort-key")))
}

func TestRedisStore_ActiveStreamKeepsTempKeyVisible(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store := New(client, log.NewNopLogger()).(*RedisStore)
	ctx := context.Background()

	sink, err := store.BeginSet(ctx, "active-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)

	writer := sink.(*redisStoreWriter)
	_, err = sink.Write([]byte("streaming"))
	require.NoError(t, err)

	require.True(t, mr.Exists(writer.tempKey))
	value, err := mr.Get(writer.tempKey)
	require.NoError(t, err)
	require.Equal(t, "streaming", value)
	require.NoError(t, sink.Abort())
}

func TestRedisStore_ZeroByteWrite(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger).(*RedisStore)

	ctx := context.Background()
	key := "zero-byte-key"
	metadata := &daramjwee.Metadata{CacheTag: "v0", IsNegative: true}

	writer, err := store.BeginSet(ctx, key, metadata)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, meta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Len(t, body, 0)
	assert.Equal(t, metadata.CacheTag, meta.CacheTag)
	assert.True(t, meta.IsNegative)
}
