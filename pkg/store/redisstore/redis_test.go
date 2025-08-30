
package redisstore

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
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

func TestRedisStore_SetAndGet(t *testing.T) {
	mr := setupMiniRedis(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	logger := log.NewNopLogger()
	store := New(client, logger)

	ctx := context.Background()
	key := "test-key"
	testData := []byte("hello world")
	testMetadata := &daramjwee.Metadata{
		ETag:       "v1.0.0",
		CachedAt:   time.Now().UTC().Truncate(time.Second),
		IsNegative: false,
	}

	// 1. Set data using SetWithWriter
	writer, err := store.SetWithWriter(ctx, key, testMetadata)
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
	assert.Equal(t, testMetadata.ETag, meta.ETag)
	assert.Equal(t, testMetadata.CachedAt, meta.CachedAt.UTC().Truncate(time.Second))
	assert.Equal(t, testMetadata.IsNegative, meta.IsNegative)

	// 4. Verify data
	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)
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
		ETag:       "v1.0.1",
		CachedAt:   time.Now().UTC().Truncate(time.Second),
		IsNegative: false,
	}

	// 1. Set data first
	writer, err := store.SetWithWriter(ctx, key, testMetadata)
	require.NoError(t, err)
	_, err = writer.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// 2. Stat the key
	meta, err := store.Stat(ctx, key)
	require.NoError(t, err)

	// 3. Verify metadata
	assert.Equal(t, testMetadata.ETag, meta.ETag)
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
	testMetadata := &daramjwee.Metadata{ETag: "v1"}

	// 1. Set data first
	writer, err := store.SetWithWriter(ctx, key, testMetadata)
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
