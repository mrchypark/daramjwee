
package memcachedstore

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const memcachedTestServer = "localhost:11211"

func setupMemcached(t *testing.T) *memcache.Client {
	mc := memcache.New(memcachedTestServer)
	if err := mc.Ping(); err != nil {
		t.Skipf("skipping test; could not connect to memcached on %s: %v", memcachedTestServer, err)
	}
	// Clean up any previous test data
	mc.DeleteAll()
	t.Cleanup(func() { mc.DeleteAll() })
	return mc
}

func TestMemcachedStore_SetAndGet(t *testing.T) {
	mc := setupMemcached(t)
	logger := log.NewNopLogger()
	store := New(mc, logger)

	ctx := context.Background()
	key := "test-key"
	testData := []byte("hello memcached")
	testMetadata := &daramjwee.Metadata{
		ETag:       "v2.0.0",
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

func TestMemcachedStore_Stat(t *testing.T) {
	mc := setupMemcached(t)
	logger := log.NewNopLogger()
	store := New(mc, logger)

	ctx := context.Background()
	key := "test-stat-key"
	testData := []byte("hello stat")
	testMetadata := &daramjwee.Metadata{
		ETag:       "v2.0.1",
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

func TestMemcachedStore_Delete(t *testing.T) {
	mc := setupMemcached(t)
	logger := log.NewNopLogger()
	store := New(mc, logger)

	ctx := context.Background()
	key := "test-delete-key"
	testData := []byte("data to be deleted")
	testMetadata := &daramjwee.Metadata{ETag: "v2"}

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
	_, err = store.Stat(ctx, key)
	require.Error(t, err, "Stat should return an error after delete")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "Stat should return ErrNotFound after delete")
}
