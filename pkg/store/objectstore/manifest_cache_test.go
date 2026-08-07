package objectstore

import (
	"context"
	"io"
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

type countingManifestBucket struct {
	objstore.Bucket
	mu           sync.Mutex
	manifestGets int
}

func (b *countingManifestBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.Bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if strings.Contains(name, "manifests/") {
		b.mu.Lock()
		b.manifestGets++
		b.mu.Unlock()
	}
	return reader, nil
}

func (b *countingManifestBucket) manifestCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.manifestGets
}

func TestStore_ManifestCacheAvoidsRepeatedManifestFetch(t *testing.T) {
	ctx := context.Background()
	bucket := &countingManifestBucket{Bucket: objstore.NewInMemBucket()}
	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithManifestCache(1<<20),
		WithManifestTTL(time.Minute),
	)
	store.autoFlush = false

	const key = "manifest-cached"
	blobPath := store.blobPath(key, "00000000000000000000-000001")
	require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader("manifest fallback body")))

	m := manifest{
		Version:  "00000000000000000000-000001",
		Layout:   layoutWhole,
		BlobPath: blobPath,
		Size:     int64(len("manifest fallback body")),
		Metadata: daramjwee.Metadata{CacheTag: "v1"},
	}
	data, err := json.Marshal(&m)
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, store.manifestPath(key), strings.NewReader(string(data))))

	for range 2 {
		stream, meta, err := store.GetStream(ctx, key)
		require.NoError(t, err)
		body, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.NoError(t, stream.Close())
		assert.Equal(t, "manifest fallback body", string(body))
		assert.Equal(t, "v1", meta.CacheTag)
	}

	assert.Equal(t, 1, bucket.manifestCalls())
}

func TestStore_ManifestCacheReloadsAfterTTL(t *testing.T) {
	ctx := context.Background()
	bucket := &countingManifestBucket{Bucket: objstore.NewInMemBucket()}
	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithManifestCache(1<<20),
		WithManifestTTL(time.Second),
	)
	store.autoFlush = false

	const key = "manifest-ttl"
	blobPath := store.blobPath(key, "00000000000000000000-000001")
	require.NoError(t, bucket.Upload(ctx, blobPath, strings.NewReader("manifest ttl body")))

	now := time.Unix(1_000, 0)
	store.now = func() time.Time { return now }

	m := manifest{
		Version:  "00000000000000000000-000001",
		Layout:   layoutWhole,
		BlobPath: blobPath,
		Size:     int64(len("manifest ttl body")),
	}
	data, err := json.Marshal(&m)
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, store.manifestPath(key), strings.NewReader(string(data))))

	stream, _, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	_, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, 1, bucket.manifestCalls())

	now = now.Add(2 * time.Second)
	stream, _, err = store.GetStream(ctx, key)
	require.NoError(t, err)
	_, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, 2, bucket.manifestCalls())
}

func TestManifestCacheNilSafe(t *testing.T) {
	var c *manifestCache

	_, ok := c.Get("key")
	assert.False(t, ok)

	c.Set("key", &manifest{})
}

func TestManifestCacheLRUEviction(t *testing.T) {
	now := time.Now()
	c := newManifestCache(1, time.Hour, func() time.Time { return now })

	m1 := &manifest{Version: "v1", Metadata: daramjwee.Metadata{CacheTag: "t1"}}
	m2 := &manifest{Version: "v2", Metadata: daramjwee.Metadata{CacheTag: "t2"}}

	c.Set("key1", m1)
	c.Set("key2", m2)

	c.mu.Lock()
	assert.Empty(t, c.entries, "all entries should be evicted when each exceeds maxBytes")
	c.mu.Unlock()
}
