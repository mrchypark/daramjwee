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
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

type countingManifestBucket struct {
	inner        objstore.Bucket
	mu           sync.Mutex
	manifestGets int
}

func (b *countingManifestBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.inner.Get(ctx, name)
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

func (b *countingManifestBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	return b.inner.Upload(ctx, name, r, opts...)
}

func (b *countingManifestBucket) Delete(ctx context.Context, name string) error {
	return b.inner.Delete(ctx, name)
}

func (b *countingManifestBucket) Name() string {
	return b.inner.Name()
}

func (b *countingManifestBucket) Close() error {
	return b.inner.Close()
}

func (b *countingManifestBucket) Provider() objstore.ObjProvider {
	return b.inner.Provider()
}

func (b *countingManifestBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return b.inner.Iter(ctx, dir, f, options...)
}

func (b *countingManifestBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	return b.inner.IterWithAttributes(ctx, dir, f, options...)
}

func (b *countingManifestBucket) Exists(ctx context.Context, name string) (bool, error) {
	return b.inner.Exists(ctx, name)
}

func (b *countingManifestBucket) IsObjNotFoundErr(err error) bool {
	return b.inner.IsObjNotFoundErr(err)
}

func (b *countingManifestBucket) IsAccessDeniedErr(err error) bool {
	return b.inner.IsAccessDeniedErr(err)
}

func (b *countingManifestBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.inner.GetRange(ctx, name, off, length)
}

func (b *countingManifestBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return b.inner.Attributes(ctx, name)
}

func (b *countingManifestBucket) SupportedIterOptions() []objstore.IterOptionType {
	return b.inner.SupportedIterOptions()
}

func (b *countingManifestBucket) manifestCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.manifestGets
}

func TestStore_ManifestCacheAvoidsRepeatedManifestFetch(t *testing.T) {
	ctx := context.Background()
	bucket := &countingManifestBucket{inner: objstore.NewInMemBucket()}
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
	bucket := &countingManifestBucket{inner: objstore.NewInMemBucket()}
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
