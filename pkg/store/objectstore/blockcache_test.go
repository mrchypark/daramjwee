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

func TestStore_BlockCacheWarmHitAvoidsRemoteRangeRead(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	seedRemotePackedStore(t, ctx, bucket, "warm-key", strings.Repeat("a", 96))

	store := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(1024),
		WithPageSize(64),
		WithMemoryBlockCache(1<<20),
	)
	store.autoFlush = false

	readAllStoreKey(t, ctx, store, "warm-key")
	firstReads := bucket.rangeCalls()
	require.Greater(t, firstReads, 0)

	readAllStoreKey(t, ctx, store, "warm-key")
	assert.Equal(t, firstReads, bucket.rangeCalls())
}

func TestStore_BlockCacheCoalescesConcurrentSameBlockMisses(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{
		Bucket: objstore.NewInMemBucket(),
		delay:  25 * time.Millisecond,
	}
	seedRemotePackedStore(t, ctx, bucket, "coalesce-key", strings.Repeat("b", 32))

	store := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(1024),
		WithPageSize(64),
		WithMemoryBlockCache(1<<20),
	)
	store.autoFlush = false

	const readers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			stream, _, err := store.GetStream(ctx, "coalesce-key")
			if err != nil {
				errs <- err
				return
			}
			defer stream.Close()
			_, err = io.ReadAll(stream)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, 1, bucket.rangeCalls())
}

func TestStore_BlockCacheUsesSegmentPathAndBlockIndexNotLogicalKey(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	flushed := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(1024),
		WithPageSize(64),
	)
	flushed.autoFlush = false
	keyA, keyB := sameShardKeys("shared-block")

	for _, tc := range []struct {
		key  string
		body string
	}{
		{keyA, strings.Repeat("a", 40)},
		{keyB, strings.Repeat("b", 40)},
	} {
		writer, err := flushed.BeginSet(ctx, tc.key, &daramjwee.Metadata{ETag: tc.key})
		require.NoError(t, err)
		_, err = io.WriteString(writer, tc.body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.NoError(t, flushed.flushPending(ctx))

	store := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(1024),
		WithPageSize(64),
		WithMemoryBlockCache(1<<20),
	)
	store.autoFlush = false

	readAllStoreKey(t, ctx, store, keyA)
	require.Equal(t, 1, bucket.rangeCalls())

	readAllStoreKey(t, ctx, store, keyB)
	assert.Equal(t, 2, bucket.rangeCalls())
}

func seedRemotePackedStore(t *testing.T, ctx context.Context, bucket objstore.Bucket, key, body string) {
	t.Helper()
	store := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(1024),
		WithPageSize(64),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{ETag: key})
	require.NoError(t, err)
	_, err = io.WriteString(writer, body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, store.flushPending(ctx))
}

func readAllStoreKey(t *testing.T, ctx context.Context, store *Store, key string) string {
	t.Helper()
	stream, _, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	return string(body)
}

type countingRangeBucket struct {
	objstore.Bucket
	mu    sync.Mutex
	reads int
	delay time.Duration
}

func (b *countingRangeBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	reader, err := b.Bucket.GetRange(ctx, name, off, length)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.reads++
	b.mu.Unlock()
	return reader, nil
}

func (b *countingRangeBucket) rangeCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reads
}
