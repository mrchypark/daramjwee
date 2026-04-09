package objectstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
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
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithBlockCache(1<<20),
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
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithBlockCache(1<<20),
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
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
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
		writer, err := flushed.BeginSet(ctx, tc.key, &daramjwee.Metadata{CacheTag: tc.key})
		require.NoError(t, err)
		_, err = io.WriteString(writer, tc.body)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	}
	require.NoError(t, flushed.flushPending(ctx))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithBlockCache(1<<20),
	)
	store.autoFlush = false

	readAllStoreKey(t, ctx, store, keyA)
	require.Equal(t, 1, bucket.rangeCalls())

	readAllStoreKey(t, ctx, store, keyB)
	assert.Equal(t, 2, bucket.rangeCalls())
}

func TestStore_RemotePackedReadWithoutBlockCacheUsesSingleLogicalRange(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	seedRemotePackedStore(t, ctx, bucket, "cold-packed-key", strings.Repeat("x", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
	)
	store.autoFlush = false

	body := readAllStoreKey(t, ctx, store, "cold-packed-key")
	assert.Equal(t, strings.Repeat("x", 256), body)
	assert.Equal(t, 1, bucket.rangeCalls())
}

func TestStore_RemotePackedReadWithoutBlockCacheDoesNotCoalesceConcurrentReaders(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{
		Bucket: objstore.NewInMemBucket(),
		delay:  25 * time.Millisecond,
	}
	seedRemotePackedStore(t, ctx, bucket, "cold-packed-concurrent", strings.Repeat("z", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
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
			stream, _, err := store.GetStream(ctx, "cold-packed-concurrent")
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

	assert.Greater(t, bucket.rangeCalls(), 1)
}

func TestStore_RemotePackedReadWithLocalWholeObjectCacheReusesLocalFileAfterWarmup(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	seedRemotePackedStore(t, ctx, bucket, "local-whole-object-cache", strings.Repeat("m", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithPackedWholeObjectCache(1<<20),
	)
	store.autoFlush = false

	body := readAllStoreKey(t, ctx, store, "local-whole-object-cache")
	require.Equal(t, strings.Repeat("m", 256), body)
	require.Equal(t, 1, bucket.rangeCalls())

	body = readAllStoreKey(t, ctx, store, "local-whole-object-cache")
	require.Equal(t, strings.Repeat("m", 256), body)
	assert.Equal(t, 1, bucket.rangeCalls())
}

func TestStore_RemotePackedReadWithLocalWholeObjectCacheDiscardsPartialRead(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	seedRemotePackedStore(t, ctx, bucket, "local-whole-object-partial", strings.Repeat("p", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithPackedWholeObjectCache(1<<20),
	)
	store.autoFlush = false

	stream, _, err := store.GetStream(ctx, "local-whole-object-partial")
	require.NoError(t, err)
	buf := make([]byte, 64)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 64, n)
	require.NoError(t, stream.Close())
	require.Equal(t, 1, bucket.rangeCalls())

	body := readAllStoreKey(t, ctx, store, "local-whole-object-partial")
	require.Equal(t, strings.Repeat("p", 256), body)
	assert.Equal(t, 2, bucket.rangeCalls())
}

func TestStore_RemotePackedReadWithLocalWholeObjectCacheEvictsOldEntries(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	seedRemotePackedStore(t, ctx, bucket, "local-whole-object-evict-a", strings.Repeat("a", 256))
	seedRemotePackedStore(t, ctx, bucket, "local-whole-object-evict-b", strings.Repeat("b", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithPackedWholeObjectCache(300),
	)
	store.autoFlush = false

	readAllStoreKey(t, ctx, store, "local-whole-object-evict-a")
	require.Equal(t, 1, bucket.rangeCalls())

	readAllStoreKey(t, ctx, store, "local-whole-object-evict-b")
	require.Equal(t, 2, bucket.rangeCalls())

	readAllStoreKey(t, ctx, store, "local-whole-object-evict-a")
	assert.Equal(t, 3, bucket.rangeCalls())
}

func TestStore_RemotePackedReadWithLocalWholeObjectCacheIsNoOpWhenBlockCacheEnabled(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	seedRemotePackedStore(t, ctx, bucket, "whole-object-cache-noop", strings.Repeat("n", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithBlockCache(1<<20),
		WithPackedWholeObjectCache(1<<20),
	)
	store.autoFlush = false

	readAllStoreKey(t, ctx, store, "whole-object-cache-noop")
	firstReads := bucket.rangeCalls()
	require.Greater(t, firstReads, 0)
	require.Equal(t, 0, countReadCacheFiles(t, store))

	readAllStoreKey(t, ctx, store, "whole-object-cache-noop")
	assert.Equal(t, firstReads, bucket.rangeCalls())
	assert.Equal(t, 0, countReadCacheFiles(t, store))
}

func TestStore_RemotePackedReadWithLocalWholeObjectCacheDiscardsTempFileOnReadError(t *testing.T) {
	ctx := context.Background()
	bucket := &faultyRangeBucket{
		Bucket:    objstore.NewInMemBucket(),
		failAfter: 64,
		readErr:   errors.New("boom"),
		failReads: 1,
	}
	seedRemotePackedStore(t, ctx, bucket, "local-whole-object-read-error", strings.Repeat("r", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithPackedWholeObjectCache(1<<20),
	)
	store.autoFlush = false

	stream, _, err := store.GetStream(ctx, "local-whole-object-read-error")
	require.NoError(t, err)
	_, err = io.ReadAll(stream)
	require.EqualError(t, err, "boom")
	require.NoError(t, stream.Close())
	require.Equal(t, 0, countReadCacheFiles(t, store))

	body := readAllStoreKey(t, ctx, store, "local-whole-object-read-error")
	require.Equal(t, strings.Repeat("r", 256), body)
	assert.Equal(t, 2, bucket.rangeCalls())
	assert.Equal(t, 1, countReadCacheFiles(t, store))
}

func TestStore_RemotePackedReadWithLocalWholeObjectCacheDiscardsTempFileOnCloseError(t *testing.T) {
	ctx := context.Background()
	bucket := &faultyRangeBucket{
		Bucket:    objstore.NewInMemBucket(),
		closeErr:  errors.New("close boom"),
		failClose: 1,
	}
	seedRemotePackedStore(t, ctx, bucket, "local-whole-object-close-error", strings.Repeat("c", 256))

	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithPackedWholeObjectCache(1<<20),
	)
	store.autoFlush = false

	stream, _, err := store.GetStream(ctx, "local-whole-object-close-error")
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("c", 256), string(body))
	require.EqualError(t, stream.Close(), "close boom")
	require.Equal(t, 0, countReadCacheFiles(t, store))

	warmBody := readAllStoreKey(t, ctx, store, "local-whole-object-close-error")
	require.Equal(t, strings.Repeat("c", 256), warmBody)
	assert.Equal(t, 2, bucket.rangeCalls())
	assert.Equal(t, 1, countReadCacheFiles(t, store))
}

func TestStore_RemotePackedReadWithLocalWholeObjectCacheCleansReadCacheOnRestart(t *testing.T) {
	ctx := context.Background()
	bucket := &countingRangeBucket{Bucket: objstore.NewInMemBucket()}
	seedRemotePackedStore(t, ctx, bucket, "local-whole-object-restart", strings.Repeat("s", 256))

	dir := t.TempDir()
	store := New(bucket, log.NewNopLogger(),
		WithDir(dir),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithPackedWholeObjectCache(1<<20),
	)
	store.autoFlush = false

	readAllStoreKey(t, ctx, store, "local-whole-object-restart")
	require.Equal(t, 1, countReadCacheFiles(t, store))

	restarted := New(bucket, log.NewNopLogger(),
		WithDir(dir),
		WithPackThreshold(1024),
		WithPageSize(64),
		WithPackedWholeObjectCache(1<<20),
	)
	restarted.autoFlush = false

	require.Equal(t, 0, countReadCacheFiles(t, restarted))
	body := readAllStoreKey(t, ctx, restarted, "local-whole-object-restart")
	require.Equal(t, strings.Repeat("s", 256), body)
}

func seedRemotePackedStore(t *testing.T, ctx context.Context, bucket objstore.Bucket, key, body string) {
	t.Helper()
	store := New(bucket, log.NewNopLogger(),
		WithDir(t.TempDir()),
		WithPackThreshold(1024),
		WithPageSize(64),
	)
	store.autoFlush = false

	writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: key})
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

type faultyRangeBucket struct {
	objstore.Bucket
	mu        sync.Mutex
	reads     int
	failReads int
	failAfter int
	readErr   error
	failClose int
	closeErr  error
}

func (b *faultyRangeBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	reader, err := b.Bucket.GetRange(ctx, name, off, length)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.reads++
	failRead := b.failReads > 0
	if b.failReads > 0 {
		b.failReads--
	}
	failClose := b.failClose > 0
	if b.failClose > 0 {
		b.failClose--
	}
	readErr := b.readErr
	failAfter := b.failAfter
	closeErr := b.closeErr
	b.mu.Unlock()

	if failRead || failClose {
		return &faultyReadCloser{
			reader:    reader,
			failRead:  failRead,
			failAfter: failAfter,
			readErr:   readErr,
			failClose: failClose,
			closeErr:  closeErr,
		}, nil
	}
	return reader, nil
}

func (b *faultyRangeBucket) rangeCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reads
}

type faultyReadCloser struct {
	reader    io.ReadCloser
	read      int
	failRead  bool
	failAfter int
	readErr   error
	failClose bool
	closeErr  error
}

func (r *faultyReadCloser) Read(p []byte) (int, error) {
	if r.failRead && r.read >= r.failAfter {
		return 0, r.readErr
	}

	if r.failRead {
		remaining := r.failAfter - r.read
		if remaining > 0 && remaining < len(p) {
			p = p[:remaining]
		}
	}

	n, err := r.reader.Read(p)
	r.read += n
	return n, err
}

func (r *faultyReadCloser) Close() error {
	closeErr := r.reader.Close()
	if r.failClose {
		return r.closeErr
	}
	return closeErr
}

func countReadCacheFiles(t *testing.T, store *Store) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(store.dataDir, "readcache"))
	require.NoError(t, err)
	return len(entries)
}
