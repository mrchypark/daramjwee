package daramjwee_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/mrchypark/daramjwee/pkg/store/redisstore"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

const topWriteLivenessTimeout = 500 * time.Millisecond

func TestCache_AbandonedTopWriteSinkDoesNotBlockSameKeySetForBuiltInStagingStores(t *testing.T) {
	for _, tc := range topWriteLivenessStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := daramjwee.New(nil, daramjwee.WithTiers(tc.store), daramjwee.WithOpTimeout(2*time.Second))
			require.NoError(t, err)
			defer cache.Close()

			first, err := cache.Set(context.Background(), "same-key", &daramjwee.Metadata{CacheTag: "abandoned"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = first.Abort() })
			_, err = first.Write([]byte("abandoned-value"))
			require.NoError(t, err)

			setAndCloseWithin(t, cache, "same-key", "user-value", "user-v2", topWriteLivenessTimeout)
			requireStoreValue(t, tc.store, "same-key", "user-value", "user-v2")
		})
	}
}

func TestCache_PartialMissStreamDoesNotBlockSameKeySetForBuiltInStagingStores(t *testing.T) {
	for _, tc := range topWriteLivenessStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := daramjwee.New(nil, daramjwee.WithTiers(tc.store), daramjwee.WithOpTimeout(2*time.Second))
			require.NoError(t, err)
			defer cache.Close()

			source := newBlockingReadCloser([]byte("origin"), []byte("-value"))
			fetcher := &blockingSourceFetcher{source: source, metadata: &daramjwee.Metadata{CacheTag: "origin-v1"}}

			resp, err := cache.Get(context.Background(), "same-key", daramjwee.GetRequest{}, fetcher)
			require.NoError(t, err)
			require.NotNil(t, resp)

			buf := make([]byte, len("origin"))
			n, err := resp.Read(buf)
			require.NoError(t, err)
			require.Equal(t, len("origin"), n)
			require.Equal(t, "origin", string(buf[:n]))

			setAndCloseWithin(t, cache, "same-key", "user-value", "user-v2", topWriteLivenessTimeout)

			source.Release()
			rest, err := io.ReadAll(resp)
			require.NoError(t, err)
			require.Equal(t, "-value", string(rest))
			require.ErrorIs(t, resp.Close(), daramjwee.ErrTopWriteInvalidated)

			requireStoreValue(t, tc.store, "same-key", "user-value", "user-v2")
		})
	}
}

func TestCache_NewerAbortedSetDoesNotInvalidateOlderSetForBuiltInStagingStores(t *testing.T) {
	for _, tc := range topWriteLivenessStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := daramjwee.New(nil, daramjwee.WithTiers(tc.store), daramjwee.WithOpTimeout(2*time.Second))
			require.NoError(t, err)
			defer cache.Close()

			older := beginSetWithin(t, cache, "same-key", "older-v1", topWriteLivenessTimeout)
			t.Cleanup(func() { _ = older.Abort() })
			_, err = older.Write([]byte("older-value"))
			require.NoError(t, err)

			newer := beginSetWithin(t, cache, "same-key", "newer-v2", topWriteLivenessTimeout)
			_, err = newer.Write([]byte("newer-value"))
			require.NoError(t, err)
			require.NoError(t, newer.Abort())

			require.NoError(t, closeSinkWithin(t, older, topWriteLivenessTimeout))
			requireStoreValue(t, tc.store, "same-key", "older-value", "older-v1")
		})
	}
}

func TestCache_OlderSetCanPublishWhileNewerSetIsStillStagedForBuiltInStagingStores(t *testing.T) {
	for _, tc := range topWriteLivenessStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := daramjwee.New(nil, daramjwee.WithTiers(tc.store), daramjwee.WithOpTimeout(2*time.Second))
			require.NoError(t, err)
			defer cache.Close()

			older := beginSetWithin(t, cache, "same-key", "older-v1", topWriteLivenessTimeout)
			t.Cleanup(func() { _ = older.Abort() })
			_, err = older.Write([]byte("older-value"))
			require.NoError(t, err)

			newer := beginSetWithin(t, cache, "same-key", "newer-v2", topWriteLivenessTimeout)
			t.Cleanup(func() { _ = newer.Abort() })
			_, err = newer.Write([]byte("newer-value"))
			require.NoError(t, err)

			require.NoError(t, closeSinkWithin(t, older, topWriteLivenessTimeout))
			requireStoreValue(t, tc.store, "same-key", "older-value", "older-v1")

			require.NoError(t, newer.Abort())
			requireStoreValue(t, tc.store, "same-key", "older-value", "older-v1")
		})
	}
}

func TestCache_StaleSetCloseReportsInvalidatedForBuiltInStagingStores(t *testing.T) {
	for _, tc := range topWriteLivenessStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := daramjwee.New(nil, daramjwee.WithTiers(tc.store), daramjwee.WithOpTimeout(2*time.Second))
			require.NoError(t, err)
			defer cache.Close()

			older := beginSetWithin(t, cache, "same-key", "older-v1", topWriteLivenessTimeout)
			t.Cleanup(func() { _ = older.Abort() })
			_, err = older.Write([]byte("older-value"))
			require.NoError(t, err)

			setAndCloseWithin(t, cache, "same-key", "newer-value", "newer-v2", topWriteLivenessTimeout)

			require.ErrorIs(t, closeSinkWithin(t, older, topWriteLivenessTimeout), daramjwee.ErrTopWriteInvalidated)
			requireStoreValue(t, tc.store, "same-key", "newer-value", "newer-v2")
		})
	}
}

func TestCache_AbortedSetDoesNotPoisonLaterMissFillForBuiltInStagingStores(t *testing.T) {
	for _, tc := range topWriteLivenessStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := daramjwee.New(nil, daramjwee.WithTiers(tc.store), daramjwee.WithOpTimeout(2*time.Second))
			require.NoError(t, err)
			defer cache.Close()

			aborted := beginSetWithin(t, cache, "same-key", "aborted-v1", topWriteLivenessTimeout)
			_, err = aborted.Write([]byte("aborted-value"))
			require.NoError(t, err)
			require.NoError(t, aborted.Abort())

			source := newBlockingReadCloser([]byte("origin"), []byte("-value"))
			source.Release()
			fetcher := &blockingSourceFetcher{source: source, metadata: &daramjwee.Metadata{CacheTag: "origin-v2"}}
			resp, err := cache.Get(context.Background(), "same-key", daramjwee.GetRequest{}, fetcher)
			require.NoError(t, err)
			body, err := io.ReadAll(resp)
			require.NoError(t, err)
			require.Equal(t, "origin-value", string(body))
			require.NoError(t, resp.Close())

			requireStoreValue(t, tc.store, "same-key", "origin-value", "origin-v2")
		})
	}
}

func TestCache_CanceledSetContextDoesNotPublishContextAwareStagingStores(t *testing.T) {
	for _, tc := range contextAwareTopWriteLivenessStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := daramjwee.New(nil, daramjwee.WithTiers(tc.store), daramjwee.WithOpTimeout(2*time.Second))
			require.NoError(t, err)
			defer cache.Close()

			ctx, cancel := context.WithCancel(context.Background())
			sink, err := cache.Set(ctx, "same-key", &daramjwee.Metadata{CacheTag: "canceled-v1"})
			require.NoError(t, err)
			_, err = sink.Write([]byte("canceled-value"))
			require.NoError(t, err)
			cancel()

			require.ErrorIs(t, closeSinkWithin(t, sink, topWriteLivenessTimeout), context.Canceled)
			_, _, err = tc.store.GetStream(context.Background(), "same-key")
			require.ErrorIs(t, err, daramjwee.ErrNotFound)
		})
	}
}

func FuzzCacheTopWriteLiveness(f *testing.F) {
	f.Add([]byte{0, 3, 1, 4, 3, 5, 2, 6})
	f.Add([]byte{4, 3, 4, 3, 5, 6, 0, 3, 1})
	f.Add([]byte{0, 0, 3, 6, 1, 2, 4, 5})

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 18 {
			ops = ops[:18]
		}

		hot := memstore.New(0, nil)
		cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
		require.NoError(t, err)
		defer cache.Close()

		keys := []string{"liveness-key-0", "liveness-key-1", "liveness-key-2"}
		var openSinks []daramjwee.WriteSink
		var openStreams []openFuzzStream
		defer func() {
			for _, sink := range openSinks {
				_ = sink.Abort()
			}
			for _, stream := range openStreams {
				stream.source.Release()
				_ = stream.resp.Close()
			}
		}()

		for i, op := range ops {
			key := keys[int(op>>3)%len(keys)]
			value := fmt.Sprintf("value-%d-%d", i, op)

			switch op % 7 {
			case 0:
				sink := beginSetWithin(t, cache, key, value, topWriteLivenessTimeout)
				_, err := sink.Write([]byte(value))
				require.NoError(t, err)
				openSinks = append(openSinks, sink)
				if len(openSinks) > 4 {
					_ = openSinks[0].Abort()
					openSinks = openSinks[1:]
				}
			case 1:
				if len(openSinks) == 0 {
					continue
				}
				last := openSinks[len(openSinks)-1]
				openSinks = openSinks[:len(openSinks)-1]
				err := closeSinkWithin(t, last, topWriteLivenessTimeout)
				if err != nil && !errors.Is(err, daramjwee.ErrTopWriteInvalidated) {
					t.Fatalf("close open sink: %v", err)
				}
			case 2:
				if len(openSinks) == 0 {
					continue
				}
				last := openSinks[len(openSinks)-1]
				openSinks = openSinks[:len(openSinks)-1]
				require.NoError(t, last.Abort())
			case 3:
				setAndCloseWithin(t, cache, key, value, value, topWriteLivenessTimeout)
			case 4:
				stream := beginPartialMissWithin(t, cache, key, value, topWriteLivenessTimeout)
				openStreams = append(openStreams, stream)
				if len(openStreams) > 4 {
					closeFuzzStreamWithin(t, openStreams[0], topWriteLivenessTimeout)
					openStreams = openStreams[1:]
				}
			case 5:
				if len(openStreams) == 0 {
					continue
				}
				last := openStreams[len(openStreams)-1]
				openStreams = openStreams[:len(openStreams)-1]
				closeFuzzStreamWithin(t, last, topWriteLivenessTimeout)
			case 6:
				deleteWithin(t, cache, key, topWriteLivenessTimeout)
			}
		}
	})
}

type topWriteLivenessStore struct {
	name  string
	store daramjwee.Store
}

func topWriteLivenessStores(t *testing.T) []topWriteLivenessStore {
	t.Helper()

	fileStore, err := filestore.New(t.TempDir(), log.NewNopLogger())
	require.NoError(t, err)

	objectStore := objectstore.New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		objectstore.WithDir(t.TempDir()),
	)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	return []topWriteLivenessStore{
		{name: "memstore", store: memstore.New(0, nil)},
		{name: "filestore", store: fileStore},
		{name: "objectstore", store: objectStore},
		{name: "redisstore", store: redisstore.New(client, log.NewNopLogger())},
	}
}

func contextAwareTopWriteLivenessStores(t *testing.T) []topWriteLivenessStore {
	t.Helper()

	objectStore := objectstore.New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		objectstore.WithDir(t.TempDir()),
	)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	return []topWriteLivenessStore{
		{name: "objectstore", store: objectStore},
		{name: "redisstore", store: redisstore.New(client, log.NewNopLogger())},
	}
}

func setAndCloseWithin(t *testing.T, cache daramjwee.Cache, key, value, tag string, timeout time.Duration) {
	t.Helper()
	sink := beginSetWithin(t, cache, key, tag, timeout)
	_, err := sink.Write([]byte(value))
	require.NoError(t, err)
	require.NoError(t, closeSinkWithin(t, sink, timeout))
}

func beginSetWithin(t *testing.T, cache daramjwee.Cache, key, tag string, timeout time.Duration) daramjwee.WriteSink {
	t.Helper()

	type result struct {
		sink daramjwee.WriteSink
		err  error
	}
	done := make(chan result, 1)
	go func() {
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: tag})
		done <- result{sink: sink, err: err}
	}()

	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.NotNil(t, result.sink)
		return result.sink
	case <-time.After(timeout * 2):
		t.Fatalf("cache.Set(%q) blocked past %s", key, timeout)
		return nil
	}
}

func closeSinkWithin(t *testing.T, sink daramjwee.WriteSink, timeout time.Duration) error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- sink.Close()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout * 2):
		t.Fatalf("sink.Close blocked past %s", timeout)
		return nil
	}
}

func deleteWithin(t *testing.T, cache daramjwee.Cache, key string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- cache.Delete(ctx, key)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(timeout * 2):
		t.Fatalf("cache.Delete(%q) blocked past %s", key, timeout)
	}
}

type openFuzzStream struct {
	resp   *daramjwee.GetResponse
	source *blockingReadCloser
}

func beginPartialMissWithin(t *testing.T, cache daramjwee.Cache, key, tag string, timeout time.Duration) openFuzzStream {
	t.Helper()

	source := newBlockingReadCloser([]byte("origin"), []byte("-"+tag))
	fetcher := &blockingSourceFetcher{source: source, metadata: &daramjwee.Metadata{CacheTag: tag}}

	type result struct {
		resp *daramjwee.GetResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, fetcher)
		done <- result{resp: resp, err: err}
	}()

	var got result
	select {
	case got = <-done:
		require.NoError(t, got.err)
		require.NotNil(t, got.resp)
	case <-time.After(timeout * 2):
		t.Fatalf("cache.Get(%q) blocked before returning miss stream", key)
	}

	buf := make([]byte, len("origin"))
	readDone := make(chan error, 1)
	go func() {
		n, err := io.ReadFull(got.resp, buf)
		if err == nil && n != len(buf) {
			err = io.ErrUnexpectedEOF
		}
		readDone <- err
	}()

	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(timeout * 2):
		t.Fatalf("first read for %q blocked past %s", key, timeout)
	}

	return openFuzzStream{resp: got.resp, source: source}
}

func closeFuzzStreamWithin(t *testing.T, stream openFuzzStream, timeout time.Duration) {
	t.Helper()

	stream.source.Release()
	done := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(stream.resp)
		closeErr := stream.resp.Close()
		done <- errors.Join(readErr, closeErr)
	}()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, daramjwee.ErrTopWriteInvalidated) {
			t.Fatalf("close fuzz stream: %v", err)
		}
	case <-time.After(timeout * 2):
		t.Fatalf("stream close blocked past %s", timeout)
	}
}

func requireStoreValue(t *testing.T, store daramjwee.Store, key, wantValue, wantTag string) {
	t.Helper()

	reader, meta, err := store.GetStream(context.Background(), key)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, wantValue, string(body))
	require.NotNil(t, meta)
	require.Equal(t, wantTag, meta.CacheTag)
}
