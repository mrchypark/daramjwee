package objectstore

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/thanos-io/objstore"
)

func BenchmarkStore_ReadLocalPublished(b *testing.B) {
	store := benchmarkLocalPublishedStore(b)
	benchmarkReadAll(b, store, "bench-key")
}

func BenchmarkStore_ReadRemotePackedCold(b *testing.B) {
	store := benchmarkRemotePackedStore(b, false)
	benchmarkReadAll(b, store, "bench-key")
}

func BenchmarkStore_ReadRemotePackedWarm(b *testing.B) {
	store := benchmarkRemotePackedStore(b, true)
	stream, _, err := store.GetStream(context.Background(), "bench-key")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, stream); err != nil {
		b.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		b.Fatal(err)
	}

	benchmarkReadAll(b, store, "bench-key")
}

func benchmarkLocalPublishedStore(b *testing.B) *Store {
	store := New(objstore.NewInMemBucket(), log.NewNopLogger(),
		WithPackedObjectThreshold(1<<20),
		WithPageSize(32<<10),
		WithDataDir(b.TempDir()),
	)
	store.autoFlush = false
	writer, err := store.BeginSet(context.Background(), "bench-key", &daramjwee.Metadata{ETag: "bench"})
	if err != nil {
		panic(err)
	}
	if _, err := writer.Write(bytes.Repeat([]byte("0123456789abcdef"), 1<<16)); err != nil {
		panic(err)
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	return store
}

func benchmarkRemotePackedStore(b *testing.B, enableCache bool) *Store {
	bucket := objstore.NewInMemBucket()
	opts := []Option{
		WithPackedObjectThreshold(1 << 20),
		WithPageSize(32 << 10),
		WithDataDir(b.TempDir()),
	}
	if enableCache {
		opts = append(opts, WithMemoryBlockCache(4<<20))
	}

	store := New(bucket, log.NewNopLogger(), opts...)
	store.autoFlush = false
	writer, err := store.BeginSet(context.Background(), "bench-key", &daramjwee.Metadata{ETag: "bench"})
	if err != nil {
		panic(err)
	}
	if _, err := writer.Write(bytes.Repeat([]byte("0123456789abcdef"), 1<<16)); err != nil {
		panic(err)
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	if err := store.flushPending(context.Background()); err != nil {
		panic(err)
	}

	remoteOpts := []Option{
		WithPackedObjectThreshold(1 << 20),
		WithPageSize(32 << 10),
		WithDataDir(b.TempDir()),
	}
	if enableCache {
		remoteOpts = append(remoteOpts, WithMemoryBlockCache(4<<20))
	}
	remote := New(bucket, log.NewNopLogger(), remoteOpts...)
	remote.autoFlush = false
	return remote
}

func benchmarkReadAll(b *testing.B, store *Store, key string) {
	b.Helper()
	b.ReportAllocs()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		stream, _, err := store.GetStream(ctx, key)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, stream); err != nil {
			b.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
