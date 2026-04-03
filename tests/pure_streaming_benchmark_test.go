package daramjwee_test

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
)

const (
	benchmarkSmallPayload = 32 * 1024
	benchmarkLargePayload = 1 * 1024 * 1024
	benchmarkFirstByteLag = 2 * time.Millisecond
)

func BenchmarkCacheGet_HotHitDefaultTTL(b *testing.B) {
	benchmarkHotHitDefaultTTL(b, benchmarkSmallPayload)
}

func BenchmarkCacheGet_HotHitFresh(b *testing.B) {
	benchmarkHotHitFresh(b, benchmarkSmallPayload)
}

func BenchmarkCacheGet_HotHitStale(b *testing.B) {
	benchmarkHotHitStale(b, benchmarkSmallPayload)
}

func BenchmarkCacheGet_LowerTierHit(b *testing.B) {
	benchmarkLowerTierHitStreamThrough(b, benchmarkSmallPayload)
}

func BenchmarkCacheGet_LowerTierHit_1MiB(b *testing.B) {
	benchmarkLowerTierHitStreamThrough(b, benchmarkLargePayload)
}

func BenchmarkCacheGet_Miss(b *testing.B) {
	benchmarkMissStreamThrough(b, benchmarkSmallPayload)
}

func BenchmarkCacheGet_Miss_1MiB(b *testing.B) {
	benchmarkMissStreamThrough(b, benchmarkLargePayload)
}

func BenchmarkCacheGet_Miss_1MiB_PrebuiltFetcher(b *testing.B) {
	benchmarkMissStreamThroughWithFixtures(b, bytes.Repeat([]byte("c"), benchmarkLargePayload), newMockStore(), benchmarkFetcherModePrebuilt)
}

func BenchmarkCacheGet_Miss_1MiB_DirectSink(b *testing.B) {
	benchmarkMissStreamThroughWithFixtures(b, bytes.Repeat([]byte("c"), benchmarkLargePayload), &benchmarkDirectSinkStore{}, benchmarkFetcherModeString)
}

func BenchmarkCacheGet_Miss_1MiB_PrebuiltFetcher_DirectSink(b *testing.B) {
	benchmarkMissStreamThroughWithFixtures(b, bytes.Repeat([]byte("c"), benchmarkLargePayload), &benchmarkDirectSinkStore{}, benchmarkFetcherModePrebuilt)
}

func BenchmarkCacheGet_MissPartialReadAbort(b *testing.B) {
	hot := newMockStore()
	payload := strings.Repeat("p", benchmarkLargePayload)

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	buf := make([]byte, 4*1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := benchmarkKey("partial-miss", i)
		fetcher := &mockFetcher{content: payload, etag: "v1"}

		stream, err := cache.Get(context.Background(), key, fetcher)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if _, err := io.ReadFull(stream, buf); err != nil {
			b.Fatalf("read: %v", err)
		}
		if err := stream.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

func BenchmarkCacheGet_MissFirstRead(b *testing.B) {
	hot := newMockStore()

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	buf := make([]byte, 1)
	rest := bytes.Repeat([]byte("m"), benchmarkSmallPayload-1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := benchmarkKey("first-byte-miss", i)
		fetcher := &benchmarkDelayedFetcher{
			first: []byte("m"),
			rest:  rest,
			delay: benchmarkFirstByteLag,
			etag:  "v1",
		}

		stream, err := cache.Get(context.Background(), key, fetcher)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if _, err := io.ReadFull(stream, buf); err != nil {
			b.Fatalf("read first byte: %v", err)
		}
		b.StopTimer()
		if err := stream.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
		b.StartTimer()
	}
}

func BenchmarkCacheGet_LowerTierFirstRead(b *testing.B) {
	hot := newMockStore()
	cold := &benchmarkDelayedColdStore{
		first:    []byte("c"),
		rest:     bytes.Repeat([]byte("c"), benchmarkSmallPayload-1),
		delay:    benchmarkFirstByteLag,
		metadata: &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()},
	}

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	buf := make([]byte, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := benchmarkKey("first-byte-cold", i)
		stream, err := cache.Get(context.Background(), key, &mockFetcher{})
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if _, err := io.ReadFull(stream, buf); err != nil {
			b.Fatalf("read first byte: %v", err)
		}
		b.StopTimer()
		if err := stream.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
		b.StartTimer()
	}
}

func benchmarkHotHitDefaultTTL(b *testing.B, payloadSize int) {
	hot := newMockStore()
	payload := strings.Repeat("a", payloadSize)
	hot.setData("bench-key", payload, &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)

	runReadAllBenchmark(b, func(i int) (io.ReadCloser, error) {
		return cache.Get(context.Background(), "bench-key", &mockFetcher{})
	})
}

func benchmarkHotHitFresh(b *testing.B, payloadSize int) {
	hot := newMockStore()
	payload := strings.Repeat("f", payloadSize)
	hot.setData("fresh-key", payload, &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot),
		daramjwee.WithFreshness(time.Hour, 0),
		daramjwee.WithOpTimeout(2*time.Second),
	)

	runReadAllBenchmark(b, func(i int) (io.ReadCloser, error) {
		return cache.Get(context.Background(), "fresh-key", &mockFetcher{})
	})
}

func benchmarkHotHitStale(b *testing.B, payloadSize int) {
	hot := newMockStore()
	payload := strings.Repeat("s", payloadSize)
	hot.setData("stale-key", payload, &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now().Add(-time.Hour)})

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot),
		daramjwee.WithFreshness(time.Nanosecond, 0),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(1_000_000),
		daramjwee.WithWorkerTimeout(time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	runReadAllBenchmark(b, func(i int) (io.ReadCloser, error) {
		return cache.Get(context.Background(), "stale-key", fetcher)
	})
}

func benchmarkLowerTierHitStreamThrough(b *testing.B, payloadSize int) {
	hot := newMockStore()
	cold := newMockStore()
	payload := strings.Repeat("b", payloadSize)

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithOpTimeout(2*time.Second),
	)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := benchmarkKey("cold-key", i)
		cold.setData(key, payload, &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

		stream, err := cache.Get(context.Background(), key, &mockFetcher{})
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if _, err := io.Copy(io.Discard, stream); err != nil {
			b.Fatalf("copy: %v", err)
		}
		if err := stream.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

func benchmarkMissStreamThrough(b *testing.B, payloadSize int) {
	hot := newMockStore()
	payload := strings.Repeat("c", payloadSize)

	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := benchmarkKey("miss-key", i)
		fetcher := &mockFetcher{content: payload, etag: "v1"}

		stream, err := cache.Get(context.Background(), key, fetcher)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if _, err := io.Copy(io.Discard, stream); err != nil {
			b.Fatalf("copy: %v", err)
		}
		if err := stream.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

type benchmarkFetcherMode int

const (
	benchmarkFetcherModeString benchmarkFetcherMode = iota
	benchmarkFetcherModePrebuilt
)

func benchmarkMissStreamThroughWithFixtures(b *testing.B, payload []byte, hot daramjwee.Store, mode benchmarkFetcherMode) {
	cache := mustNewBenchmarkCache(
		b,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)

	var (
		payloadString string
		fetcher       daramjwee.Fetcher
	)
	switch mode {
	case benchmarkFetcherModePrebuilt:
		fetcher = &benchmarkBytesFetcher{body: payload, etag: "v1"}
	default:
		payloadString = string(payload)
		fetcher = &mockFetcher{content: payloadString, etag: "v1"}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := benchmarkKey("miss-fixture", i)
		stream, err := cache.Get(context.Background(), key, fetcher)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if _, err := io.Copy(io.Discard, stream); err != nil {
			b.Fatalf("copy: %v", err)
		}
		if err := stream.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

func mustNewBenchmarkCache(b *testing.B, opts ...daramjwee.Option) daramjwee.Cache {
	b.Helper()

	cache, err := daramjwee.New(nil, opts...)
	if err != nil {
		b.Fatalf("new cache: %v", err)
	}
	b.Cleanup(cache.Close)
	return cache
}

func runReadAllBenchmark(b *testing.B, getStream func(i int) (io.ReadCloser, error)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stream, err := getStream(i)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if _, err := io.Copy(io.Discard, stream); err != nil {
			b.Fatalf("copy: %v", err)
		}
		if err := stream.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

func benchmarkKey(prefix string, i int) string {
	return prefix + "-" + strconv.Itoa(i)
}

type benchmarkDelayedFetcher struct {
	first []byte
	rest  []byte
	delay time.Duration
	etag  string
}

func (f *benchmarkDelayedFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     newBenchmarkDelayedReadCloser(f.first, f.rest, f.delay),
		Metadata: &daramjwee.Metadata{ETag: f.etag},
	}, nil
}

type benchmarkBytesFetcher struct {
	body []byte
	etag string
}

func (f *benchmarkBytesFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader(f.body)),
		Metadata: &daramjwee.Metadata{ETag: f.etag},
	}, nil
}

type benchmarkDirectSinkStore struct{}

func (s *benchmarkDirectSinkStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, daramjwee.ErrNotFound
}

func (s *benchmarkDirectSinkStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &benchmarkDiscardSink{}, nil
}

func (s *benchmarkDirectSinkStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *benchmarkDirectSinkStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return nil, daramjwee.ErrNotFound
}

type benchmarkDelayedColdStore struct {
	first    []byte
	rest     []byte
	delay    time.Duration
	metadata *daramjwee.Metadata
}

func (s *benchmarkDelayedColdStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return newBenchmarkDelayedReadCloser(s.first, s.rest, s.delay), s.metadata, nil
}

func (s *benchmarkDelayedColdStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &benchmarkDiscardSink{}, nil
}

func (s *benchmarkDelayedColdStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *benchmarkDelayedColdStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.metadata, nil
}

type benchmarkDiscardSink struct{}

func (s *benchmarkDiscardSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *benchmarkDiscardSink) Close() error                { return nil }
func (s *benchmarkDiscardSink) Abort() error                { return nil }

type benchmarkDelayedReadCloser struct {
	first []byte
	rest  []byte
	delay time.Duration
	stage int
}

func newBenchmarkDelayedReadCloser(first, rest []byte, delay time.Duration) *benchmarkDelayedReadCloser {
	return &benchmarkDelayedReadCloser{
		first: bytes.Clone(first),
		rest:  bytes.Clone(rest),
		delay: delay,
	}
}

func (r *benchmarkDelayedReadCloser) Read(p []byte) (int, error) {
	switch r.stage {
	case 0:
		r.stage = 1
		return copy(p, r.first), nil
	case 1:
		if r.delay > 0 {
			time.Sleep(r.delay)
		}
		r.stage = 2
		return copy(p, r.rest), nil
	default:
		return 0, io.EOF
	}
}

func (r *benchmarkDelayedReadCloser) Close() error { return nil }
