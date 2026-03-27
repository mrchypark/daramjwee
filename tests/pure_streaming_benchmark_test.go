package daramjwee_test

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
)

func BenchmarkCacheGet_HotHit(b *testing.B) {
	hot := newMockStore()
	payload := strings.Repeat("a", 32*1024)
	hot.setData("bench-key", payload, &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(2*time.Second))
	if err != nil {
		b.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stream, err := cache.Get(context.Background(), "bench-key", &mockFetcher{})
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

func BenchmarkCacheGet_ColdHitStreamThrough(b *testing.B) {
	hot := newMockStore()
	cold := newMockStore()
	payload := strings.Repeat("b", 32*1024)

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithHotStore(hot),
		daramjwee.WithColdStore(cold),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	if err != nil {
		b.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "cold-key-" + strconv.Itoa(i)
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

func BenchmarkCacheGet_MissStreamThrough(b *testing.B) {
	hot := newMockStore()
	payload := strings.Repeat("c", 32*1024)

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithDefaultTimeout(2*time.Second))
	if err != nil {
		b.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "miss-key-" + strconv.Itoa(i)
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
