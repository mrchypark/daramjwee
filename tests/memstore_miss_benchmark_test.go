package daramjwee_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// BenchmarkMissMemStoreLargePayload measures the miss path allocation cost
// with the real MemStore backend for a 1MiB payload.
func BenchmarkMissMemStoreLargePayload(b *testing.B) {
	hot := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	if err != nil {
		b.Fatal(err)
	}

	payload := bytes.Repeat([]byte("c"), 1<<20)
	fetcher := &benchmarkBytesFetcher{body: payload, etag: "v1"}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := benchmarkKey("memstore-miss", i)
		stream, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, fetcher)
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
