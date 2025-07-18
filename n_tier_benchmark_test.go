package daramjwee

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
)

// BenchmarkNTierVs2Tier compares performance between N-tier and traditional 2-tier setups
func BenchmarkNTierVs2Tier(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("2-tier-cache-hits", func(b *testing.B) {
		// Traditional 2-tier setup (hot + cold)
		hotStore := newMockStore()
		coldStore := newMockStore()

		cache, err := New(logger, WithStores(hotStore, coldStore))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		// Pre-populate hot store
		testData := []byte("benchmark data")
		hotStore.data["bench-key"] = testData
		hotStore.meta["bench-key"] = &Metadata{
			ETag:     "bench-etag",
			CachedAt: time.Now(),
		}

		fetcher := &benchmarkFetcher{data: "should not be called"}
		ctx := context.Background()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				reader, err := cache.Get(ctx, "bench-key", fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
			}
		})
	})

	b.Run("3-tier-cache-hits", func(b *testing.B) {
		// N-tier setup (memory + file + cloud)
		memoryStore := newMockStore()
		fileStore := newMockStore()
		cloudStore := newMockStore()

		cache, err := New(logger, WithStores(memoryStore, fileStore, cloudStore))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		// Pre-populate memory store
		testData := []byte("benchmark data")
		memoryStore.data["bench-key"] = testData
		memoryStore.meta["bench-key"] = &Metadata{
			ETag:     "bench-etag",
			CachedAt: time.Now(),
		}

		fetcher := &benchmarkFetcher{data: "should not be called"}
		ctx := context.Background()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				reader, err := cache.Get(ctx, "bench-key", fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
			}
		})
	})

	b.Run("5-tier-cache-hits", func(b *testing.B) {
		// Extended N-tier setup
		stores := make([]Store, 5)
		for i := 0; i < 5; i++ {
			stores[i] = newMockStore()
		}

		cache, err := New(logger, WithStores(stores...))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		// Pre-populate primary store
		testData := []byte("benchmark data")
		stores[0].(*mockStore).data["bench-key"] = testData
		stores[0].(*mockStore).meta["bench-key"] = &Metadata{
			ETag:     "bench-etag",
			CachedAt: time.Now(),
		}

		fetcher := &benchmarkFetcher{data: "should not be called"}
		ctx := context.Background()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				reader, err := cache.Get(ctx, "bench-key", fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
			}
		})
	})
}

// BenchmarkTierPromotion tests promotion performance across different tier configurations
func BenchmarkTierPromotion(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("2-tier-promotion", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: "should not be called"}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("promo-key-%d", i%1000)

				// Pre-populate tier1 only
				testData := []byte("promotion data")
				tier1.data[key] = testData
				tier1.meta[key] = &Metadata{
					ETag:     "promo-etag",
					CachedAt: time.Now(),
				}

				reader, err := cache.Get(ctx, key, fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
				i++
			}
		})
	})

	b.Run("3-tier-promotion", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()
		tier2 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1, tier2))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: "should not be called"}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("promo-key-%d", i%1000)

				// Pre-populate tier2 only
				testData := []byte("promotion data")
				tier2.data[key] = testData
				tier2.meta[key] = &Metadata{
					ETag:     "promo-etag",
					CachedAt: time.Now(),
				}

				reader, err := cache.Get(ctx, key, fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
				i++
			}
		})
	})

	b.Run("5-tier-promotion", func(b *testing.B) {
		stores := make([]Store, 5)
		for i := 0; i < 5; i++ {
			stores[i] = newMockStore()
		}

		cache, err := New(logger, WithStores(stores...))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: "should not be called"}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("promo-key-%d", i%1000)

				// Pre-populate last tier only
				testData := []byte("promotion data")
				stores[4].(*mockStore).data[key] = testData
				stores[4].(*mockStore).meta[key] = &Metadata{
					ETag:     "promo-etag",
					CachedAt: time.Now(),
				}

				reader, err := cache.Get(ctx, key, fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
				i++
			}
		})
	})
}

// BenchmarkConcurrentAccess tests concurrent access patterns
func BenchmarkNTierConcurrentAccess(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("concurrent-gets-2-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		// Pre-populate with test data
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("concurrent-key-%d", i)
			testData := []byte(fmt.Sprintf("concurrent data %d", i))
			tier0.data[key] = testData
			tier0.meta[key] = &Metadata{
				ETag:     fmt.Sprintf("etag-%d", i),
				CachedAt: time.Now(),
			}
		}

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: "should not be called"}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("concurrent-key-%d", i%100)
				reader, err := cache.Get(ctx, key, fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
				i++
			}
		})
	})

	b.Run("concurrent-gets-3-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()
		tier2 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1, tier2))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		// Pre-populate with test data
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("concurrent-key-%d", i)
			testData := []byte(fmt.Sprintf("concurrent data %d", i))
			tier0.data[key] = testData
			tier0.meta[key] = &Metadata{
				ETag:     fmt.Sprintf("etag-%d", i),
				CachedAt: time.Now(),
			}
		}

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: "should not be called"}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("concurrent-key-%d", i%100)
				reader, err := cache.Get(ctx, key, fetcher)
				if err != nil {
					b.Errorf("Get failed: %v", err)
					continue
				}
				io.Copy(io.Discard, reader)
				reader.Close()
				i++
			}
		})
	})
}

// BenchmarkMemoryAllocation tests memory allocation patterns
func BenchmarkMemoryAllocation(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("memory-allocation-2-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: "origin data for memory test"}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("mem-key-%d", i%10)
			reader, err := cache.Get(ctx, key, fetcher)
			if err != nil {
				b.Errorf("Get failed: %v", err)
				continue
			}
			io.Copy(io.Discard, reader)
			reader.Close()
		}
	})

	b.Run("memory-allocation-3-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()
		tier2 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1, tier2))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: "origin data for memory test"}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("mem-key-%d", i%10)
			reader, err := cache.Get(ctx, key, fetcher)
			if err != nil {
				b.Errorf("Get failed: %v", err)
				continue
			}
			io.Copy(io.Discard, reader)
			reader.Close()
		}
	})
}

// BenchmarkSetOperations tests Set operation performance across different tier configurations
func BenchmarkSetOperations(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("set-2-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		testData := "benchmark set data"

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("set-key-%d", i%1000)
				writer, err := cache.Set(ctx, key, &Metadata{ETag: "set-etag"})
				if err != nil {
					b.Errorf("Set failed: %v", err)
					continue
				}
				writer.Write([]byte(testData))
				writer.Close()
				i++
			}
		})
	})

	b.Run("set-3-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()
		tier2 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1, tier2))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		testData := "benchmark set data"

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("set-key-%d", i%1000)
				writer, err := cache.Set(ctx, key, &Metadata{ETag: "set-etag"})
				if err != nil {
					b.Errorf("Set failed: %v", err)
					continue
				}
				writer.Write([]byte(testData))
				writer.Close()
				i++
			}
		})
	})
}

// BenchmarkDeleteOperations tests Delete operation performance
func BenchmarkDeleteOperations(b *testing.B) {
	logger := log.NewNopLogger()

	b.Run("delete-2-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("delete-key-%d", i)

			// Pre-populate stores
			testData := []byte("delete test data")
			tier0.data[key] = testData
			tier1.data[key] = testData

			err := cache.Delete(ctx, key)
			if err != nil {
				b.Errorf("Delete failed: %v", err)
			}
		}
	})

	b.Run("delete-3-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()
		tier2 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1, tier2))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("delete-key-%d", i)

			// Pre-populate stores
			testData := []byte("delete test data")
			tier0.data[key] = testData
			tier1.data[key] = testData
			tier2.data[key] = testData

			err := cache.Delete(ctx, key)
			if err != nil {
				b.Errorf("Delete failed: %v", err)
			}
		}
	})
}

// benchmarkFetcher implements Fetcher interface for benchmarking
type benchmarkFetcher struct {
	data   string
	called bool
	mu     sync.Mutex
}

func (f *benchmarkFetcher) Fetch(ctx context.Context, metadata *Metadata) (*FetchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.called = true
	return &FetchResult{
		Body: io.NopCloser(strings.NewReader(f.data)),
		Metadata: &Metadata{
			ETag:     "benchmark-etag",
			CachedAt: time.Now(),
		},
	}, nil
}
