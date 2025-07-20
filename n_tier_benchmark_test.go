package daramjwee

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	os "os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
)

// BenchmarkNTierVs2Tier compares performance between N-tier and traditional 2-tier setups
func BenchmarkNTierVs2Tier(b *testing.B) {
	logger := log.NewNopLogger()

	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
		{"XLarge_1MB", 1024 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			// Generate test data of specified size
			testData := make([]byte, ds.size)
			_, err := rand.Read(testData)
			if err != nil {
				b.Fatalf("Failed to generate test data: %v", err)
			}

			b.Run("2-tier-primary-hits", func(b *testing.B) {
				// Traditional 2-tier setup (hot + cold)
				hotStore := newMockStore()
				coldStore := newMockStore()

				cache, err := New(logger, WithStores(hotStore, coldStore))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate hot store
				writer, err := cache.Set(context.Background(), "bench-key", &Metadata{ETag: "bench-etag", CachedAt: time.Now()})
				if err != nil {
					b.Fatalf("Failed to set data: %v", err)
				}
				_, err = writer.Write(testData)
				if err != nil {
					b.Fatalf("Failed to write data: %v", err)
				}
				err = writer.Close()
				if err != nil {
					b.Fatalf("Failed to close writer: %v", err)
				}

				fetcher := &benchmarkFetcher{data: "should not be called"}
				ctx := context.Background()

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
			})

			b.Run("3-tier-primary-hits", func(b *testing.B) {
				// N-tier setup (memory + file + cloud)
				memoryStore := newMockStore()
				fileStore := newMockStore()
				cloudStore := newMockStore()

				// Clean up the test_file_store directory before each benchmark run
				b.Cleanup(func() {
					os.RemoveAll("test_file_store")
				})

				cache, err := New(logger, WithStores(memoryStore, fileStore, cloudStore))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate memory store
				memoryStore.data["bench-key"] = testData
				memoryStore.meta["bench-key"] = &Metadata{
					ETag:     "bench-etag",
					CachedAt: time.Now(),
				}

				fetcher := &benchmarkFetcher{data: "should not be called"}
				ctx := context.Background()

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
			})

			b.Run("5-tier-primary-hits", func(b *testing.B) {
				// Extended N-tier setup
				stores := make([]Store, 5)
				for i := range 5 {
					stores[i] = newMockStore()
				}

				cache, err := New(logger, WithStores(stores...))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate primary store
				stores[0].(*mockStore).data["bench-key"] = testData
				stores[0].(*mockStore).meta["bench-key"] = &Metadata{
					ETag:     "bench-etag",
					CachedAt: time.Now(),
				}

				fetcher := &benchmarkFetcher{data: "should not be called"}
				ctx := context.Background()

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
			})
		})
	}
}

// BenchmarkTierPromotion tests promotion performance across different tier configurations
func BenchmarkTierPromotion(b *testing.B) {
	logger := log.NewNopLogger()

	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			// Generate test data of specified size
			testData := make([]byte, ds.size)
			_, err := rand.Read(testData)
			if err != nil {
				b.Fatalf("Failed to generate test data: %v", err)
			}

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

				// Pre-populate tier1 with test data
				keys := make([]string, 1000)
				for i := range 1000 {
					key := fmt.Sprintf("promo-key-%d", i)
					keys[i] = key
					tier1.data[key] = testData
					tier1.meta[key] = &Metadata{
						ETag:     "promo-etag",
						CachedAt: time.Now(),
					}
				}

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := keys[i%len(keys)]

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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
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

				// Pre-populate tier2 with test data
				keys := make([]string, 1000)
				for i := range 1000 {
					key := fmt.Sprintf("promo-key-%d", i)
					keys[i] = key
					tier2.data[key] = testData
					tier2.meta[key] = &Metadata{
						ETag:     "promo-etag",
						CachedAt: time.Now(),
					}
				}

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := keys[i%len(keys)]

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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})

			b.Run("5-tier-promotion", func(b *testing.B) {
				stores := make([]Store, 5)
				for i := range 5 {
					stores[i] = newMockStore()
				}

				cache, err := New(logger, WithStores(stores...))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: "should not be called"}

				// Pre-populate last tier with test data
				keys := make([]string, 1000)
				for i := range 1000 {
					key := fmt.Sprintf("promo-key-%d", i)
					keys[i] = key
					stores[4].(*mockStore).data[key] = testData
					stores[4].(*mockStore).meta[key] = &Metadata{
						ETag:     "promo-etag",
						CachedAt: time.Now(),
					}
				}

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := keys[i%len(keys)]

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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})
		})
	}
}

// BenchmarkConcurrentAccess tests concurrent access patterns with realistic workloads
func BenchmarkNTierConcurrentAccess(b *testing.B) {
	logger := log.NewNopLogger()

	concurrencyLevels := []int{1, 4, 8, 16, 32}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			// Generate test data
			testData := make([]byte, 32*1024) // 32KB test data
			_, err := rand.Read(testData)
			if err != nil {
				b.Fatalf("Failed to generate test data: %v", err)
			}

			b.Run("2-tier-mixed-workload", func(b *testing.B) {
				tier0 := newMockStore()
				tier1 := newMockStore()

				cache, err := New(logger, WithStores(tier0, tier1))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate with mixed data distribution
				numKeys := 1000
				keys := make([]string, numKeys)
				for i := range numKeys {
					key := fmt.Sprintf("concurrent-key-%d", i)
					keys[i] = key

					switch i % 3 {
					case 0: // 33% in tier0 (hot hits)
						tier0.data[key] = testData
						tier0.meta[key] = &Metadata{
							ETag:     fmt.Sprintf("etag-%d", i),
							CachedAt: time.Now(),
						}
					case 1: // 33% in tier1 (cold hits, will be promoted)
						tier1.data[key] = testData
						tier1.meta[key] = &Metadata{
							ETag:     fmt.Sprintf("etag-%d", i),
							CachedAt: time.Now(),
						}
						// case 2: 33% cache miss
					}
				}

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.SetParallelism(concurrency)
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := keys[i%numKeys]
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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})

			b.Run("3-tier-mixed-workload", func(b *testing.B) {
				tier0 := newMockStore()
				tier1 := newMockStore()
				tier2 := newMockStore()

				cache, err := New(logger, WithStores(tier0, tier1, tier2))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate with mixed data distribution
				numKeys := 1000
				keys := make([]string, numKeys)
				for i := range numKeys {
					key := fmt.Sprintf("concurrent-key-%d", i)
					keys[i] = key

					switch i % 4 {
					case 0: // 25% in tier0 (hot hits)
						tier0.data[key] = testData
						tier0.meta[key] = &Metadata{
							ETag:     fmt.Sprintf("etag-%d", i),
							CachedAt: time.Now(),
						}
					case 1: // 25% in tier1 (warm hits, promote to tier0)
						tier1.data[key] = testData
						tier1.meta[key] = &Metadata{
							ETag:     fmt.Sprintf("etag-%d", i),
							CachedAt: time.Now(),
						}
					case 2: // 25% in tier2 (cold hits, promote to tier0+tier1)
						tier2.data[key] = testData
						tier2.meta[key] = &Metadata{
							ETag:     fmt.Sprintf("etag-%d", i),
							CachedAt: time.Now(),
						}
						// case 3: 25% cache miss
					}
				}

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.SetParallelism(concurrency)
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := keys[i%numKeys]
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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})

			b.Run("5-tier-mixed-workload", func(b *testing.B) {
				stores := make([]Store, 5)
				for i := range 5 {
					stores[i] = newMockStore()
				}

				cache, err := New(logger, WithStores(stores...))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate with mixed data distribution across all tiers
				numKeys := 1000
				keys := make([]string, numKeys)
				for i := range numKeys {
					key := fmt.Sprintf("concurrent-key-%d", i)
					keys[i] = key

					tierIndex := i % 6 // 0-4 for tiers, 5 for miss
					if tierIndex < 5 {
						stores[tierIndex].(*mockStore).data[key] = testData
						stores[tierIndex].(*mockStore).meta[key] = &Metadata{
							ETag:     fmt.Sprintf("etag-%d", i),
							CachedAt: time.Now(),
						}
					}
				}

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.SetParallelism(concurrency)
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := keys[i%numKeys]
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

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})
		})
	}
}

// BenchmarkMemoryAllocation tests memory allocation patterns with detailed profiling
func BenchmarkMemoryAllocation(b *testing.B) {
	logger := log.NewNopLogger()

	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
		{"XLarge_1MB", 1024 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			// Generate test data
			testData := make([]byte, ds.size)
			_, err := rand.Read(testData)
			if err != nil {
				b.Fatalf("Failed to generate test data: %v", err)
			}

			b.Run("2-tier-cache-miss", func(b *testing.B) {
				tier0 := newMockStore()
				tier1 := newMockStore()

				cache, err := New(logger, WithStores(tier0, tier1))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Force GC and measure baseline
				runtime.GC()
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					key := fmt.Sprintf("mem-key-%d", i%100)
					reader, err := cache.Get(ctx, key, fetcher)
					if err != nil {
						b.Errorf("Get failed: %v", err)
						continue
					}
					io.Copy(io.Discard, reader)
					reader.Close()
				}

				b.StopTimer()
				runtime.GC()
				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				// Report detailed memory metrics
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/float64(b.N), "alloc-per-op-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})

			b.Run("3-tier-cache-miss", func(b *testing.B) {
				tier0 := newMockStore()
				tier1 := newMockStore()
				tier2 := newMockStore()

				cache, err := New(logger, WithStores(tier0, tier1, tier2))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Force GC and measure baseline
				runtime.GC()
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					key := fmt.Sprintf("mem-key-%d", i%100)
					reader, err := cache.Get(ctx, key, fetcher)
					if err != nil {
						b.Errorf("Get failed: %v", err)
						continue
					}
					io.Copy(io.Discard, reader)
					reader.Close()
				}

				b.StopTimer()
				runtime.GC()
				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				// Report detailed memory metrics
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/float64(b.N), "alloc-per-op-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})

			b.Run("5-tier-cache-miss", func(b *testing.B) {
				stores := make([]Store, 5)
				for i := range 5 {
					stores[i] = newMockStore()
				}

				cache, err := New(logger, WithStores(stores...))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Force GC and measure baseline
				runtime.GC()
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					key := fmt.Sprintf("mem-key-%d", i%100)
					reader, err := cache.Get(ctx, key, fetcher)
					if err != nil {
						b.Errorf("Get failed: %v", err)
						continue
					}
					io.Copy(io.Discard, reader)
					reader.Close()
				}

				b.StopTimer()
				runtime.GC()
				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				// Report detailed memory metrics
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/float64(b.N), "alloc-per-op-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
				b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
			})
		})
	}
}

// BenchmarkSetOperations tests Set operation performance across different tier configurations
func BenchmarkSetOperations(b *testing.B) {
	logger := log.NewNopLogger()

	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			// Generate test data
			testData := make([]byte, ds.size)
			_, err := rand.Read(testData)
			if err != nil {
				b.Fatalf("Failed to generate test data: %v", err)
			}

			b.Run("set-2-tier", func(b *testing.B) {
				tier0 := newMockStore()
				tier1 := newMockStore()

				cache, err := New(logger, WithStores(tier0, tier1))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				ctx := context.Background()

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := fmt.Sprintf("set-key-%d", i%1000)
						writer, err := cache.Set(ctx, key, &Metadata{ETag: "set-etag"})
						if err != nil {
							b.Errorf("Set failed: %v", err)
							continue
						}
						writer.Write(testData)
						writer.Close()
						i++
					}
				})

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
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

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := fmt.Sprintf("set-key-%d", i%1000)
						writer, err := cache.Set(ctx, key, &Metadata{ETag: "set-etag"})
						if err != nil {
							b.Errorf("Set failed: %v", err)
							continue
						}
						writer.Write(testData)
						writer.Close()
						i++
					}
				})

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
			})

							b.Run("set-5-tier", func(b *testing.B) {
				stores := make([]Store, 5)
				for i := range 5 {
					stores[i] = newMockStore()
				}

				cache, err := New(logger, WithStores(stores...))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				ctx := context.Background()

				// Measure memory before benchmark
				var memBefore runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := fmt.Sprintf("set-key-%d", i%1000)
						writer, err := cache.Set(ctx, key, &Metadata{ETag: "set-etag"})
						if err != nil {
							b.Errorf("Set failed: %v", err)
							continue
						}
						writer.Write(testData)
						writer.Close()
						i++
					}
				})

				b.StopTimer()
				var memAfter runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&memAfter)

				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
				b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
			})
		})
	}
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
		testData := []byte("delete test data")

		// Measure memory before benchmark
		var memBefore runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memBefore)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("delete-key-%d", i)

			// Pre-populate stores
			tier0.data[key] = testData
			tier1.data[key] = testData

			err := cache.Delete(ctx, key)
			if err != nil {
				b.Errorf("Delete failed: %v", err)
			}
		}

		b.StopTimer()
		var memAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memAfter)

		b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
		b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
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
		testData := []byte("delete test data")

		// Measure memory before benchmark
		var memBefore runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memBefore)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("delete-key-%d", i)

			// Pre-populate stores
			tier0.data[key] = testData
			tier1.data[key] = testData
			tier2.data[key] = testData

			err := cache.Delete(ctx, key)
			if err != nil {
				b.Errorf("Delete failed: %v", err)
			}
		}

		b.StopTimer()
		var memAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memAfter)

		b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
		b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
	})

	b.Run("delete-5-tier", func(b *testing.B) {
		stores := make([]Store, 5)
		for i := range 5 {
			stores[i] = newMockStore()
		}

		cache, err := New(logger, WithStores(stores...))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		testData := []byte("delete test data")

		// Measure memory before benchmark
		var memBefore runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memBefore)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("delete-key-%d", i)

			// Pre-populate all stores
			for j := range 5 {
				stores[j].(*mockStore).data[key] = testData
			}

			err := cache.Delete(ctx, key)
			if err != nil {
				b.Errorf("Delete failed: %v", err)
			}
		}

		b.StopTimer()
		var memAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memAfter)

		b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
		b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
	})
}

// BenchmarkGCPressure measures garbage collection pressure with different tier configurations
func BenchmarkGCPressure(b *testing.B) {
	logger := log.NewNopLogger()

	dataSizes := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_256KB", 256 * 1024},
	}

	for _, ds := range dataSizes {
		b.Run(ds.name, func(b *testing.B) {
			// Generate test data
			testData := make([]byte, ds.size)
			_, err := rand.Read(testData)
			if err != nil {
				b.Fatalf("Failed to generate test data: %v", err)
			}

			b.Run("gc-pressure-2-tier", func(b *testing.B) {
				tier0 := newMockStore()
				tier1 := newMockStore()

				cache, err := New(logger, WithStores(tier0, tier1))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate tier1 to trigger promotions
				numKeys := 100
				keys := make([]string, numKeys)
				for i := range numKeys {
					key := fmt.Sprintf("gc-key-%d", i)
					keys[i] = key
					tier1.data[key] = testData
					tier1.meta[key] = &Metadata{ETag: "gc-etag", CachedAt: time.Now()}
				}

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Force initial GC and measure baseline
				runtime.GC()
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()

				// Run intensive operations to trigger GC
				for i := 0; i < b.N; i++ {
					key := keys[i%numKeys]
					reader, err := cache.Get(ctx, key, fetcher)
					if err != nil {
						b.Errorf("Get failed: %v", err)
						continue
					}
					io.Copy(io.Discard, reader)
					reader.Close()

					// Create additional allocation pressure
					_ = make([]byte, 1024)
				}

				b.StopTimer()
				runtime.GC()
				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				// Report GC-specific metrics
				gcCycles := memAfter.NumGC - memBefore.NumGC
				totalPauseNs := memAfter.PauseTotalNs - memBefore.PauseTotalNs

				b.ReportMetric(float64(gcCycles), "gc-cycles")
				b.ReportMetric(float64(totalPauseNs)/1e6, "gc-pause-total-ms")
				if gcCycles > 0 {
					avgPauseNs := float64(totalPauseNs) / float64(gcCycles)
					b.ReportMetric(avgPauseNs/1e6, "gc-pause-avg-ms")
				}
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
			})

			b.Run("gc-pressure-5-tier", func(b *testing.B) {
				stores := make([]Store, 5)
				for i := range 5 {
					stores[i] = newMockStore()
				}

				cache, err := New(logger, WithStores(stores...))
				if err != nil {
					b.Fatalf("Failed to create cache: %v", err)
				}
				defer cache.Close()

				// Pre-populate last tier to trigger maximum promotions
				numKeys := 100
				keys := make([]string, numKeys)
				for i := range numKeys {
					key := fmt.Sprintf("gc-key-%d", i)
					keys[i] = key
					stores[4].(*mockStore).data[key] = testData
					stores[4].(*mockStore).meta[key] = &Metadata{ETag: "gc-etag", CachedAt: time.Now()}
				}

				ctx := context.Background()
				fetcher := &benchmarkFetcher{data: string(testData)}

				// Force initial GC and measure baseline
				runtime.GC()
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				b.ResetTimer()

				// Run intensive operations to trigger GC
				for i := 0; i < b.N; i++ {
					key := keys[i%numKeys]
					reader, err := cache.Get(ctx, key, fetcher)
					if err != nil {
						b.Errorf("Get failed: %v", err)
						continue
					}
					io.Copy(io.Discard, reader)
					reader.Close()

					// Create additional allocation pressure
					_ = make([]byte, 1024)
				}

				b.StopTimer()
				runtime.GC()
				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				// Report GC-specific metrics
				gcCycles := memAfter.NumGC - memBefore.NumGC
				totalPauseNs := memAfter.PauseTotalNs - memBefore.PauseTotalNs

				b.ReportMetric(float64(gcCycles), "gc-cycles")
				b.ReportMetric(float64(totalPauseNs)/1e6, "gc-pause-total-ms")
				if gcCycles > 0 {
					avgPauseNs := float64(totalPauseNs) / float64(gcCycles)
					b.ReportMetric(avgPauseNs/1e6, "gc-pause-avg-ms")
				}
				b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
			})
		})
	}
}

// BenchmarkThroughput measures overall throughput with different tier configurations
func BenchmarkThroughput(b *testing.B) {
	logger := log.NewNopLogger()

	// Test with realistic data size
	testData := make([]byte, 64*1024) // 64KB
	_, err := rand.Read(testData)
	if err != nil {
		b.Fatalf("Failed to generate test data: %v", err)
	}

	b.Run("throughput-2-tier", func(b *testing.B) {
		tier0 := newMockStore()
		tier1 := newMockStore()

		cache, err := New(logger, WithStores(tier0, tier1))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		// Create realistic workload: 70% hits, 20% promotions, 10% misses
		numKeys := 1000
		keys := make([]string, numKeys)
		for i := range numKeys {
			key := fmt.Sprintf("throughput-key-%d", i)
			keys[i] = key

			switch i % 10 {
			case 0, 1, 2, 3, 4, 5, 6: // 70% in tier0 (hot hits)
				tier0.data[key] = testData
				tier0.meta[key] = &Metadata{ETag: "hot-etag", CachedAt: time.Now()}
			case 7, 8: // 20% in tier1 (promotions)
				tier1.data[key] = testData
				tier1.meta[key] = &Metadata{ETag: "cold-etag", CachedAt: time.Now()}
				// case 9: 10% cache miss
			}
		}

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: string(testData)}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keys[i%numKeys]
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

		// Calculate and report throughput
		duration := b.Elapsed()
		throughputMBps := float64(b.N*len(testData)) / duration.Seconds() / (1024 * 1024)
		b.ReportMetric(throughputMBps, "throughput-MB/s")
	})

	b.Run("throughput-5-tier", func(b *testing.B) {
		stores := make([]Store, 5)
		for i := range 5 {
			stores[i] = newMockStore()
		}

		cache, err := New(logger, WithStores(stores...))
		if err != nil {
			b.Fatalf("Failed to create cache: %v", err)
		}
		defer cache.Close()

		// Create realistic workload distributed across tiers
		numKeys := 1000
		keys := make([]string, numKeys)
		for i := range numKeys {
			key := fmt.Sprintf("throughput-key-%d", i)
			keys[i] = key

			tierIndex := i % 6 // 0-4 for tiers, 5 for miss
			if tierIndex < 5 {
				stores[tierIndex].(*mockStore).data[key] = testData
				stores[tierIndex].(*mockStore).meta[key] = &Metadata{
					ETag:     fmt.Sprintf("tier%d-etag", tierIndex),
					CachedAt: time.Now(),
				}
			}
		}

		ctx := context.Background()
		fetcher := &benchmarkFetcher{data: string(testData)}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keys[i%numKeys]
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

		// Calculate and report throughput
		duration := b.Elapsed()
		throughputMBps := float64(b.N*len(testData)) / duration.Seconds() / (1024 * 1024)
		b.ReportMetric(throughputMBps, "throughput-MB/s")
	})
}

// BenchmarkScalability tests how performance scales with number of tiers
func BenchmarkScalability(b *testing.B) {
	logger := log.NewNopLogger()

	// Test data
	testData := make([]byte, 32*1024) // 32KB
	_, err := rand.Read(testData)
	if err != nil {
		b.Fatalf("Failed to generate test data: %v", err)
	}

	tierCounts := []int{1, 2, 3, 5, 8, 10}

	for _, tierCount := range tierCounts {
		b.Run(fmt.Sprintf("Tiers_%d", tierCount), func(b *testing.B) {
			// Create stores
			stores := make([]Store, tierCount)
			for i := range tierCount {
				stores[i] = newMockStore()
			}

			cache, err := New(logger, WithStores(stores...))
			if err != nil {
				b.Fatalf("Failed to create cache: %v", err)
			}
			defer cache.Close()

			// Pre-populate last tier to test worst-case promotion scenario
			numKeys := 100
			keys := make([]string, numKeys)
			lastTierIndex := tierCount - 1
			for i := range numKeys {
				key := fmt.Sprintf("scale-key-%d", i)
				keys[i] = key
				stores[lastTierIndex].(*mockStore).data[key] = testData
				stores[lastTierIndex].(*mockStore).meta[key] = &Metadata{
					ETag:     "scale-etag",
					CachedAt: time.Now(),
				}
			}

			ctx := context.Background()
			fetcher := &benchmarkFetcher{data: string(testData)}

			// Measure memory before benchmark
			var memBefore runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&memBefore)

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := keys[i%numKeys]
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

			b.StopTimer()
			var memAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&memAfter)

			b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc), "total-alloc-bytes")
			b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs), "mallocs")
			b.ReportMetric(float64(tierCount), "tier-count")
		})
	}
}

// benchmarkFetcher implements Fetcher interface for benchmarking
type benchmarkFetcher struct {
	data   string
	called bool
	mu     sync.Mutex
}

func (f *benchmarkFetcher) Fetch(_ context.Context, _ *Metadata) (*FetchResult, error) {
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
