package daramjwee_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

func setupBenchmarkCache(b *testing.B) daramjwee.Cache {
	b.Helper()
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*1000000000), // 5 seconds
	)
	if err != nil {
		b.Fatal(err)
	}
	return cache
}

// BenchmarkConcurrentGet_HotHit measures concurrent read performance on hot keys.
func BenchmarkConcurrentGet_HotHit(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = sink.Write([]byte(value))
		_ = sink.Close()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%100)
			resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, noopFetcher{})
			if err != nil {
				b.Fatal(err)
			}
			_, _ = io.ReadAll(resp)
			_ = resp.Close()
			i++
		}
	})
}

// BenchmarkConcurrentSet measures concurrent write performance.
func BenchmarkConcurrentSet(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
			if err != nil {
				b.Fatal(err)
			}
			_, _ = sink.Write([]byte(value))
			_ = sink.Close()
			i++
		}
	})
}

// BenchmarkConcurrentMixed measures mixed read/write performance.
func BenchmarkConcurrentMixed(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = sink.Write([]byte(value))
		_ = sink.Close()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%3 == 0 {
				// Write
				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)
				sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
				if err != nil {
					b.Fatal(err)
				}
				_, _ = sink.Write([]byte(value))
				_ = sink.Close()
			} else {
				// Read
				key := fmt.Sprintf("key-%d", i%100)
				resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, noopFetcher{})
				if err != nil {
					b.Fatal(err)
				}
				_, _ = io.ReadAll(resp)
				_ = resp.Close()
			}
			i++
		}
	})
}

// BenchmarkConcurrentDelete measures concurrent delete performance.
func BenchmarkConcurrentDelete(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			_ = cache.Delete(context.Background(), key)
			i++
		}
	})
}

// BenchmarkConcurrentGetSetDelete measures mixed operations with contention.
func BenchmarkConcurrentGetSetDelete(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = sink.Write([]byte(value))
		_ = sink.Close()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%100)
			switch i % 10 {
			case 0, 1, 2, 3, 4: // 50% reads
				resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, noopFetcher{})
				if err != nil {
					continue
				}
				_, _ = io.ReadAll(resp)
				_ = resp.Close()
			case 5, 6, 7: // 30% writes
				value := fmt.Sprintf("value-%d", i)
				sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
				if err != nil {
					continue
				}
				_, _ = sink.Write([]byte(value))
				_ = sink.Close()
			case 8: // 10% deletes
				_ = cache.Delete(context.Background(), key)
			case 9: // 10% conditional reads
				resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{IfNoneMatch: "v0"}, nil)
				if err != nil {
					continue
				}
				_, _ = io.ReadAll(resp)
				_ = resp.Close()
			}
			i++
		}
	})
}

// BenchmarkLockContention_HighContention measures lock contention on a single key.
func BenchmarkLockContention_HighContention(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	// Pre-populate single key
	sink, err := cache.Set(context.Background(), "hot-key", &daramjwee.Metadata{CacheTag: "v0"})
	if err != nil {
		b.Fatal(err)
	}
	_, _ = sink.Write([]byte("hot-value"))
	_ = sink.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := cache.Get(context.Background(), "hot-key", daramjwee.GetRequest{}, noopFetcher{})
			if err != nil {
				continue
			}
			_, _ = io.ReadAll(resp)
			_ = resp.Close()
		}
	})
}

// BenchmarkLockContention_WriteHeavy measures write-heavy lock contention.
func BenchmarkLockContention_WriteHeavy(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%10)
			value := fmt.Sprintf("value-%d", i)
			sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
			if err != nil {
				continue
			}
			_, _ = sink.Write([]byte(value))
			_ = sink.Close()
			i++
		}
	})
}

// BenchmarkLockContention_ReadHeavy measures read-heavy lock contention.
func BenchmarkLockContention_ReadHeavy(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = sink.Write([]byte(value))
		_ = sink.Close()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%100)
			resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, noopFetcher{})
			if err != nil {
				continue
			}
			_, _ = io.ReadAll(resp)
			_ = resp.Close()
			i++
		}
	})
}

// BenchmarkLockContention_DeleteHeavy measures delete-heavy lock contention.
func BenchmarkLockContention_DeleteHeavy(b *testing.B) {
	cache := setupBenchmarkCache(b)
	defer cache.Close()

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = sink.Write([]byte(value))
		_ = sink.Close()
	}

	var mu sync.Mutex
	counter := 0

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			key := fmt.Sprintf("key-%d", counter%1000)
			counter++
			mu.Unlock()
			_ = cache.Delete(context.Background(), key)
		}
	})
}
