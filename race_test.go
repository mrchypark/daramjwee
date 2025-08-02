package daramjwee_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/cache"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// createTestCacheForRace creates a cache with proper freshness settings for race tests
func createTestCacheForRace() (daramjwee.Cache, error) {
	logger := log.NewNopLogger()
	memStore := memstore.New(1*1024*1024, policy.NewLRU())

	return daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithCache(1*time.Minute),
	)
}

// TestConcurrentAccess tests the actual production code for race conditions
// This test verifies that multiple goroutines can safely access the cache simultaneously
func TestConcurrentAccess(t *testing.T) {
	baseCache, err := createTestCacheForRace()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer baseCache.Close()

	stringCache := cache.NewGeneric[string](baseCache)
	ctx := context.Background()

	const numGoroutines = 100
	const numOperations = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOperations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d", j%5) // 5개의 키를 반복 사용

				// Get 또는 Set 랜덤하게 실행
				if (id+j)%2 == 0 {
					// Get with fetcher
					fetcher := cache.GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
						return fmt.Sprintf("value-%d-%d", id, j), &daramjwee.Metadata{ETag: "test"}, nil
					})

					_, err := stringCache.Get(ctx, key, fetcher)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Get failed: %v", id, err)
					}
				} else {
					// Set
					value := fmt.Sprintf("set-value-%d-%d", id, j)
					err := stringCache.Set(ctx, key, value, &daramjwee.Metadata{ETag: "test"})
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Set failed: %v", id, err)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 에러 체크
	for err := range errors {
		t.Error(err)
	}
}

// TestConcurrentRefresh tests concurrent background refresh operations
func TestConcurrentRefresh(t *testing.T) {
	baseCache, err := createTestCacheForRace()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer baseCache.Close()

	stringCache := cache.NewGeneric[string](baseCache)
	ctx := context.Background()

	// 먼저 값을 설정
	key := "refresh-test"
	err = stringCache.Set(ctx, key, "initial-value", &daramjwee.Metadata{ETag: "v1"})
	if err != nil {
		t.Fatalf("Initial set failed: %v", err)
	}

	const numGoroutines = 50
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// 동시에 여러 고루틴에서 ScheduleRefresh 호출
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			fetcher := cache.GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
				return fmt.Sprintf("refreshed-value-%d", id), &daramjwee.Metadata{ETag: fmt.Sprintf("v%d", id)}, nil
			})

			err := stringCache.ScheduleRefresh(ctx, key, fetcher)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: ScheduleRefresh failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 에러 체크
	for err := range errors {
		t.Error(err)
	}

	// 잠시 대기하여 백그라운드 작업 완료
	time.Sleep(100 * time.Millisecond)
}

// TestConcurrentMixedOperations tests various cache operations running concurrently
func TestConcurrentMixedOperations(t *testing.T) {
	baseCache, err := createTestCacheForRace()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer baseCache.Close()

	stringCache := cache.NewGeneric[string](baseCache)
	ctx := context.Background()

	const numGoroutines = 20
	const numOperations = 5
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOperations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("mixed-key-%d", j%3)

				switch (id + j) % 4 {
				case 0: // Get
					fetcher := cache.GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
						return fmt.Sprintf("fetched-%d-%d", id, j), &daramjwee.Metadata{ETag: "test"}, nil
					})
					_, err := stringCache.Get(ctx, key, fetcher)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Get failed: %v", id, err)
					}

				case 1: // Set
					value := fmt.Sprintf("set-%d-%d", id, j)
					err := stringCache.Set(ctx, key, value, &daramjwee.Metadata{ETag: "test"})
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Set failed: %v", id, err)
					}

				case 2: // GetOrSet
					factory := func() (string, *daramjwee.Metadata, error) {
						return fmt.Sprintf("factory-%d-%d", id, j), &daramjwee.Metadata{ETag: "test"}, nil
					}
					_, err := stringCache.GetOrSet(ctx, key, factory)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: GetOrSet failed: %v", id, err)
					}

				case 3: // Delete
					err := stringCache.Delete(ctx, key)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Delete failed: %v", id, err)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 에러 체크
	for err := range errors {
		t.Error(err)
	}
}

// BenchmarkConcurrentAccess benchmarks concurrent cache operations
func BenchmarkConcurrentAccess(b *testing.B) {
	baseCache, err := createTestCacheForRace()
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer baseCache.Close()

	stringCache := cache.NewGeneric[string](baseCache)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			key := fmt.Sprintf("bench-key-%d", i%10)

			if i%2 == 0 {
				// Get operation
				currentI := i // 클로저에서 사용할 값을 복사
				fetcher := cache.GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
					return fmt.Sprintf("bench-value-%d", currentI), &daramjwee.Metadata{ETag: "bench"}, nil
				})
				_, _ = stringCache.Get(ctx, key, fetcher)
			} else {
				// Set operation
				value := fmt.Sprintf("bench-set-%d", i)
				_ = stringCache.Set(ctx, key, value, &daramjwee.Metadata{ETag: "bench"})
			}
			i++
		}
	})
}
