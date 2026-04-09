package daramjwee_test

import (
	"context"
	"errors"
	"fmt"
	"io"
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
		daramjwee.WithTiers(memStore),
		daramjwee.WithOpTimeout(10*time.Second),
		daramjwee.WithFreshness(1*time.Minute, 0),
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
				key := fmt.Sprintf("key-%d", j%5) // Use 5 keys repeatedly to induce contention

				// Get 또는 Set 랜덤하게 실행
				if (id+j)%2 == 0 {
					// Get with fetcher
					fetcher := cache.GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
						return fmt.Sprintf("value-%d-%d", id, j), &daramjwee.Metadata{CacheTag: "test"}, nil
					})

					_, err := stringCache.Get(ctx, key, fetcher)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Get failed: %v", id, err)
					}
				} else {
					// Set
					value := fmt.Sprintf("set-value-%d-%d", id, j)
					err := stringCache.Set(ctx, key, value, &daramjwee.Metadata{CacheTag: "test"})
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
	err = stringCache.Set(ctx, key, "initial-value", &daramjwee.Metadata{CacheTag: "v1"})
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
				return fmt.Sprintf("refreshed-value-%d", id), &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", id)}, nil
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
						return fmt.Sprintf("fetched-%d-%d", id, j), &daramjwee.Metadata{CacheTag: "test"}, nil
					})
					_, err := stringCache.Get(ctx, key, fetcher)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Get failed: %v", id, err)
					}

				case 1: // Set
					value := fmt.Sprintf("set-%d-%d", id, j)
					err := stringCache.Set(ctx, key, value, &daramjwee.Metadata{CacheTag: "test"})
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: Set failed: %v", id, err)
					}

				case 2: // GetOrSet
					factory := func() (string, *daramjwee.Metadata, error) {
						return fmt.Sprintf("factory-%d-%d", id, j), &daramjwee.Metadata{CacheTag: "test"}, nil
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

func TestConcurrentStaleLowerTierHitsDoNotPublishPartialTopOnRefreshFailure(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	key := "stale-lower-refresh-failure"
	lower.setData(key, "stale-value", &daramjwee.Metadata{
		CacheTag: "v0",
		CachedAt: time.Now().Add(-time.Hour),
	})

	cache, err := daramjwee.New(
		log.NewNopLogger(),
		daramjwee.WithTiers(top, lower),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFreshness(time.Millisecond, 0),
		daramjwee.WithWorkers(4),
		daramjwee.WithWorkerQueue(64),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	fetcher := &concurrentFailingRefreshFetcher{
		contentPrefix: []byte("new-"),
		err:           errors.New("midstream refresh failure"),
	}

	const numGoroutines = 32
	var wg sync.WaitGroup
	readErrors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, fetcher)
			if err != nil {
				readErrors <- err
				return
			}
			defer resp.Close()

			body, err := io.ReadAll(resp)
			if err != nil {
				readErrors <- err
				return
			}
			if resp.Status != daramjwee.GetStatusOK {
				readErrors <- fmt.Errorf("unexpected status: %s", resp.Status)
				return
			}
			if string(body) != "stale-value" {
				readErrors <- fmt.Errorf("unexpected body: %q", string(body))
			}
		}()
	}

	wg.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Error(err)
	}

	time.Sleep(150 * time.Millisecond)

	_, _, err = top.GetStream(context.Background(), key)
	if !errors.Is(err, daramjwee.ErrNotFound) {
		t.Fatalf("expected top tier to remain empty after failed refreshes, got %v", err)
	}
	if fetcher.getFetchCount() == 0 {
		t.Fatal("expected at least one background refresh attempt")
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
					return fmt.Sprintf("bench-value-%d", currentI), &daramjwee.Metadata{CacheTag: "bench"}, nil
				})
				_, _ = stringCache.Get(ctx, key, fetcher)
			} else {
				// Set operation
				value := fmt.Sprintf("bench-set-%d", i)
				_ = stringCache.Set(ctx, key, value, &daramjwee.Metadata{CacheTag: "bench"})
			}
			i++
		}
	})
}

type concurrentFailingRefreshFetcher struct {
	mu            sync.Mutex
	fetchCount    int
	contentPrefix []byte
	err           error
}

func (f *concurrentFailingRefreshFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	f.mu.Lock()
	f.fetchCount++
	f.mu.Unlock()

	return &daramjwee.FetchResult{
		Body:     newFailingReadCloser(f.contentPrefix, f.err),
		Metadata: &daramjwee.Metadata{CacheTag: "v1"},
	}, nil
}

func (f *concurrentFailingRefreshFetcher) getFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchCount
}
