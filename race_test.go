//go:build race

package daramjwee

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestCache_Chaos_RaceCondition is a chaotic test designed to be run with the -race flag.
// It concurrently performs random Get, Set, and Delete operations to uncover
// potential data races under high contention.
func TestCache_Chaos_RaceCondition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	// 1. 테스트를 위한 캐시 생성
	// defer cache.Close()를 여기서 제거합니다.
	cache, hot, _ := setupCache(t)

	ctx := context.Background()
	const numKeys = 50
	var keys []string
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("chaos-key-%d", i)
		keys = append(keys, key)
		hot.setData(key, "initial-data", &Metadata{})
	}

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			// 각 고루틴마다 별도의 시드를 가진 난수 생성기를 사용합니다.
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for j := 0; j < 100; j++ {
				key := keys[r.Intn(len(keys))]
				op := r.Intn(3)

				switch op {
				case 0: // Get
					fetcher := &mockFetcher{content: "fetched-data", etag: "v-fetch"}
					stream, err := cache.Get(ctx, key, fetcher)
					if err == nil && stream != nil {
						_, _ = io.Copy(io.Discard, stream)
						stream.Close()
					}
				case 1: // Set
					writer, err := cache.Set(ctx, key, &Metadata{ETag: "v-set"})
					if err == nil && writer != nil {
						writer.Write([]byte("set-data"))
						writer.Close()
					}
				case 2: // Delete
					cache.Delete(ctx, key)
				}
			}
		}(i)
	}

	// 2. 모든 'chaos' 고루틴이 작업을 마칠 때까지 명시적으로 기다립니다.
	wg.Wait()

	// 3. 'chaos'가 완전히 끝난 후에, 캐시를 닫습니다.
	// 이렇게 하면 순환 대기 문제가 발생하지 않습니다.
	cache.Close()

	// 테스트의 주 목적은 레이스 컨디션 탐지이므로, 최종 상태 검증은 생략합니다.
}
