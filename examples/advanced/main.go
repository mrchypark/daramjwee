// Package main demonstrates advanced features and performance testing examples
// 이 파일은 압축, 성능 테스트, 고급 기능들을 보여주는 예제들을 포함합니다
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/comp"
	"github.com/mrchypark/daramjwee/pkg/lock"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// CompressibleFetcher는 압축 가능한 데이터를 생성하는 Fetcher입니다
type CompressibleFetcher struct {
	size int
	seed int64
}

func NewCompressibleFetcher(size int, seed int64) *CompressibleFetcher {
	return &CompressibleFetcher{size: size, seed: seed}
}

func (f *CompressibleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	// 압축하기 좋은 반복적인 데이터 생성
	rng := rand.New(rand.NewSource(f.seed))

	var buf bytes.Buffer
	patterns := []string{
		"이것은 반복되는 텍스트 패턴입니다. ",
		"압축 테스트를 위한 데이터입니다. ",
		"daramjwee 캐시 시스템 테스트 중입니다. ",
		"반복적인 내용으로 압축률을 높입니다. ",
	}

	for buf.Len() < f.size {
		pattern := patterns[rng.Intn(len(patterns))]
		buf.WriteString(pattern)
	}

	// 정확한 크기로 자르기
	data := buf.Bytes()[:f.size]

	metadata := &daramjwee.Metadata{
		ETag:     fmt.Sprintf("compress-%d-%d", f.size, f.seed),
		CachedAt: time.Now(),
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader(data)),
		Metadata: metadata,
	}, nil
}

// PerformanceFetcher는 성능 테스트용 Fetcher입니다
type PerformanceFetcher struct {
	data  []byte
	delay time.Duration
}

func NewPerformanceFetcher(data []byte, delay time.Duration) *PerformanceFetcher {
	return &PerformanceFetcher{data: data, delay: delay}
}

func (f *PerformanceFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}

	metadata := &daramjwee.Metadata{
		ETag:     fmt.Sprintf("perf-%d", time.Now().UnixNano()),
		CachedAt: time.Now(),
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader(f.data)),
		Metadata: metadata,
	}, nil
}

// TestErrorFetcher는 에러 처리 테스트용 Fetcher입니다
type TestErrorFetcher struct {
	shouldError bool
	data        []byte
}

func (f *TestErrorFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	if f.shouldError {
		return nil, fmt.Errorf("의도적인 에러 발생")
	}

	return &daramjwee.FetchResult{
		Body: io.NopCloser(bytes.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{
			ETag:     "error-test",
			CachedAt: time.Now(),
		},
	}, nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	logger = level.NewFilter(logger, level.AllowInfo())

	fmt.Println("=== daramjwee 고급 기능 및 성능 테스트 예제들 ===")

	// 1. 압축 기능 테스트
	compressionTest(logger)

	// 2. 성능 벤치마크 테스트
	performanceBenchmark(logger)

	// 3. 동시성 테스트
	concurrencyTest(logger)

	// 4. 메모리 사용량 테스트
	memoryUsageTest(logger)

	// 5. 캐시 정책 비교 테스트
	cachePolicyComparison(logger)

	// 6. 락 전략 성능 비교
	lockStrategyComparison(logger)

	// 7. TTL 및 만료 테스트
	ttlExpirationTest(logger)

	// 8. 에러 처리 및 복구 테스트
	errorHandlingTest(logger)

	fmt.Println("\n모든 고급 기능 테스트가 완료되었습니다!")
}

// 1. 압축 기능 테스트
func compressionTest(logger log.Logger) {
	fmt.Println("\n1. 압축 기능 테스트")

	// 다양한 압축기 테스트
	compressors := map[string]daramjwee.Compressor{
		"None":      daramjwee.NewNoneCompressor(),
		"Gzip-Fast": comp.NewDefaultGzipCompressor(),
		"Gzip-Best": func() daramjwee.Compressor { c, _ := comp.NewGzipCompressor(9); return c }(),
	}

	testData := []int{1024, 10240, 102400} // 1KB, 10KB, 100KB

	// 압축 테스트
	for _, compressor := range compressors {
		fmt.Printf("\n압축기: %s (레벨: %d)\n", compressor.Algorithm(), compressor.Level())

		for _, size := range testData {
			// 압축 테스트
			fetcher := NewCompressibleFetcher(size, 12345)
			result, err := fetcher.Fetch(context.Background(), nil)
			if err != nil {
				fmt.Printf("  데이터 생성 실패 (%d bytes): %v\n", size, err)
				continue
			}

			originalData, _ := io.ReadAll(result.Body)
			result.Body.Close()

			// 압축 수행
			var compressedBuf bytes.Buffer
			compressedSize, err := compressor.Compress(&compressedBuf, bytes.NewReader(originalData))
			if err != nil {
				fmt.Printf("  압축 실패 (%d bytes): %v\n", size, err)
				continue
			}

			// 압축 해제 수행
			var decompressedBuf bytes.Buffer
			decompressedSize, err := compressor.Decompress(&decompressedBuf, &compressedBuf)
			if err != nil {
				fmt.Printf("  압축 해제 실패 (%d bytes): %v\n", size, err)
				continue
			}

			// 결과 검증
			if !bytes.Equal(originalData, decompressedBuf.Bytes()) {
				fmt.Printf("  데이터 무결성 실패 (%d bytes)\n", size)
				continue
			}

			ratio := float64(compressedSize) / float64(len(originalData)) * 100
			fmt.Printf("  %d bytes: 원본=%d, 압축=%d, 해제=%d, 비율=%.1f%%\n",
				size, len(originalData), compressedSize, decompressedSize, ratio)
		}
	}
}

// 2. 성능 벤치마크 테스트
func performanceBenchmark(logger log.Logger) {
	fmt.Println("\n2. 성능 벤치마크 테스트")

	// 테스트 데이터 생성
	testData := make([]byte, 1024) // 1KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// 다양한 캐시 설정으로 성능 테스트
	configs := []struct {
		name  string
		setup func() daramjwee.Cache
	}{
		{
			name: "단일-tier-LRU",
			setup: func() daramjwee.Cache {
				store := memstore.New(10*1024*1024, policy.NewLRUPolicy())
				cache, _ := daramjwee.New(logger, daramjwee.WithStores(store))
				return cache
			},
		},
		{
			name: "단일-tier-SIEVE",
			setup: func() daramjwee.Cache {
				store := memstore.New(10*1024*1024, policy.NewSievePolicy())
				cache, _ := daramjwee.New(logger, daramjwee.WithStores(store))
				return cache
			},
		},
		{
			name: "이중-tier-메모리+파일",
			setup: func() daramjwee.Cache {
				memStore := memstore.New(5*1024*1024, policy.NewLRUPolicy())
				fileStore, _ := filestore.New("./perf-cache", logger, 20*1024*1024, nil)
				cache, _ := daramjwee.New(logger, daramjwee.WithStores(memStore, fileStore))
				return cache
			},
		},
	}

	for _, config := range configs {
		fmt.Printf("\n%s 성능 테스트:\n", config.name)
		cache := config.setup()

		// 워밍업
		fetcher := NewPerformanceFetcher(testData, 0)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("warmup-%d", i)
			reader, _ := cache.Get(context.Background(), key, fetcher)
			if reader != nil {
				reader.Close()
			}
		}

		// 성능 측정
		numOps := 1000
		start := time.Now()

		for i := 0; i < numOps; i++ {
			key := fmt.Sprintf("perf-%d", i%100) // 100개 키 순환
			reader, err := cache.Get(context.Background(), key, fetcher)
			if err != nil {
				continue
			}
			io.Copy(io.Discard, reader)
			reader.Close()
		}

		duration := time.Since(start)
		opsPerSec := float64(numOps) / duration.Seconds()

		fmt.Printf("  %d 작업 완료: %v (%.0f ops/sec)\n", numOps, duration, opsPerSec)
		cache.Close()
	}
}

// 3. 동시성 테스트
func concurrencyTest(logger log.Logger) {
	fmt.Println("\n3. 동시성 테스트")

	// 높은 동시성을 위한 N-tier 캐시 설정
	memStore := memstore.New(
		30*1024*1024, // 30MB memory tier
		policy.NewSievePolicy(),
		memstore.WithLocker(lock.NewStripeLock(1024)),
	)

	// 추가 파일 기반 tier로 더 큰 용량 제공
	fileStore, err := filestore.New("./concurrency-cache", logger, 100*1024*1024, nil)
	if err != nil {
		fmt.Printf("동시성 테스트 파일 스토어 생성 실패: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		// N-tier 설정: 메모리 + 파일 tier
		daramjwee.WithStores(memStore, fileStore),
		daramjwee.WithWorker("pool", 20, 2000, 30*time.Second),
	)
	if err != nil {
		fmt.Printf("동시성 테스트 캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	testData := make([]byte, 512)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	concurrencyLevels := []int{1, 10, 50, 100}

	for _, concurrency := range concurrencyLevels {
		fmt.Printf("\n동시성 레벨: %d\n", concurrency)

		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				fetcher := NewPerformanceFetcher(testData, 1*time.Millisecond)

				for j := 0; j < 100; j++ {
					key := fmt.Sprintf("concurrent-%d-%d", workerID, j)
					reader, err := cache.Get(context.Background(), key, fetcher)
					if err != nil {
						continue
					}
					io.Copy(io.Discard, reader)
					reader.Close()
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(start)
		totalOps := concurrency * 100
		opsPerSec := float64(totalOps) / duration.Seconds()

		fmt.Printf("  %d 작업 완료: %v (%.0f ops/sec)\n", totalOps, duration, opsPerSec)
	}
}

// 4. 메모리 사용량 테스트
func memoryUsageTest(logger log.Logger) {
	fmt.Println("\n4. 메모리 사용량 테스트")

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// 메모리 사용량 측정을 위한 N-tier 캐시 생성
	memStore := memstore.New(
		80*1024*1024, // 80MB memory tier
		policy.NewLRUPolicy(),
	)

	// 추가 파일 tier로 메모리 압박 시 백업 제공
	fileStore, err := filestore.New("./memory-test-cache", logger, 200*1024*1024, nil)
	if err != nil {
		fmt.Printf("메모리 테스트 파일 스토어 생성 실패: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithStores(memStore, fileStore), // N-tier 설정
	)
	if err != nil {
		fmt.Printf("메모리 테스트 캐시 생성 실패: %v\n", err)
		return
	}

	// 대량의 데이터 캐싱
	largeData := make([]byte, 10*1024) // 10KB per item
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	fetcher := NewPerformanceFetcher(largeData, 0)

	fmt.Println("  1000개 항목 캐싱 중...")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("memory-test-%d", i)
		reader, err := cache.Get(context.Background(), key, fetcher)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, reader)
		reader.Close()

		if i%200 == 0 {
			runtime.GC()
			fmt.Printf("    %d개 완료\n", i)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	memUsed := m2.Alloc - m1.Alloc
	fmt.Printf("  메모리 사용량: %d bytes (%.2f MB)\n", memUsed, float64(memUsed)/(1024*1024))
	fmt.Printf("  GC 실행 횟수: %d\n", m2.NumGC-m1.NumGC)

	cache.Close()
}

// 5. 캐시 정책 비교 테스트
func cachePolicyComparison(logger log.Logger) {
	fmt.Println("\n5. 캐시 정책 비교 테스트")

	capacity := int64(1024 * 1024) // 1MB
	policies := map[string]daramjwee.EvictionPolicy{
		"LRU":     policy.NewLRUPolicy(),
		"SIEVE":   policy.NewSievePolicy(),
		"S3-FIFO": policy.NewS3FIFOPolicy(capacity, 0.1),
	}

	testData := make([]byte, 1024) // 1KB per item
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	for policyName, pol := range policies {
		fmt.Printf("\n%s 정책 테스트:\n", policyName)

		memStore := memstore.New(capacity, pol)
		// N-tier 설정으로 정책 테스트 (단일 tier)
		cache, _ := daramjwee.New(logger, daramjwee.WithStores(memStore))

		fetcher := NewPerformanceFetcher(testData, 0)

		// 캐시 채우기 (용량 초과하도록)
		numItems := 2000 // 2MB worth of data (캐시 용량의 2배)
		hits := 0

		start := time.Now()

		for i := 0; i < numItems; i++ {
			key := fmt.Sprintf("policy-test-%d", i)
			reader, err := cache.Get(context.Background(), key, fetcher)
			if err != nil {
				continue
			}

			// 첫 번째 접근인지 확인 (간단한 히트율 측정)
			data, _ := io.ReadAll(reader)
			if len(data) > 0 {
				// 재접근으로 히트율 테스트
				reader2, _ := cache.Get(context.Background(), key, fetcher)
				if reader2 != nil {
					hits++
					reader2.Close()
				}
			}
			reader.Close()
		}

		duration := time.Since(start)
		hitRate := float64(hits) / float64(numItems) * 100

		fmt.Printf("  %d 항목 처리: %v\n", numItems, duration)
		fmt.Printf("  히트율: %.1f%%\n", hitRate)

		cache.Close()
	}
}

// 6. 락 전략 성능 비교
func lockStrategyComparison(logger log.Logger) {
	fmt.Println("\n6. 락 전략 성능 비교")

	lockStrategies := map[string]daramjwee.Locker{
		"Mutex":       lock.NewMutexLock(),
		"Stripe-256":  lock.NewStripeLock(256),
		"Stripe-1024": lock.NewStripeLock(1024),
	}

	testData := make([]byte, 256)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	for strategyName, locker := range lockStrategies {
		fmt.Printf("\n%s 락 전략 테스트:\n", strategyName)

		memStore := memstore.New(
			10*1024*1024,
			policy.NewLRUPolicy(),
			memstore.WithLocker(locker),
		)
		// N-tier 설정으로 락 전략 테스트 (단일 tier)
		cache, _ := daramjwee.New(logger, daramjwee.WithStores(memStore))

		fetcher := NewPerformanceFetcher(testData, 0)

		// 동시성 테스트
		concurrency := 50
		opsPerWorker := 100

		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for j := 0; j < opsPerWorker; j++ {
					key := fmt.Sprintf("lock-test-%d", j%20) // 20개 키 순환 (충돌 유발)
					reader, err := cache.Get(context.Background(), key, fetcher)
					if err != nil {
						continue
					}
					io.Copy(io.Discard, reader)
					reader.Close()
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(start)
		totalOps := concurrency * opsPerWorker
		opsPerSec := float64(totalOps) / duration.Seconds()

		fmt.Printf("  %d 작업 완료: %v (%.0f ops/sec)\n", totalOps, duration, opsPerSec)

		cache.Close()
	}
}

// 7. TTL 및 만료 테스트
func ttlExpirationTest(logger log.Logger) {
	fmt.Println("\n7. TTL 및 만료 테스트")

	memStore := memstore.New(10*1024*1024, nil)

	// 짧은 TTL로 N-tier 캐시 설정
	cache, err := daramjwee.New(logger,
		daramjwee.WithStores(memStore),             // N-tier 설정 (단일 tier)
		daramjwee.WithCache(2*time.Second),         // 2초 양성 캐시
		daramjwee.WithNegativeCache(1*time.Second), // 1초 음성 캐시
		daramjwee.WithWorker("pool", 5, 100, 10*time.Second),
	)
	if err != nil {
		fmt.Printf("TTL 테스트 캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	testData := []byte("TTL 테스트 데이터")
	fetcher := NewPerformanceFetcher(testData, 100*time.Millisecond)

	key := "ttl-test-key"

	fmt.Println("  초기 캐시 저장...")
	start := time.Now()
	reader1, err := cache.Get(context.Background(), key, fetcher)
	if err != nil {
		fmt.Printf("  초기 저장 실패: %v\n", err)
		return
	}
	reader1.Close()
	initialTime := time.Since(start)

	fmt.Printf("  초기 저장 시간: %v\n", initialTime)

	// 즉시 재조회 (캐시 히트)
	start = time.Now()
	reader2, err := cache.Get(context.Background(), key, fetcher)
	if err != nil {
		fmt.Printf("  캐시 히트 실패: %v\n", err)
		return
	}
	reader2.Close()
	hitTime := time.Since(start)

	fmt.Printf("  캐시 히트 시간: %v\n", hitTime)

	// TTL 만료 대기
	fmt.Println("  TTL 만료 대기 중 (3초)...")
	time.Sleep(3 * time.Second)

	// 만료 후 재조회 (캐시 미스)
	start = time.Now()
	reader3, err := cache.Get(context.Background(), key, fetcher)
	if err != nil {
		fmt.Printf("  만료 후 조회 실패: %v\n", err)
		return
	}
	reader3.Close()
	expiredTime := time.Since(start)

	fmt.Printf("  만료 후 조회 시간: %v\n", expiredTime)
	fmt.Printf("  성능 개선: 히트 %.2fx, 만료 %.2fx\n",
		float64(initialTime)/float64(hitTime),
		float64(initialTime)/float64(expiredTime))
}

// 8. 에러 처리 및 복구 테스트
func errorHandlingTest(logger log.Logger) {
	fmt.Println("\n8. 에러 처리 및 복구 테스트")

	memStore := memstore.New(1024*1024, nil)
	cache, err := daramjwee.New(logger, daramjwee.WithStores(memStore)) // N-tier 설정
	if err != nil {
		fmt.Printf("에러 테스트 캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	// 에러를 발생시키는 Fetcher
	errorFetcher := &TestErrorFetcher{shouldError: true, data: []byte("정상 데이터")}
	key := "error-test-key"

	// 에러 상황 테스트
	fmt.Println("  에러 상황 테스트...")
	_, err = cache.Get(context.Background(), key, errorFetcher)
	if err != nil {
		fmt.Printf("  예상된 에러 발생: %v\n", err)
	} else {
		fmt.Println("  에러가 발생하지 않음 (예상과 다름)")
	}

	// 복구 상황 테스트
	fmt.Println("  복구 상황 테스트...")
	errorFetcher.shouldError = false

	reader, err := cache.Get(context.Background(), key, errorFetcher)
	if err != nil {
		fmt.Printf("  복구 실패: %v\n", err)
	} else {
		data, _ := io.ReadAll(reader)
		reader.Close()
		fmt.Printf("  복구 성공: %s\n", string(data))
	}

	// 캐시된 데이터 재조회
	fmt.Println("  캐시된 데이터 재조회...")
	reader2, err := cache.Get(context.Background(), key, errorFetcher)
	if err != nil {
		fmt.Printf("  캐시 조회 실패: %v\n", err)
	} else {
		data, _ := io.ReadAll(reader2)
		reader2.Close()
		fmt.Printf("  캐시 조회 성공: %s\n", string(data))
	}
}
