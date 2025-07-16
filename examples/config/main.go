// Package main demonstrates comprehensive configuration examples for daramjwee cache
// 이 파일은 daramjwee 캐시의 모든 설정 경우의 수를 보여주는 예제들을 포함합니다
package main

import (
	"fmt"
	"os"
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

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	logger = level.NewFilter(logger, level.AllowInfo())

	fmt.Println("=== daramjwee 캐시 설정 예제들 ===")

	// 1. 기본 메모리 캐시 (최소 설정)
	basicMemoryCache(logger)

	// 2. 고급 메모리 캐시 (모든 옵션)
	advancedMemoryCache(logger)

	// 3. 파일 스토어 캐시 (기본)
	basicFileCache(logger)

	// 4. 파일 스토어 캐시 (고급 옵션들)
	advancedFileCache(logger)

	// 5. 하이브리드 멀티티어 캐시
	hybridMultiTierCache(logger)

	// 6. 워커 전략별 설정
	workerStrategiesExample(logger)

	// 7. 캐시 정책별 설정
	cachePoliciesExample(logger)

	// 8. 락 전략별 설정
	lockStrategiesExample(logger)

	// 9. 압축 설정 예제
	compressionExample(logger)

	// 10. 타임아웃 및 TTL 설정
	timeoutAndTTLExample(logger)

	fmt.Println("\n모든 예제가 완료되었습니다!")
}

// 1. 기본 메모리 캐시 (최소 설정)
func basicMemoryCache(logger log.Logger) {
	fmt.Println("\n1. 기본 메모리 캐시 (최소 설정)")

	// 가장 간단한 메모리 캐시 설정
	hotStore := memstore.New(
		1024*1024, // 1MB 용량
		nil,       // 기본 정책 (제거 없음)
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
	)
	if err != nil {
		fmt.Printf("캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Println("✓ 기본 메모리 캐시 생성 완료")
}

// 2. 고급 메모리 캐시 (모든 옵션)
func advancedMemoryCache(logger log.Logger) {
	fmt.Println("\n2. 고급 메모리 캐시 (모든 옵션)")

	// LRU 정책과 스트라이프 락을 사용하는 메모리 캐시
	lruPolicy := policy.NewLRUPolicy()
	stripeLock := lock.NewStripeLock(1024) // 1024개 락 슬롯

	hotStore := memstore.New(
		10*1024*1024, // 10MB 용량
		lruPolicy,
		memstore.WithLocker(stripeLock),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithWorker("pool", 5, 1000, 30*time.Second),
		daramjwee.WithCache(5*time.Minute),         // 5분 양성 캐시
		daramjwee.WithNegativeCache(1*time.Minute), // 1분 음성 캐시
		daramjwee.WithShutdownTimeout(30*time.Second),
	)
	if err != nil {
		fmt.Printf("고급 메모리 캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Println("✓ 고급 메모리 캐시 생성 완료 (LRU + 스트라이프 락)")
}

// 3. 파일 스토어 캐시 (기본)
func basicFileCache(logger log.Logger) {
	fmt.Println("\n3. 파일 스토어 캐시 (기본)")

	// 임시 디렉토리 생성
	tmpDir, err := os.MkdirTemp("", "daramjwee-basic-file-*")
	if err != nil {
		fmt.Printf("임시 디렉토리 생성 실패: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	// 기본 파일 스토어
	fileStore, err := filestore.New(
		tmpDir,
		logger,
		100*1024*1024, // 100MB 용량
		nil,           // 기본 정책
	)
	if err != nil {
		fmt.Printf("파일 스토어 생성 실패: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(fileStore),
	)
	if err != nil {
		fmt.Printf("파일 캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Printf("✓ 기본 파일 캐시 생성 완료 (디렉토리: %s)\n", tmpDir)
}

// 4. 파일 스토어 캐시 (고급 옵션들)
func advancedFileCache(logger log.Logger) {
	fmt.Println("\n4. 파일 스토어 캐시 (고급 옵션들)")

	tmpDir, err := os.MkdirTemp("", "daramjwee-advanced-file-*")
	if err != nil {
		fmt.Printf("임시 디렉토리 생성 실패: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	// S3-FIFO 정책과 해시된 키를 사용하는 파일 스토어
	s3fifoPolicy := policy.NewS3FIFOPolicy(100*1024*1024, 0.1) // 100MB 총 용량, 10% 스몰 큐
	mutexLock := lock.NewMutexLock()

	fileStore, err := filestore.New(
		tmpDir,
		logger,
		100*1024*1024, // 100MB 용량
		s3fifoPolicy,
		filestore.WithHashedKeys(2, 2),  // 2단계 디렉토리, 각 2글자
		filestore.WithCopyAndTruncate(), // NFS 호환성
		filestore.WithLocker(mutexLock),
	)
	if err != nil {
		fmt.Printf("고급 파일 스토어 생성 실패: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(fileStore),
		daramjwee.WithWorker("all", 1, 100, 60*time.Second), // 모든 작업 즉시 실행
	)
	if err != nil {
		fmt.Printf("고급 파일 캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Printf("✓ 고급 파일 캐시 생성 완료 (S3-FIFO + 해시된 키, 디렉토리: %s)\n", tmpDir)
}

// 5. 하이브리드 멀티티어 캐시
func hybridMultiTierCache(logger log.Logger) {
	fmt.Println("\n5. 하이브리드 멀티티어 캐시")

	// Hot Tier: 빠른 메모리 캐시 (SIEVE 정책)
	sievePolicy := policy.NewSievePolicy()
	stripeLock := lock.NewStripeLock(512)

	hotStore := memstore.New(
		50*1024*1024, // 50MB 메모리 캐시
		sievePolicy,
		memstore.WithLocker(stripeLock),
	)

	// Cold Tier: 대용량 파일 캐시 (LRU 정책)
	tmpDir, err := os.MkdirTemp("", "daramjwee-hybrid-*")
	if err != nil {
		fmt.Printf("임시 디렉토리 생성 실패: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	lruPolicy := policy.NewLRUPolicy()
	coldStore, err := filestore.New(
		tmpDir,
		logger,
		1024*1024*1024, // 1GB 파일 캐시
		lruPolicy,
		filestore.WithHashedKeys(3, 2), // 3단계 디렉토리
	)
	if err != nil {
		fmt.Printf("콜드 스토어 생성 실패: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
		daramjwee.WithWorker("pool", 10, 2000, 45*time.Second),
		daramjwee.WithCache(10*time.Minute),        // 10분 양성 캐시
		daramjwee.WithNegativeCache(2*time.Minute), // 2분 음성 캐시
		daramjwee.WithDefaultTimeout(15*time.Second),
		daramjwee.WithShutdownTimeout(60*time.Second),
	)
	if err != nil {
		fmt.Printf("하이브리드 캐시 생성 실패: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Printf("✓ 하이브리드 멀티티어 캐시 생성 완료 (메모리 50MB + 파일 1GB)\n")
}

// 6. 워커 전략별 설정
func workerStrategiesExample(logger log.Logger) {
	fmt.Println("\n6. 워커 전략별 설정")

	hotStore := memstore.New(10*1024*1024, nil)

	// Pool 전략 (기본)
	poolCache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithWorker("pool", 8, 1500, 30*time.Second),
	)
	if err != nil {
		fmt.Printf("풀 워커 캐시 생성 실패: %v\n", err)
		return
	}
	defer poolCache.Close()
	fmt.Println("✓ Pool 워커 전략 캐시 생성 완료 (8개 워커, 1500 큐 크기)")

	// All 전략 (모든 작업 즉시 실행)
	allCache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithWorker("all", 1, 100, 60*time.Second),
	)
	if err != nil {
		fmt.Printf("All 워커 캐시 생성 실패: %v\n", err)
		return
	}
	defer allCache.Close()
	fmt.Println("✓ All 워커 전략 캐시 생성 완료 (모든 작업 즉시 실행)")
}

// 7. 캐시 정책별 설정
func cachePoliciesExample(logger log.Logger) {
	fmt.Println("\n7. 캐시 정책별 설정")

	capacity := int64(20 * 1024 * 1024) // 20MB

	// LRU 정책
	lruStore := memstore.New(capacity, policy.NewLRUPolicy())
	lruCache, _ := daramjwee.New(logger, daramjwee.WithHotStore(lruStore))
	defer lruCache.Close()
	fmt.Println("✓ LRU 정책 캐시 생성 완료")

	// S3-FIFO 정책
	s3fifoStore := memstore.New(capacity, policy.NewS3FIFOPolicy(capacity, 0.2)) // 20% 스몰 큐
	s3fifoCache, _ := daramjwee.New(logger, daramjwee.WithHotStore(s3fifoStore))
	defer s3fifoCache.Close()
	fmt.Println("✓ S3-FIFO 정책 캐시 생성 완료 (20% 스몰 큐)")

	// SIEVE 정책
	sieveStore := memstore.New(capacity, policy.NewSievePolicy())
	sieveCache, _ := daramjwee.New(logger, daramjwee.WithHotStore(sieveStore))
	defer sieveCache.Close()
	fmt.Println("✓ SIEVE 정책 캐시 생성 완료")

	// 정책 없음 (무제한)
	nullStore := memstore.New(0, nil) // 용량 제한 없음
	nullCache, _ := daramjwee.New(logger, daramjwee.WithHotStore(nullStore))
	defer nullCache.Close()
	fmt.Println("✓ 정책 없음 캐시 생성 완료 (무제한)")
}

// 8. 락 전략별 설정
func lockStrategiesExample(logger log.Logger) {
	fmt.Println("\n8. 락 전략별 설정")

	capacity := int64(10 * 1024 * 1024)
	lruPolicy := policy.NewLRUPolicy()

	// Mutex 락
	mutexStore := memstore.New(capacity, lruPolicy, memstore.WithLocker(lock.NewMutexLock()))
	mutexCache, _ := daramjwee.New(logger, daramjwee.WithHotStore(mutexStore))
	defer mutexCache.Close()
	fmt.Println("✓ Mutex 락 캐시 생성 완료")

	// Stripe 락 (다양한 슬롯 수)
	stripe256Store := memstore.New(capacity, lruPolicy, memstore.WithLocker(lock.NewStripeLock(256)))
	stripe256Cache, _ := daramjwee.New(logger, daramjwee.WithHotStore(stripe256Store))
	defer stripe256Cache.Close()
	fmt.Println("✓ Stripe 락 캐시 생성 완료 (256 슬롯)")

	stripe2048Store := memstore.New(capacity, lruPolicy, memstore.WithLocker(lock.NewStripeLock(2048)))
	stripe2048Cache, _ := daramjwee.New(logger, daramjwee.WithHotStore(stripe2048Store))
	defer stripe2048Cache.Close()
	fmt.Println("✓ Stripe 락 캐시 생성 완료 (2048 슬롯)")
}

// 9. 압축 설정 예제
func compressionExample(logger log.Logger) {
	fmt.Println("\n9. 압축 설정 예제")

	// 압축 없음
	noneCompressor := daramjwee.NewNoneCompressor()
	fmt.Printf("✓ 압축 없음: %s (레벨: %d)\n", noneCompressor.Algorithm(), noneCompressor.Level())

	// Gzip 압축 (다양한 레벨)
	gzipDefault := comp.NewDefaultGzipCompressor()
	fmt.Printf("✓ Gzip 기본: %s (레벨: %d)\n", gzipDefault.Algorithm(), gzipDefault.Level())

	gzipFast, _ := comp.NewGzipCompressor(1) // 빠른 압축
	fmt.Printf("✓ Gzip 빠른: %s (레벨: %d)\n", gzipFast.Algorithm(), gzipFast.Level())

	gzipBest, _ := comp.NewGzipCompressor(9) // 최고 압축
	fmt.Printf("✓ Gzip 최고: %s (레벨: %d)\n", gzipBest.Algorithm(), gzipBest.Level())

	// LZ4 압축 (미구현)
	lz4Default := comp.NewDefaultLZ4Compressor()
	fmt.Printf("✓ LZ4 기본: %s (레벨: %d) - 미구현\n", lz4Default.Algorithm(), lz4Default.Level())

	// Zstd 압축 (미구현)
	zstdDefault := comp.NewDefaultZstdCompressor()
	fmt.Printf("✓ Zstd 기본: %s (레벨: %d) - 미구현\n", zstdDefault.Algorithm(), zstdDefault.Level())
}

// 10. 타임아웃 및 TTL 설정
func timeoutAndTTLExample(logger log.Logger) {
	fmt.Println("\n10. 타임아웃 및 TTL 설정")

	hotStore := memstore.New(10*1024*1024, nil)

	// 다양한 타임아웃 설정
	timeoutConfigs := []struct {
		name            string
		defaultTimeout  time.Duration
		shutdownTimeout time.Duration
		positiveTTL     time.Duration
		negativeTTL     time.Duration
	}{
		{
			name:            "빠른 응답",
			defaultTimeout:  5 * time.Second,
			shutdownTimeout: 10 * time.Second,
			positiveTTL:     1 * time.Minute,
			negativeTTL:     10 * time.Second,
		},
		{
			name:            "표준 설정",
			defaultTimeout:  30 * time.Second,
			shutdownTimeout: 30 * time.Second,
			positiveTTL:     5 * time.Minute,
			negativeTTL:     1 * time.Minute,
		},
		{
			name:            "긴 캐시",
			defaultTimeout:  60 * time.Second,
			shutdownTimeout: 60 * time.Second,
			positiveTTL:     30 * time.Minute,
			negativeTTL:     5 * time.Minute,
		},
		{
			name:            "즉시 만료",
			defaultTimeout:  15 * time.Second,
			shutdownTimeout: 15 * time.Second,
			positiveTTL:     0, // 즉시 stale
			negativeTTL:     0, // 즉시 stale
		},
	}

	for _, config := range timeoutConfigs {
		cache, err := daramjwee.New(logger,
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithDefaultTimeout(config.defaultTimeout),
			daramjwee.WithShutdownTimeout(config.shutdownTimeout),
			daramjwee.WithCache(config.positiveTTL),
			daramjwee.WithNegativeCache(config.negativeTTL),
		)
		if err != nil {
			fmt.Printf("%s 캐시 생성 실패: %v\n", config.name, err)
			continue
		}
		defer cache.Close()

		fmt.Printf("✓ %s 캐시 생성 완료 (기본: %v, 종료: %v, 양성: %v, 음성: %v)\n",
			config.name, config.defaultTimeout, config.shutdownTimeout,
			config.positiveTTL, config.negativeTTL)
	}
}
