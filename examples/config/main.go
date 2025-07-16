// Package main demonstrates comprehensive configuration examples for daramjwee cache
// This file contains examples showing all configuration cases for daramjwee cache
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

	fmt.Println("=== daramjwee Cache Configuration Examples ===")

	// 1. Basic memory cache (minimal configuration)
	basicMemoryCache(logger)

	// 2. Advanced memory cache (all options)
	advancedMemoryCache(logger)

	// 3. File store cache (basic)
	basicFileCache(logger)

	// 4. File store cache (advanced options)
	advancedFileCache(logger)

	// 5. Hybrid multi-tier cache
	hybridMultiTierCache(logger)

	// 6. Worker strategy configurations
	workerStrategiesExample(logger)

	// 7. Cache policy configurations
	cachePoliciesExample(logger)

	// 8. Lock strategy configurations
	lockStrategiesExample(logger)

	// 9. Compression configuration examples
	compressionExample(logger)

	// 10. Timeout and TTL configurations
	timeoutAndTTLExample(logger)

	fmt.Println("\nAll examples completed!")
}

// 1. Basic memory cache (minimal configuration)
func basicMemoryCache(logger log.Logger) {
	fmt.Println("\n1. Basic Memory Cache (Minimal Configuration)")

	// Simplest memory cache configuration
	hotStore := memstore.New(
		1024*1024, // 1MB capacity
		nil,       // Default policy (no eviction)
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Println("✓ Basic memory cache created successfully")
}

// 2. Advanced memory cache (all options)
func advancedMemoryCache(logger log.Logger) {
	fmt.Println("\n2. Advanced Memory Cache (All Options)")

	// Memory cache using LRU policy and stripe lock
	lruPolicy := policy.NewLRUPolicy()
	stripeLock := lock.NewStripeLock(1024) // 1024 lock slots

	hotStore := memstore.New(
		10*1024*1024, // 10MB capacity
		lruPolicy,
		memstore.WithLocker(stripeLock),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithWorker("pool", 5, 1000, 30*time.Second),
		daramjwee.WithCache(5*time.Minute),         // 5-minute positive cache
		daramjwee.WithNegativeCache(1*time.Minute), // 1-minute negative cache
		daramjwee.WithShutdownTimeout(30*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create advanced memory cache: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Println("✓ Advanced memory cache created successfully (LRU + Stripe Lock)")
}

// 3. File store cache (basic)
func basicFileCache(logger log.Logger) {
	fmt.Println("\n3. File Store Cache (Basic)")

	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "daramjwee-basic-file-*")
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	// Basic file store
	fileStore, err := filestore.New(
		tmpDir,
		logger,
		100*1024*1024, // 100MB capacity
		nil,           // Default policy
	)
	if err != nil {
		fmt.Printf("Failed to create file store: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(fileStore),
	)
	if err != nil {
		fmt.Printf("Failed to create file cache: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Printf("✓ Basic file cache created successfully (directory: %s)\n", tmpDir)
}

// 4. File store cache (advanced options)
func advancedFileCache(logger log.Logger) {
	fmt.Println("\n4. File Store Cache (Advanced Options)")

	tmpDir, err := os.MkdirTemp("", "daramjwee-advanced-file-*")
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	// File store using S3-FIFO policy and hashed keys
	s3fifoPolicy := policy.NewS3FIFOPolicy(100*1024*1024, 0.1) // 100MB total capacity, 10% small queue
	mutexLock := lock.NewMutexLock()

	fileStore, err := filestore.New(
		tmpDir,
		logger,
		100*1024*1024, // 100MB capacity
		s3fifoPolicy,
		filestore.WithHashedKeys(2, 2),  // 2-level directory, 2 chars each
		filestore.WithCopyAndTruncate(), // NFS compatibility
		filestore.WithLocker(mutexLock),
	)
	if err != nil {
		fmt.Printf("Failed to create advanced file store: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(fileStore),
		daramjwee.WithWorker("all", 1, 100, 60*time.Second), // Execute all tasks immediately
	)
	if err != nil {
		fmt.Printf("Failed to create advanced file cache: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Printf("✓ Advanced file cache created successfully (S3-FIFO + hashed keys, directory: %s)\n", tmpDir)
}

// 5. Hybrid multi-tier cache
func hybridMultiTierCache(logger log.Logger) {
	fmt.Println("\n5. Hybrid Multi-tier Cache")

	// Hot Tier: Fast memory cache (SIEVE policy)
	sievePolicy := policy.NewSievePolicy()
	stripeLock := lock.NewStripeLock(512)

	hotStore := memstore.New(
		50*1024*1024, // 50MB memory cache
		sievePolicy,
		memstore.WithLocker(stripeLock),
	)

	// Cold Tier: Large capacity file cache (LRU policy)
	tmpDir, err := os.MkdirTemp("", "daramjwee-hybrid-*")
	if err != nil {
		fmt.Printf("Failed to create temporary directory: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	lruPolicy := policy.NewLRUPolicy()
	coldStore, err := filestore.New(
		tmpDir,
		logger,
		1024*1024*1024, // 1GB file cache
		lruPolicy,
		filestore.WithHashedKeys(3, 2), // 3-level directory
	)
	if err != nil {
		fmt.Printf("Failed to create cold store: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
		daramjwee.WithWorker("pool", 10, 2000, 45*time.Second),
		daramjwee.WithCache(10*time.Minute),        // 10-minute positive cache
		daramjwee.WithNegativeCache(2*time.Minute), // 2-minute negative cache
		daramjwee.WithDefaultTimeout(15*time.Second),
		daramjwee.WithShutdownTimeout(60*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create hybrid cache: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Printf("✓ Hybrid multi-tier cache created successfully (50MB memory + 1GB file)\n")
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
