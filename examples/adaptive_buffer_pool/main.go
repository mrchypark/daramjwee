package main

import (
	"fmt"
	"log"
	"os"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

func main() {
	// Create logger
	logger := kitlog.NewLogfmtLogger(os.Stderr)

	// Create memory store with LRU policy
	hotStore := memstore.New(
		100*1024*1024, // 100MB capacity
		policy.NewLRUPolicy(),
	)

	// Create cache with adaptive buffer pool
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
		daramjwee.WithBufferPoolMetrics(true, 10*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close()

	fmt.Println("✅ daramjwee cache created successfully with adaptive buffer pool!")
	fmt.Println("🚀 Ready for high-performance caching operations")

	// Get buffer pool stats
	if daramjweeCache, ok := cache.(*daramjwee.DaramjweeCache); ok {
		stats := daramjweeCache.BufferPool.GetStats()
		fmt.Printf("📊 Buffer Pool Stats: Gets=%d, Puts=%d, Hits=%d, Misses=%d",
			stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses)
	}
}
