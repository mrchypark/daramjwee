// Package main demonstrates scenario-based configuration examples for daramjwee cache
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/lock"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// SimpleFetcher is a simple Fetcher implementation for testing
type SimpleFetcher struct {
	data string
}

func (f *SimpleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	// Simulate ETag-based conditional requests
	currentETag := "etag-123"
	if oldMetadata != nil && oldMetadata.ETag == currentETag {
		return nil, daramjwee.ErrNotModified
	}

	return &daramjwee.FetchResult{
		Body: io.NopCloser(strings.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{
			ETag:     currentETag,
			CachedAt: time.Now(),
		},
	}, nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	logger = level.NewFilter(logger, level.AllowInfo())

	fmt.Println("=== daramjwee Scenario-based Configuration Examples ===")

	// 1. Web proxy cache scenario
	webProxyScenario(logger)

	// 2. API response cache scenario
	apiResponseCacheScenario(logger)

	// 3. Image/media cache scenario
	mediaCacheScenario(logger)

	// 4. Database query cache scenario
	databaseQueryCacheScenario(logger)

	// 5. CDN edge cache scenario
	cdnEdgeCacheScenario(logger)

	// 6. Microservice inter-communication cache scenario
	microserviceCacheScenario(logger)

	// 7. Development/test environment cache scenario
	developmentCacheScenario(logger)

	// 8. High-performance read-only cache scenario
	highPerformanceReadOnlyScenario(logger)

	fmt.Println("\nAll scenario examples completed!")
}

// 1. Web proxy cache scenario
// - High throughput and fast response times are critical
// - Memory + disk hybrid configuration
// - Short TTL to maintain freshness
func webProxyScenario(logger log.Logger) {
	fmt.Println("\n1. Web Proxy Cache Scenario")

	// Hot Tier: Fast memory cache (frequently accessed content)
	hotStore := memstore.New(
		256*1024*1024, // 256MB memory
		policy.NewLRUPolicy(),
		memstore.WithLocker(lock.NewStripeLock(1024)), // High concurrency
	)

	// Cold Tier: Large capacity disk cache
	tmpDir, _ := os.MkdirTemp("", "proxy-cache-*")
	defer os.RemoveAll(tmpDir)

	coldStore, _ := filestore.New(
		tmpDir,
		logger,
		2*1024*1024*1024, // 2GB disk
		policy.NewS3FIFOPolicy(2*1024*1024*1024, 0.1),
		filestore.WithHashedKeys(2, 2), // Efficient file distribution
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
		daramjwee.WithWorker("pool", 20, 5000, 10*time.Second), // High throughput
		daramjwee.WithDefaultTimeout(5*time.Second),            // Fast response
		daramjwee.WithCache(5*time.Minute),                     // 5-minute cache
		daramjwee.WithNegativeCache(30*time.Second),            // Short negative cache
	)
	if err != nil {
		fmt.Printf("Failed to create web proxy cache: %v\n", err)
		return
	}
	defer cache.Close()

	// Usage example
	fetcher := &SimpleFetcher{data: "Web page content"}
	ctx := context.Background()

	reader, err := cache.Get(ctx, "https://example.com/page", fetcher)
	if err != nil {
		fmt.Printf("Failed to query cache: %v\n", err)
	} else {
		defer reader.Close()
		fmt.Println("✓ Web proxy cache configuration completed (256MB memory + 2GB disk)")
	}
}

// 2. API response cache scenario
// - Fast caching of API responses
// - Short TTL and efficient memory usage
// - SIEVE policy for high hit rates
func apiResponseCacheScenario(logger log.Logger) {
	fmt.Println("\n2. API Response Cache Scenario")

	// Memory-based cache (API responses are usually small)
	hotStore := memstore.New(
		128*1024*1024,           // 128MB
		policy.NewSievePolicy(), // High hit rate
		memstore.WithLocker(lock.NewStripeLock(512)),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithWorker("pool", 10, 2000, 15*time.Second),
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithCache(2*time.Minute),          // 2-minute cache (API responses)
		daramjwee.WithNegativeCache(15*time.Second), // Short negative cache
	)
	if err != nil {
		fmt.Printf("Failed to create API response cache: %v\n", err)
		return
	}
	defer cache.Close()

	// Usage example
	fetcher := &SimpleFetcher{data: `{"users": [{"id": 1, "name": "John"}]}`}
	ctx := context.Background()

	reader, err := cache.Get(ctx, "api/users", fetcher)
	if err != nil {
		fmt.Printf("Failed to query API cache: %v\n", err)
	} else {
		defer reader.Close()
		fmt.Println("✓ API response cache configuration completed (128MB memory, SIEVE policy)")
	}
}

// 3. Image/media cache scenario
// - Large file handling
// - Disk-based storage
// - Long TTL (media files change infrequently)
func mediaCacheScenario(logger log.Logger) {
	fmt.Println("\n3. Image/Media Cache Scenario")

	tmpDir, _ := os.MkdirTemp("", "media-cache-*")
	defer os.RemoveAll(tmpDir)

	// Disk-based cache for large files
	mediaStore, _ := filestore.New(
		tmpDir,
		logger,
		10*1024*1024*1024, // 10GB capacity
		policy.NewLRUPolicy(),
		filestore.WithHashedKeys(3, 2),  // Deep directory structure
		filestore.WithCopyAndTruncate(), // NFS compatibility
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(mediaStore),
		daramjwee.WithWorker("pool", 5, 500, 60*time.Second), // Long task duration
		daramjwee.WithDefaultTimeout(30*time.Second),
		daramjwee.WithCache(24*time.Hour),          // 24-hour cache
		daramjwee.WithNegativeCache(5*time.Minute), // Negative cache
	)
	if err != nil {
		fmt.Printf("Failed to create media cache: %v\n", err)
		return
	}
	defer cache.Close()

	// Usage example
	fetcher := &SimpleFetcher{data: "Image binary data..."}
	ctx := context.Background()

	reader, err := cache.Get(ctx, "images/photo123.jpg", fetcher)
	if err != nil {
		fmt.Printf("Failed to query media cache: %v\n", err)
	} else {
		defer reader.Close()
		fmt.Println("✓ Media cache configuration completed (10GB disk, 24-hour TTL)")
	}
}

// 4. Database query cache scenario
// - Caching complex query results
// - Medium TTL
// - Memory-first with disk backup
func databaseQueryCacheScenario(logger log.Logger) {
	fmt.Println("\n4. Database Query Cache Scenario")

	// Hot: Frequently used query results
	hotStore := memstore.New(
		512*1024*1024,                              // 512MB
		policy.NewS3FIFOPolicy(512*1024*1024, 0.2), // 20% small queue
		memstore.WithLocker(lock.NewStripeLock(256)),
	)

	// Cold: Large query results or less frequently used results
	tmpDir, _ := os.MkdirTemp("", "db-cache-*")
	defer os.RemoveAll(tmpDir)

	coldStore, _ := filestore.New(
		tmpDir,
		logger,
		5*1024*1024*1024, // 5GB
		policy.NewLRUPolicy(),
		filestore.WithHashedKeys(2, 3),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
		daramjwee.WithWorker("pool", 8, 1000, 30*time.Second),
		daramjwee.WithDefaultTimeout(20*time.Second),
		daramjwee.WithCache(15*time.Minute),        // 15-minute cache
		daramjwee.WithNegativeCache(2*time.Minute), // Negative cache
	)
	if err != nil {
		fmt.Printf("Failed to create DB query cache: %v\n", err)
		return
	}
	defer cache.Close()

	// Usage example
	fetcher := &SimpleFetcher{data: "SELECT * FROM users WHERE active=1 results..."}
	ctx := context.Background()

	reader, err := cache.Get(ctx, "query:users:active", fetcher)
	if err != nil {
		fmt.Printf("Failed to query DB cache: %v\n", err)
	} else {
		defer reader.Close()
		fmt.Println("✓ DB query cache configuration completed (512MB memory + 5GB disk)")
	}
}

// 5. CDN edge cache scenario
// - Very high throughput
// - Regional content caching
// - Efficient storage management
func cdnEdgeCacheScenario(logger log.Logger) {
	fmt.Println("\n5. CDN Edge Cache Scenario")

	// Very large memory cache (edge server)
	hotStore := memstore.New(
		2*1024*1024*1024, // 2GB memory
		policy.NewSievePolicy(),
		memstore.WithLocker(lock.NewStripeLock(4096)), // Very high concurrency
	)

	// Large capacity disk cache
	tmpDir, _ := os.MkdirTemp("", "cdn-cache-*")
	defer os.RemoveAll(tmpDir)

	coldStore, _ := filestore.New(
		tmpDir,
		logger,
		50*1024*1024*1024, // 50GB
		policy.NewS3FIFOPolicy(50*1024*1024*1024, 0.05), // 5% small queue
		filestore.WithHashedKeys(4, 2),                  // Very deep structure
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
		daramjwee.WithWorker("pool", 50, 10000, 5*time.Second), // Very high throughput
		daramjwee.WithDefaultTimeout(3*time.Second),            // Very fast response
		daramjwee.WithCache(1*time.Hour),                       // 1-hour cache
		daramjwee.WithNegativeCache(1*time.Minute),
	)
	if err != nil {
		fmt.Printf("Failed to create CDN edge cache: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Println("✓ CDN edge cache configuration completed (2GB memory + 50GB disk, ultra-high performance)")
}

// 6. Microservice inter-communication cache scenario
// - Inter-service communication optimization
// - Medium-sized cache
// - Moderate TTL
func microserviceCacheScenario(logger log.Logger) {
	fmt.Println("\n6. Microservice Inter-communication Cache Scenario")

	hotStore := memstore.New(
		64*1024*1024, // 64MB (microservices use small memory)
		policy.NewLRUPolicy(),
		memstore.WithLocker(lock.NewMutexLock()), // Simple lock
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithWorker("all", 1, 100, 10*time.Second), // Simple worker
		daramjwee.WithDefaultTimeout(5*time.Second),
		daramjwee.WithCache(3*time.Minute), // 3-minute cache
		daramjwee.WithNegativeCache(30*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create microservice cache: %v\n", err)
		return
	}
	defer cache.Close()

	// Usage example
	fetcher := &SimpleFetcher{data: `{"service": "user-service", "data": {...}}`}
	ctx := context.Background()

	reader, err := cache.Get(ctx, "service:user:profile:123", fetcher)
	if err != nil {
		fmt.Printf("Failed to query microservice cache: %v\n", err)
	} else {
		defer reader.Close()
		fmt.Println("✓ Microservice cache configuration completed (64MB, lightweight setup)")
	}
}

// 7. Development/test environment cache scenario
// - Configuration for rapid development
// - Small resource usage
// - Short TTL (quick change reflection)
func developmentCacheScenario(logger log.Logger) {
	fmt.Println("\n7. Development/Test Environment Cache Scenario")

	hotStore := memstore.New(
		16*1024*1024, // 16MB (development environment)
		nil,          // No policy (simplicity)
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithWorker("all", 1, 50, 5*time.Second), // Simple configuration
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithCache(30*time.Second),        // 30-second cache (quick changes)
		daramjwee.WithNegativeCache(5*time.Second), // Very short negative cache
	)
	if err != nil {
		fmt.Printf("Failed to create development cache: %v\n", err)
		return
	}
	defer cache.Close()

	fmt.Println("✓ Development/test environment cache configuration completed (16MB, simple setup)")
}

// 8. High-performance read-only cache scenario
// - Read performance optimization
// - Long TTL
// - Optimized lock strategy
func highPerformanceReadOnlyScenario(logger log.Logger) {
	fmt.Println("\n8. High-performance Read-only Cache Scenario")

	hotStore := memstore.New(
		1024*1024*1024,                                // 1GB
		policy.NewSievePolicy(),                       // Read optimization
		memstore.WithLocker(lock.NewStripeLock(8192)), // Very high concurrency
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithWorker("pool", 4, 200, 60*time.Second), // Fewer background tasks
		daramjwee.WithDefaultTimeout(1*time.Second),          // Very fast response
		daramjwee.WithCache(6*time.Hour),                     // 6-hour cache
		daramjwee.WithNegativeCache(10*time.Minute),
	)
	if err != nil {
		fmt.Printf("Failed to create high-performance read-only cache: %v\n", err)
		return
	}
	defer cache.Close()

	// Usage example
	fetcher := &SimpleFetcher{data: "Static content data..."}
	ctx := context.Background()

	reader, err := cache.Get(ctx, "static:content:123", fetcher)
	if err != nil {
		fmt.Printf("Failed to query high-performance cache: %v\n", err)
	} else {
		defer reader.Close()
		fmt.Println("✓ High-performance read-only cache configuration completed (1GB, 8192 stripes)")
	}
}
