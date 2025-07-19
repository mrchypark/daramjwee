// Package main demonstrates N-tier cache architecture with multiple storage tiers
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// Enhanced origin fetcher with tier-aware logging
type multiTierFetcher struct {
	key  string
	data map[string]struct {
		content string
		etag    string
	}
}

func newMultiTierFetcher() *multiTierFetcher {
	return &multiTierFetcher{
		data: map[string]struct {
			content string
			etag    string
		}{
			"small-data":  {"Small data for memory tier testing", "v1-small"},
			"medium-data": {strings.Repeat("Medium data for file tier testing. ", 100), "v1-medium"},
			"large-data":  {strings.Repeat("Large data for cloud tier testing. ", 1000), "v1-large"},
			"user-1":      {"User profile data for user 1", "v1-user1"},
			"user-2":      {"User profile data for user 2", "v1-user2"},
			"config":      {"Application configuration data", "v1-config"},
		},
	}
}

func (f *multiTierFetcher) withKey(key string) *multiTierFetcher {
	return &multiTierFetcher{key: key, data: f.data}
}

func (f *multiTierFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	oldETag := "none"
	if oldMetadata != nil {
		oldETag = oldMetadata.ETag
	}

	fmt.Printf("[Origin] Fetching key: %s (old ETag: %s)\n", f.key, oldETag)

	// Simulate network latency
	time.Sleep(200 * time.Millisecond)

	obj, exists := f.data[f.key]
	if !exists {
		return nil, daramjwee.ErrCacheableNotFound
	}

	// Handle conditional requests
	if oldMetadata != nil && oldMetadata.ETag == obj.etag {
		return nil, daramjwee.ErrNotModified
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(obj.content))),
		Metadata: &daramjwee.Metadata{ETag: obj.etag},
	}, nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	logger = level.NewFilter(logger, level.AllowDebug())

	fmt.Println("=== daramjwee N-Tier Cache Architecture Demo ===")

	// Clean up any existing cache directories
	os.RemoveAll("./cache-tier1")
	os.RemoveAll("./cache-tier2")

	// Demonstrate different N-tier configurations
	fmt.Println("\n1. Single Tier Configuration (Memory Only)")
	runSingleTierDemo(logger)

	fmt.Println("\n2. Two Tier Configuration (Memory + File)")
	runTwoTierDemo(logger)

	fmt.Println("\n3. Three Tier Configuration (Memory + File + File)")
	runThreeTierDemo(logger)

	fmt.Println("\n4. Legacy Configuration Migration Demo")
	runLegacyMigrationDemo(logger)

	fmt.Println("\n5. Performance Comparison")
	runPerformanceComparison(logger)

	fmt.Println("\n6. HTTP Server with N-Tier Cache")
	runHTTPServer(logger)
}

// Single tier configuration demo
func runSingleTierDemo(logger log.Logger) {
	// Memory-only cache
	memStore := memstore.New(10*1024*1024, policy.NewLRUPolicy()) // 10MB

	cache, err := daramjwee.New(logger,
		daramjwee.WithStores(memStore), // Single tier
		daramjwee.WithDefaultTimeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create single-tier cache: %v\n", err)
		return
	}
	defer cache.Close()

	fetcher := newMultiTierFetcher()

	// Test cache operations
	fmt.Println("  Testing single-tier cache operations...")
	testCacheOperations(cache, fetcher, "single-tier")
}

// Two tier configuration demo
func runTwoTierDemo(logger log.Logger) {
	// Memory tier (fast, limited capacity)
	memStore := memstore.New(5*1024*1024, policy.NewLRUPolicy()) // 5MB

	// File tier (slower, larger capacity)
	fileStore, err := filestore.New("./cache-tier1", log.With(logger, "tier", "file"), 50*1024*1024, nil)
	if err != nil {
		fmt.Printf("Failed to create file store: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithStores(memStore, fileStore), // Two tiers
		daramjwee.WithDefaultTimeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create two-tier cache: %v\n", err)
		return
	}
	defer cache.Close()

	fetcher := newMultiTierFetcher()

	fmt.Println("  Testing two-tier cache operations...")
	testCacheOperations(cache, fetcher, "two-tier")

	// Demonstrate promotion behavior
	fmt.Println("  Testing tier promotion...")
	testTierPromotion(cache, fetcher)
}

// Three tier configuration demo
func runThreeTierDemo(logger log.Logger) {
	// Memory tier (fastest, smallest)
	memStore := memstore.New(2*1024*1024, policy.NewLRUPolicy()) // 2MB

	// File tier 1 (medium speed, medium capacity)
	fileStore1, err := filestore.New("./cache-tier1", log.With(logger, "tier", "file1"), 20*1024*1024, nil)
	if err != nil {
		fmt.Printf("Failed to create file store 1: %v\n", err)
		return
	}

	// File tier 2 (slower, largest capacity)
	fileStore2, err := filestore.New("./cache-tier2", log.With(logger, "tier", "file2"), 100*1024*1024, nil)
	if err != nil {
		fmt.Printf("Failed to create file store 2: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithStores(memStore, fileStore1, fileStore2), // Three tiers
		daramjwee.WithDefaultTimeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create three-tier cache: %v\n", err)
		return
	}
	defer cache.Close()

	fetcher := newMultiTierFetcher()

	fmt.Println("  Testing three-tier cache operations...")
	testCacheOperations(cache, fetcher, "three-tier")
}

// Legacy configuration migration demo
func runLegacyMigrationDemo(logger log.Logger) {
	fmt.Println("  Demonstrating legacy configuration compatibility...")

	// Legacy configuration (still works)
	memStore := memstore.New(5*1024*1024, policy.NewLRUPolicy())
	fileStore, _ := filestore.New("./cache-legacy", log.With(logger, "tier", "legacy"), 20*1024*1024, nil)

	legacyCache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(memStore),   // Legacy hot store
		daramjwee.WithColdStore(fileStore), // Legacy cold store
		daramjwee.WithDefaultTimeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create legacy cache: %v\n", err)
		return
	}
	defer legacyCache.Close()

	// Equivalent N-tier configuration
	memStore2 := memstore.New(5*1024*1024, policy.NewLRUPolicy())
	fileStore2, _ := filestore.New("./cache-ntier", log.With(logger, "tier", "ntier"), 20*1024*1024, nil)

	ntierCache, err := daramjwee.New(logger,
		daramjwee.WithStores(memStore2, fileStore2), // N-tier equivalent
		daramjwee.WithDefaultTimeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create n-tier cache: %v\n", err)
		return
	}
	defer ntierCache.Close()

	fetcher := newMultiTierFetcher()

	// Test both configurations
	fmt.Println("    Testing legacy configuration...")
	testCacheOperations(legacyCache, fetcher, "legacy")

	fmt.Println("    Testing equivalent N-tier configuration...")
	testCacheOperations(ntierCache, fetcher, "ntier")

	fmt.Println("    Both configurations should behave identically!")
}

// Performance comparison between different configurations
func runPerformanceComparison(logger log.Logger) {
	configurations := []struct {
		name  string
		setup func() daramjwee.Cache
	}{
		{
			name: "Single-Tier (Memory)",
			setup: func() daramjwee.Cache {
				store := memstore.New(10*1024*1024, policy.NewLRUPolicy())
				cache, _ := daramjwee.New(logger, daramjwee.WithStores(store))
				return cache
			},
		},
		{
			name: "Two-Tier (Memory + File)",
			setup: func() daramjwee.Cache {
				memStore := memstore.New(5*1024*1024, policy.NewLRUPolicy())
				fileStore, _ := filestore.New("./perf-cache", logger, 20*1024*1024, nil)
				cache, _ := daramjwee.New(logger, daramjwee.WithStores(memStore, fileStore))
				return cache
			},
		},
	}

	fetcher := newMultiTierFetcher()

	for _, config := range configurations {
		fmt.Printf("  Testing %s performance...\n", config.name)
		cache := config.setup()

		start := time.Now()
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("perf-test-%d", i%10) // 10 unique keys
			stream, err := cache.Get(context.Background(), key, fetcher.withKey("small-data"))
			if err != nil {
				continue
			}
			io.Copy(io.Discard, stream)
			stream.Close()
		}
		duration := time.Since(start)

		fmt.Printf("    100 operations completed in %v (%.2f ops/sec)\n",
			duration, float64(100)/duration.Seconds())

		cache.Close()
	}
}

// Test basic cache operations
func testCacheOperations(cache daramjwee.Cache, fetcher *multiTierFetcher, prefix string) {
	keys := []string{"small-data", "medium-data", "user-1"}

	for _, key := range keys {
		fmt.Printf("    Testing key: %s\n", key)

		// First access (cache miss)
		start := time.Now()
		stream, err := cache.Get(context.Background(), key, fetcher.withKey(key))
		if err != nil {
			fmt.Printf("      Error: %v\n", err)
			continue
		}
		data, _ := io.ReadAll(stream)
		stream.Close()
		missTime := time.Since(start)

		// Second access (cache hit)
		start = time.Now()
		stream, err = cache.Get(context.Background(), key, fetcher.withKey(key))
		if err != nil {
			fmt.Printf("      Error: %v\n", err)
			continue
		}
		stream.Close()
		hitTime := time.Since(start)

		fmt.Printf("      Miss: %v, Hit: %v, Data size: %d bytes\n",
			missTime, hitTime, len(data))
	}
}

// Test tier promotion behavior
func testTierPromotion(cache daramjwee.Cache, fetcher *multiTierFetcher) {
	key := "promotion-test"

	// First access - should cache in primary tier
	fmt.Printf("    First access (origin fetch)...\n")
	stream, err := cache.Get(context.Background(), key, fetcher.withKey("large-data"))
	if err != nil {
		fmt.Printf("      Error: %v\n", err)
		return
	}
	stream.Close()

	// Simulate eviction from primary tier by accessing many other keys
	fmt.Printf("    Simulating primary tier eviction...\n")
	for i := 0; i < 20; i++ {
		evictKey := fmt.Sprintf("evict-%d", i)
		stream, _ := cache.Get(context.Background(), evictKey, fetcher.withKey("medium-data"))
		if stream != nil {
			stream.Close()
		}
	}

	// Access original key again - should hit secondary tier and promote
	fmt.Printf("    Second access (should promote from secondary tier)...\n")
	stream, err = cache.Get(context.Background(), key, fetcher.withKey("large-data"))
	if err != nil {
		fmt.Printf("      Error: %v\n", err)
		return
	}
	stream.Close()

	// Third access - should hit primary tier again
	fmt.Printf("    Third access (should hit primary tier)...\n")
	stream, err = cache.Get(context.Background(), key, fetcher.withKey("large-data"))
	if err != nil {
		fmt.Printf("      Error: %v\n", err)
		return
	}
	stream.Close()
}

// HTTP server demo with N-tier cache
func runHTTPServer(logger log.Logger) {
	// Create a comprehensive 3-tier cache
	memStore := memstore.New(10*1024*1024, policy.NewSievePolicy()) // 10MB memory
	fileStore, err := filestore.New("./http-cache", log.With(logger, "tier", "http"), 50*1024*1024, nil)
	if err != nil {
		fmt.Printf("Failed to create HTTP cache: %v\n", err)
		return
	}

	cache, err := daramjwee.New(logger,
		daramjwee.WithStores(memStore, fileStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithCache(2*time.Minute),
		daramjwee.WithNegativeCache(30*time.Second),
		daramjwee.WithWorker("pool", 5, 100, 30*time.Second),
	)
	if err != nil {
		fmt.Printf("Failed to create HTTP server cache: %v\n", err)
		return
	}
	defer cache.Close()

	fetcher := newMultiTierFetcher()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `
<!DOCTYPE html>
<html>
<head>
    <title>daramjwee N-Tier Cache Demo</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .tier { color: #666; font-size: 0.9em; }
    </style>
</head>
<body>
    <h1>daramjwee N-Tier Cache Demo</h1>
    <p>This server demonstrates N-tier cache architecture with Memory + File tiers.</p>
    
    <h2>Test Endpoints:</h2>
    <div class="endpoint">
        <strong><a href="/data/small-data">/data/small-data</a></strong><br>
        <span class="tier">Small data (good for memory tier)</span>
    </div>
    <div class="endpoint">
        <strong><a href="/data/medium-data">/data/medium-data</a></strong><br>
        <span class="tier">Medium data (may be evicted to file tier)</span>
    </div>
    <div class="endpoint">
        <strong><a href="/data/large-data">/data/large-data</a></strong><br>
        <span class="tier">Large data (likely to be evicted to file tier)</span>
    </div>
    <div class="endpoint">
        <strong><a href="/data/user-1">/data/user-1</a></strong><br>
        <span class="tier">User profile data</span>
    </div>
    <div class="endpoint">
        <strong><a href="/data/nonexistent">/data/nonexistent</a></strong><br>
        <span class="tier">Test negative caching (404)</span>
    </div>
    
    <h2>Cache Behavior:</h2>
    <ul>
        <li><strong>First request:</strong> Fetch from origin, cache in memory tier</li>
        <li><strong>Subsequent requests:</strong> Serve from memory tier (fast)</li>
        <li><strong>After eviction:</strong> Serve from file tier, promote to memory</li>
        <li><strong>Cache miss:</strong> Fetch from origin, cache in memory tier</li>
    </ul>
    
    <p>Check the server logs to see tier-specific cache behavior!</p>
</body>
</html>
		`)
	})

	http.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/data/")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		fmt.Printf("--- HTTP Request for key: %s ---\n", key)

		stream, err := cache.Get(r.Context(), key, fetcher.withKey(key))
		if err != nil {
			if err == daramjwee.ErrNotFound || err == daramjwee.ErrCacheableNotFound {
				fmt.Printf("[HTTP] Key not found: %s\n", key)
				http.Error(w, "Data Not Found", http.StatusNotFound)
			} else {
				fmt.Printf("[HTTP] Error: %v\n", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		defer stream.Close()

		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "public, max-age=60")

		fmt.Printf("[HTTP] Streaming response for key: %s\n", key)
		if _, err := io.Copy(w, stream); err != nil {
			fmt.Printf("[HTTP] Error streaming response: %v\n", err)
		}
		fmt.Printf("[HTTP] Response completed for key: %s\n", key)
	})

	fmt.Println("  N-Tier Cache HTTP Server starting on :8080")
	fmt.Println("  Visit http://localhost:8080 for interactive demo")
	fmt.Println("  Press Ctrl+C to stop the server")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("HTTP server error: %v\n", err)
	}
}
