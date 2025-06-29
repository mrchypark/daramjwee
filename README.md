# daramjwee 🐿️ `/dɑːrɑːmdʒwiː/`

A pragmatic and lightweight hybrid caching middleware for Go.

`daramjwee` sits between your application and your origin data source (e.g., a database or an API), providing an efficient, stream-based hybrid caching layer. It is designed with a focus on simplicity and core functionality to achieve high throughput at a low cost in cloud-native environments.

## Core Design Philosophy

`daramjwee` is built on two primary principles:

1.  **Purely Stream-Based API:** All data is processed through `io.Reader` and `io.Writer` interfaces. This means that even large objects are handled without memory overhead from intermediate buffering, guaranteeing optimal performance for proxying use cases. **Crucially, the user must always `Close()` the stream to finalize operations and prevent resource leaks.**
2.  **Modular and Pluggable Architecture:** Key components such as the storage backend (`Store`), eviction strategy (`EvictionPolicy`), and asynchronous task runner (`Worker`) are all designed as interfaces. This allows users to easily swap in their own implementations to fit specific needs.

## Current Status & Key Implementations

`daramjwee` is more than a proof-of-concept; it is a stable and mature library ready for production use. Its robustness is verified by a comprehensive test suite, including unit, integration, and stress tests.

  * **Robust Storage Backends (`Store`):**

      * **`FileStore`**: Guarantees atomic writes by default using a "write-to-temp-then-rename" pattern to prevent data corruption. It also offers a copy-based alternative (`WithCopyAndTruncate`) for compatibility with network filesystems, though this option is **not atomic and may leave orphan files on failure**.
      * **`MemStore`**: A thread-safe, high-throughput in-memory store with fully integrated capacity-based eviction logic. Its performance is optimized using `sync.Pool` to reduce memory allocations under high concurrency.
      * **`objstore` Adapter**: A built-in adapter for `thanos-io/objstore` allows immediate use of major cloud object stores (S3, GCS, Azure Blob Storage) as a Cold Tier. It supports true, memory-efficient streaming uploads using `io.Pipe`. **Note: Concurrent writes to the same key are not protected against race conditions by default.**

  * **Advanced Eviction Policies (`EvictionPolicy`):**

      * In addition to the traditional **LRU**, it implements modern, high-performance algorithms like **S3-FIFO** and **SIEVE**, allowing you to choose the optimal policy for your workload.

  * **Reliable Concurrency Management:**

      * **Worker Pool (`Worker`):** A configurable worker pool manages background tasks like cache refreshes, preventing unbounded goroutine creation and ensuring stable resource usage under load.
      * **Striped Locking (`FileLockManager`):** `FileStore` uses striped locking instead of a single global lock, minimizing lock contention for different keys during concurrent requests.

  * **Efficient Caching Logic:**

      * **ETag-based Optimization:** Avoids unnecessary data transfer by exchanging ETags with the origin server. If content is not modified (`ErrNotModified`), the fetch is skipped, saving network bandwidth.
      * **Negative Caching:** Caches the "not found" state for non-existent keys, preventing repeated, wasteful requests to the origin.
      * **Stale-While-Revalidate:** Can serve stale data while asynchronously refreshing it in the background, minimizing latency while maintaining data freshness. This replaces the previous "Grace Period" concept.

## Data Retrieval Flow

The data retrieval process in `daramjwee` follows a clear, tiered approach to maximize performance and efficiency.

```mermaid
flowchart TD
    A[Client Request for a Key] --> B{Check Hot Tier};

    B -- Hit --> C{Is item stale?};
    C -- No --> D[Stream data to Client];
    D --> E[End];
    C -- Yes --> F[Stream STALE data to Client];
    F --> G(Schedule Background Refresh);
    G --> E;

    B -- Miss --> H{Check Cold Tier};

    H -- Hit --> I[Stream data to Client & Promote to Hot Tier];
    I --> E;

    H -- Miss --> J[Fetch from Origin];
    J -- Success --> K[Stream data to Client & Write to Hot Tier];
    K --> L(Optionally: Schedule write to Cold Tier);
    L --> E;
    
    J -- Not Found (Cacheable) --> M[Cache Negative Entry];
    M --> N[Return 'Not Found' to Client];
    N --> E;
    
    J -- Not Modified (304) --> O{Re-fetch from Hot Tier};
    O -- Success --> D;
    O -- Failure (e.g., evicted) --> N;

1.  **Check Hot Tier:** Looks for the object in the Hot Tier.
      * **Hit (Fresh):** Immediately returns the object stream to the client.
      * **Hit (Stale):** Immediately returns the **stale** object stream to the client and schedules a background task to refresh the cache from the origin.
2.  **Check Cold Tier:** If not in the Hot Tier, it checks the Cold Tier.
      * **Hit:** Streams the object to the client while simultaneously promoting it to the Hot Tier for faster access next time.
3.  **Fetch from Origin:** If the object is in neither tier (Cache Miss), it invokes the user-provided `Fetcher`.
      * **Success:** The fetched data stream is sent to the client and written to the Hot Tier at the same time.
      * **Not Modified:** If the origin returns `ErrNotModified`, `daramjwee` attempts to re-serve the data from the Hot Tier.
      * **Not Found:** If the origin returns `ErrCacheableNotFound`, a negative entry is stored to prevent repeated fetches.

## Getting Started

Here is a simple example of using `daramjwee` in a web server.

```go
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
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
)

// 1. Define how to fetch data from your origin.
type originFetcher struct {
	key string
}

// A simple in-memory origin for demonstration.
var fakeOrigin = map[string]struct {
	data string
	etag string
}{
	"hello": {"Hello, Daramjwee! This is the first object.", "v1"},
	"world": {"World is beautiful. This is the second object.", "v2"},
}

func (f *originFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    // oldMetadata의 존재 여부를 먼저 확인합니다.
    oldETagVal := "none"
    if oldMetadata != nil {
        oldETagVal = oldMetadata.ETag
    }
    fmt.Printf("[Origin] Fetching key: %s (Old ETag: %s)\n", f.key, oldETagVal)

	// In a real application, this would be a DB query or an API call.
	obj, ok := fakeOrigin[f.key]
	if !ok {
		return nil, daramjwee.ErrCacheableNotFound
	}

	// If the ETag matches, notify that the content has not been modified.
    // oldMetadata nil 체크는 이미 위에서 수행되었거나, 이 로직에서 다시 확인됩니다.
	if oldMetadata != nil && oldMetadata.ETag == obj.etag {
		return nil, daramjwee.ErrNotModified
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(obj.data))),
		Metadata: &daramjwee.Metadata{ETag: obj.etag},
	}, nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	logger = level.NewFilter(logger, level.AllowDebug())

	// 2. Create a store for the Hot Tier (e.g., FileStore).
	// The New function signature was updated.
	hotStore, err := filestore.New("./daramjwee-cache", log.With(logger, "tier", "hot"))
	if err != nil {
		panic(err)
	}

	// 3. Create a daramjwee cache instance with your configuration.
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithDefaultTimeout(5*time.Second),
		// New options like WithCache and WithShutdownTimeout are available.
		daramjwee.WithCache(1*time.Minute),
		daramjwee.WithNegativeCache(30*time.Second),
		daramjwee.WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	// 4. Use the cache in your HTTP handlers.
	http.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/objects/")

		// Call cache.Get() to retrieve the data stream.
		stream, err := cache.Get(r.Context(), key, &originFetcher{key: key})
		if err != nil {
			if err == daramjwee.ErrNotFound {
				http.Error(w, "Object Not Found", http.StatusNotFound)
			} else if err == daramjwee.ErrCacheClosed {
				http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		// CRITICAL: Always defer Close() immediately after checking for an error.
		defer stream.Close()

		// Stream the response directly to the client.
		io.Copy(w, stream)
	})

	fmt.Println("Server is running on :8080")
	http.ListenAndServe(":8080", nil)
}

## Benchmarks

```
goos: linux
goarch: amd64
pkg: github.com/mrchypark/daramjwee
cpu: AMD EPYC 7763 64-Core Processor                
BenchmarkCache_Get_HotHit-4    	  473588	      2125 ns/op	    1477 B/op	      26 allocs/op
BenchmarkCache_Get_ColdHit-4   	  542630	      2107 ns/op	    1475 B/op	      26 allocs/op
BenchmarkCache_Get_Miss-4      	  287169	      3841 ns/op	    2111 B/op	      36 allocs/op
PASS
ok  	github.com/mrchypark/daramjwee	3.662s
?   	github.com/mrchypark/daramjwee/cmd/daramjwee	[no test files]
?   	github.com/mrchypark/daramjwee/examples	[no test files]
PASS
ok  	github.com/mrchypark/daramjwee/internal/worker	0.275s
goos: linux
goarch: amd64
pkg: github.com/mrchypark/daramjwee/pkg/policy
cpu: AMD EPYC 7763 64-Core Processor                
BenchmarkLRU_Churn-4      	 6995300	       162.1 ns/op	      57 B/op	       2 allocs/op
BenchmarkS3FIFO_Churn-4   	 6716258	       170.4 ns/op	      64 B/op	       3 allocs/op
BenchmarkSieve_Churn-4    	 6867210	       175.9 ns/op	      61 B/op	       2 allocs/op
PASS
ok  	github.com/mrchypark/daramjwee/pkg/policy	4.040s
PASS
ok  	github.com/mrchypark/daramjwee/pkg/store/adapter	0.106s
goos: linux
goarch: amd64
pkg: github.com/mrchypark/daramjwee/pkg/store/filestore
cpu: AMD EPYC 7763 64-Core Processor                
BenchmarkFileStore_Set_RenameStrategy-4   	   14197	     84084 ns/op	    1208 B/op	      22 allocs/op
BenchmarkFileStore_Set_CopyStrategy-4     	    9544	    108605 ns/op	    1049 B/op	      26 allocs/op
BenchmarkFileStore_Get_RenameStrategy-4   	  112516	     10180 ns/op	     549 B/op	      16 allocs/op
BenchmarkFileStore_Get_CopyStrategy-4     	  113934	     10238 ns/op	     549 B/op	      16 allocs/op
PASS
ok  	github.com/mrchypark/daramjwee/pkg/store/filestore	7.109s
goos: linux
goarch: amd64
pkg: github.com/mrchypark/daramjwee/pkg/store/memstore
cpu: AMD EPYC 7763 64-Core Processor                
BenchmarkMemStore_ConcurrentReadWrite-4   	 6358420	       175.7 ns/op	      64 B/op	       2 allocs/op
PASS
ok  	github.com/mrchypark/daramjwee/pkg/store/memstore	1.315s
```
