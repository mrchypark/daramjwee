# daramjwee 🐿️ `/dɑːrɑːmdʒwiː/`

A pragmatic and lightweight hybrid caching middleware for Go.

`daramjwee` sits between your application and your origin data source (e.g., a database or an API), providing an efficient, stream-based hybrid caching layer. It is designed with a focus on simplicity and core functionality to achieve high throughput at a low cost in cloud-native environments.

## Core Design Philosophy

`daramjwee` is built on two primary principles:

1.  **Purely Stream-Based API:** All data is processed through `io.Reader` and `io.Writer` interfaces. This means that even large objects are handled without memory overhead from intermediate buffering, guaranteeing optimal performance for proxying use cases.

2.  **Modular and Pluggable Architecture:** Key components such as the storage backend (`Store`), eviction strategy (`EvictionPolicy`), and asynchronous task runner (`Worker`) are all designed as interfaces. This allows users to easily swap in their own implementations to fit specific needs.

## Key Features

* 🐿️ **Hybrid Cache Tiers:** Combines a fast local store (Hot Tier) with a large-capacity remote store (Cold Tier) to optimize for both performance and cost. The Cold Tier is optional; if not configured, a `nullStore` is used to reduce code complexity.

* 🔥 **Flexible Storage Backends:** Comes with built-in support for in-memory (`MemStore`) and filesystem (`FileStore`) backends.

* ☁️ **Cloud-Native:** An adapter for `thanos-io/objstore` provides out-of-the-box support for major cloud object stores like AWS S3, Google Cloud Storage, and Azure Blob Storage to be used as a Cold Tier.

* 🔄 **Background Refresh & Promotion:** On a cache hit, it serves the user request immediately while asynchronously refreshing the cache in the background via a worker (`ScheduleRefresh`). Data hit in the Cold Tier is automatically promoted to the Hot Tier.

* ⚙️ **ETag-based Efficiency:** Exchanges ETags with the origin server to check for content changes. If the content is not modified (`ErrNotModified`), it avoids unnecessary data transfer, saving network bandwidth.

* 🧩 **Pluggable Eviction Policies:** Includes LRU (Least Recently Used) and S3-FIFO policies by default. Users can apply custom strategies by implementing the `EvictionPolicy` interface.

## Architectural Highlights

`daramjwee` is engineered with several core principles to ensure high performance and robustness in demanding environments.

* **True Streaming-First API**: At the heart of `daramjwee` is a strict adherence to stream-based I/O (`io.Reader` and `io.Writer`). This design ensures that objects of any size can be proxied without being fully buffered in memory, leading to minimal memory footprint and low latency. Operations like promoting an object from a cold tier to a hot tier happen concurrently while streaming the data to the client, thanks to `io.TeeReader`.

* **Robust and Modular Storage**: The `Store` interface provides a clean abstraction for storage backends.
    * The built-in `FileStore` is designed for safety, using an atomic "write-to-temp-then-rename" pattern to prevent data corruption from partial writes. It also offers a copy-based alternative for compatibility with network filesystems like NFS.
    * The `objstore` adapter for `thanos-io/objstore` leverages `io.Pipe` to achieve true streaming uploads to cloud providers, which is highly memory-efficient.

* **High-Concurrency Ready**: Performance under concurrent load is a primary focus.
    * `FileStore` uses striped locking instead of a single global lock to minimize lock contention for different keys.
    * Background tasks (like cache refreshes) are managed by a configurable worker pool, preventing unbounded goroutine creation and ensuring stable resource usage.

## Roadmap

* **Cache Stampede Prevention**: Implement a `singleflight` mechanism for origin fetches. This will prevent multiple concurrent requests for the same missing key from overwhelming the origin data source (the "thundering herd" problem).
* **More Eviction Policies**: Explore and add other modern eviction policies.
* **Metrics**: Expose Prometheus metrics for cache hits, misses, latency, and stored object counts to improve observability.

## How It Works

The data retrieval flow in `daramjwee` is as follows:

1.  **Check Hot Tier:** Looks for the object in the Hot Tier.
    * **Hit:** Immediately returns the object stream and schedules a background task to refresh the cache.
2.  **Check Cold Tier:** If not in the Hot Tier, it checks the Cold Tier.
    * **Hit:** Streams the object to the client while simultaneously promoting it to the Hot Tier.
3.  **Fetch from Origin:** If the object is in neither tier (Cache Miss), it invokes the user-provided `Fetcher` to retrieve the data from the origin.
    * The fetched data stream is sent to the client and written to the Hot Tier at the same time.
    * A background job can be scheduled to also write the data to the Cold Tier.

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

func (f *originFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	oldETagVal := "none"
	if oldMetadata != nil {
		oldETagVal = oldMetadata.ETag
	}
	fmt.Printf("[Origin] Fetching key: %s (Old ETag: %s)\n", f.key, oldETagVal)
	// In a real application, this would be a DB query or an API call.
	const originData = "Hello, Daramjwee!"
	const originETag = "v1"

	// If the ETag matches, notify that the content has not been modified.
	if oldMetadata != nil && oldMetadata.ETag == originETag {
		return nil, daramjwee.ErrNotModified
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(originData))),
		Metadata: &daramjwee.Metadata{ETag: originETag},
	}, nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	logger = level.NewFilter(logger, level.AllowDebug())

	// 2. Create a store for the Hot Tier (e.g., FileStore).
	hotStore, err := filestore.New("./daramjwee-cache", log.With(logger, "tier", "hot"))
	if err != nil {
		panic(err)
	}

	// 3. Create a daramjwee cache instance with your configuration.
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithDefaultTimeout(5*time.Second),
		// Optionally add a ColdStore or custom Worker settings.
		// daramjwee.WithColdStore(coldStore),
		// daramjwee.WithWorker("pool", 20, 100, 1*time.Minute),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	// 4. Use the cache in your HTTP handlers.
	http.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/objects/"):]

		// Call cache.Get() to retrieve the data stream.
		stream, err := cache.Get(r.Context(), key, &originFetcher{key: key})
		if err != nil {
			if err == daramjwee.ErrNotFound {
				http.Error(w, "Object Not Found", http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		defer stream.Close()

		// Stream the response directly to the client.
		io.Copy(w, stream)
	})

	fmt.Println("Server is running on :8080")
	http.ListenAndServe(":8080", nil)
}
```
