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

// fakeOrigin simulates a virtual origin server with predefined key-value pairs.
var fakeOrigin = map[string]struct {
	data string
	etag string
}{
	"hello": {"Hello, Daramjwee! This is the first object.", "v1"},
	"world": {"World is beautiful. This is the second object.", "v2"},
}

// originFetcher is a Fetcher implementation that interacts with the fakeOrigin.
type originFetcher struct {
	key string
}

// Fetch retrieves data from the simulated origin.
// It introduces a delay to simulate network latency and handles ETag-based
// conditional fetching, returning daramjwee.ErrNotModified if the ETag matches.
func (f *originFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	oldETagVal := "none"
	if oldMetadata != nil {
		oldETagVal = oldMetadata.ETag
	}
	fmt.Printf("[Origin] Fetching key: %s, (old ETag: '%s')", f.key, oldETagVal)
	time.Sleep(500 * time.Millisecond) // Simulate network latency

	obj, ok := fakeOrigin[f.key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}

	// If ETag matches, indicate that the data has not been modified.
	if oldMetadata != nil && oldMetadata.ETag == obj.etag {
		return nil, daramjwee.ErrNotModified
	}

	// Return new data and metadata.
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(obj.data))),
		Metadata: &daramjwee.Metadata{ETag: obj.etag},
	}, nil
}

// ExampleOriginFetcher_Fetch demonstrates how to use originFetcher.
func ExampleOriginFetcher_Fetch() {
	fetcher := &originFetcher{key: "hello"}
	result, err := fetcher.Fetch(context.Background(), nil)
	if err != nil {
		fmt.Printf("Error fetching: %v\n", err)
		return
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		fmt.Printf("Error reading body: %v\n", err)
		return
	}
	fmt.Printf("Fetched data: %s\n", string(body))

	// Simulate a conditional fetch with a matching ETag
	fmt.Println("\n--- Conditional Fetch (Not Modified) ---")
	result, err = fetcher.Fetch(context.Background(), result.Metadata)
	if err != nil {
		if err == daramjwee.ErrNotModified {
			fmt.Println("Data not modified as expected.")
		} else {
			fmt.Printf("Error fetching (conditional): %v\n", err)
		}
		return
	}
	result.Body.Close() // Close if not ErrNotModified
	fmt.Println("Fetched data again (should not happen if not modified).")

	// Output:
	// [Origin] Fetching key: hello, (old ETag: 'none')Fetched data: Hello, Daramjwee! This is the first object.
	//
	// --- Conditional Fetch (Not Modified) ---
	// [Origin] Fetching key: hello, (old ETag: 'v1')Data not modified as expected.
}

// main sets up and runs a daramjwee cache example with a file-based hot store
// and a simulated origin server, exposing cached content via an HTTP server.
func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	logger = level.NewFilter(logger, level.AllowDebug())

	hotStoreDir := "./daramjwee-hot-store"
	if err := os.MkdirAll(hotStoreDir, 0755); err != nil {
		panic(err)
	}

	hotStore, err := filestore.New(hotStoreDir, log.With(logger, "tier", "hot"))
	if err != nil {
		panic(err)
	}

	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithDefaultTimeout(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	http.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/objects/")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		fmt.Printf("--- Handling request for key: %s ---", key)

		stream, err := cache.Get(r.Context(), key, &originFetcher{key: key})
		if err != nil {
			if err == daramjwee.ErrNotFound {
				fmt.Println("[Handler] Object not found.")
				http.Error(w, "Object Not Found", http.StatusNotFound)
			} else {
				fmt.Printf("[Handler] Error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		defer func() {
			if err := stream.Close(); err != nil {
				fmt.Printf("[Handler] Error closing stream: %v", err)
			}
		}()

		fmt.Println("[Handler] Streaming response to client...")
		w.Header().Set("Content-Type", "text/plain")
		if _, err := io.Copy(w, stream); err != nil {
			fmt.Printf("[Handler] Error copying stream to response: %v", err)
		}
		fmt.Println("[Handler] Done.")
	})

	fmt.Println("daramjwee example server is running on :8080")
	fmt.Println("Try visiting:")
	fmt.Println("  http://localhost:8080/objects/hello")
	fmt.Println("  http://localhost:8080/objects/world")
	fmt.Println("  http://localhost:8080/objects/not-exist")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Failed to start server: %v", err)
		os.Exit(1)
	}
}
