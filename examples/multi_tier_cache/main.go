package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// SimpleFetcher is a basic implementation of daramjwee.Fetcher.
type SimpleFetcher struct {
	data string
}

func (f *SimpleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	fmt.Println("Fetching data from origin...")
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{ETag: "v1"},
	}, nil
}

func main() {
	ctx := context.Background()

	// Create a temporary directory for the filestore (cold cache).
	baseDir, err := os.MkdirTemp("", "daramjwee-multi-tier-example-")
	if err != nil {
		log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)).Log("msg", "Failed to create temp dir", "err", err)
		os.Exit(1)
	}
	defer os.RemoveAll(baseDir) // Clean up the directory when done.

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	// Initialize MemStore as hot cache with LRU policy.
	memStore := memstore.New(1*1024*1024, policy.NewLRUPolicy())

	// Initialize FileStore as cold cache.
	fileStore, err := filestore.New(baseDir, logger)
	if err != nil {
		logger.Log("msg", "Failed to create filestore", "err", err)
		os.Exit(1)
	}

	// Create a new cache instance with hot and cold stores.
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),
		daramjwee.WithColdStore(fileStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
	)
	if err != nil {
		logger.Log("msg", "Failed to create cache", "err", err)
		os.Exit(1)
	}
	defer cache.Close()

	// 1. Get a key that doesn't exist. This will trigger a fetch and store in both tiers.
	fmt.Println("--- First Get (Cache Miss - Both Tiers) ---")
	fetcher := &SimpleFetcher{data: "Data from Origin for Multi-Tier!"}
	reader, err := cache.Get(ctx, "multi-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ := io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))

	// 2. Get the same key again. This should be a hot cache hit.
	fmt.Println("--- Second Get (Hot Cache Hit) ---")
	reader, err = cache.Get(ctx, "multi-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))

	// 3. Simulate hot cache eviction (e.g., by filling it up with other data).
	// For simplicity, we'll just delete from hot cache directly.
	fmt.Println("--- Simulating Hot Cache Eviction ---")
	err = memStore.Delete(ctx, "multi-key")
	if err != nil {
		logger.Log("msg", "Failed to delete from hot cache", "err", err)
		os.Exit(1)
	}
	fmt.Println("Key 'multi-key' removed from hot cache.\n")

	// 4. Get the key again. This should be a cold cache hit.
	fmt.Println("--- Third Get (Cold Cache Hit) ---")
	reader, err = cache.Get(ctx, "multi-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))

	// 5. Set a new value for the key.
	fmt.Println("--- Set New Value ---")
	writer, err := cache.Set(ctx, "multi-key", &daramjwee.Metadata{ETag: "v2"})
	if err != nil {
		logger.Log("msg", "Failed to set key", "err", err)
		os.Exit(1)
	}
	_, err = writer.Write([]byte("Updated data in Multi-Tier Cache"))
	if err != nil {
		logger.Log("msg", "Failed to write data", "err", err)
		os.Exit(1)
	}
	writer.Close()
	fmt.Println("Set complete.\n")

	// 6. Get the key again to see the updated value.
	fmt.Println("--- Fourth Get (Cache Hit) ---")
	reader, err = cache.Get(ctx, "multi-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))

	// 7. Delete the key.
	fmt.Println("--- Delete Key ---")
	err = cache.Delete(ctx, "multi-key")
	if err != nil {
		logger.Log("msg", "Failed to delete key", "err", err)
		os.Exit(1)
	}
	fmt.Println("Delete complete.\n")

	// 8. Get the key one last time. Should be a cache miss again.
	fmt.Println("--- Fifth Get (Cache Miss) ---")
	reader, err = cache.Get(ctx, "multi-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))
}