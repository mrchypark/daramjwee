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
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
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

	// Create a temporary directory for the filestore.
	baseDir, err := os.MkdirTemp("", "daramjwee-filestore-example-")
	if err != nil {
		log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)).Log("msg", "Failed to create temp dir", "err", err)
		os.Exit(1)
	}
	defer os.RemoveAll(baseDir) // Clean up the directory when done.

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	fileStore, err := filestore.New(baseDir, logger)
	if err != nil {
		logger.Log("msg", "Failed to create filestore", "err", err)
		os.Exit(1)
	}

	// Create a new cache instance using the filestore as the hot store.
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(fileStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
	)
	if err != nil {
		logger.Log("msg", "Failed to create cache", "err", err)
		os.Exit(1)
	}
	defer cache.Close()

	// 1. Get a key that doesn't exist. This will trigger a fetch.
	fmt.Println("--- First Get (Cache Miss) ---")
	fetcher := &SimpleFetcher{data: "Hello from FileStore!"}
	reader, err := cache.Get(ctx, "file-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ := io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))

	// 2. Get the same key again. This should be a cache hit.
	fmt.Println("--- Second Get (Cache Hit) ---")
	reader, err = cache.Get(ctx, "file-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))

	// 3. Set a new value for the key.
	fmt.Println("--- Set New Value ---")
	writer, err := cache.Set(ctx, "file-key", &daramjwee.Metadata{ETag: "v2"})
	if err != nil {
		logger.Log("msg", "Failed to set key", "err", err)
		os.Exit(1)
	}
	_, err = writer.Write([]byte("Updated data in FileStore"))
	if err != nil {
		logger.Log("msg", "Failed to write data", "err", err)
		os.Exit(1)
	}
	writer.Close()
	fmt.Println("Set complete.\n")

	// 4. Get the key again to see the updated value.
	fmt.Println("--- Third Get (Cache Hit) ---")
	reader, err = cache.Get(ctx, "file-key", fetcher) // Always provide a fetcher for potential refresh
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))

	// 5. Delete the key.
	fmt.Println("--- Delete Key ---")
	err = cache.Delete(ctx, "file-key")
	if err != nil {
		logger.Log("msg", "Failed to delete key", "err", err)
		os.Exit(1)
	}
	fmt.Println("Delete complete.\n")

	// 6. Get the key one last time. Should be a cache miss again.
	fmt.Println("--- Fourth Get (Cache Miss) ---")
	reader, err = cache.Get(ctx, "file-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s\n\n", string(body))
}