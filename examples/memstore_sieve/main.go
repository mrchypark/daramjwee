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
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// SimpleFetcher is a basic implementation of daramjwee.Fetcher that returns a predefined string as its data.
type SimpleFetcher struct {
	data string
}

// Fetch simulates fetching data from an origin.
func (f *SimpleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	fmt.Println("Fetching data from origin...")
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{ETag: "v1"},
	}, nil
}

// ExampleSimpleFetcher_Fetch demonstrates how to use SimpleFetcher.
func ExampleSimpleFetcher_Fetch() {
	fetcher := &SimpleFetcher{data: "Hello, Daramjwee!"}
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
	// Output:
	// Fetching data from origin...
	// Fetched data: Hello, Daramjwee!
}

// main showcases the usage of daramjwee cache with a memory store and Sieve eviction policy.
func main() {
	ctx := context.Background()
	memStore := memstore.New(1*1024*1024, policy.NewSieve())

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		logger.Log("msg", "Failed to create cache", "err", err)
		os.Exit(1)
	}
	defer cache.Close()

	fmt.Println("--- First Get (Cache Miss) ---")
	fetcher := &SimpleFetcher{data: "Hello, Daramjwee!"}
	reader, err := cache.Get(ctx, "my-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ := io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s", string(body))

	fmt.Println("--- Second Get (Cache Hit) ---")
	reader, err = cache.Get(ctx, "my-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s", string(body))

	fmt.Println("--- Set New Value ---")
	writer, err := cache.Set(ctx, "my-key", &daramjwee.Metadata{ETag: "v2"})
	if err != nil {
		logger.Log("msg", "Failed to set key", "err", err)
		os.Exit(1)
	}
	_, err = writer.Write([]byte("Updated data"))
	if err != nil {
		logger.Log("msg", "Failed to write data", "err", err)
		os.Exit(1)
	}
	writer.Close()
	fmt.Println("Set complete.")

	fmt.Println("--- Third Get (Cache Hit) ---")
	reader, err = cache.Get(ctx, "my-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s", string(body))

	fmt.Println("--- Delete Key ---")
	err = cache.Delete(ctx, "my-key")
	if err != nil {
		logger.Log("msg", "Failed to delete key", "err", err)
		os.Exit(1)
	}
	fmt.Println("Delete complete.")

	fmt.Println("--- Fourth Get (Cache Miss) ---")
	reader, err = cache.Get(ctx, "my-key", fetcher)
	if err != nil {
		logger.Log("msg", "Failed to get key", "err", err)
		os.Exit(1)
	}
	body, _ = io.ReadAll(reader)
	reader.Close()
	fmt.Printf("Got data: %s", string(body))
}
