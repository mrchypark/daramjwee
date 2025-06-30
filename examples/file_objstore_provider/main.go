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
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/adapter"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/thanos-io/objstore/providers/azure"
)

// SimpleFetcher is a basic implementation of daramjwee.Fetcher.
// It returns a predefined string as its data.
type SimpleFetcher struct {
	data string
}

// Fetch simulates fetching data from an origin.
// In a real application, this would involve network calls to external services.
func (f SimpleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	fmt.Println("Fetching data from origin...")
	// For simplicity, we always return a fixed data string and ETag.
	// In a real fetcher, the ETag would come from the origin's response.
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{ETag: "v1"},
	}, nil
}

// main is the entry point of the example.
// It showcases a two-tier cache setup: a local fileStore as the hot tier
// and an Azure Blob Storage-backed objStore adapter as the cold tier.
// This architecture is robust for production use, providing fast access
// for frequently used items while leveraging cheaper, durable storage for
// less-frequently accessed items.
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel() // Ensure the context is cancelled when main exits.

	// --- 1. Setup Logger ---
	// A structured logger is essential for production observability.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller) // Add timestamp and caller for better logs.

	// --- 2. Load Azure Configuration ---
	// We externalize configuration into a YAML file, a common practice
	// for separating config from code. This allows for easier management
	// in different environments (dev, staging, prod).
	conf, err := os.ReadFile("config.yaml")
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to read config.yaml. Please create it based on the README.md", "err", err)
		os.Exit(1)
	}

	// --- 3. Initialize Cold Tier: Azure Object Store Adapter ---
	// The cold tier is our persistent, long-term storage.
	// We use the official Thanos objstore client for Azure, which is battle-tested.
	azureClient, err := azure.NewBucket(logger, conf, "daramjwee", func(transport http.RoundTripper) http.RoundTripper {
		// This callback allows injecting custom http.RoundTrippers,
		// useful for logging, metrics, or retries.
		return &loggingRoundTripper{
			next: transport,
		}
	})
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to create azure client", "err", err)
		os.Exit(1)
	}
	// The adapter makes the objstore client compatible with daramjwee's Store interface.
	// It handles the streaming translation between daramjwee and the object storage backend.
	coldStore := adapter.NewObjstoreAdapter(azureClient, logger)
	logger.Log("level", "info", "msg", "Cold cache (Azure objStore) initialized.")

	// --- 4. Initialize Hot Tier: File Store ---
	// The hot tier is for fast, local access. We create a temporary directory
	// to simulate a local cache storage volume.
	hotStoreDir, err := os.MkdirTemp("", "daramjwee-hot-cache-*")
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to create temp dir for hot cache", "err", err)
		os.Exit(1)
	}
	// We defer removal for cleanup. In a real scenario, this directory
	// would persist across application restarts, acting as a true hot cache.
	defer func() {
		logger.Log("level", "info", "msg", "Cleaning up hot cache directory.", "path", hotStoreDir)
		if err := os.RemoveAll(hotStoreDir); err != nil {
			logger.Log("level", "error", "msg", "Failed to remove hot cache directory during cleanup.", "path", hotStoreDir, "err", err)
		}
	}()

	logger.Log("level", "info", "msg", "Hot cache (fileStore) initialized", "path", hotStoreDir)
	hotStore, err := filestore.New(hotStoreDir, logger)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to create file store", "err", err)
		os.Exit(1)
	}

	// --- 5. Initialize Daramjwee Cache ---
	// We instantiate the cache with our two tiers. Requests will first check
	// the hotStore, then the coldStore, and finally the origin (fetcher).
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
	)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to initialize daramjwee cache", "err", err)
		os.Exit(1)
	}
	logger.Log("level", "info", "msg", "Daramjwee cache initialized with fileStore (Hot) and Azure (Cold) tiers.")

	const cacheKey = "my-azure-object"
	originData := []byte("Hello from Origin!") // Data expected from the fetcher
	f := SimpleFetcher{
		data: string(originData),
	}

	// --- 6. First Get: Cache Miss (Hot & Cold) -> Fetch from Origin ---
	logger.Log("msg", "---> SCENARIO 1: First Get - Expecting full cache miss (hot and cold).")
	getAndCompare(ctx, logger, cache, cacheKey, f, originData)

	// --- 7. Second Get: Hot Cache Hit ---
	logger.Log("msg", "---> SCENARIO 2: Second Get - Expecting HOT cache hit.")
	// Origin should NOT be called this time as the item is in the hot cache.
	getAndCompare(ctx, logger, cache, cacheKey, f, originData)

	// --- 8. Third Get: Cold Cache Hit (After Simulating Hot Cache Loss) ---
	logger.Log("msg", "---> SCENARIO 3: Third Get - Simulating node restart (deleting hot cache).")
	logger.Log("msg", "EXEC: Deleting hot cache directory to simulate cache eviction or restart.", "path", hotStoreDir)

	// Delete the hot cache directory.
	if err := os.RemoveAll(hotStoreDir); err != nil {
		logger.Log("level", "error", "msg", "Failed to remove hot cache directory.", "path", hotStoreDir, "err", err)
		os.Exit(1)
	}

	// Re-initialize the hot store to simulate a clean cache on startup.
	hotStore, err = filestore.New(hotStoreDir, logger)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to re-create file store", "err", err)
		os.Exit(1)
	}
	// Re-initialize the daramjwee cache with the new, empty hot store.
	cache, err = daramjwee.New(
		logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
	)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to re-initialize daramjwee cache", "err", err)
		os.Exit(1)
	}
	logger.Log("level", "info", "msg", "Cache re-initialized with an empty hot tier.")

	logger.Log("msg", "---> Expecting COLD cache hit from Azure and promotion to hot tier.")
	getAndCompare(ctx, logger, cache, cacheKey, f, originData)

	logger.Log("msg", "✅ All scenarios completed successfully!")
}

// getAndCompare is a helper function to execute a cache Get request,
// read the result, and verify it against expected data.
func getAndCompare(ctx context.Context, logger log.Logger, cache daramjwee.Cache, key string, fetcher daramjwee.Fetcher, expectedData []byte) {
	rc, err := cache.Get(ctx, key, fetcher)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: Failed to get key from cache", "key", key, "err", err)
		os.Exit(1)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			logger.Log("level", "error", "msg", "Failed to close ReadCloser from cache Get", "key", key, "err", closeErr)
		}
	}()

	// Read the data from the returned ReadCloser. This is crucial as the
	// data is streamed from the underlying store.
	readData, err := io.ReadAll(rc)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: Failed to read data from cache stream", "key", key, "err", err)
		os.Exit(1)
	}

	// Verification
	if !bytes.Equal(readData, expectedData) {
		logger.Log("level", "error", "msg", "FATAL: Data mismatch", "key", key, "expected", string(expectedData), "got", string(readData))
		os.Exit(1)
	}
	logger.Log("msg", "SUCCESS: Got correct data for key", "key", key, "data", string(readData))
}

// loggingRoundTripper is a custom http.RoundTripper that logs request details
// and their duration, useful for debugging and monitoring network calls.
type loggingRoundTripper struct {
	next http.RoundTripper // The actual http.RoundTripper that will perform the request.
}

// RoundTrip executes a single HTTP transaction, logging its start, end, and duration.
func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// A new logger instance is created for each request to ensure isolation if needed.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	// Log the request just before it's sent to Azure.
	logger.Log("msg", "[AZURE REQUEST START]", "method", req.Method, "url", req.URL.Host+req.URL.Path)
	start := time.Now()

	resp, err := l.next.RoundTrip(req) // Execute the actual HTTP request.

	duration := time.Since(start)
	if err != nil {
		logger.Log("level", "error", "msg", "[AZURE REQUEST FAILED]", "err", err, "duration", duration)
	} else {
		logger.Log("msg", "[AZURE REQUEST COMPLETE]", "status", resp.Status, "duration", duration)
	}

	return resp, err
}
