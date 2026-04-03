package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/thanos-io/objstore/providers/gcs"
)

// SimpleFetcher is a basic implementation of daramjwee.Fetcher.
type SimpleFetcher struct {
	data string
}

// Fetch simulates fetching data from an origin.
func (f SimpleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	fmt.Println("Fetching data from origin...")
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{ETag: "v1"},
	}, nil
}

// main showcases an ordered tier chain: a local fileStore as tier 0 and a
// Google Cloud Storage-backed objectstore as the next tier. This architecture
// provides fast local hits while keeping a larger backing store behind it.
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	conf, err := os.ReadFile("config.yaml")
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to read config.yaml. Please create it based on the README.md", "err", err)
		os.Exit(1)
	}

	gcsClient, err := gcs.NewBucket(ctx, logger, conf, "daramjwee", func(transport http.RoundTripper) http.RoundTripper {
		return &loggingRoundTripper{
			next: transport,
		}
	})
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to create GCS client", "err", err)
		os.Exit(1)
	}
	tier1DataDir, err := os.MkdirTemp("", "daramjwee-objectstore-*")
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to create temp dir for tier 1 objectstore workspace", "err", err)
		os.Exit(1)
	}
	defer func() {
		logger.Log("level", "info", "msg", "Cleaning up tier 1 objectstore workspace.", "path", tier1DataDir)
		if err := os.RemoveAll(tier1DataDir); err != nil {
			logger.Log("level", "error", "msg", "Failed to remove tier 1 objectstore workspace during cleanup.", "path", tier1DataDir, "err", err)
		}
	}()

	tier1Store := objectstore.New(
		gcsClient,
		log.With(logger, "tier", "1"),
		objectstore.WithDir(filepath.Join(tier1DataDir, "workspace")),
		objectstore.WithPrefix("examples/file-objstore-provider"),
		objectstore.WithPackThreshold(1<<20),
		objectstore.WithPageSize(256<<10),
		objectstore.WithBlockCache(64<<20),
		objectstore.WithCheckpointCache(16<<20),
		objectstore.WithCheckpointTTL(2*time.Second),
	)
	logger.Log(
		"level", "info",
		"msg", "Tier 1 (GCS objectstore) initialized",
		"data_dir", filepath.Join(tier1DataDir, "workspace"),
		"prefix", "examples/file-objstore-provider",
		"packed_threshold", 1<<20,
		"page_size", 256<<10,
		"memory_block_cache", 64<<20,
		"memory_checkpoint_cache", 16<<20,
		"checkpoint_cache_ttl", 2*time.Second,
	)

	hotStoreDir, err := os.MkdirTemp("", "daramjwee-hot-cache-*")
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to create temp dir for tier 0 cache", "err", err)
		os.Exit(1)
	}
	defer func() {
		logger.Log("level", "info", "msg", "Cleaning up tier 0 cache directory.", "path", hotStoreDir)
		if err := os.RemoveAll(hotStoreDir); err != nil {
			logger.Log("level", "error", "msg", "Failed to remove tier 0 cache directory during cleanup.", "path", hotStoreDir, "err", err)
		}
	}()

	logger.Log("level", "info", "msg", "Tier 0 (fileStore) initialized", "path", hotStoreDir)
	hotStore, err := filestore.New(hotStoreDir, logger)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to create file store", "err", err)
		os.Exit(1)
	}

	cache, err := daramjwee.New(logger, daramjwee.WithTiers(hotStore, tier1Store))
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to initialize daramjwee cache", "err", err)
		os.Exit(1)
	}
	logger.Log("level", "info", "msg", "Daramjwee cache initialized with ordered tiers: fileStore tier 0 and GCS tier 1.")

	const cacheKey = "my-gcs-object"
	originData := []byte("Hello from Origin!")
	f := SimpleFetcher{
		data: string(originData),
	}

	logger.Log("msg", "---> SCENARIO 1: First Get - Expecting a miss in both tiers.")
	getAndCompare(ctx, logger, cache, cacheKey, f, originData)

	logger.Log("msg", "---> SCENARIO 2: Second Get - Expecting tier-0 hit.")
	getAndCompare(ctx, logger, cache, cacheKey, f, originData)

	logger.Log("msg", "---> SCENARIO 3: Third Get - Simulating node restart (deleting tier 0 cache).")
	logger.Log("msg", "EXEC: Deleting the tier 0 cache directory to simulate cache eviction or restart.", "path", hotStoreDir)

	if err := os.RemoveAll(hotStoreDir); err != nil {
		logger.Log("level", "error", "msg", "Failed to remove tier 0 cache directory.", "path", hotStoreDir, "err", err)
		os.Exit(1)
	}

	hotStore, err = filestore.New(hotStoreDir, logger)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to re-create file store", "err", err)
		os.Exit(1)
	}
	cache, err = daramjwee.New(
		logger,
		daramjwee.WithTiers(hotStore, tier1Store),
	)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: failed to re-initialize daramjwee cache", "err", err)
		os.Exit(1)
	}
	logger.Log("level", "info", "msg", "Cache re-initialized with an empty tier 0.")

	logger.Log("msg", "---> Expecting lower-tier hit from GCS and promotion to tier 0.")
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

	readData, err := io.ReadAll(rc)
	if err != nil {
		logger.Log("level", "error", "msg", "FATAL: Failed to read data from cache stream", "key", key, "err", err)
		os.Exit(1)
	}

	if !bytes.Equal(readData, expectedData) {
		logger.Log("level", "error", "msg", "FATAL: Data mismatch", "key", key, "expected", string(expectedData), "got", string(readData))
		os.Exit(1)
	}
	logger.Log("msg", "SUCCESS: Got correct data for key", "key", key, "data", string(readData))
}

// loggingRoundTripper is a custom http.RoundTripper that logs request details
// and their duration, useful for debugging and monitoring network calls.
type loggingRoundTripper struct {
	next http.RoundTripper
}

// RoundTrip executes a single HTTP transaction, logging its start, end, and duration.
func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	logger.Log("msg", "[GCS REQUEST START]", "method", req.Method, "url", req.URL.Host+req.URL.Path)
	start := time.Now()

	resp, err := l.next.RoundTrip(req)

	duration := time.Since(start)
	if err != nil {
		logger.Log("level", "error", "msg", "[GCS REQUEST FAILED]", "err", err, "duration", duration)
	} else {
		logger.Log("msg", "[GCS REQUEST COMPLETE]", "status", resp.Status, "duration", duration)
	}

	return resp, err
}
