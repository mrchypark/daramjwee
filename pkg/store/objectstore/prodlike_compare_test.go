package objectstore

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/thanos-io/objstore"
	azureprovider "github.com/thanos-io/objstore/providers/azure"
)

type providerStats struct {
	DurationMs    int64 `json:"duration_ms"`
	UploadCalls   int   `json:"upload_calls"`
	GetCalls      int   `json:"get_calls"`
	GetRangeCalls int   `json:"get_range_calls"`
	IterCalls     int   `json:"iter_calls"`
	ExistsCalls   int   `json:"exists_calls"`
	AttrCalls     int   `json:"attr_calls"`
	DeleteCalls   int   `json:"delete_calls"`
	ReadCalls     int   `json:"read_calls"`
	BytesSent     int64 `json:"bytes_sent"`
	BytesReceived int64 `json:"bytes_received"`
}

type objectstoreCompareResult struct {
	Version              string         `json:"version"`
	Provider             string         `json:"provider"`
	KeyCount             int            `json:"key_count"`
	TotalBytes           int64          `json:"total_bytes"`
	UniqueShards         int            `json:"unique_shards"`
	PackedThreshold      int64          `json:"packed_threshold"`
	PageSize             int64          `json:"page_size"`
	BlockCacheBytes      int64          `json:"block_cache_bytes"`
	CheckpointCacheBytes int64          `json:"checkpoint_cache_bytes"`
	CheckpointCacheTTLMS int64          `json:"checkpoint_cache_ttl_ms"`
	WorkloadCounts       map[string]int `json:"workload_counts"`
	Write                providerStats  `json:"write"`
	ColdRead             providerStats  `json:"cold_read"`
	WarmRead             providerStats  `json:"warm_read"`
}

type workloadItem struct {
	Key      string
	Category string
	Body     []byte
	ETag     string
}

func TestObjstoreProdLikeCompareHarness(t *testing.T) {
	if os.Getenv("DJ_RUN_PRODLIKE_COMPARE") != "1" {
		t.Skip("set DJ_RUN_PRODLIKE_COMPARE=1 to run the prod-like compare harness")
	}

	ctx := context.Background()
	items, counts, totalBytes := buildObjectstoreProdLikeWorkload()
	uniqueShards := countUniqueShards(items)

	const (
		packedThreshold      = int64(1 << 20)
		pageSize             = int64(256 << 10)
		blockCacheBytes      = int64(64 << 20)
		checkpointCacheBytes = int64(16 << 20)
		checkpointCacheTTL   = 2 * time.Second
	)

	bucket := newAzuriteRecordingBucket(t)
	t.Cleanup(func() { _ = bucket.Close() })

	writeDir := t.TempDir()
	store := New(bucket, log.NewNopLogger(),
		WithDataDir(writeDir),
		WithPackedObjectThreshold(packedThreshold),
		WithPageSize(pageSize),
		WithMemoryBlockCache(blockCacheBytes),
		WithMemoryCheckpointCache(checkpointCacheBytes),
		WithCheckpointCacheTTL(checkpointCacheTTL),
	)
	store.autoFlush = false

	writeStart := time.Now()
	for _, item := range items {
		sink, err := store.BeginSet(ctx, item.Key, &daramjwee.Metadata{ETag: item.ETag})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sink.Write(item.Body); err != nil {
			t.Fatal(err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	writeStats := bucket.snapshot()
	writeStats.DurationMs = time.Since(writeStart).Milliseconds()
	bucket.reset()

	coldStore := New(bucket, log.NewNopLogger(),
		WithDataDir(t.TempDir()),
		WithPackedObjectThreshold(packedThreshold),
		WithPageSize(pageSize),
		WithMemoryBlockCache(blockCacheBytes),
		WithMemoryCheckpointCache(checkpointCacheBytes),
		WithCheckpointCacheTTL(checkpointCacheTTL),
	)
	coldStore.autoFlush = false

	coldStart := time.Now()
	readAllObjectstoreItems(t, ctx, coldStore, bucket, items)
	coldStats := bucket.snapshot()
	coldStats.DurationMs = time.Since(coldStart).Milliseconds()
	bucket.reset()

	warmStart := time.Now()
	readAllObjectstoreItems(t, ctx, coldStore, bucket, items)
	warmStats := bucket.snapshot()
	warmStats.DurationMs = time.Since(warmStart).Milliseconds()

	result := objectstoreCompareResult{
		Version:              "current",
		Provider:             "azure",
		KeyCount:             len(items),
		TotalBytes:           totalBytes,
		UniqueShards:         uniqueShards,
		PackedThreshold:      packedThreshold,
		PageSize:             pageSize,
		BlockCacheBytes:      blockCacheBytes,
		CheckpointCacheBytes: checkpointCacheBytes,
		CheckpointCacheTTLMS: checkpointCacheTTL.Milliseconds(),
		WorkloadCounts:       counts,
		Write:                writeStats,
		ColdRead:             coldStats,
		WarmRead:             warmStats,
	}

	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("DJ_PRODLIKE_COMPARE=%s\n", payload)
}

func readAllObjectstoreItems(t *testing.T, ctx context.Context, store *Store, bucket *recordingBucket, items []workloadItem) {
	t.Helper()

	for _, item := range items {
		reader, meta, err := store.GetStream(ctx, item.Key)
		if err != nil {
			t.Fatal(err)
		}
		if meta == nil || meta.ETag != item.ETag {
			t.Fatalf("unexpected metadata for %q: %#v", item.Key, meta)
		}

		// Count one logical read per object key, for parity with filestore metrics.
		bucket.recordLogicalRead()

		if _, err := io.Copy(io.Discard, reader); err != nil {
			if closeErr := reader.Close(); closeErr != nil {
				t.Fatalf("copy failed for %q: %v; close failed: %v", item.Key, err, closeErr)
			}
			t.Fatalf("copy failed for %q: %v", item.Key, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func newAzuriteRecordingBucket(t *testing.T) *recordingBucket {
	t.Helper()

	connString := os.Getenv("DJ_AZURITE_CONNECTION_STRING")
	if connString == "" {
		connString = defaultAzuriteConnectionString()
	}

	conf := azureprovider.Config{
		StorageAccountName:      "devstoreaccount1",
		StorageConnectionString: connString,
		StorageCreateContainer:  true,
		ContainerName:           fmt.Sprintf("prodlike-%d", time.Now().UnixNano()),
	}

	bucket, err := azureprovider.NewBucketWithConfig(log.NewNopLogger(), conf, "daramjwee", wrapAzuriteTransport)
	if err != nil {
		t.Fatalf("failed to create azurite bucket: %v", err)
	}
	return &recordingBucket{Bucket: bucket}
}

func wrapAzuriteTransport(rt http.RoundTripper) http.RoundTripper {
	if transport, ok := rt.(*http.Transport); ok {
		cloned := transport.Clone()
		if cloned.TLSClientConfig == nil {
			cloned.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		} else {
			cloned.TLSClientConfig = cloned.TLSClientConfig.Clone()
			cloned.TLSClientConfig.InsecureSkipVerify = true
		}
		return cloned
	}
	return rt
}

func defaultAzuriteConnectionString() string {
	return "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=" +
		"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" +
		";BlobEndpoint=https://127.0.0.1:10000/devstoreaccount1;"
}

type recordingBucket struct {
	objstore.Bucket

	mu            sync.Mutex
	stats         providerStats
	bytesSent     atomic.Int64
	bytesReceived atomic.Int64
}

func (b *recordingBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	b.stats.UploadCalls++
	b.mu.Unlock()

	return b.Bucket.Upload(ctx, name, &countingReader{Reader: r, onRead: func(n int) {
		b.bytesSent.Add(int64(n))
	}}, opts...)
}

func (b *recordingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.Bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.stats.GetCalls++
	b.mu.Unlock()

	return &countingReadCloser{ReadCloser: reader, parent: b}, nil
}

func (b *recordingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	reader, err := b.Bucket.GetRange(ctx, name, off, length)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.stats.GetRangeCalls++
	b.mu.Unlock()

	return &countingReadCloser{ReadCloser: reader, parent: b}, nil
}

func (b *recordingBucket) Iter(ctx context.Context, dir string, f func(name string) error, options ...objstore.IterOption) error {
	b.mu.Lock()
	b.stats.IterCalls++
	b.mu.Unlock()
	return b.Bucket.Iter(ctx, dir, f, options...)
}

func (b *recordingBucket) Exists(ctx context.Context, name string) (bool, error) {
	b.mu.Lock()
	b.stats.ExistsCalls++
	b.mu.Unlock()
	return b.Bucket.Exists(ctx, name)
}

func (b *recordingBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	b.mu.Lock()
	b.stats.AttrCalls++
	b.mu.Unlock()
	return b.Bucket.Attributes(ctx, name)
}

func (b *recordingBucket) Delete(ctx context.Context, name string) error {
	b.mu.Lock()
	b.stats.DeleteCalls++
	b.mu.Unlock()
	return b.Bucket.Delete(ctx, name)
}

func (b *recordingBucket) snapshot() providerStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	stats := b.stats
	stats.BytesSent = b.bytesSent.Load()
	stats.BytesReceived = b.bytesReceived.Load()
	return stats
}

func (b *recordingBucket) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats = providerStats{}
	b.bytesSent.Store(0)
	b.bytesReceived.Store(0)
}

func (b *recordingBucket) recordLogicalRead() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats.ReadCalls++
}

type countingReadCloser struct {
	io.ReadCloser
	parent *recordingBucket
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.parent.bytesReceived.Add(int64(n))
	return n, err
}

type countingReader struct {
	io.Reader
	onRead func(int)
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 && r.onRead != nil {
		r.onRead(n)
	}
	return n, err
}

func countUniqueShards(items []workloadItem) int {
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		seen[shardForKey(item.Key)] = struct{}{}
	}
	return len(seen)
}

func buildObjectstoreProdLikeWorkload() ([]workloadItem, map[string]int, int64) {
	type spec struct {
		category string
		count    int
		size     int
		prefix   string
	}

	specs := []spec{
		{category: "metrics_negative_15m", count: 240, size: 77, prefix: "metrics/device/panel/15m/metrics_negative_15m"},
		{category: "plant_small", count: 16, size: 800, prefix: "plant/plant/metadata/plant_small"},
		{category: "registry_medium", count: 8, size: 32 << 10, prefix: "registry/registry/registry_medium"},
		{category: "blueprint_medium", count: 8, size: 96 << 10, prefix: "blueprint/blueprint/manager/blueprint_medium"},
		{category: "metrics_positive_15m_medium", count: 12, size: 128 << 10, prefix: "metrics/device/panel/15m/metrics_positive_15m_medium"},
		{category: "metrics_positive_5m_large", count: 6, size: 768 << 10, prefix: "metrics/device/panel/5m/metrics_positive_5m_large"},
		{category: "metrics_positive_5m_direct", count: 2, size: 2 << 20, prefix: "metrics/device/panel/5m/metrics_positive_5m_direct"},
	}

	items := make([]workloadItem, 0, 292)
	counts := make(map[string]int, len(specs))
	var totalBytes int64

	for _, spec := range specs {
		for i := 0; i < spec.count; i++ {
			key := fmt.Sprintf("%s/%03d/%s", spec.prefix, i, workloadDate(i))
			body := workloadBody(spec.category, i, spec.size)
			items = append(items, workloadItem{
				Key:      key,
				Category: spec.category,
				Body:     body,
				ETag:     fmt.Sprintf("%s-%03d", spec.category, i),
			})
			counts[spec.category]++
			totalBytes += int64(len(body))
		}
	}

	return items, counts, totalBytes
}

func workloadDate(i int) string {
	if i%2 == 0 {
		return fmt.Sprintf("2026-03-%02d", (i%28)+1)
	}
	return fmt.Sprintf("2025-03-%02d", (i%28)+1)
}

func workloadBody(category string, index, size int) []byte {
	sum := sha1.Sum([]byte(fmt.Sprintf("%s-%03d", category, index)))
	pattern := []byte(hex.EncodeToString(sum[:]))
	body := make([]byte, size)
	for i := range body {
		body[i] = pattern[i%len(pattern)]
	}
	return body
}
