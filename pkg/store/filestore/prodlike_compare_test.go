package filestore

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
)

type compareStats struct {
	DurationMs    int64 `json:"duration_ms"`
	WriteCalls    int   `json:"write_calls"`
	ReadCalls     int   `json:"read_calls"`
	ListCalls     int   `json:"list_calls"`
	BytesSent     int64 `json:"bytes_sent"`
	BytesReceived int64 `json:"bytes_received"`
}

type compareResult struct {
	Version        string         `json:"version"`
	Provider       string         `json:"provider"`
	KeyCount       int            `json:"key_count"`
	TotalBytes     int64          `json:"total_bytes"`
	WorkloadCounts map[string]int `json:"workload_counts"`
	Write          compareStats   `json:"write"`
	ColdRead       compareStats   `json:"cold_read"`
	WarmRead       compareStats   `json:"warm_read"`
}

type workloadItem struct {
	Key      string
	Category string
	Body     []byte
	ETag     string
}

func TestFileStoreProdLikeCompareHarness(t *testing.T) {
	if os.Getenv("DJ_RUN_PRODLIKE_COMPARE") != "1" {
		t.Skip("set DJ_RUN_PRODLIKE_COMPARE=1 to run the prod-like compare harness")
	}

	ctx := context.Background()
	items, counts, totalBytes := buildProdLikeWorkload()

	dir, err := os.MkdirTemp("", "daramjwee-filestore-compare-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir, log.NewNopLogger())
	if err != nil {
		t.Fatal(err)
	}

	var writeStats compareStats
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
		writeStats.WriteCalls++
	}
	writeStats.DurationMs = time.Since(writeStart).Milliseconds()

	coldStore, err := New(dir, log.NewNopLogger())
	if err != nil {
		t.Fatal(err)
	}

	coldReadStats := readAllFilestoreItems(t, ctx, coldStore, items)
	warmReadStats := readAllFilestoreItems(t, ctx, coldStore, items)

	result := compareResult{
		Version:        "current",
		Provider:       "filestore",
		KeyCount:       len(items),
		TotalBytes:     totalBytes,
		WorkloadCounts: counts,
		Write:          writeStats,
		ColdRead:       coldReadStats,
		WarmRead:       warmReadStats,
	}

	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("DJ_PRODLIKE_COMPARE=%s\n", payload)
}

func readAllFilestoreItems(t *testing.T, ctx context.Context, store *FileStore, items []workloadItem) compareStats {
	t.Helper()

	var stats compareStats
	start := time.Now()
	for _, item := range items {
		reader, meta, err := store.GetStream(ctx, item.Key)
		if err != nil {
			t.Fatal(err)
		}
		if meta == nil || meta.ETag != item.ETag {
			t.Fatalf("unexpected metadata for %q: %#v", item.Key, meta)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			if closeErr := reader.Close(); closeErr != nil {
				t.Fatalf("copy failed for %q: %v; close failed: %v", item.Key, err, closeErr)
			}
			t.Fatalf("copy failed for %q: %v", item.Key, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		stats.ReadCalls++
	}
	stats.DurationMs = time.Since(start).Milliseconds()
	return stats
}

func buildProdLikeWorkload() ([]workloadItem, map[string]int, int64) {
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
