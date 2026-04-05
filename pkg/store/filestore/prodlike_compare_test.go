package filestore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/storetest"
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

func TestFileStoreProdLikeCompareHarness(t *testing.T) {
	if os.Getenv("DJ_RUN_PRODLIKE_COMPARE") != "1" {
		t.Skip("set DJ_RUN_PRODLIKE_COMPARE=1 to run the prod-like compare harness")
	}

	ctx := context.Background()
	items, counts, totalBytes := storetest.BuildProdLikeWorkload()

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
		sink, err := store.BeginSet(ctx, item.Key, &daramjwee.Metadata{CacheTag: item.CacheTag})
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

func readAllFilestoreItems(t *testing.T, ctx context.Context, store *FileStore, items []storetest.ProdLikeWorkloadItem) compareStats {
	t.Helper()

	var stats compareStats
	start := time.Now()
	for _, item := range items {
		reader, meta, err := store.GetStream(ctx, item.Key)
		if err != nil {
			t.Fatal(err)
		}
		if meta == nil || meta.CacheTag != item.CacheTag {
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
