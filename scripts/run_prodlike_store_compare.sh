#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKTREE_DIR="$(mktemp -d -t daramjwee-prodlike-v0310-XXXXXX)"
LOG_DIR="$(mktemp -d -t daramjwee-prodlike-logs-XXXXXX)"
AZURITE_DIR="$(mktemp -d -t daramjwee-azurite-XXXXXX)"
AZURITE_CERT_DIR="$(mktemp -d -t daramjwee-azurite-certs-XXXXXX)"
AZURITE_PID=""

export DJ_RUN_PRODLIKE_COMPARE=1
export DJ_AZURITE_CONNECTION_STRING="${DJ_AZURITE_CONNECTION_STRING:-DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=https://127.0.0.1:10000/devstoreaccount1;}"
KEEP_ARTIFACTS="${KEEP_ARTIFACTS:-0}"

cleanup() {
  if [[ -n "${AZURITE_PID}" ]] && kill -0 "${AZURITE_PID}" >/dev/null 2>&1; then
    kill "${AZURITE_PID}" >/dev/null 2>&1 || true
    wait "${AZURITE_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -d "${WORKTREE_DIR}" ]]; then
    git -C "${ROOT_DIR}" worktree remove --force "${WORKTREE_DIR}" >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_ARTIFACTS}" != "1" ]]; then
    rm -rf "${LOG_DIR}" "${AZURITE_DIR}" "${AZURITE_CERT_DIR}"
  fi
}
trap cleanup EXIT

if [[ ! -f "${AZURITE_CERT_DIR}/cert.pem" || ! -f "${AZURITE_CERT_DIR}/key.pem" ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "${AZURITE_CERT_DIR}/key.pem" \
    -out "${AZURITE_CERT_DIR}/cert.pem" \
    -days 1 \
    -subj "/CN=127.0.0.1" >/dev/null 2>&1
fi

if ! lsof -nP -iTCP:10000 -sTCP:LISTEN >/dev/null 2>&1; then
  npx azurite \
    --blobHost 127.0.0.1 \
    --blobPort 10000 \
    --location "${AZURITE_DIR}" \
    --cert "${AZURITE_CERT_DIR}/cert.pem" \
    --key "${AZURITE_CERT_DIR}/key.pem" \
    --skipApiVersionCheck \
    >"${LOG_DIR}/azurite.log" 2>&1 &
  AZURITE_PID=$!
  for _ in $(seq 1 30); do
    if lsof -nP -iTCP:10000 -sTCP:LISTEN >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

git -C "${ROOT_DIR}" worktree add --detach "${WORKTREE_DIR}" v0.3.10 >/dev/null

cat >"${WORKTREE_DIR}/pkg/store/filestore/prodlike_compare_test.go" <<'EOF'
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
	Version        string                 `json:"version"`
	Provider       string                 `json:"provider"`
	KeyCount       int                    `json:"key_count"`
	TotalBytes     int64                  `json:"total_bytes"`
	WorkloadCounts map[string]int         `json:"workload_counts"`
	Write          compareStats           `json:"write"`
	ColdRead       compareStats           `json:"cold_read"`
	WarmRead       compareStats           `json:"warm_read"`
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
		sink, err := store.SetWithWriter(ctx, item.Key, &daramjwee.Metadata{ETag: item.ETag})
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
		Version:        "v0.3.10",
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
			_ = reader.Close()
			t.Fatal(err)
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
EOF

cat >"${WORKTREE_DIR}/pkg/store/adapter/prodlike_compare_test.go" <<'EOF'
package adapter

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
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

type compareResult struct {
	Version        string         `json:"version"`
	Provider       string         `json:"provider"`
	KeyCount       int            `json:"key_count"`
	TotalBytes     int64          `json:"total_bytes"`
	WorkloadCounts map[string]int `json:"workload_counts"`
	Write          providerStats  `json:"write"`
	ColdRead       providerStats  `json:"cold_read"`
	WarmRead       providerStats  `json:"warm_read"`
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
	items, counts, totalBytes := buildProdLikeWorkload()
	bucket := newAzuriteRecordingBucket(t)
	defer bucket.Close()

	store := NewObjstoreAdapter(bucket, log.NewNopLogger())

	writeStart := time.Now()
	for _, item := range items {
		sink, err := store.SetWithWriter(ctx, item.Key, &daramjwee.Metadata{ETag: item.ETag})
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
	writeStats := bucket.snapshot()
	writeStats.DurationMs = time.Since(writeStart).Milliseconds()
	bucket.reset()

	coldStart := time.Now()
	readAllAdapterItems(t, ctx, store, items)
	coldStats := bucket.snapshot()
	coldStats.DurationMs = time.Since(coldStart).Milliseconds()
	bucket.reset()

	warmStart := time.Now()
	readAllAdapterItems(t, ctx, store, items)
	warmStats := bucket.snapshot()
	warmStats.DurationMs = time.Since(warmStart).Milliseconds()

	result := compareResult{
		Version:        "v0.3.10",
		Provider:       "azure",
		KeyCount:       len(items),
		TotalBytes:     totalBytes,
		WorkloadCounts: counts,
		Write:          writeStats,
		ColdRead:       coldStats,
		WarmRead:       warmStats,
	}

	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("DJ_PRODLIKE_COMPARE=%s\n", payload)
}

func readAllAdapterItems(t *testing.T, ctx context.Context, store daramjwee.Store, items []workloadItem) {
	t.Helper()

	for _, item := range items {
		reader, meta, err := store.GetStream(ctx, item.Key)
		if err != nil {
			t.Fatal(err)
		}
		if meta == nil || meta.ETag != item.ETag {
			t.Fatalf("unexpected metadata for %q: %#v", item.Key, meta)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			t.Fatal(err)
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
		connString = "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=" +
			"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" +
			";BlobEndpoint=https://127.0.0.1:10000/devstoreaccount1;"
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

type recordingBucket struct {
	objstore.Bucket

	mu    sync.Mutex
	stats providerStats
}

func (b *recordingBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.stats.UploadCalls++
	b.stats.BytesSent += int64(len(data))
	b.mu.Unlock()
	return b.Bucket.Upload(ctx, name, bytes.NewReader(data), opts...)
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
	return b.stats
}

func (b *recordingBucket) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats = providerStats{}
}

type countingReadCloser struct {
	io.ReadCloser
	parent *recordingBucket
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.parent.mu.Lock()
	r.parent.stats.ReadCalls++
	r.parent.stats.BytesReceived += int64(n)
	r.parent.mu.Unlock()
	return n, err
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
EOF

echo "== current filestore =="
(
  cd "${ROOT_DIR}"
  go test ./pkg/store/filestore -run TestFileStoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/current-filestore.log"
)

echo "== current objectstore (azurite) =="
(
  cd "${ROOT_DIR}"
  go test ./pkg/store/objectstore -run TestObjstoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/current-objectstore-azure.log"
)

echo "== v0.3.10 filestore =="
(
  cd "${WORKTREE_DIR}"
  go test ./pkg/store/filestore -run TestFileStoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/v0310-filestore.log"
)

echo "== v0.3.10 objectstore adapter (azurite) =="
(
  cd "${WORKTREE_DIR}"
  go test ./pkg/store/adapter -run TestObjstoreProdLikeCompareHarness -count=1 -v | tee "${LOG_DIR}/v0310-objectstore-azure.log"
)

echo
if [[ "${KEEP_ARTIFACTS}" == "1" ]]; then
  echo "logs written to ${LOG_DIR}"
else
  echo "set KEEP_ARTIFACTS=1 to retain logs under ${LOG_DIR}"
fi
