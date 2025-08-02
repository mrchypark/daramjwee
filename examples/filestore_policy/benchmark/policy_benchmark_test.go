package benchmark

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
)

// BenchmarkFileStorePolicy_LRU benchmarks FileStore with LRU policy
func BenchmarkFileStorePolicy_LRU(b *testing.B) {
	benchmarkFileStorePolicy(b, "LRU", policy.NewLRU())
}

// BenchmarkFileStorePolicy_S3FIFO benchmarks FileStore with S3-FIFO policy
func BenchmarkFileStorePolicy_S3FIFO(b *testing.B) {
	benchmarkFileStorePolicy(b, "S3FIFO", policy.NewS3FIFO(1024*1024, 10)) // 1MB total, 10% small queue
}

// BenchmarkFileStorePolicy_SIEVE benchmarks FileStore with SIEVE policy
func BenchmarkFileStorePolicy_SIEVE(b *testing.B) {
	benchmarkFileStorePolicy(b, "SIEVE", policy.NewSieve())
}

// BenchmarkFileStorePolicy_NoEviction benchmarks FileStore without eviction policy
func BenchmarkFileStorePolicy_NoEviction(b *testing.B) {
	benchmarkFileStorePolicy(b, "NoEviction", nil)
}

func benchmarkFileStorePolicy(b *testing.B, policyName string, pol daramjwee.EvictionPolicy) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("bench-filestore-%s-*", strings.ToLower(policyName)))
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create logger (silent for benchmarks)
	logger := log.NewNopLogger()

	// Create FileStore with policy
	var store *filestore.FileStore
	if pol != nil {
		store, err = filestore.New(
			tempDir,
			logger,
			filestore.WithCapacity(10*1024*1024), // 10MB capacity to avoid frequent evictions
			filestore.WithEvictionPolicy(pol),
		)
	} else {
		store, err = filestore.New(tempDir, logger)
	}
	if err != nil {
		b.Fatalf("Failed to create file store: %v", err)
	}

	ctx := context.Background()
	data := strings.Repeat("X", 1024) // 1KB data per file

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		key := fmt.Sprintf("file-%d", i)

		// Write operation
		metadata := &daramjwee.Metadata{
			ETag:     fmt.Sprintf("etag-%d", i),
			CachedAt: time.Now(),
		}

		writer, err := store.SetWithWriter(ctx, key, metadata)
		if err != nil {
			b.Fatalf("Failed to get writer: %v", err)
		}

		if _, err := io.WriteString(writer, data); err != nil {
			writer.Close()
			b.Fatalf("Failed to write data: %v", err)
		}

		if err := writer.Close(); err != nil {
			b.Fatalf("Failed to close writer: %v", err)
		}

		// Occasionally read some files to simulate access patterns
		if i%10 == 0 && i > 0 {
			readKey := fmt.Sprintf("file-%d", i-5) // Read a recent file
			reader, _, err := store.GetStream(ctx, readKey)
			if err == nil {
				reader.Close()
			}
		}
	}
}

// BenchmarkFileStorePolicyMixed tests mixed read/write workloads
func BenchmarkFileStorePolicyMixed_LRU(b *testing.B) {
	benchmarkMixedWorkload(b, "LRU", policy.NewLRU())
}

func BenchmarkFileStorePolicyMixed_S3FIFO(b *testing.B) {
	benchmarkMixedWorkload(b, "S3FIFO", policy.NewS3FIFO(1024*1024, 10))
}

func BenchmarkFileStorePolicyMixed_SIEVE(b *testing.B) {
	benchmarkMixedWorkload(b, "SIEVE", policy.NewSieve())
}

func benchmarkMixedWorkload(b *testing.B, policyName string, pol daramjwee.EvictionPolicy) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("bench-mixed-%s-*", strings.ToLower(policyName)))
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create logger (silent for benchmarks)
	logger := log.NewNopLogger()

	// Create FileStore with policy
	store, err := filestore.New(
		tempDir,
		logger,
		filestore.WithCapacity(5*1024*1024), // 5MB capacity for moderate eviction
		filestore.WithEvictionPolicy(pol),
	)
	if err != nil {
		b.Fatalf("Failed to create file store: %v", err)
	}

	ctx := context.Background()
	data := strings.Repeat("X", 512) // 512B data per file

	// Pre-populate some files
	for i := range 50 {
		key := fmt.Sprintf("init-file-%d", i)
		metadata := &daramjwee.Metadata{
			ETag:     fmt.Sprintf("etag-%d", i),
			CachedAt: time.Now(),
		}

		writer, err := store.SetWithWriter(ctx, key, metadata)
		if err != nil {
			b.Fatalf("Failed to get writer during pre-population: %v", err)
		}
		if _, err := io.WriteString(writer, data); err != nil {
			writer.Close()
			b.Fatalf("Failed to write data during pre-population: %v", err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("Failed to close writer during pre-population: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		if i%3 == 0 {
			// Write operation (33% of operations)
			key := fmt.Sprintf("file-%d", i)
			metadata := &daramjwee.Metadata{
				ETag:     fmt.Sprintf("etag-%d", i),
				CachedAt: time.Now(),
			}

			writer, err := store.SetWithWriter(ctx, key, metadata)
			if err != nil {
				continue
			}
			io.WriteString(writer, data)
			writer.Close()
		} else {
			// Read operation (67% of operations)
			var key string
			if i%6 == 1 {
				// Read recent file (hot data)
				key = fmt.Sprintf("file-%d", max(0, i-10))
			} else {
				// Read older file (cold data)
				key = fmt.Sprintf("init-file-%d", i%50)
			}

			reader, _, err := store.GetStream(ctx, key)
			if err == nil {
				reader.Close()
			}
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
