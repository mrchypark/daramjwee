package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
)

func main() {
	// Create logger
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	fmt.Println("🔍 FileStore Policy Comparison Demo")
	fmt.Println("===================================")
	fmt.Println()

	// Test different policies with the same scenario
	policies := []struct {
		name   string
		policy daramjwee.EvictionPolicy
		desc   string
	}{
		{
			name:   "LRU",
			policy: policy.NewLRU(),
			desc:   "Least Recently Used - evicts oldest accessed items",
		},
		{
			name:   "S3FIFO",
			policy: policy.NewS3FIFO(1000, 20), // 1000 bytes total, 20% for small queue
			desc:   "S3-FIFO - combines FIFO with frequency-based eviction",
		},
		{
			name:   "SIEVE",
			policy: policy.NewSieve(),
			desc:   "SIEVE - low-overhead eviction with hand pointer scanning",
		},
	}

	for i, p := range policies {
		fmt.Printf("📋 Test %d: %s Policy\n", i+1, p.name)
		fmt.Printf("   %s\n", p.desc)
		fmt.Println()

		result := testPolicy(logger, p.name, p.policy)
		printResult(p.name, result)

		if i < len(policies)-1 {
			fmt.Println()
			fmt.Println("---")
			fmt.Println()
		}
	}

	fmt.Println()
	fmt.Println("✅ Comparison complete!")
	fmt.Println()
	fmt.Println("💡 Key Observations:")
	fmt.Println("   • LRU: Predictable, evicts least recently accessed")
	fmt.Println("   • S3FIFO: Better hit rates, frequency-aware")
	fmt.Println("   • SIEVE: Low overhead, good balance")
}

type TestResult struct {
	InitialFiles map[string]bool
	AfterAccess  map[string]bool
	FinalFiles   map[string]bool
}

func testPolicy(logger log.Logger, policyName string, pol daramjwee.EvictionPolicy) TestResult {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("policy-demo-%s-*", strings.ToLower(policyName)))
	if err != nil {
		panic(fmt.Sprintf("Failed to create temp dir: %v", err))
	}
	defer os.RemoveAll(tempDir)

	// Create FileStore with small capacity to trigger evictions
	store, err := filestore.New(
		tempDir,
		logger,
		filestore.WithCapacity(800), // 800 bytes capacity
		filestore.WithEvictionPolicy(pol),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create file store: %v", err))
	}

	ctx := context.Background()
	result := TestResult{
		InitialFiles: make(map[string]bool),
		AfterAccess:  make(map[string]bool),
		FinalFiles:   make(map[string]bool),
	}

	// Helper functions
	writeData := func(key string, size int) error {
		data := strings.Repeat("X", size)
		metadata := &daramjwee.Metadata{
			ETag:     fmt.Sprintf("etag-%s", key),
			CachedAt: time.Now(),
		}

		writer, err := store.SetWithWriter(ctx, key, metadata)
		if err != nil {
			return err
		}
		defer writer.Close()

		_, err = io.WriteString(writer, data)
		return err
	}

	exists := func(key string) bool {
		_, err := store.Stat(ctx, key)
		return err == nil
	}

	checkFiles := func() map[string]bool {
		files := map[string]bool{
			"file1": exists("file1"),
			"file2": exists("file2"),
			"file3": exists("file3"),
		}
		return files
	}

	// Test scenario
	fmt.Printf("   📝 Writing 3 files (200 bytes each)...\n")
	writeData("file1", 200)
	writeData("file2", 200)
	writeData("file3", 200)
	result.InitialFiles = checkFiles()

	fmt.Printf("   👆 Accessing file1 to make it 'recently used'...\n")
	if exists("file1") {
		reader, _, _ := store.GetStream(ctx, "file1")
		if reader != nil {
			reader.Close()
		}
	}
	result.AfterAccess = checkFiles()

	fmt.Printf("   📝 Writing large file (400 bytes) to trigger eviction...\n")
	writeData("large_file", 400)
	result.FinalFiles = checkFiles()
	result.FinalFiles["large_file"] = exists("large_file")

	return result
}

func printResult(policyName string, result TestResult) {
	fmt.Printf("   📊 Results for %s:\n", policyName)

	fmt.Printf("      Initial state: ")
	printFileStatus(result.InitialFiles)

	fmt.Printf("      After access:  ")
	printFileStatus(result.AfterAccess)

	fmt.Printf("      After eviction: ")
	printFileStatus(result.FinalFiles)

	// Analysis
	evicted := []string{}
	for file, exists := range result.FinalFiles {
		if file != "large_file" && !exists {
			evicted = append(evicted, file)
		}
	}

	if len(evicted) > 0 {
		fmt.Printf("      🗑️  Evicted: %v\n", evicted)
	} else {
		fmt.Printf("      ✅ No eviction occurred\n")
	}
}

func printFileStatus(files map[string]bool) {
	status := []string{}
	for _, file := range []string{"file1", "file2", "file3", "large_file"} {
		if exists, ok := files[file]; ok {
			if exists {
				status = append(status, fmt.Sprintf("%s✅", file))
			} else {
				status = append(status, fmt.Sprintf("%s❌", file))
			}
		}
	}
	fmt.Printf("%s\n", strings.Join(status, " "))
}
