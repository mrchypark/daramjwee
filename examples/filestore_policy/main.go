// Package main demonstrates basic FileStore usage with LRU eviction policy.
// This is a simple example showing how to create a FileStore with capacity limits
// and observe eviction behavior.
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
	fmt.Println("🗂️  FileStore Basic Demo")
	fmt.Println("=======================")
	fmt.Println()

	// Create logger
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "filestore-demo-*")
	if err != nil {
		panic(fmt.Sprintf("Failed to create temp dir: %v", err))
	}
	defer os.RemoveAll(tempDir)

	fmt.Printf("📁 Using temporary directory: %s\n", tempDir)
	fmt.Println()

	// Create FileStore with LRU policy and small capacity
	store, err := filestore.New(
		tempDir,
		logger,
		filestore.WithCapacity(1024), // 1KB capacity
		filestore.WithEvictionPolicy(policy.NewLRU()),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create file store: %v", err))
	}

	ctx := context.Background()

	// Helper functions
	writeFile := func(key string, size int) error {
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

	fileExists := func(key string) bool {
		_, err := store.Stat(ctx, key)
		return err == nil
	}

	// Demo scenario
	fmt.Println("📝 Writing files to demonstrate eviction:")
	fmt.Println()

	// Write files that will exceed capacity
	fmt.Printf("   Writing file1 (300 bytes)... ")
	if err := writeFile("file1", 300); err != nil {
		panic(err)
	}
	fmt.Printf("✅ exists: %v\n", fileExists("file1"))

	fmt.Printf("   Writing file2 (400 bytes)... ")
	if err := writeFile("file2", 400); err != nil {
		panic(err)
	}
	fmt.Printf("✅ exists: %v\n", fileExists("file2"))

	fmt.Printf("   Writing file3 (500 bytes)... ")
	if err := writeFile("file3", 500); err != nil {
		panic(err)
	}
	fmt.Printf("✅ exists: %v\n", fileExists("file3"))

	fmt.Println()
	fmt.Println("📊 File status after writes:")
	for _, key := range []string{"file1", "file2", "file3"} {
		exists := fileExists(key)
		status := "❌"
		if exists {
			status = "✅"
		}
		fmt.Printf("   %s: %s\n", key, status)
	}

	fmt.Println()
	fmt.Println("👆 Accessing file2 to make it recently used...")
	if fileExists("file2") {
		reader, _, _ := store.GetStream(ctx, "file2")
		if reader != nil {
			reader.Close()
		}
		fmt.Println("   ✅ file2 accessed")
	}

	fmt.Println()
	fmt.Printf("📝 Writing file4 (400 bytes) to trigger more eviction... ")
	if err := writeFile("file4", 400); err != nil {
		panic(err)
	}
	fmt.Printf("✅ exists: %v\n", fileExists("file4"))

	fmt.Println()
	fmt.Println("📊 Final file status:")
	for _, key := range []string{"file1", "file2", "file3", "file4"} {
		exists := fileExists(key)
		status := "❌"
		if exists {
			status = "✅"
		}
		fmt.Printf("   %s: %s\n", key, status)
	}

	fmt.Println()
	fmt.Println("💡 Notice how LRU policy kept file2 (recently accessed) and evicted others!")
	fmt.Println()
	fmt.Println("✅ Demo complete!")
}
