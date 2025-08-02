package main

import (
	"context"
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

// TestFileStoreWithLRUPolicy tests basic FileStore functionality with LRU policy
func TestFileStoreWithLRUPolicy(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "filestore-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := log.NewNopLogger()
	store, err := filestore.New(
		tempDir,
		logger,
		filestore.WithCapacity(1000), // Capacity to hold file1+file2, but not all three.
		filestore.WithEvictionPolicy(policy.NewLRU()),
	)
	if err != nil {
		t.Fatalf("Failed to create file store: %v", err)
	}

	ctx := context.Background()

	// Helper functions
	writeFile := func(key string, size int) error {
		data := strings.Repeat("X", size)
		metadata := &daramjwee.Metadata{
			ETag:     "test-etag",
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

	// Test scenario: write files that fit within capacity
	if err := writeFile("file1", 300); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	if err := writeFile("file2", 300); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	// Both files should exist initially
	if !fileExists("file1") || !fileExists("file2") {
		t.Error("Expected both files to exist initially")
	}

	// Access file1 to make it recently used
	if reader, _, err := store.GetStream(ctx, "file1"); err == nil {
		reader.Close()
	}

	// Write a large file that should trigger eviction
	if err := writeFile("large_file", 500); err != nil {
		t.Fatalf("Failed to write large_file: %v", err)
	}

	// large_file should exist
	if !fileExists("large_file") {
		t.Error("Expected large_file to exist")
	}

	// With LRU, file1 (recently accessed) might be kept, file2 might be evicted
	// But due to capacity constraints, we just check that eviction occurred
	totalFiles := 0
	if fileExists("file1") {
		totalFiles++
	}
	if fileExists("file2") {
		totalFiles++
	}
	if fileExists("large_file") {
		totalFiles++
	}

	// Should have fewer than 3 files due to capacity constraints
	if totalFiles > 2 {
		t.Errorf("Expected eviction to occur, but found %d files", totalFiles)
	}
}

// TestFileStoreWithS3FIFOPolicy tests FileStore with S3-FIFO policy
func TestFileStoreWithS3FIFOPolicy(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "filestore-s3fifo-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := log.NewNopLogger()
	store, err := filestore.New(
		tempDir,
		logger,
		filestore.WithCapacity(1024),
		filestore.WithEvictionPolicy(policy.NewS3FIFO(1024, 20)), // 20% for small queue
	)
	if err != nil {
		t.Fatalf("Failed to create file store: %v", err)
	}

	ctx := context.Background()

	// Test basic functionality
	metadata := &daramjwee.Metadata{
		ETag:     "test-etag",
		CachedAt: time.Now(),
	}

	// Write a file
	writer, err := store.SetWithWriter(ctx, "test-key", metadata)
	if err != nil {
		t.Fatalf("Failed to get writer: %v", err)
	}

	if _, err := io.WriteString(writer, "test data"); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read the file back
	reader, readMeta, err := store.GetStream(ctx, "test-key")
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if string(data) != "test data" {
		t.Errorf("Expected 'test data', got '%s'", string(data))
	}

	if readMeta.ETag != "test-etag" {
		t.Errorf("Expected ETag 'test-etag', got '%s'", readMeta.ETag)
	}
}

// TestFileStoreWithSIEVEPolicy tests FileStore with SIEVE policy
func TestFileStoreWithSIEVEPolicy(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "filestore-sieve-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := log.NewNopLogger()
	store, err := filestore.New(
		tempDir,
		logger,
		filestore.WithCapacity(500),
		filestore.WithEvictionPolicy(policy.NewSieve()),
	)
	if err != nil {
		t.Fatalf("Failed to create file store: %v", err)
	}

	ctx := context.Background()

	// Test file operations
	writeFile := func(key string, size int) error {
		data := strings.Repeat("X", size)
		metadata := &daramjwee.Metadata{
			ETag:     "test-etag",
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

	// Write files
	if err := writeFile("file1", 100); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	if err := writeFile("file2", 100); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	// Check files exist
	if _, err := store.Stat(ctx, "file1"); err != nil {
		t.Errorf("Expected file1 to exist: %v", err)
	}

	if _, err := store.Stat(ctx, "file2"); err != nil {
		t.Errorf("Expected file2 to exist: %v", err)
	}

	// Delete a file
	if err := store.Delete(ctx, "file1"); err != nil {
		t.Fatalf("Failed to delete file1: %v", err)
	}

	// Check file is deleted
	if _, err := store.Stat(ctx, "file1"); err == nil {
		t.Error("Expected file1 to be deleted")
	}

	// file2 should still exist
	if _, err := store.Stat(ctx, "file2"); err != nil {
		t.Errorf("Expected file2 to still exist: %v", err)
	}
}

// TestFileStoreWithoutEvictionPolicy tests FileStore without eviction policy
func TestFileStoreWithoutEvictionPolicy(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "filestore-no-eviction-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := log.NewNopLogger()
	store, err := filestore.New(tempDir, logger) // No capacity or policy
	if err != nil {
		t.Fatalf("Failed to create file store: %v", err)
	}

	ctx := context.Background()

	// Test basic operations without eviction
	metadata := &daramjwee.Metadata{
		ETag:     "no-eviction-test",
		CachedAt: time.Now(),
	}

	writer, err := store.SetWithWriter(ctx, "test-key", metadata)
	if err != nil {
		t.Fatalf("Failed to get writer: %v", err)
	}

	testData := "This is test data for no-eviction store"
	if _, err := io.WriteString(writer, testData); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back
	reader, readMeta, err := store.GetStream(ctx, "test-key")
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if string(data) != testData {
		t.Errorf("Expected '%s', got '%s'", testData, string(data))
	}

	if readMeta.ETag != "no-eviction-test" {
		t.Errorf("Expected ETag 'no-eviction-test', got '%s'", readMeta.ETag)
	}
}
