package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestStore is a helper to create a temporary filestore for testing.
func setupTestStore(t *testing.T, opts ...Option) *FileStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "filestore-test-*")
	require.NoError(t, err, "failed to create test directory")

	t.Cleanup(func() {
		// Ensure all permissions are restored before removal
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil // Ignore errors during cleanup walk
			}
			_ = os.Chmod(path, 0755)
			return nil
		})
		os.RemoveAll(dir)
	})

	logger := level.NewFilter(log.NewLogfmtLogger(os.Stderr), level.AllowDebug())
	// Note: capacity and policy are set to defaults (0 and nil) unless specified in tests.
	fs, err := New(dir, logger, 0, nil, opts...)
	require.NoError(t, err, "failed to create filestore")

	return fs
}

// TestFileStore_SetAndGet tests the basic Set and Get functionality.
func TestFileStore_SetAndGet(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "my-object"
	etag := "v1.0.0"
	content := "hello daramjwee"

	writer, err := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, meta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, etag, meta.ETag)

	readBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, string(readBytes))
}

// TestFileStore_Get_NotFound tests that getting a non-existent key returns the correct error.
func TestFileStore_Get_NotFound(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	_, _, err := fs.GetStream(ctx, "non-existent-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

// TestFileStore_Stat tests getting metadata without file content.
func TestFileStore_Stat(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "stat-key"
	etag := "etag-for-stat"

	writer, _ := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, writer.Close())

	meta, err := fs.Stat(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, etag, meta.ETag)

	_, err = fs.Stat(ctx, "non-existent-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

// TestFileStore_Delete tests the object deletion functionality.
func TestFileStore_Delete(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "delete-key"

	writer, _ := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, writer.Close())

	err := fs.Delete(ctx, key)
	require.NoError(t, err)

	dataPath := fs.toDataPath(key)
	_, err = os.Stat(dataPath)
	assert.True(t, os.IsNotExist(err), "data file should be deleted")

	err = fs.Delete(ctx, key) // Deleting again should not error
	require.NoError(t, err)
}

// TestFileStore_Overwrite tests overwriting an existing object.
func TestFileStore_Overwrite(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "overwrite-key"

	// Write version 1
	writer1, _ := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
	_, err := writer1.Write([]byte("version 1"))
	require.NoError(t, err)
	require.NoError(t, writer1.Close())

	// Write version 2
	writer2, _ := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v2"})
	_, err = writer2.Write([]byte("version 2"))
	require.NoError(t, err)
	require.NoError(t, writer2.Close())

	reader, meta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, "v2", meta.ETag)
	content, _ := io.ReadAll(reader)
	assert.Equal(t, "version 2", string(content))
}

// TestFileStore_PathTraversal tests that path traversal attempts are prevented for non-hashed keys.
func TestFileStore_PathTraversal(t *testing.T) {
	fs := setupTestStore(t) // This test is for the default (non-hashed) key mode.
	ctx := context.Background()

	maliciousKey := "../malicious-file"
	writer, err := fs.SetWithWriter(ctx, maliciousKey, &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	expectedPath := filepath.Join(fs.baseDir, "malicious-file")
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err, "file should be created inside the base directory")

	outsidePath := filepath.Join(fs.baseDir, "..", "malicious-file")
	_, err = os.Stat(outsidePath)
	assert.True(t, os.IsNotExist(err), "file should not be created outside the base directory")
}

// TestFileStore_SetWithCopyAndTruncate tests the copy-and-truncate strategy.
func TestFileStore_SetWithCopyAndTruncate(t *testing.T) {
	fs := setupTestStore(t, WithCopyAndTruncate())
	ctx := context.Background()
	key := "copy-test"
	content := "data for copy"

	writer, err := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v-copy"})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, _, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	readBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, string(readBytes))
}

// TestFileStore_Set_ErrorOnFinalize_Rename tests that an error during rename cleans up the temp file.
func TestFileStore_Set_ErrorOnFinalize_Rename(t *testing.T) {
	fs := setupTestStore(t) // Default rename strategy
	ctx := context.Background()
	key := "rename-fail-key"
	dataPath := fs.toDataPath(key)

	// Create a directory where the file should be, to cause os.Rename to fail.
	err := os.Mkdir(dataPath, 0755)
	require.NoError(t, err)

	writer, err := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = writer.Write([]byte("some data"))
	require.NoError(t, err)

	err = writer.Close()
	require.Error(t, err, "Close() should fail on rename error")

	// Check that the temp file was cleaned up.
	files, _ := os.ReadDir(fs.baseDir)
	for _, file := range files {
		if file.Name() == filepath.Base(dataPath) { // Keep the directory we created to cause the error
			continue
		}
		assert.False(t, strings.HasPrefix(file.Name(), "daramjwee-tmp-"), "Temp file should be cleaned up")
	}
}

// TestFileStore_Set_ErrorOnFinalize_Copy tests that an error during copy cleans up the temp file.
func TestFileStore_Set_ErrorOnFinalize_Copy(t *testing.T) {
	fs := setupTestStore(t, WithCopyAndTruncate())
	ctx := context.Background()
	key := "subdir/copy-fail-key"
	destDir := filepath.Dir(fs.toDataPath(key))

	// Create the destination directory, then make it read-only.
	require.NoError(t, os.MkdirAll(destDir, 0755))
	require.NoError(t, os.Chmod(destDir, 0555)) // Read and execute only

	writer, err := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err, "SetWithWriter should succeed as temp file creation is unaffected")
	_, err = writer.Write([]byte("some data"))
	require.NoError(t, err)

	err = writer.Close()
	require.Error(t, err, "Close() should fail on copy error due to read-only destination")
	assert.ErrorContains(t, err, "permission denied")

	// Check that the temp file was cleaned up.
	files, _ := os.ReadDir(fs.baseDir)
	for _, file := range files {
		if file.IsDir() && file.Name() == "subdir" {
			continue
		}
		assert.False(t, strings.HasPrefix(file.Name(), "daramjwee-tmp-"), "Temp file should be cleaned up")
	}
}

// TestFileStore_MetadataFields ensures all metadata fields are stored and retrieved correctly.
func TestFileStore_MetadataFields(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "metadata-test-key"
	now := time.Now().Truncate(time.Millisecond) // Truncate for reliable comparison

	originalMeta := &daramjwee.Metadata{
		ETag:       "v-complex",
		CachedAt:   now,
		IsNegative: true,
	}
	writer, err := fs.SetWithWriter(ctx, key, originalMeta)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, retrievedMeta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, originalMeta.ETag, retrievedMeta.ETag)
	assert.True(t, originalMeta.CachedAt.Equal(retrievedMeta.CachedAt))
	assert.Equal(t, originalMeta.IsNegative, retrievedMeta.IsNegative)
}

// TestFileStore_New_WithInvalidHashedKeys_NegativeDepth tests for negative depth.
func TestFileStore_New_WithInvalidHashedKeys_NegativeDepth(t *testing.T) {
	dir := t.TempDir()
	logger := level.NewFilter(log.NewLogfmtLogger(os.Stderr), level.AllowDebug())
	_, err := New(dir, logger, 0, nil, WithHashedKeys(-1, 2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid dirDepth for hashed keys: -1")
}

// TestFileStore_New_WithInvalidHashedKeys_ZeroPrefixLength tests for zero prefix length.
func TestFileStore_New_WithInvalidHashedKeys_ZeroPrefixLength(t *testing.T) {
	dir := t.TempDir()
	logger := level.NewFilter(log.NewLogfmtLogger(os.Stderr), level.AllowDebug())
	_, err := New(dir, logger, 0, nil, WithHashedKeys(2, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid dirPrefixLength for hashed keys: 0")
}

// TestFileStore_New_WithInvalidHashedKeys_ExcessiveLength tests for excessive length.
func TestFileStore_New_WithInvalidHashedKeys_ExcessiveLength(t *testing.T) {
	dir := t.TempDir()
	logger := level.NewFilter(log.NewLogfmtLogger(os.Stderr), level.AllowDebug())
	_, err := New(dir, logger, 0, nil, WithHashedKeys(10, 10))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds hash size")
}

// TestFileStore_ToDataPath_HashedKeys tests the path generation with hashed keys.
func TestFileStore_ToDataPath_HashedKeys(t *testing.T) {
	fs := setupTestStore(t, WithHashedKeys(2, 2))
	key := "my-test-key"

	hasher := sha256.New()
	hasher.Write([]byte(key))
	hashedKey := hex.EncodeToString(hasher.Sum(nil))

	// e.g., if hash is "abcdef1234...", path should be ".../baseDir/ab/cd/abcdef1234..."
	expectedPath := filepath.Join(fs.baseDir, hashedKey[:2], hashedKey[2:4], hashedKey)

	actualPath := fs.toDataPath(key)
	assert.Equal(t, expectedPath, actualPath)
}

// TestFileStore_SetAndGet_WithHashedKeys tests the full cycle with hashed keys.
func TestFileStore_SetAndGet_WithHashedKeys(t *testing.T) {
	fs := setupTestStore(t, WithHashedKeys(2, 2))
	ctx := context.Background()
	key := "my-hashed-object"
	etag := "v-hashed"
	content := "hello hashed daramjwee"

	writer, err := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Verify the file exists at the hashed path
	hashedPath := fs.toDataPath(key)
	_, err = os.Stat(hashedPath)
	require.NoError(t, err, "file should exist at hashed path")

	reader, meta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)

	assert.Equal(t, etag, meta.ETag)

	readBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	// Close the reader immediately after use to release the read lock.
	err = reader.Close()
	require.NoError(t, err)

	assert.Equal(t, content, string(readBytes))

	// Test Deletion, which requires a write lock.
	err = fs.Delete(ctx, key)
	require.NoError(t, err)
	_, err = os.Stat(hashedPath)
	assert.True(t, os.IsNotExist(err), "file should be deleted from hashed path")
}

// mockPolicy is a simple FIFO eviction policy for testing.
type mockPolicy struct {
	mu    sync.Mutex
	keys  []string
	sizes map[string]int64
}

func newMockPolicy() *mockPolicy {
	return &mockPolicy{
		keys:  make([]string, 0),
		sizes: make(map[string]int64),
	}
}

func (p *mockPolicy) Add(key string, size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Remove if exists to move to back (though for FIFO, we don't expect updates)
	p.remove(key)
	p.keys = append(p.keys, key)
	p.sizes[key] = size
}

func (p *mockPolicy) Touch(key string) {
	// No-op for FIFO
}

func (p *mockPolicy) Remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.remove(key)
}

// remove is an internal, non-locking version of Remove.
func (p *mockPolicy) remove(key string) {
	for i, k := range p.keys {
		if k == key {
			p.keys = append(p.keys[:i], p.keys[i+1:]...)
			delete(p.sizes, key)
			return
		}
	}
}

func (p *mockPolicy) Evict() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.keys) == 0 {
		return nil
	}
	keyToEvict := p.keys[0]
	// The filestore is responsible for calling Remove, so we just return the candidate.
	return []string{keyToEvict}
}

// TestFileStore_Eviction_Debug is for debugging eviction logic.
func TestFileStore_Eviction_Debug(t *testing.T) {
	t.Skip("Skipping debug test, enable for manual debugging of sizes")
	policy := newMockPolicy()
	fs, err := New(t.TempDir(), log.NewNopLogger(), 1000, policy)
	require.NoError(t, err)

	ctx := context.Background()
	content := "0123456789" // 10 bytes of data

	writer, err := fs.SetWithWriter(ctx, "debugkey", &daramjwee.Metadata{ETag: "debug"})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	info, err := os.Stat(fs.toDataPath("debugkey"))
	require.NoError(t, err)
	t.Logf("DEBUG: File size with 10 bytes of content and empty metadata is %d bytes", info.Size())
}

// TestFileStore_Eviction tests the eviction of items when capacity is exceeded.
func TestFileStore_Eviction(t *testing.T) {
	policy := newMockPolicy()
	// A file with empty metadata and 10 bytes of content is 83 bytes.
	// Set capacity to hold two files (166 bytes), but not three (249 bytes).
	fs, err := New(t.TempDir(), log.NewNopLogger(), 167, policy)
	require.NoError(t, err)

	ctx := context.Background()
	content := "0123456789" // 10 bytes of data

	// Add item 1
	writer1, err := fs.SetWithWriter(ctx, "key1", &daramjwee.Metadata{})
	require.NoError(t, err)
	_, err = writer1.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, writer1.Close())

	// Add item 2
	writer2, err := fs.SetWithWriter(ctx, "key2", &daramjwee.Metadata{})
	require.NoError(t, err)
	_, err = writer2.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, writer2.Close())

	// Verify the first two files exist and get their sizes.
	_, err = os.Stat(fs.toDataPath("key1"))
	require.NoError(t, err, "key1 should exist before eviction")
	_, err = os.Stat(fs.toDataPath("key2"))
	require.NoError(t, err, "key2 should exist before eviction")

	// Add item 3, which should trigger eviction of key1 (FIFO).
	writer3, err := fs.SetWithWriter(ctx, "key3", &daramjwee.Metadata{})
	require.NoError(t, err)
	_, err = writer3.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, writer3.Close())

	// Check that key1 is gone
	_, _, err = fs.GetStream(ctx, "key1")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "key1 should have been evicted")

	// Check that key2 and key3 still exist
	_, _, err = fs.GetStream(ctx, "key2")
	assert.NoError(t, err, "key2 should still exist")
	_, _, err = fs.GetStream(ctx, "key3")
	assert.NoError(t, err, "key3 should still exist")

	// Check internal size tracking
	info3, err := os.Stat(fs.toDataPath("key3"))
	require.NoError(t, err)
	info2, err := os.Stat(fs.toDataPath("key2")) // Get info2 here, after key1 is evicted
	require.NoError(t, err)
	expectedSize := info2.Size() + info3.Size()
	assert.Equal(t, expectedSize, fs.currentSize, "internal currentSize should be correct after eviction")
}

// TestFileStore_Delete_UpdatesSize tests that Delete correctly updates the store's internal size.
func TestFileStore_Delete_UpdatesSize(t *testing.T) {
	policy := newMockPolicy()
	fs, err := New(t.TempDir(), level.NewFilter(log.NewLogfmtLogger(os.Stderr), level.AllowDebug()), 1024, policy)
	require.NoError(t, err)

	ctx := context.Background()
	key := "size-test-key"
	content := "some content"

	writer, err := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	t.Logf("DEBUG: Calling writer.Close() for key %s", key)
	err = writer.Close()
	require.NoError(t, err, "writer.Close() should not return an error")
	t.Logf("DEBUG: writer.Close() completed for key %s", key)
	initialSize := fs.currentSize
	t.Logf("DEBUG: initialSize = %d", initialSize)
	require.Greater(t, initialSize, int64(0))

	// Delete the key
	err = fs.Delete(ctx, key)
	require.NoError(t, err)

	assert.Equal(t, int64(0), fs.currentSize, "currentSize should be 0 after deleting the only item")

	// Deleting again should not change the size
	err = fs.Delete(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(0), fs.currentSize, "currentSize should remain 0 after deleting a non-existent item")
}

// TestFileStore_CorruptedFile tests behavior when reading a corrupted file.
func TestFileStore_CorruptedFile(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "corrupted-key"
	dataPath := fs.toDataPath(key)

	// Write a file with an invalid metadata length prefix (length 256, but no data follows)
	err := os.WriteFile(dataPath, []byte{0x00, 0x00, 0x01, 0x00}, 0644)
	require.NoError(t, err)

	_, _, err = fs.GetStream(ctx, key)
	require.Error(t, err)
	assert.NotErrorIs(t, err, daramjwee.ErrNotFound, "corrupted file should not be treated as 'not found'")
	assert.Contains(t, err.Error(), "failed to read metadata bytes")
}

// TestFileStore_CurrentSize_RaceCondition tests that currentSize is updated correctly under concurrent operations.
func TestFileStore_CurrentSize_RaceCondition(t *testing.T) {
	policy := newMockPolicy()
	// Set a large capacity so eviction doesn't interfere with size tracking during this test.
	fs, err := New(t.TempDir(), log.NewNopLogger(), 1024*1024, policy)
	require.NoError(t, err)

	ctx := context.Background()
	content := "some data for concurrent ops" // ~30 bytes

	const numOps = 100
	const numKeys = 10 // Use a few keys to ensure some overwrites and deletions happen on same keys
	var wg sync.WaitGroup
	wg.Add(numOps)

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("concurrent-key-%d", i)
	}

	for i := 0; i < numOps; i++ {
		go func(opIdx int) {
			defer wg.Done()
			key := keys[opIdx%numKeys] // Cycle through keys

			opType := opIdx % 3 // 0: Set, 1: Delete, 2: Get (to trigger Touch)

			switch opType {
			case 0: // Set
				writer, err := fs.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: fmt.Sprintf("v-%d", opIdx)})
				require.NoError(t, err)
				_, err = writer.Write([]byte(content))
				require.NoError(t, err)
				require.NoError(t, writer.Close())
			case 1: // Delete
				fs.Delete(ctx, key)
			case 2: // Get (to trigger Touch, but also just to add more concurrent activity)
				reader, _, err := fs.GetStream(ctx, key)
				if err == nil {
					io.Copy(io.Discard, reader)
					reader.Close()
				}
			}
		}(i)
	}
	wg.Wait()

	// After all operations, calculate the expected total size by iterating through the policy's known items.
	// This relies on the mock policy accurately tracking sizes.
	policy.mu.Lock() // Lock the policy to get a consistent view
	expectedTotalSize := int64(0)
	for _, size := range policy.sizes {
		expectedTotalSize += size
	}
	policy.mu.Unlock()

	assert.Equal(t, expectedTotalSize, fs.currentSize, "currentSize should match the sum of sizes in policy after concurrent ops")

	// Optional: Verify actual files on disk match policy
	// This would be more complex as it requires iterating through the baseDir and checking hashed paths.
	// For now, relying on policy's internal state and fs.currentSize.
}

// setupBenchmarkStore is a helper for benchmarks.
func setupBenchmarkStore(b *testing.B, opts ...Option) *FileStore {
	b.Helper()
	dir, err := os.MkdirTemp("", "filestore-bench-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(dir) })

	logger := level.NewFilter(log.NewLogfmtLogger(os.Stderr), level.AllowDebug())
	fs, err := New(dir, logger, 0, nil, opts...)
	require.NoError(b, err)
	return fs
}

// benchmarkFileStoreSet benchmarks the Set operation for FileStore.
func benchmarkFileStoreSet(b *testing.B, store *FileStore) {
	ctx := context.Background()
	data := []byte("this is benchmark data")
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v-bench"})
		if err != nil {
			b.Fatalf("SetWithWriter failed: %v", err)
		}
		_, err = writer.Write(data)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("Close failed: %v", err)
		}
	}
}

// BenchmarkFileStore_Set_RenameStrategy benchmarks the Set operation using the rename strategy.
func BenchmarkFileStore_Set_RenameStrategy(b *testing.B) {
	store := setupBenchmarkStore(b)
	benchmarkFileStoreSet(b, store)
}

// BenchmarkFileStore_Set_CopyStrategy benchmarks the Set operation using the copy-and-truncate strategy.
func BenchmarkFileStore_Set_CopyStrategy(b *testing.B) {
	store := setupBenchmarkStore(b, WithCopyAndTruncate())
	benchmarkFileStoreSet(b, store)
}

// benchmarkFileStoreGet benchmarks the Get operation for FileStore.
func benchmarkFileStoreGet(b *testing.B, store *FileStore) {
	ctx := context.Background()
	data := []byte("this is benchmark data")
	numItems := 1000

	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v-bench"})
		if err != nil {
			b.Fatalf("Setup: SetWithWriter failed: %v", err)
		}
		_, err = writer.Write(data)
		if err != nil {
			b.Fatalf("Setup: Write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("Setup: Close failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i%numItems)
		reader, _, err := store.GetStream(ctx, key)
		if err != nil {
			b.Fatalf("GetStream failed: %v", err)
		}

		_, err = io.Copy(io.Discard, reader)
		if err != nil {
			b.Fatalf("io.Copy to Discard failed: %v", err)
		}

		if err := reader.Close(); err != nil {
			b.Fatalf("Reader.Close failed: %v", err)
		}
	}
}

// BenchmarkFileStore_Get_RenameStrategy benchmarks the Get operation using the rename strategy.
func BenchmarkFileStore_Get_RenameStrategy(b *testing.B) {
	store := setupBenchmarkStore(b)
	benchmarkFileStoreGet(b, store)
}

// BenchmarkFileStore_Get_CopyStrategy benchmarks the Get operation using the copy-and-truncate strategy.
func BenchmarkFileStore_Get_CopyStrategy(b *testing.B) {
	store := setupBenchmarkStore(b, WithCopyAndTruncate())
	benchmarkFileStoreGet(b, store)
}