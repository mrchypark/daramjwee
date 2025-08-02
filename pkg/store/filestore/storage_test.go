package filestore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
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

	logger := log.NewNopLogger()
	fs, err := New(dir, logger, opts...)
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

// TestFileStore_PathTraversal tests that path traversal attempts are prevented.
func TestFileStore_PathTraversal(t *testing.T) {
	fs := setupTestStore(t)
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

// TestFileStore_NestedPaths tests that nested directory paths work correctly.
func TestFileStore_NestedPaths(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	testCases := []struct {
		name string
		key  string
	}{
		{"simple nested", "dir1/file1"},
		{"deep nested", "dir1/dir2/dir3/file2"},
		{"with dots", "dir1/file.with.dots"},
		{"mixed separators", "dir1\\dir2/file3"}, // Should be normalized
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use unique key to avoid lock conflicts
			uniqueKey := fmt.Sprintf("%s_%d", tc.key, i)
			content := fmt.Sprintf("content for %s", uniqueKey)

			// Write the file
			writer, err := fs.SetWithWriter(ctx, uniqueKey, &daramjwee.Metadata{ETag: "v1"})
			require.NoError(t, err, "SetWithWriter should succeed for key: %s", uniqueKey)

			_, err = writer.Write([]byte(content))
			require.NoError(t, err, "Write should succeed")

			err = writer.Close()
			require.NoError(t, err, "Close should succeed")

			// Read the file back
			reader, meta, err := fs.GetStream(ctx, uniqueKey)
			require.NoError(t, err, "GetStream should succeed for key: %s", uniqueKey)

			assert.Equal(t, "v1", meta.ETag)

			readContent, err := io.ReadAll(reader)
			require.NoError(t, err, "ReadAll should succeed")
			assert.Equal(t, content, string(readContent))
			reader.Close()

			// Test Stat
			statMeta, err := fs.Stat(ctx, uniqueKey)
			require.NoError(t, err, "Stat should succeed")
			assert.Equal(t, "v1", statMeta.ETag)

			// Test Delete
			err = fs.Delete(ctx, uniqueKey)
			require.NoError(t, err, "Delete should succeed")

			// Verify deletion
			_, _, err = fs.GetStream(ctx, uniqueKey)
			assert.ErrorIs(t, err, daramjwee.ErrNotFound, "File should be deleted")
		})
	}
}

// TestFileStore_PathSafety tests various potentially problematic paths.
func TestFileStore_PathSafety(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	testCases := []struct {
		name        string
		key         string
		shouldWork  bool
		description string
	}{
		{"absolute path", "/etc/passwd", true, "should be made relative"},
		{"parent traversal", "../../../etc/passwd", true, "should be sanitized"},
		{"current dir", "./file", true, "should work normally"},
		{"multiple dots", "dir/../file", true, "should be cleaned"},
		{"empty key", "", true, "empty key should be handled"},
		{"only dots", "..", true, "should be sanitized"},
		{"mixed traversal", "good/../../bad", true, "should be sanitized"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			content := fmt.Sprintf("content for %s", tc.key)

			writer, err := fs.SetWithWriter(ctx, tc.key, &daramjwee.Metadata{ETag: "v1"})
			if !tc.shouldWork {
				// For cases that shouldn't work, we might still get a writer
				// but operations should fail gracefully
				if err != nil {
					return // Expected failure
				}
			} else {
				require.NoError(t, err, "SetWithWriter should succeed for: %s", tc.description)
			}

			if writer != nil {
				_, err = writer.Write([]byte(content))
				if tc.shouldWork {
					require.NoError(t, err, "Write should succeed")
				}

				err = writer.Close()
				if tc.shouldWork {
					require.NoError(t, err, "Close should succeed")

					// Verify the file was created within the base directory
					dataPath := fs.toDataPath(tc.key)
					absBase, _ := filepath.Abs(fs.baseDir)
					absPath, _ := filepath.Abs(dataPath)
					assert.True(t, strings.HasPrefix(absPath, absBase),
						"File should be within base directory. Base: %s, Path: %s", absBase, absPath)
				}
			}
		})
	}
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

// setupBenchmarkStore is a helper for benchmarks.
func setupBenchmarkStore(b *testing.B, opts ...Option) *FileStore {
	b.Helper()
	dir, err := os.MkdirTemp("", "filestore-bench-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(dir) })

	logger := log.NewNopLogger()
	fs, err := New(dir, logger, opts...)
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
