package filestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/storetest"
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

func writeLegacyMetadataForTest(t *testing.T, w io.Writer, cacheTag string) {
	t.Helper()
	metaBytes := []byte(fmt.Sprintf(`{"ETag":%q,"IsNegative":false,"CachedAt":"0001-01-01T00:00:00Z"}`, cacheTag))

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(metaBytes)))

	_, err := w.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = w.Write(metaBytes)
	require.NoError(t, err)
}

func TestStoredMetadataJSONRoundTripPreservesStoredKey(t *testing.T) {
	key := "encoded/key"
	encoded, err := json.Marshal(storedMetadata{
		Metadata:  daramjwee.Metadata{CacheTag: "v1"},
		StoredKey: &key,
	})
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"__stored_key":"encoded/key"`)

	var decoded storedMetadata
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, "v1", decoded.CacheTag)
	require.NotNil(t, decoded.StoredKey)
	require.Equal(t, key, *decoded.StoredKey)
}

func TestFileStore_WriteSinkConformance(t *testing.T) {
	storetest.RunWriteSinkConformance(t, func(t *testing.T) daramjwee.Store {
		t.Helper()
		return setupTestStore(t)
	})
}

// TestFileStore_SetAndGet tests the basic Set and Get functionality.
func TestFileStore_SetAndGet(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "my-object"
	etag := "v1.0.0"
	content := "hello daramjwee"

	writer, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: etag})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, meta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, etag, meta.CacheTag)

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

	writer, _ := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: etag})
	require.NoError(t, writer.Close())

	meta, err := fs.Stat(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, etag, meta.CacheTag)

	_, err = fs.Stat(ctx, "non-existent-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

// TestFileStore_Delete tests the object deletion functionality.
func TestFileStore_Delete(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "delete-key"

	writer, _ := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, writer.Close())

	err := fs.Delete(ctx, key)
	require.NoError(t, err)

	dataPath := fs.toDataPath(key)
	_, err = os.Stat(dataPath)
	assert.True(t, os.IsNotExist(err), "data file should be deleted")

	err = fs.Delete(ctx, key) // Deleting again should not error
	require.NoError(t, err)
}

func TestFileStore_DeleteWaitsForPathLockBeforeUpdatingTracking(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "delete-blocked"

	writer, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = writer.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	path := fs.toDataPath(key)
	fs.lockManager.Lock(path)
	pathLocked := true
	defer func() {
		if pathLocked {
			fs.lockManager.Unlock(path)
		}
	}()

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- fs.Delete(ctx, key)
	}()

	require.Eventually(t, func() bool {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		_, exists := fs.fileSizes[key]
		return exists && fs.currentSize > 0
	}, time.Second, 10*time.Millisecond)

	fs.lockManager.Unlock(path)
	pathLocked = false
	require.NoError(t, <-deleteDone)
	fs.lockManager.Lock(path)
	pathLocked = true

	fs.mu.RLock()
	_, exists := fs.fileSizes[key]
	currentSize := fs.currentSize
	fs.mu.RUnlock()
	assert.False(t, exists)
	assert.Zero(t, currentSize)
}

func TestFileStore_EvictionDoesNotDropTrackingBeforeFileRemoval(t *testing.T) {
	fs := setupTestStore(t, WithEviction(policy.NewLRU()))
	ctx := context.Background()

	keys := []string{"evict-victim", ""}
	for i := 0; i < 32; i++ {
		candidate := fmt.Sprintf("evict-trigger-%d", i)
		if fs.lockManager.getSlot(fs.toDataPath(keys[0])) != fs.lockManager.getSlot(fs.toDataPath(candidate)) {
			keys[1] = candidate
			break
		}
	}
	require.NotEmpty(t, keys[1])
	require.NotEqual(t, fs.lockManager.getSlot(fs.toDataPath(keys[0])), fs.lockManager.getSlot(fs.toDataPath(keys[1])))

	first, err := fs.BeginSet(ctx, keys[0], &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = first.Write([]byte(strings.Repeat("a", 64)))
	require.NoError(t, err)
	require.NoError(t, first.Close())

	fs.mu.Lock()
	fs.capacity = fs.currentSize + 1
	fs.mu.Unlock()

	victimPath := fs.toDataPath(keys[0])
	fs.lockManager.Lock(victimPath)
	victimLocked := true
	defer func() {
		if victimLocked {
			fs.lockManager.Unlock(victimPath)
		}
	}()

	writeDone := make(chan error, 1)
	go func() {
		writer, err := fs.BeginSet(ctx, keys[1], &daramjwee.Metadata{CacheTag: "v2"})
		if err != nil {
			writeDone <- err
			return
		}
		if _, err := writer.Write([]byte(strings.Repeat("b", 64))); err != nil {
			writeDone <- err
			return
		}
		writeDone <- writer.Close()
	}()

	require.Eventually(t, func() bool {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		_, victimExists := fs.fileSizes[keys[0]]
		_, triggerExists := fs.fileSizes[keys[1]]
		return victimExists && triggerExists && fs.currentSize > fs.capacity
	}, time.Second, 10*time.Millisecond)

	fs.lockManager.Unlock(victimPath)
	victimLocked = false
	require.NoError(t, <-writeDone)
	fs.lockManager.Lock(victimPath)
	victimLocked = true

	fs.mu.RLock()
	_, victimExists := fs.fileSizes[keys[0]]
	_, triggerExists := fs.fileSizes[keys[1]]
	fs.mu.RUnlock()
	assert.False(t, victimExists)
	assert.True(t, triggerExists)
}

func TestFileStore_CloseReleasesEncodedLockBeforeLegacyCleanup(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := ""
	for i := 0; i < 64; i++ {
		candidate := fmt.Sprintf("legacy-cleanup-%d", i)
		if fs.lockManager.getSlot(fs.toDataPath(candidate)) != fs.lockManager.getSlot(fs.legacyDataPath(candidate)) {
			key = candidate
			break
		}
	}
	require.NotEmpty(t, key)

	writer, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = writer.Write([]byte("payload"))
	require.NoError(t, err)

	legacyPath := fs.legacyDataPath(key)
	encodedPath := fs.toDataPath(key)
	fs.lockManager.Lock(legacyPath)
	legacyLocked := true
	defer func() {
		if legacyLocked {
			fs.lockManager.Unlock(legacyPath)
		}
	}()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- writer.Close()
	}()

	encodedAcquired := make(chan struct{}, 1)
	go func() {
		fs.lockManager.Lock(encodedPath)
		fs.lockManager.Unlock(encodedPath)
		encodedAcquired <- struct{}{}
	}()

	require.Eventually(t, func() bool {
		select {
		case <-encodedAcquired:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	fs.lockManager.Unlock(legacyPath)
	legacyLocked = false
	require.NoError(t, <-closeDone)
}

// TestFileStore_Overwrite tests overwriting an existing object.
func TestFileStore_Overwrite(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "overwrite-key"

	// Write version 1
	writer1, _ := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	_, err := writer1.Write([]byte("version 1"))
	require.NoError(t, err)
	require.NoError(t, writer1.Close())

	// Write version 2
	writer2, _ := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v2"})
	_, err = writer2.Write([]byte("version 2"))
	require.NoError(t, err)
	require.NoError(t, writer2.Close())

	reader, meta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, "v2", meta.CacheTag)
	content, _ := io.ReadAll(reader)
	assert.Equal(t, "version 2", string(content))
}

func TestFileStore_BeginSetDoesNotHoldPathLockForWriterLifetime(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "same-key-overlap"

	first, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = first.Write([]byte("first"))
	require.NoError(t, err)

	type beginResult struct {
		sink daramjwee.WriteSink
		err  error
	}
	secondReady := make(chan beginResult, 1)
	go func() {
		sink, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v2"})
		secondReady <- beginResult{sink: sink, err: err}
	}()

	var second daramjwee.WriteSink
	select {
	case result := <-secondReady:
		require.NoError(t, result.err)
		second = result.sink
	case <-time.After(100 * time.Millisecond):
		require.NoError(t, first.Abort())
		result := <-secondReady
		if result.sink != nil {
			_ = result.sink.Abort()
		}
		t.Fatal("second BeginSet blocked on first writer lifetime")
	}

	require.NoError(t, second.Abort())
	require.NoError(t, first.Abort())
}

func TestFileStore_LateCloseDoesNotOverwriteNewerVisibleValue(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "same-key-late-close"

	first, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = first.Write([]byte("first"))
	require.NoError(t, err)

	type beginResult struct {
		sink daramjwee.WriteSink
		err  error
	}
	secondReady := make(chan beginResult, 1)
	go func() {
		sink, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v2"})
		secondReady <- beginResult{sink: sink, err: err}
	}()

	var second daramjwee.WriteSink
	select {
	case result := <-secondReady:
		require.NoError(t, result.err)
		second = result.sink
	case <-time.After(100 * time.Millisecond):
		require.NoError(t, first.Abort())
		result := <-secondReady
		if result.sink != nil {
			_ = result.sink.Abort()
		}
		t.Fatal("second BeginSet blocked on first writer lifetime")
	}

	_, err = second.Write([]byte("second"))
	require.NoError(t, err)
	require.NoError(t, second.Close())
	require.NoError(t, first.Close())

	reader, meta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "v2", meta.CacheTag)
	assert.Equal(t, "second", string(body))
}

// TestFileStore_PathTraversal tests that path traversal attempts are prevented.
func TestFileStore_PathTraversal(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	maliciousKey := "../malicious-file"
	writer, err := fs.BeginSet(ctx, maliciousKey, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	expectedPath := fs.toDataPath(maliciousKey)
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
			writer, err := fs.BeginSet(ctx, uniqueKey, &daramjwee.Metadata{CacheTag: "v1"})
			require.NoError(t, err, "BeginSet should succeed for key: %s", uniqueKey)

			_, err = writer.Write([]byte(content))
			require.NoError(t, err, "Write should succeed")

			err = writer.Close()
			require.NoError(t, err, "Close should succeed")

			// Read the file back
			reader, meta, err := fs.GetStream(ctx, uniqueKey)
			require.NoError(t, err, "GetStream should succeed for key: %s", uniqueKey)

			assert.Equal(t, "v1", meta.CacheTag)

			readContent, err := io.ReadAll(reader)
			require.NoError(t, err, "ReadAll should succeed")
			assert.Equal(t, content, string(readContent))
			reader.Close()

			// Test Stat
			statMeta, err := fs.Stat(ctx, uniqueKey)
			require.NoError(t, err, "Stat should succeed")
			assert.Equal(t, "v1", statMeta.CacheTag)

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

			writer, err := fs.BeginSet(ctx, tc.key, &daramjwee.Metadata{CacheTag: "v1"})
			if !tc.shouldWork {
				// For cases that shouldn't work, we might still get a writer
				// but operations should fail gracefully
				if err != nil {
					return // Expected failure
				}
			} else {
				require.NoError(t, err, "BeginSet should succeed for: %s", tc.description)
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
					assert.True(t, absPath == absBase || strings.HasPrefix(absPath, absBase+string(os.PathSeparator)),
						"File should be within base directory. Base: %s, Path: %s", absBase, absPath)
				}
			}
		})
	}
}

// TestFileStore_SetWithCopyWrite tests the copy-and-truncate strategy.
func TestFileStore_SetWithCopyWrite(t *testing.T) {
	fs := setupTestStore(t, WithCopyWrite())
	ctx := context.Background()
	key := "copy-test"
	content := "data for copy"

	writer, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v-copy"})
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
	err := os.MkdirAll(filepath.Dir(dataPath), 0755)
	require.NoError(t, err)
	err = os.Mkdir(dataPath, 0755)
	require.NoError(t, err)

	writer, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
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
	fs := setupTestStore(t, WithCopyWrite())
	ctx := context.Background()
	key := "copy-fail-key"
	dataPath := fs.toDataPath(key)

	// Create a directory where the file should be, so the final copy fails.
	require.NoError(t, os.MkdirAll(dataPath, 0755))

	writer, err := fs.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err, "BeginSet should succeed as temp file creation is unaffected")
	_, err = writer.Write([]byte("some data"))
	require.NoError(t, err)

	err = writer.Close()
	require.Error(t, err, "Close() should fail on copy error due to directory destination")

	// Check that the temp file was cleaned up.
	files, _ := os.ReadDir(fs.baseDir)
	for _, file := range files {
		if file.IsDir() && file.Name() == filepath.Base(dataPath) {
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
		CacheTag:   "v-complex",
		CachedAt:   now,
		IsNegative: true,
	}
	writer, err := fs.BeginSet(ctx, key, originalMeta)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, retrievedMeta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, originalMeta.CacheTag, retrievedMeta.CacheTag)
	assert.True(t, originalMeta.CachedAt.Equal(retrievedMeta.CachedAt))
	assert.Equal(t, originalMeta.IsNegative, retrievedMeta.IsNegative)
}

func TestFileStore_DistinctKeysDoNotCollideOnDisk(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	writerA, err := fs.BeginSet(ctx, "", &daramjwee.Metadata{CacheTag: "empty"})
	require.NoError(t, err)
	_, err = writerA.Write([]byte("empty-key-data"))
	require.NoError(t, err)
	require.NoError(t, writerA.Close())

	writerB, err := fs.BeginSet(ctx, "empty_key", &daramjwee.Metadata{CacheTag: "literal"})
	require.NoError(t, err)
	_, err = writerB.Write([]byte("literal-data"))
	require.NoError(t, err)
	require.NoError(t, writerB.Close())

	readerA, metaA, err := fs.GetStream(ctx, "")
	require.NoError(t, err)
	defer readerA.Close()
	bodyA, err := io.ReadAll(readerA)
	require.NoError(t, err)
	assert.Equal(t, "empty", metaA.CacheTag)
	assert.Equal(t, "empty-key-data", string(bodyA))

	readerB, metaB, err := fs.GetStream(ctx, "empty_key")
	require.NoError(t, err)
	defer readerB.Close()
	bodyB, err := io.ReadAll(readerB)
	require.NoError(t, err)
	assert.Equal(t, "literal", metaB.CacheTag)
	assert.Equal(t, "literal-data", string(bodyB))
}

func TestFileStore_LegacyPathRemainsReadableAndDeletable(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "legacy/key.txt"
	legacyPath := legacyDataPathForTest(fs.baseDir, key)

	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	writeLegacyMetadataForTest(t, f, "legacy")
	_, err = f.Write([]byte("legacy-data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	fs2, err := New(fs.baseDir, log.NewNopLogger())
	require.NoError(t, err)

	reader, meta, err := fs2.GetStream(ctx, key)
	require.NoError(t, err)

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "legacy", meta.CacheTag)
	assert.Equal(t, "legacy-data", string(body))

	require.NoError(t, fs2.Delete(ctx, key))

	_, statErr := os.Stat(legacyPath)
	assert.True(t, os.IsNotExist(statErr))
}

func TestFileStore_OverwriteLegacyPathRemovesLegacyFile(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "legacy/overwrite.txt"
	legacyPath := legacyDataPathForTest(fs.baseDir, key)

	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	writeLegacyMetadataForTest(t, f, "legacy")
	_, err = f.Write([]byte("legacy-data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	fs2, err := New(fs.baseDir, log.NewNopLogger())
	require.NoError(t, err)

	writer, err := fs2.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "new"})
	require.NoError(t, err)
	_, err = writer.Write([]byte("new-data"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, statErr := os.Stat(legacyPath)
	assert.True(t, os.IsNotExist(statErr), "legacy file should be removed after overwrite")

	reader, meta, err := fs2.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "new", meta.CacheTag)
	assert.Equal(t, "new-data", string(body))
}

func TestFileStore_LegacyFallbackDoesNotAliasEncodedNamespaceKeys(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	writer, err := fs.BeginSet(ctx, "foo", &daramjwee.Metadata{CacheTag: "foo"})
	require.NoError(t, err)
	_, err = writer.Write([]byte("foo-data"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, _, err = fs.GetStream(ctx, "b64_Zm9v")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	_, statErr := fs.Stat(ctx, "b64_Zm9v")
	assert.ErrorIs(t, statErr, daramjwee.ErrNotFound)

	require.NoError(t, fs.Delete(ctx, "b64_Zm9v"))

	reader, meta, err := fs.GetStream(ctx, "foo")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "foo", meta.CacheTag)
	assert.Equal(t, "foo-data", string(body))
}

func TestFileStore_EncodedNamespaceDoesNotOverwriteLegacyB64PrefixedKey(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	legacyKey := "b64_Zm9v"
	legacyPath := legacyDataPathForTest(fs.baseDir, legacyKey)

	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	writeLegacyMetadataForTest(t, f, "legacy")
	_, err = f.Write([]byte("legacy-data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	writer, err := fs.BeginSet(ctx, "foo", &daramjwee.Metadata{CacheTag: "foo"})
	require.NoError(t, err)
	_, err = writer.Write([]byte("foo-data"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	_, err = os.Stat(fs.toDataPath("foo"))
	require.NoError(t, err)
	_, err = os.Stat(legacyPath)
	require.NoError(t, err)
	require.NotEqual(t, fs.toDataPath("foo"), legacyPath)

	legacyReader, legacyMeta, err := fs.GetStream(ctx, legacyKey)
	require.NoError(t, err)
	defer legacyReader.Close()
	legacyBody, err := io.ReadAll(legacyReader)
	require.NoError(t, err)
	assert.Equal(t, "legacy", legacyMeta.CacheTag)
	assert.Equal(t, "legacy-data", string(legacyBody))

	reader, meta, err := fs.GetStream(ctx, "foo")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "foo", meta.CacheTag)
	assert.Equal(t, "foo-data", string(body))
}

func TestFileStore_EncodedNamespaceDoesNotClaimAmbiguousLegacyFile(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	legacyKey := "b64_Zm9v"
	legacyPath := legacyDataPathForTest(fs.baseDir, legacyKey)

	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	writeLegacyMetadataForTest(t, f, "legacy-ambiguous")
	_, err = f.Write([]byte("legacy-ambiguous-data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = fs.GetStream(ctx, "foo")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	_, statErr := fs.Stat(ctx, "foo")
	assert.ErrorIs(t, statErr, daramjwee.ErrNotFound)

	require.NoError(t, fs.Delete(ctx, "foo"))
	_, statErr = os.Stat(legacyPath)
	require.NoError(t, statErr, "delete for encoded owner must not remove ambiguous legacy file")

	reader, meta, err := fs.GetStream(ctx, legacyKey)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "legacy-ambiguous", meta.CacheTag)
	assert.Equal(t, "legacy-ambiguous-data", string(body))
}

func TestFileStore_EncodedNamespaceDoesNotClaimAmbiguousLegacyFileWithTrailingSlashBaseDir(t *testing.T) {
	dir := t.TempDir()
	logger := log.NewNopLogger()
	ctx := context.Background()

	fs, err := New(dir+string(os.PathSeparator), logger)
	require.NoError(t, err)

	legacyKey := "b64_Zm9v"
	legacyPath := legacyDataPathForTest(dir, legacyKey)

	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	writeLegacyMetadataForTest(t, f, "legacy-trailing")
	_, err = f.Write([]byte("legacy-trailing-data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = fs.GetStream(ctx, "foo")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	require.NoError(t, fs.Delete(ctx, "foo"))
	_, statErr := os.Stat(legacyPath)
	require.NoError(t, statErr)

	reader, meta, err := fs.GetStream(ctx, legacyKey)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "legacy-trailing", meta.CacheTag)
	assert.Equal(t, "legacy-trailing-data", string(body))
}

func TestFileStore_LegacyB64PrefixedKeyRemainsReachable(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "b64_Zm9v"
	legacyPath := legacyDataPathForTest(fs.baseDir, key)

	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	writeLegacyMetadataForTest(t, f, "legacy-b64")
	_, err = f.Write([]byte("legacy-b64-data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reader, meta, err := fs.GetStream(ctx, key)
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "legacy-b64", meta.CacheTag)
	assert.Equal(t, "legacy-b64-data", string(body))

	statMeta, err := fs.Stat(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "legacy-b64", statMeta.CacheTag)

	require.NoError(t, fs.Delete(ctx, key))
	_, statErr := os.Stat(legacyPath)
	assert.True(t, os.IsNotExist(statErr), "legacy b64-prefixed file should still be deletable after upgrade")
}

func TestFileStore_ReopenPreservesLegacyB64PrefixedKeyTracking(t *testing.T) {
	dir := t.TempDir()
	logger := log.NewNopLogger()
	ctx := context.Background()
	key := "b64_Zm9v"
	legacyPath := legacyDataPathForTest(dir, key)

	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	writeLegacyMetadataForTest(t, f, "legacy-b64")
	_, err = f.Write([]byte("legacy-b64-data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	fs, err := New(dir, logger)
	require.NoError(t, err)

	meta, err := fs.Stat(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "legacy-b64", meta.CacheTag)

	require.NoError(t, fs.Delete(ctx, key))
	_, statErr := os.Stat(legacyPath)
	assert.True(t, os.IsNotExist(statErr), "reopened store should still track and delete legacy b64-prefixed files")
}

func TestFileStore_InitializeCurrentSizeDoesNotDoubleCountDuplicateLogicalKeys(t *testing.T) {
	dir := t.TempDir()
	logger := log.NewNopLogger()
	key := "foo"
	legacyPath := legacyDataPathForTest(dir, key)
	encodedPath := filepath.Join(dir, encodedKeyDir, encodeKey(key))

	writeStoreFile := func(path, storedKey, etag, body string) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		f, err := os.Create(path)
		require.NoError(t, err)
		if storedKey != "" {
			require.NoError(t, writeStoredMetadata(f, storedKey, &daramjwee.Metadata{CacheTag: etag}, 0))
		} else {
			require.NoError(t, writeMetadata(f, &daramjwee.Metadata{CacheTag: etag}))
		}
		_, err = f.Write([]byte(body))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	writeStoreFile(legacyPath, "", "legacy", "legacy-data")
	writeStoreFile(encodedPath, key, "encoded", "encoded-data")

	fs, err := New(dir, logger)
	require.NoError(t, err)
	require.NoError(t, err)

	fs.mu.RLock()
	currentSize := fs.currentSize
	trackedSize := fs.fileSizes[key]
	fs.mu.RUnlock()

	legacyInfo, err := os.Stat(legacyPath)
	require.NoError(t, err)
	encodedInfo, err := os.Stat(encodedPath)
	require.NoError(t, err)
	assert.Equal(t, trackedSize, currentSize)
	assert.Contains(t, []int64{legacyInfo.Size(), encodedInfo.Size()}, trackedSize)
	assert.NotEqual(t, legacyInfo.Size()+encodedInfo.Size(), currentSize)
}

func TestLockedWriteCloser_DoesNotCommitWhenFileCloseFails(t *testing.T) {
	var committed bool
	var aborted bool

	badFile := os.NewFile(^uintptr(0), "bad-file")
	writer := newLockedWriteCloser(
		badFile,
		func() error {
			committed = true
			return nil
		},
		func() error {
			aborted = true
			return nil
		},
	)

	err := writer.Close()
	require.Error(t, err)
	assert.False(t, committed)
	assert.True(t, aborted)
}

func legacyDataPathForTest(baseDir, key string) string {
	safeFallback := func(key string) string {
		safeKey := strings.ReplaceAll(key, "..", "")
		safeKey = strings.ReplaceAll(safeKey, string(os.PathSeparator), "_")
		safeKey = strings.ReplaceAll(safeKey, "/", "_")
		if safeKey == "" {
			safeKey = "safe_fallback"
		}
		return filepath.Join(baseDir, safeKey)
	}

	if key == "" {
		return filepath.Join(baseDir, "empty_key")
	}

	slashedKey := filepath.ToSlash(key)
	cleanKey := filepath.Clean("/" + slashedKey)
	cleanKey = strings.TrimPrefix(cleanKey, "/")

	if cleanKey == "" || cleanKey == "." {
		cleanKey = "root_file"
	}

	fullPath := filepath.Join(baseDir, cleanKey)

	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return safeFallback(key)
	}

	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return safeFallback(key)
	}

	if !strings.HasPrefix(absPath+string(os.PathSeparator), absBase+string(os.PathSeparator)) && absPath != absBase {
		return safeFallback(key)
	}

	return fullPath
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
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v-bench"})
		if err != nil {
			b.Fatalf("BeginSet failed: %v", err)
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
	store := setupBenchmarkStore(b, WithCopyWrite())
	benchmarkFileStoreSet(b, store)
}

// benchmarkFileStoreGet benchmarks the Get operation for FileStore.
func benchmarkFileStoreGet(b *testing.B, store *FileStore) {
	ctx := context.Background()
	data := []byte("this is benchmark data")
	numItems := 1000

	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{CacheTag: "v-bench"})
		if err != nil {
			b.Fatalf("Setup: BeginSet failed: %v", err)
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
	store := setupBenchmarkStore(b, WithCopyWrite())
	benchmarkFileStoreGet(b, store)
}
