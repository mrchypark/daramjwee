package adapter

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

var (
	testStore  daramjwee.Store
	testLogger = log.NewNopLogger()
)

func TestMain(m *testing.M) {
	inMemBucket := objstore.NewInMemBucket()
	testStore = NewObjstoreAdapter(inMemBucket, testLogger)

	os.Exit(m.Run())
}

// TestObjstoreAdapter_SetAndGetStream tests the basic upload and download functionality.
func TestObjstoreAdapter_SetAndGetStream(t *testing.T) {
	ctx := context.Background()
	key := "test-set-and-get"
	etag := "etag-123"
	content := "hello, daramjwee!"

	wc, err := testStore.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err, "SetWithWriter should not return an error")
	require.NotNil(t, wc, "writer should not be nil")

	_, err = io.WriteString(wc, content)
	require.NoError(t, err, "writing to writer should not fail")
	err = wc.Close()
	require.NoError(t, err, "closing writer should not fail")

	rc, meta, err := testStore.GetStream(ctx, key)
	require.NoError(t, err, "GetStream should not return an error after setting data")
	require.NotNil(t, rc, "reader should not be nil")
	require.NotNil(t, meta, "metadata should not be nil")
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("Error closing reader: %v", err)
		}
	}()

	readBytes, err := io.ReadAll(rc)
	require.NoError(t, err, "reading from stream should not fail")

	assert.Equal(t, content, string(readBytes), "retrieved content should match original content")
	assert.Equal(t, etag, meta.ETag, "retrieved etag should match original etag")
}

// TestObjstoreAdapter_Stat tests the metadata retrieval functionality.
func TestObjstoreAdapter_Stat(t *testing.T) {
	ctx := context.Background()
	key := "test-stat"
	etag := "etag-for-stat"

	// Stat an object that does not exist
	_, err := testStore.Stat(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "Stat for a non-existent key should return ErrNotFound")

	// Create an object to test Stat
	wc, err := testStore.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)
	_, err = wc.Write([]byte("some data"))
	require.NoError(t, err)
	err = wc.Close()
	require.NoError(t, err)

	// Stat the existing object
	meta, err := testStore.Stat(ctx, key)
	require.NoError(t, err, "Stat for an existing key should not return an error")
	require.NotNil(t, meta, "metadata should not be nil")
	assert.Equal(t, etag, meta.ETag, "Stat should return the correct ETag")
}

// TestObjstoreAdapter_Delete tests the deletion functionality.
func TestObjstoreAdapter_Delete(t *testing.T) {
	ctx := context.Background()
	key := "test-delete"
	etag := "etag-for-delete"

	// Create an object to delete
	wc, err := testStore.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)
	_, err = wc.Write([]byte("data to be deleted"))
	require.NoError(t, err)
	err = wc.Close()
	require.NoError(t, err)

	// Ensure it exists before deleting
	_, err = testStore.Stat(ctx, key)
	require.NoError(t, err, "object should exist before deletion")

	// Delete the object
	err = testStore.Delete(ctx, key)
	require.NoError(t, err, "Delete should not return an error for an existing key")

	// Verify it no longer exists
	_, err = testStore.Stat(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "Stat after delete should return ErrNotFound")

	// Try getting the stream, which should also fail
	_, _, err = testStore.GetStream(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "GetStream after delete should return ErrNotFound")
}

// TestObjstoreAdapter_StreamingWriter_UploadError tests error handling
// when the underlying upload fails.
func TestObjstoreAdapter_StreamingWriter_UploadError(t *testing.T) {
	ctx := context.Background()
	key := "test-upload-error"
	etag := "etag-upload-error"

	// Temporarily replace the bucket used by the adapter to simulate errors.
	// We wouldn't do this in actual production code, but it's a useful technique in tests.
	originalBucket := testStore.(*objstoreAdapter).bucket

	mockBucket := &errorBucket{originalBucket}
	testStore.(*objstoreAdapter).bucket = mockBucket
	defer func() {
		// Restore the original bucket when the test ends.
		testStore.(*objstoreAdapter).bucket = originalBucket
	}()

	wc, err := testStore.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)

	_, err = wc.Write([]byte("this will fail"))
	require.NoError(t, err)

	// Close() should propagate the internal upload error.
	err = wc.Close()
	require.Error(t, err, "Close should return an error if upload fails")
	assert.Contains(t, err.Error(), "simulated upload error", "error message should indicate upload failure")
}

// errorBucket is a mock bucket that always fails on Upload.
type errorBucket struct {
	objstore.Bucket
}

// A specific error to be returned by the mock for clearer testing.
var errSimulatedUpload = errors.New("simulated upload error")

func (b *errorBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	// Consume the reader to allow the pipe to close, but return an error.
	_, _ = io.ReadAll(r)
	return errSimulatedUpload // Return the predefined custom error.
}

// TestObjstoreAdapter_NegativeCache_NoBody tests that setting an item with IsNegative=true
// and no body results in a zero-byte object.
func TestObjstoreAdapter_NegativeCache_NoBody(t *testing.T) {
	ctx := context.Background()
	key := "negative-cache-key"

	// 1. Set an item with IsNegative=true and an empty body.
	meta := &daramjwee.Metadata{ETag: "v-neg", IsNegative: true}
	wc, err := testStore.SetWithWriter(ctx, key, meta)
	require.NoError(t, err)
	// Write *no* data.
	err = wc.Close()
	require.NoError(t, err)

	// 2. Verify via GetStream.
	rc, retrievedMeta, err := testStore.GetStream(ctx, key)
	require.NoError(t, err)
	defer rc.Close()

	// Check metadata
	assert.True(t, retrievedMeta.IsNegative)
	assert.Equal(t, "v-neg", retrievedMeta.ETag)

	// Check body
	readBytes, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Len(t, readBytes, 0, "Retrieved body should be empty")
}

// TestObjstoreAdapter_MetadataFields ensures all metadata fields are stored and retrieved correctly.
func TestObjstoreAdapter_MetadataFields(t *testing.T) {
	ctx := context.Background()
	key := "metadata-test-key"
	now := time.Now().Truncate(time.Millisecond) // Truncate for reliable comparison

	// 1. Set data with complex metadata
	originalMeta := &daramjwee.Metadata{
		ETag:       "v-complex",
		CachedAt:   now,
		IsNegative: true,
	}
	wc, err := testStore.SetWithWriter(ctx, key, originalMeta)
	require.NoError(t, err)
	_, err = wc.Write([]byte("data"))
	require.NoError(t, err)
	err = wc.Close()
	require.NoError(t, err)

	// 2. Get data and verify metadata
	_, retrievedMeta, err := testStore.GetStream(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, retrievedMeta)

	assert.Equal(t, originalMeta.ETag, retrievedMeta.ETag)
	assert.True(t, originalMeta.CachedAt.Equal(retrievedMeta.CachedAt), "GraceUntil should be equal")
	assert.Equal(t, originalMeta.IsNegative, retrievedMeta.IsNegative)

	// 3. Stat data and verify metadata
	retrievedMetaFromStat, err := testStore.Stat(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, retrievedMetaFromStat)

	assert.Equal(t, originalMeta.ETag, retrievedMetaFromStat.ETag)
	assert.True(t, originalMeta.CachedAt.Equal(retrievedMetaFromStat.CachedAt), "GraceUntil from Stat should be equal")
	assert.Equal(t, originalMeta.IsNegative, retrievedMetaFromStat.IsNegative)
}

// TestObjstoreAdapter_GoroutineLeakOnContextCancel verifies that when the context
// is canceled during streaming upload, the background upload goroutine terminates
// properly without leaking.
func TestObjstoreAdapter_GoroutineLeakOnContextCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping leak test in short mode")
	}

	// 1. Record the number of goroutines before the test starts.
	initialGoroutines := runtime.NumGoroutine()

	// 2. Prepare only the channel to signal upload start. (Remove termination channel)
	uploadStarted := make(chan struct{})
	mockBucket := &errorBucketWithFunc{
		uploadFunc: func(ctx context.Context, name string, r io.Reader) error {
			close(uploadStarted) // Signal upload start
			// Wait until context is canceled
			<-ctx.Done()
			// Return context error so writer.Close() can receive this error
			return ctx.Err()
		},
	}
	// Replace errorBucket's Upload method with custom function.
	originalBucket := testStore.(*objstoreAdapter).bucket
	testStore.(*objstoreAdapter).bucket = mockBucket
	defer func() {
		testStore.(*objstoreAdapter).bucket = originalBucket
	}()

	// 3. Create a cancelable context.
	ctx, cancel := context.WithCancel(context.Background())

	// 4. Call SetWithWriter to start the background upload goroutine.
	writer, err := testStore.SetWithWriter(ctx, "leak-test-key", &daramjwee.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Wait until upload goroutine starts
	<-uploadStarted

	// 5. Cancel the context immediately to trigger goroutine termination.
	cancel()

	// 6. Close the writer. This function internally waits safely for the
	//    background goroutine to terminate through wg.Wait().
	//    Since the background goroutine returns ctx.Err(), Close() will ultimately return that error.
	closeErr := writer.Close()
	assert.ErrorIs(t, closeErr, context.Canceled, "writer.Close() should propagate the context cancellation error")

	// 7. After test completion, verify that the number of goroutines is at a similar level
	//    to before the test started to finally check for leaks.
	//    Give some time for GC and scheduler to stabilize.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()

	assert.InDelta(t, initialGoroutines, finalGoroutines, 2, "Number of goroutines should not significantly increase after test")
}

// errorBucketWithFunc modifies errorBucket to allow injection of custom Upload functions.
type errorBucketWithFunc struct {
	objstore.Bucket
	uploadFunc func(ctx context.Context, name string, r io.Reader) error
}

func (b *errorBucketWithFunc) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.uploadFunc != nil {
		return b.uploadFunc(ctx, name, r)
	}
	// Default behavior: return error immediately
	_, _ = io.ReadAll(r)
	return errors.New("simulated upload error")
}
