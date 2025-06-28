package adapter

import (
	"context"
	"errors"
	"io"
	"os"
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

// TestMain sets up the test environment using an in-memory bucket
// for all tests in this package.
func TestMain(m *testing.M) {
	// Use an in-memory bucket for fast and isolated testing.
	inMemBucket := objstore.NewInMemBucket()
	testStore = NewObjstoreAdapter(inMemBucket, testLogger)

	// Run all tests
	os.Exit(m.Run())
}

// TestObjstoreAdapter_SetAndGetStream tests the basic upload and download functionality.
func TestObjstoreAdapter_SetAndGetStream(t *testing.T) {
	ctx := context.Background()
	key := "test-set-and-get"
	etag := "etag-123"
	content := "hello, daramjwee!"

	// 1. SetWithWriter를 사용하여 스트리밍 업로더를 가져옵니다.
	wc, err := testStore.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err, "SetWithWriter should not return an error")
	require.NotNil(t, wc, "writer should not be nil")

	// 2. 데이터를 쓰고 writer를 닫습니다.
	_, err = io.WriteString(wc, content)
	require.NoError(t, err, "writing to writer should not fail")
	err = wc.Close()
	require.NoError(t, err, "closing writer should not fail")

	// 3. GetStream으로 데이터를 다시 읽어옵니다.
	rc, meta, err := testStore.GetStream(ctx, key)
	require.NoError(t, err, "GetStream should not return an error after setting data")
	require.NotNil(t, rc, "reader should not be nil")
	require.NotNil(t, meta, "metadata should not be nil")
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("Error closing reader: %v", err)
		}
	}()

	// 4. 내용과 메타데이터를 검증합니다.
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

	// 어댑터가 사용하는 버킷을 임시로 교체하여 에러를 시뮬레이션합니다.
	// 실제 프로덕션 코드에서는 이렇게 하지 않겠지만, 테스트에서는 유용한 기법입니다.
	originalBucket := testStore.(*objstoreAdapter).bucket

	mockBucket := &errorBucket{originalBucket}
	testStore.(*objstoreAdapter).bucket = mockBucket
	defer func() {
		// 테스트가 끝나면 원래 버킷으로 복원합니다.
		testStore.(*objstoreAdapter).bucket = originalBucket
	}()

	wc, err := testStore.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)

	_, err = wc.Write([]byte("this will fail"))
	require.NoError(t, err)

	// Close()에서 내부 업로드 에러가 전파되어야 합니다.
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
	return errSimulatedUpload // 미리 정의한 커스텀 에러를 반환합니다.
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
		GraceUntil: now,
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
	assert.True(t, originalMeta.GraceUntil.Equal(retrievedMeta.GraceUntil), "GraceUntil should be equal")
	assert.Equal(t, originalMeta.IsNegative, retrievedMeta.IsNegative)

	// 3. Stat data and verify metadata
	retrievedMetaFromStat, err := testStore.Stat(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, retrievedMetaFromStat)

	assert.Equal(t, originalMeta.ETag, retrievedMetaFromStat.ETag)
	assert.True(t, originalMeta.GraceUntil.Equal(retrievedMetaFromStat.GraceUntil), "GraceUntil from Stat should be equal")
	assert.Equal(t, originalMeta.IsNegative, retrievedMetaFromStat.IsNegative)
}
