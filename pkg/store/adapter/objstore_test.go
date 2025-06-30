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

// TestObjstoreAdapter_GoroutineLeakOnContextCancel는 스트리밍 업로드 중 컨텍스트가
// 취소되었을 때, 백그라운드 업로드 고루틴이 정상적으로 종료되어 누수되지 않는지 검증합니다.
func TestObjstoreAdapter_GoroutineLeakOnContextCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping leak test in short mode")
	}

	// 1. 테스트 시작 전의 고루틴 수를 기록합니다.
	initialGoroutines := runtime.NumGoroutine()

	// 2. 업로드가 시작되었음을 알릴 채널만 준비합니다. (종료 채널은 제거)
	uploadStarted := make(chan struct{})
	mockBucket := &errorBucketWithFunc{
		uploadFunc: func(ctx context.Context, name string, r io.Reader) error {
			close(uploadStarted) // 업로드 시작 신호
			// 컨텍스트가 취소될 때까지 대기
			<-ctx.Done()
			// 컨텍스트 에러를 반환하여, writer.Close()가 이 에러를 받을 수 있도록 함
			return ctx.Err()
		},
	}
	// errorBucket의 Upload 메서드를 커스텀 함수로 교체합니다.
	originalBucket := testStore.(*objstoreAdapter).bucket
	testStore.(*objstoreAdapter).bucket = mockBucket
	defer func() {
		testStore.(*objstoreAdapter).bucket = originalBucket
	}()

	// 3. 취소 가능한 컨텍스트를 생성합니다.
	ctx, cancel := context.WithCancel(context.Background())

	// 4. SetWithWriter를 호출하여 백그라운드 업로드 고루틴을 실행시킵니다.
	writer, err := testStore.SetWithWriter(ctx, "leak-test-key", &daramjwee.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, writer)

	// 업로드 고루틴이 실행될 때까지 대기
	<-uploadStarted

	// 5. 컨텍스트를 즉시 취소하여 고루틴 종료를 트리거합니다.
	cancel()

	// 6. writer를 닫습니다. 이 함수는 내부적으로 wg.Wait()를 통해
	//    백그라운드 고루틴이 종료될 때까지 안전하게 기다립니다.
	//    백그라운드 고루틴은 ctx.Err()를 반환하므로, Close()는 그 에러를 최종적으로 반환합니다.
	closeErr := writer.Close()
	assert.ErrorIs(t, closeErr, context.Canceled, "writer.Close() should propagate the context cancellation error")

	// 7. 테스트 종료 후, 고루틴 수가 테스트 시작 전과 비슷한 수준인지 확인하여 누수 여부를 최종 검증합니다.
	//    GC와 스케줄러가 안정화될 시간을 잠시 줍니다.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()

	assert.InDelta(t, initialGoroutines, finalGoroutines, 2, "Number of goroutines should not significantly increase after test")
}

// errorBucket을 수정하여 커스텀 Upload 함수를 주입할 수 있도록 합니다.
type errorBucketWithFunc struct {
	objstore.Bucket
	uploadFunc func(ctx context.Context, name string, r io.Reader) error
}

func (b *errorBucketWithFunc) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.uploadFunc != nil {
		return b.uploadFunc(ctx, name, r)
	}
	// 기본 동작: 즉시 에러 반환
	_, _ = io.ReadAll(r)
	return errors.New("simulated upload error")
}
