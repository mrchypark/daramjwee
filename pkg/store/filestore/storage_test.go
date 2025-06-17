package filestore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestStore는 각 테스트를 위한 임시 디렉토리와 FileStore 인스턴스를 생성합니다.
// t.Cleanup()을 사용하여 테스트가 끝나면 자동으로 디렉토리를 삭제합니다.
func setupTestStoreWithOptions(t *testing.T, opts ...Option) *FileStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "filestore-test-*")
	if err != nil {
		t.Fatalf("테스트 디렉토리 생성 실패: %v", err)
	}
	t.Cleanup(func() {
		// 테스트 중 권한이 변경되었을 수 있으므로, 정리 전에 권한을 복구합니다.
		_ = os.Chmod(dir, 0755)
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("Error removing test directory %s: %v", dir, err)
		}
	})

	// 테스트 중에는 로깅을 하지 않도록 No-op 로거를 사용합니다.
	logger := log.NewNopLogger()
	fs, err := New(dir, logger, opts...) // opts 파라미터를 New 함수에 전달
	if err != nil {
		t.Fatalf("FileStore 생성 실패: %v", err)
	}
	return fs
}

// 기존 테스트와의 호환성을 위해 setupTestStore는 옵션 없이 호출하는 형태로 남겨둡니다.
func setupTestStore(t *testing.T) *FileStore {
	return setupTestStoreWithOptions(t)
}

// TestFileStore_SetWithCopyAndTruncate_ErrorOnCopy는 복사/잘라내기 전략 사용 중
// 최종 파일 생성 단계에서 에러가 발생했을 때, 에러가 올바르게 전파되고
// 임시 파일이 깨끗하게 정리되는지 검증합니다.
func TestFileStore_SetWithCopyAndTruncate_ErrorOnCopy(t *testing.T) {
	// 1. WithCopyAndTruncate 옵션으로 FileStore를 설정합니다.
	fs := setupTestStoreWithOptions(t, WithCopyAndTruncate())
	ctx := context.Background()

	// 2. 에러 상황을 유도합니다.
	//    존재하지 않는 디렉토리를 포함하는 키를 사용하여 copyFile 내부의 os.Create(dst)가
	//    실패하도록 만듭니다. 이 방법은 디렉토리 쓰기 권한에 영향을 주지 않으므로
	//    임시 파일 삭제는 성공할 수 있습니다.
	key := "non-existent-dir/copy-error-key"

	writer, err := fs.SetWithWriter(ctx, key, "v1")
	if err != nil {
		t.Fatalf("SetWithWriter 초기화 실패: %v", err)
	}
	if _, err := writer.Write([]byte("this data should be cleaned up")); err != nil {
		t.Fatalf("임시 파일에 쓰기 실패: %v", err)
	}

	// 3. writer.Close()를 호출합니다. 이 때 내부적으로 copyFile이 실패해야 합니다.
	err = writer.Close()
	if err == nil {
		t.Fatal("writer.Close()가 에러를 반환해야 했지만, 성공했습니다.")
	}

	// 에러 타입을 확인하는 대신, 에러 메시지 내용을 직접 확인하여 안정성을 높입니다.
	expectedErrStr := "no such file or directory"
	if !strings.Contains(err.Error(), expectedErrStr) {
		t.Fatalf("에러 메시지에 '%s'가 포함되어야 하지만, 실제 에러는 다음과 같습니다: %v", expectedErrStr, err)
	}

	// 4. 의도한 대로 동작했는지 후속 상태를 검증합니다.
	// 4.1. 최종 목적지 파일이 생성되지 않았는지 확인합니다.
	finalPath := fs.toDataPath(key)
	if _, statErr := os.Stat(finalPath); !os.IsNotExist(statErr) {
		t.Errorf("복사 실패 후 최종 파일(%s)이 남아있습니다.", finalPath)
	}

	// 4.2. 임시 파일이 깨끗하게 삭제되었는지 확인합니다. (핵심 검증)
	files, readDirErr := os.ReadDir(fs.baseDir)
	if readDirErr != nil {
		t.Fatalf("정리 상태를 확인하기 위해 디렉토리를 읽는 데 실패했습니다: %v", readDirErr)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "daramjwee-tmp-") {
			t.Errorf("에러 발생 후 임시 파일(%s)이 삭제되지 않았습니다.", file.Name())
		}
	}
}

// TestFileStore_SetAndGet은 가장 기본적인 Set 후 Get 시나리오를 테스트합니다.
func TestFileStore_SetAndGet(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "my-object"
	etag := "v1.0.0"
	content := "hello daramjwee"

	// 1. 데이터 쓰기
	writer, err := fs.SetWithWriter(ctx, key, etag)
	if err != nil {
		t.Fatalf("SetWithWriter 실패: %v", err)
	}
	_, err = writer.Write([]byte(content))
	if err != nil {
		t.Fatalf("쓰기 실패: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("닫기 실패: %v", err)
	}

	// 2. 데이터 읽기
	reader, meta, err := fs.GetStream(ctx, key)
	if err != nil {
		t.Fatalf("GetStream 실패: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("Error closing reader: %v", err)
		}
	}()

	// 3. 메타데이터 및 콘텐츠 검증
	if meta.ETag != etag {
		t.Errorf("ETag 불일치: 기대값 '%s', 실제값 '%s'", etag, meta.ETag)
	}

	readBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("읽기 실패: %v", err)
	}
	if string(readBytes) != content {
		t.Errorf("콘텐츠 불일치: 기대값 '%s', 실제값 '%s'", content, string(readBytes))
	}
}

// TestFileStore_Get_NotFound는 존재하지 않는 키를 조회하는 경우를 테스트합니다.
func TestFileStore_Get_NotFound(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	_, _, err := fs.GetStream(ctx, "non-existent-key")
	if err != daramjwee.ErrNotFound {
		t.Errorf("기대 에러는 ErrNotFound이지만, 실제 에러는 %v 입니다", err)
	}
}

// TestFileStore_Stat은 데이터 없이 메타데이터만 조회하는 기능을 테스트합니다.
func TestFileStore_Stat(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "stat-key"
	etag := "etag-for-stat"

	// 테스트 데이터 설정
	writer, _ := fs.SetWithWriter(ctx, key, etag)
	if _, err := writer.Write([]byte("some data")); err != nil {
		t.Fatalf("Error writing data for stat test: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Error closing writer for stat test: %v", err)
	}

	// Stat 호출 및 검증
	meta, err := fs.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat 실패: %v", err)
	}
	if meta.ETag != etag {
		t.Errorf("ETag 불일치: 기대값 '%s', 실제값 '%s'", etag, meta.ETag)
	}

	// 존재하지 않는 키에 대한 Stat
	_, err = fs.Stat(ctx, "non-existent-key")
	if err != daramjwee.ErrNotFound {
		t.Errorf("기대 에러는 ErrNotFound이지만, 실제 에러는 %v 입니다", err)
	}
}

// TestFileStore_Delete는 객체 삭제 기능을 테스트합니다.
func TestFileStore_Delete(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "delete-key"

	// 테스트 데이터 설정
	writer, _ := fs.SetWithWriter(ctx, key, "v1")
	if _, err := writer.Write([]byte("to be deleted")); err != nil {
		t.Fatalf("Error writing data for delete test: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Error closing writer for delete test: %v", err)
	}

	// 삭제 실행
	if err := fs.Delete(ctx, key); err != nil {
		t.Fatalf("Delete 실패: %v", err)
	}

	// 파일들이 실제로 삭제되었는지 확인
	dataPath := fs.toDataPath(key)
	metaPath := fs.toMetaPath(dataPath)
	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Error("데이터 파일이 삭제되지 않았습니다.")
	}
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("메타데이터 파일이 삭제되지 않았습니다.")
	}

	// 이미 삭제된 키를 다시 삭제 (에러가 발생하면 안 됨)
	if err := fs.Delete(ctx, key); err != nil {
		t.Errorf("이미 삭제된 키를 다시 삭제할 때 에러 발생: %v", err)
	}
}

// TestFileStore_Overwrite는 기존 객체를 덮어쓰는 시나리오를 테스트합니다.
func TestFileStore_Overwrite(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "overwrite-key"

	// Version 1 쓰기
	writer1, _ := fs.SetWithWriter(ctx, key, "v1")
	if _, err := writer1.Write([]byte("version 1")); err != nil {
		t.Fatalf("Error writing data for overwrite test (initial): %v", err)
	}
	if err := writer1.Close(); err != nil {
		t.Fatalf("Error closing writer for overwrite test (initial): %v", err)
	}

	// Version 2 쓰기 (덮어쓰기)
	writer2, _ := fs.SetWithWriter(ctx, key, "v2")
	if _, err := writer2.Write([]byte("version 2")); err != nil {
		t.Fatalf("Error writing data for overwrite test (new): %v", err)
	}
	if err := writer2.Close(); err != nil {
		t.Fatalf("Error closing writer for overwrite test (new): %v", err)
	}

	// 최종 버전(v2)이 올바르게 저장되었는지 확인
	reader, meta, err := fs.GetStream(ctx, key)
	if err != nil {
		t.Fatalf("GetStream 실패: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("Error closing reader: %v", err)
		}
	}()

	if meta.ETag != "v2" {
		t.Errorf("ETag가 v2로 덮어써지지 않았습니다. 실제값: %s", meta.ETag)
	}
	content, _ := io.ReadAll(reader)
	if string(content) != "version 2" {
		t.Errorf("콘텐츠가 'version 2'로 덮어써지지 않았습니다. 실제값: %s", string(content))
	}
}

// TestFileStore_PathTraversal은 경로 조작 공격 시도를 방지하는지 테스트합니다.
func TestFileStore_PathTraversal(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()

	// "../"를 포함하는 악의적인 키
	maliciousKey := "../malicious-file"
	writer, err := fs.SetWithWriter(ctx, maliciousKey, "v1")
	if err != nil {
		t.Fatalf("SetWithWriter 실패: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Error closing writer for path traversal test: %v", err)
	}

	// 파일이 baseDir 바깥이 아닌 내부에 생성되었는지 확인
	// toDataPath는 "../"를 제거하므로 "malicious-file" 이라는 파일이 생성되어야 함
	expectedPath := filepath.Join(fs.baseDir, "malicious-file")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Error("경로 조작 방지 로직이 작동하지 않아, 예상된 경로에 파일이 생성되지 않았습니다.")
	}

	// 실제 상위 디렉토리에 파일이 생성되지 않았는지 확인
	outsidePath := filepath.Join(fs.baseDir, "..", "malicious-file")
	if _, err := os.Stat(outsidePath); !os.IsNotExist(err) {
		t.Error("경로 조작에 성공하여 상위 디렉토리에 파일이 생성되었습니다.")
	}
}

// 기존 파일의 맨 아래에 다음 테스트 함수들을 추가합니다.

// TestFileStore_Set_ErrorOnWriteMeta는 메타데이터 파일 쓰기에 실패했을 때
// 모든 변경사항이 롤백되고 임시 파일이 정리되는지 검증합니다.
func TestFileStore_Set_ErrorOnWriteMeta(t *testing.T) {
	fs := setupTestStore(t)
	ctx := context.Background()
	key := "meta-write-fail-key"
	metaPath := fs.toMetaPath(fs.toDataPath(key))

	// 1. 메타 파일 쓰기 실패를 유도하기 위해, 메타 파일 경로에 디렉토리를 생성합니다.
	//    이렇게 하면 os.WriteFile이 해당 경로에 파일을 쓰려고 할 때 "is a directory" 에러가 발생합니다.
	err := os.Mkdir(metaPath, 0755)
	require.NoError(t, err)

	writer, err := fs.SetWithWriter(ctx, key, "v1")
	require.NoError(t, err)

	_, err = writer.Write([]byte("this should be cleaned up"))
	require.NoError(t, err)

	// 2. Close() 호출 시 내부적으로 writeMetaFile에서 에러가 발생해야 합니다.
	err = writer.Close()
	require.Error(t, err, "Close() should fail due to meta write error")

	// 3. 디렉토리 권한은 변경되지 않았으므로, 임시 파일은 정상적으로 삭제되어야 합니다.
	files, _ := os.ReadDir(fs.baseDir)
	for _, file := range files {
		// 생성했던 metaPath 디렉토리는 남아있을 수 있으므로, 임시 파일만 없는지 확인합니다.
		if file.Name() == filepath.Base(metaPath) {
			continue
		}
		assert.False(t, strings.HasPrefix(file.Name(), "daramjwee-tmp-"), "Temporary file should be cleaned up")
	}
}

// TestFileStore_SetWithRename_ErrorOnRename은 최종 rename 단계에서 실패했을 때
// 롤백 로직(메타 파일 삭제)이 올바르게 동작하는지 검증합니다.
func TestFileStore_SetWithRename_ErrorOnRename(t *testing.T) {
	fs := setupTestStore(t) // 기본 전략: rename
	ctx := context.Background()
	key := "rename-fail-key"
	dataPath := fs.toDataPath(key)

	// 1. rename 실패를 유도하기 위해 최종 경로에 파일이 아닌 디렉토리를 생성
	err := os.Mkdir(dataPath, 0755)
	require.NoError(t, err)

	writer, err := fs.SetWithWriter(ctx, key, "v1")
	require.NoError(t, err)
	_, err = writer.Write([]byte("some data"))
	require.NoError(t, err)

	// 2. Close() 호출 시 내부적으로 os.Rename에서 에러 발생
	// (파일을 디렉토리로 덮어쓸 수 없기 때문)
	err = writer.Close()
	require.Error(t, err, "Close() should return an error on rename failure")

	// 3. 롤백 로직 검증: rename 전에 생성된 메타 파일이 삭제되었는지 확인
	metaPath := fs.toMetaPath(dataPath)
	_, err = os.Stat(metaPath)
	assert.True(t, os.IsNotExist(err), "Meta file should be removed during rollback after rename failure")
}
