package filestore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
)

// setupTestStore는 각 테스트를 위한 임시 디렉토리와 FileStore 인스턴스를 생성합니다.
// t.Cleanup()을 사용하여 테스트가 끝나면 자동으로 디렉토리를 삭제합니다.
func setupTestStore(t *testing.T) *FileStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "filestore-test-*")
	if err != nil {
		t.Fatalf("테스트 디렉토리 생성 실패: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("Error removing test directory %s: %v", dir, err)
		}
	})

	// 테스트 중에는 로깅을 하지 않도록 No-op 로거를 사용합니다.
	logger := log.NewNopLogger()
	fs, err := New(dir, logger)
	if err != nil {
		t.Fatalf("FileStore 생성 실패: %v", err)
	}
	return fs
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
