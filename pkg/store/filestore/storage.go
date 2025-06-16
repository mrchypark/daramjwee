// Package filestore provides a disk-based implementation of the daramjwee.Store interface.
package filestore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level" // Added import
	"github.com/mrchypark/daramjwee"
)

// FileStore is a disk-based implementation of the daramjwee.Store.
type FileStore struct {
	baseDir            string
	logger             log.Logger
	lockManager        *FileLockManager
	useCopyAndTruncate bool // 추가: rename 대신 copy를 사용할지 여부
}

// Option은 FileStore의 동작을 변경하는 함수 타입입니다.
type Option func(*FileStore)

// WithCopyAndTruncate는 원자적인 os.Rename 대신,
// 파일을 복사하는 방식을 사용하도록 설정합니다.
// 일부 네트워크 파일 시스템과의 호환성을 위해 필요할 수 있습니다.
func WithCopyAndTruncate() Option {
	return func(fs *FileStore) {
		fs.useCopyAndTruncate = true
	}
}

// New creates a new FileStore.
func New(dir string, logger log.Logger, opts ...Option) (*FileStore, error) { // 변경: opts 파라미터 추가
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory %s: %w", dir, err)
	}
	fs := &FileStore{
		baseDir:     dir,
		logger:      logger,
		lockManager: NewFileLockManager(2048),
	}

	// 사용자가 제공한 옵션들을 적용합니다.
	for _, opt := range opts {
		opt(fs)
	}

	return fs, nil
}

// 컴파일 타임에 인터페이스 만족 확인
var _ daramjwee.Store = (*FileStore)(nil)

// metaFilePayload defines the structure for storing metadata in a .meta.json file.
type metaFilePayload struct {
	ETag string `json:"etag"`
}

func (fs *FileStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.lockManager.RLock(path)

	meta, err := fs.readMetaFile(path)
	if err != nil {
		fs.lockManager.RUnlock(path)
		return nil, nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		fs.lockManager.RUnlock(path)
		if os.IsNotExist(err) {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	return newLockedReadCloser(file, func() { fs.lockManager.RUnlock(path) }), meta, nil
}

// SetWithWriter returns a WriteCloser for writing to the FileStore.
// To ensure atomicity and prevent data corruption from partial writes,
// data is first written to a temporary file. When the writer is closed,
// the temporary file is atomically renamed to its final destination.
func (fs *FileStore) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	path := fs.toDataPath(key)
	fs.lockManager.Lock(path)

	// 임시 파일에 먼저 써서 원자성(atomicity)을 보장
	tmpFile, err := os.CreateTemp(fs.baseDir, "daramjwee-tmp-*.data")
	if err != nil {
		fs.lockManager.Unlock(path)
		return nil, err
	}

	onClose := func() error {
		defer fs.lockManager.Unlock(path)

		// Based on the selected strategy, either use atomic rename or copy.
		// The copy strategy (WithCopyAndTruncate) is less atomic but provides
		// better compatibility with some network filesystems (e.g., NFS)
		// where rename operations across different devices can fail.
		if fs.useCopyAndTruncate {
			// 비원자적 복사 방식
			// 1. 데이터 파일을 먼저 최종 경로로 복사
			if err := copyFile(tmpFile.Name(), path); err != nil {
				if errRemove := os.Remove(tmpFile.Name()); errRemove != nil {
					level.Warn(fs.logger).Log("msg", "failed to remove temporary file", "file", tmpFile.Name(), "err", errRemove)
				}
				return fmt.Errorf("임시 파일에서 최종 파일로 복사 실패: %w", err)
			}
			if errRemove := os.Remove(tmpFile.Name()); errRemove != nil {
				level.Warn(fs.logger).Log("msg", "failed to remove temporary file", "file", tmpFile.Name(), "err", errRemove)
			}

			// 2. 데이터가 완전히 쓰인 후 메타데이터 파일 쓰기
			if err := fs.writeMetaFile(path, etag); err != nil {
				// 데이터는 쓰였지만 메타데이터 쓰기에 실패한 경우
				return fmt.Errorf("데이터 복사 후 메타데이터 쓰기 실패: %w", err)
			}
		} else {
			// 기존의 원자적 rename 방식
			// 1. 메타데이터 파일 먼저 쓰기
			if err := fs.writeMetaFile(path, etag); err != nil {
				if errRemove := os.Remove(tmpFile.Name()); errRemove != nil {
					level.Warn(fs.logger).Log("msg", "failed to remove temporary file", "file", tmpFile.Name(), "err", errRemove)
				}
				return err
			}
			// 2. 임시 데이터 파일을 최종 경로로 변경 (원자적)
			if err := os.Rename(tmpFile.Name(), path); err != nil {
				// 실패 시 롤백: 방금 쓴 메타 파일 정리
				// 롤백 중 발생하는 에러는 로그로 남기되, 원래의 에러를 반환합니다.
				if removeErr := os.Remove(fs.toMetaPath(path)); removeErr != nil {
					level.Warn(fs.logger).Log("msg", "failed to remove meta file during rollback", "path", fs.toMetaPath(path), "err", removeErr)
				}
				return err
			}
		}
		return nil
	}

	return newLockedWriteCloser(tmpFile, onClose), nil
}

func (fs *FileStore) Delete(ctx context.Context, key string) error {
	path := fs.toDataPath(key)
	fs.lockManager.Lock(path)
	defer fs.lockManager.Unlock(path)

	errData := os.Remove(path)
	errMeta := os.Remove(fs.toMetaPath(path))

	if os.IsNotExist(errData) {
		// 이미 없으면 성공으로 처리
		return nil
	}
	if errData != nil {
		return errData
	}
	if errMeta != nil && !os.IsNotExist(errMeta) {
		return errMeta
	}

	return nil
}

func (fs *FileStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.lockManager.RLock(path)
	defer fs.lockManager.RUnlock(path)
	return fs.readMetaFile(path)
}

// --- 내부 헬퍼 및 타입 ---

func (fs *FileStore) toDataPath(key string) string {
	// Path Traversal 공격을 막기 위한 간단한 조치
	safeKey := strings.ReplaceAll(key, "..", "")
	return filepath.Join(fs.baseDir, safeKey)
}

func (fs *FileStore) toMetaPath(dataPath string) string {
	return dataPath + ".meta.json"
}

func (fs *FileStore) readMetaFile(dataPath string) (*daramjwee.Metadata, error) {
	metaPath := fs.toMetaPath(dataPath)
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	var payload metaFilePayload
	if err := json.Unmarshal(metaBytes, &payload); err != nil {
		return nil, err
	}
	return &daramjwee.Metadata{ETag: payload.ETag}, nil
}

func (fs *FileStore) writeMetaFile(dataPath string, etag string) error {
	metaPath := fs.toMetaPath(dataPath)
	payload := metaFilePayload{ETag: etag}
	metaBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return os.WriteFile(metaPath, metaBytes, 0644)
}

// copyFile은 src 경로의 파일을 dst 경로로 복사합니다. dst 파일이 이미 존재하면 덮어씁니다.
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer func() {
		if closeErr := in.Close(); closeErr != nil {
			// 이미 다른 에러가 err 변수에 할당되어 있을 수 있으므로,
			// 여기서는 로깅만 하거나, 에러를 결합하는 방식을 고려할 수 있습니다.
			// 우선 로깅만 처리합니다.
			// level.Warn(fs.logger)와 같이 로거를 사용할 수 없으므로 fmt로 로깅하거나 에러를 반환값에 추가합니다.
			// 여기서는 기존 함수의 시그니처를 유지하고 fmt.Printf로 간단히 로깅합니다. (실제 프로덕션에서는 로거 주입을 고려)
			fmt.Fprintf(os.Stderr, "Error closing input file in copyFile: %v\n", closeErr)
			if err == nil { // err 가 nil 일때만 closeErr을 할당한다.
				err = closeErr
			}
		}
	}()

	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	return
}

// ... (lockedReadCloser, lockedWriteCloser는 변경 없음)
type lockedReadCloser struct {
	*os.File
	unlockFunc func()
}

func newLockedReadCloser(f *os.File, unlockFunc func()) io.ReadCloser {
	return &lockedReadCloser{File: f, unlockFunc: unlockFunc}
}
func (lrc *lockedReadCloser) Close() error {
	defer lrc.unlockFunc()
	return lrc.File.Close()
}

type lockedWriteCloser struct {
	*os.File
	onClose func() error
}

func newLockedWriteCloser(f *os.File, onClose func() error) io.WriteCloser {
	return &lockedWriteCloser{File: f, onClose: onClose}
}
func (lwc *lockedWriteCloser) Close() error {
	if err := lwc.File.Close(); err != nil {
		return err
	}
	return lwc.onClose()
}
