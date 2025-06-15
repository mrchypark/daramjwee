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
	"github.com/mrchypark/daramjwee"
)

// FileStore is a disk-based implementation of the daramjwee.Store.
type FileStore struct {
	baseDir     string
	logger      log.Logger
	lockManager *FileLockManager
}

// New creates a new FileStore.
func New(dir string, logger log.Logger) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory %s: %w", dir, err)
	}
	return &FileStore{
		baseDir:     dir,
		logger:      logger,
		lockManager: NewFileLockManager(2048),
	}, nil
}

// 컴파일 타임에 인터페이스 만족 확인
var _ daramjwee.Store = (*FileStore)(nil)
var _ daramjwee.ContextAwareStore = (*FileStore)(nil)

// --- Context-Aware Methods (ContextAwareStore implementation) ---

func (fs *FileStore) GetStreamContext(ctx context.Context, key string) (io.ReadCloser, daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.lockManager.RLock(path)

	meta, err := fs.readMetaFile(path)
	if err != nil {
		fs.lockManager.RUnlock(path)
		return nil, daramjwee.Metadata{}, err
	}

	file, err := os.Open(path)
	if err != nil {
		fs.lockManager.RUnlock(path)
		if os.IsNotExist(err) {
			return nil, daramjwee.Metadata{}, daramjwee.ErrNotFound
		}
		return nil, daramjwee.Metadata{}, err
	}

	return newLockedReadCloser(file, func() { fs.lockManager.RUnlock(path) }), meta, nil
}

func (fs *FileStore) SetWithWriterContext(ctx context.Context, key string, meta daramjwee.Metadata) (io.WriteCloser, error) {
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
		// 1. 메타데이터 파일 쓰기
		if err := fs.writeMetaFile(path, meta); err != nil {
			os.Remove(tmpFile.Name()) // 임시 데이터 파일 정리
			return err
		}
		// 2. 임시 데이터 파일을 최종 경로로 변경 (원자적)
		if err := os.Rename(tmpFile.Name(), path); err != nil {
			// 실패 시 롤백: 방금 쓴 메타 파일 정리
			os.Remove(fs.toMetaPath(path))
			return err
		}
		return nil
	}

	return newLockedWriteCloser(tmpFile, onClose), nil
}

func (fs *FileStore) DeleteContext(ctx context.Context, key string) error {
	path := fs.toDataPath(key)
	fs.lockManager.Lock(path)
	defer fs.lockManager.Unlock(path)

	errData := os.Remove(path)
	errMeta := os.Remove(fs.toMetaPath(path))

	if os.IsNotExist(errData) {
		return nil
	} // 이미 없으면 성공으로 처리
	if errData != nil {
		return errData
	}
	if errMeta != nil && !os.IsNotExist(errMeta) {
		return errMeta
	}

	return nil
}

func (fs *FileStore) StatContext(ctx context.Context, key string) (daramjwee.Metadata, error) {
	path := fs.toDataPath(key)
	fs.lockManager.RLock(path)
	defer fs.lockManager.RUnlock(path)
	return fs.readMetaFile(path)
}

// --- Base Interface Methods (for Store interface) ---

func (fs *FileStore) GetStream(key string) (io.ReadCloser, daramjwee.Metadata, error) {
	return fs.GetStreamContext(context.Background(), key)
}
func (fs *FileStore) SetWithWriter(key string, meta daramjwee.Metadata) (io.WriteCloser, error) {
	return fs.SetWithWriterContext(context.Background(), key, meta)
}
func (fs *FileStore) Delete(key string) error {
	return fs.DeleteContext(context.Background(), key)
}
func (fs *FileStore) Stat(key string) (daramjwee.Metadata, error) {
	return fs.StatContext(context.Background(), key)
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

func (fs *FileStore) readMetaFile(dataPath string) (daramjwee.Metadata, error) {
	metaPath := fs.toMetaPath(dataPath)
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return daramjwee.Metadata{}, daramjwee.ErrNotFound
		}
		return daramjwee.Metadata{}, err
	}
	var meta daramjwee.Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return daramjwee.Metadata{}, err
	}
	return meta, nil
}

func (fs *FileStore) writeMetaFile(dataPath string, meta daramjwee.Metadata) error {
	metaPath := fs.toMetaPath(dataPath)
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(metaPath, metaBytes, 0644)
}

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
		// onClose 콜백(rename 등)을 실행하기 전에 파일 닫기 실패 시, 여기서 반환
		return err
	}
	return lwc.onClose()
}
