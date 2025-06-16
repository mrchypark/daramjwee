// Package memstore provides a simple, in-memory implementation of the daramjwee.Store interface.
// It is thread-safe and implements the stream-based interface contracts.
package memstore

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/mrchypark/daramjwee" // 자신의 모듈 경로로 수정
)

// entry holds the value and etag for a single cache item.
type entry struct {
	value []byte
	etag  string
}

// MemStore is a thread-safe, in-memory implementation of the daramjwee.Store interface.
type MemStore struct {
	mu   sync.RWMutex
	data map[string]entry
}

// New creates a new, empty in-memory store.
func New() *MemStore {
	return &MemStore{
		data: make(map[string]entry),
	}
}

// 컴파일 타임에 MemStore가 daramjwee.Store 인터페이스를 만족하는지 확인합니다.
var _ daramjwee.Store = (*MemStore)(nil)

// GetStream retrieves an object as a stream from the in-memory map.
func (ms *MemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	e, ok := ms.data[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}

	// 1. 메모리에 있는 byte 슬라이스를 io.Reader로 변환합니다.
	reader := bytes.NewReader(e.value)
	// 2. io.Reader를 io.ReadCloser 인터페이스로 맞추기 위해 닫아도 아무 일도 하지 않는 Closer로 감쌉니다.
	readCloser := io.NopCloser(reader)
	meta := &daramjwee.Metadata{ETag: e.etag}

	return readCloser, meta, nil
}

// SetWithWriter returns a writer that streams data into an in-memory buffer.
// When the writer is closed, the buffered data is committed to the main map.
func (ms *MemStore) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	return &memStoreWriter{
		ms:   ms,
		key:  key,
		etag: etag,
		buf:  &bytes.Buffer{}, // 데이터를 임시로 담아둘 버퍼
	}, nil
}

// Delete removes an object from the in-memory map.
func (ms *MemStore) Delete(ctx context.Context, key string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	delete(ms.data, key)
	return nil
}

// Stat retrieves metadata for an object from the in-memory map.
func (ms *MemStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	e, ok := ms.data[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}

	meta := &daramjwee.Metadata{ETag: e.etag}
	return meta, nil
}

// --- SetWithWriter를 위한 헬퍼 구조체 ---

// memStoreWriter는 io.WriteCloser 인터페이스를 만족하는 헬퍼 타입입니다.
type memStoreWriter struct {
	ms   *MemStore
	key  string
	etag string
	buf  *bytes.Buffer
}

// Write는 받은 데이터를 내부 버퍼에 씁니다.
func (w *memStoreWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

// Close는 쓰기 작업이 완료되었을 때 호출됩니다.
// 이때 버퍼에 담긴 최종 데이터를 MemStore의 맵에 저장합니다.
func (w *memStoreWriter) Close() error {
	w.ms.mu.Lock()
	defer w.ms.mu.Unlock()

	// 버퍼의 데이터를 byte 슬라이스로 가져와서 entry를 만듭니다.
	finalData := w.buf.Bytes()
	newEntry := entry{
		value: finalData,
		etag:  w.etag,
	}

	// 맵에 최종 저장합니다.
	w.ms.data[w.key] = newEntry

	return nil
}
