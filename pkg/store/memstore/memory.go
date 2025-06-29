// Package memstore provides a simple, in-memory implementation of the daramjwee.Store interface.
// It is thread-safe and implements the stream-based interface contracts.
package memstore

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/mrchypark/daramjwee"
)

var (
	writerPool = sync.Pool{
		New: func() interface{} {
			return &memStoreWriter{
				buf: bytes.NewBuffer(make([]byte, 0, 1024)), // Pre-allocate buffer
			}
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

// entry holds the value and metadata for a single cache item.
type entry struct {
	value    []byte
	metadata *daramjwee.Metadata
}

// MemStore is a thread-safe, in-memory implementation of the daramjwee.Store interface.
type MemStore struct {
	mu          sync.RWMutex
	data        map[string]entry
	capacity    int64 // 변경: int -> int64, 바이트 단위 용량
	currentSize int64 // 추가: 현재 저장된 총 바이트 크기
	policy      daramjwee.EvictionPolicy
}

// New creates a new, empty in-memory store with a given capacity and eviction policy.
// If capacity is 0 or less, the store has no limit.
// If policy is nil, a no-op policy is used (no eviction).
func New(capacity int64, policy daramjwee.EvictionPolicy) *MemStore { // 변경: capacity 타입을 int64로
	if policy == nil {
		policy = daramjwee.NewNullEvictionPolicy()
	}
	return &MemStore{
		data:     make(map[string]entry),
		capacity: capacity,
		policy:   policy,
		// currentSize는 자동으로 0으로 초기화됩니다.
	}
}

// 컴파일 타임에 MemStore가 daramjwee.Store 인터페이스를 만족하는지 확인합니다.
var _ daramjwee.Store = (*MemStore)(nil)

// GetStream retrieves an object as a stream from the in-memory map.
func (ms *MemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	ms.mu.Lock() // RLock -> Lock. policy.Touch might modify internal state.
	defer ms.mu.Unlock()

	e, ok := ms.data[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}

	// Notify the policy that this key was accessed.
	ms.policy.Touch(key)

	reader := bytes.NewReader(e.value)
	readCloser := io.NopCloser(reader)

	return readCloser, e.metadata, nil
}

// SetWithWriter returns a writer that streams data into an in-memory buffer.
// When the writer is closed, the buffered data is committed to the main map.
func (ms *MemStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	w := writerPool.Get().(*memStoreWriter)
	w.ms = ms
	w.key = key
	w.metadata = metadata
	return w, nil
}

// Delete removes an object from the in-memory map.
func (ms *MemStore) Delete(ctx context.Context, key string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if e, ok := ms.data[key]; ok {
		// 변경: 삭제 전에 크기를 구해서 currentSize를 업데이트합니다.
		size := int64(len(e.value))
		ms.currentSize -= size

		delete(ms.data, key)
		// Notify the policy that this key was removed.
		ms.policy.Remove(key)
	}

	return nil
}

// Stat retrieves metadata for an object from the in-memory map.
func (ms *MemStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	ms.mu.Lock() // RLock -> Lock for policy.Touch
	defer ms.mu.Unlock()

	e, ok := ms.data[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}

	// Access via Stat should also be considered a "touch".
	ms.policy.Touch(key)

	return e.metadata, nil
}

// --- SetWithWriter를 위한 헬퍼 구조체 ---

// memStoreWriter는 io.WriteCloser 인터페이스를 만족하는 헬퍼 타입입니다.
type memStoreWriter struct {
	ms       *MemStore
	key      string
	metadata *daramjwee.Metadata
	buf      *bytes.Buffer
}

// Write는 받은 데이터를 내부 버퍼에 씁니다.
func (w *memStoreWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

// Close는 쓰기 작업이 완료되었을 때 호출됩니다.
func (w *memStoreWriter) Close() error {
	w.ms.mu.Lock()
	defer w.ms.mu.Unlock()

	finalData := w.buf.Bytes()
	newItemSize := int64(len(finalData))

	// 변경: 기존에 아이템이 있었다면, 기존 크기만큼 currentSize에서 뺍니다.
	if oldEntry, ok := w.ms.data[w.key]; ok {
		w.ms.currentSize -= int64(len(oldEntry.value))
	}

	newEntry := entry{
		value:    finalData,
		metadata: w.metadata,
	}

	w.ms.data[w.key] = newEntry
	// 변경: 새로 추가된 아이템의 크기만큼 currentSize를 더합니다.
	w.ms.currentSize += newItemSize
	w.ms.policy.Add(w.key, newItemSize)

	// 변경: 용량 기반 축출 로직
	// capacity가 0보다 크고, 현재 크기가 용량을 초과하면 반복적으로 축출을 수행합니다.
	if w.ms.capacity > 0 {
		for w.ms.currentSize > w.ms.capacity {
			keysToEvict := w.ms.policy.Evict()
			if len(keysToEvict) == 0 {
				// 더 이상 축출할 후보가 없으면 중단
				break
			}

			var actuallyEvicted bool
			for _, keyToEvict := range keysToEvict {
				if entryToEvict, ok := w.ms.data[keyToEvict]; ok {
					w.ms.currentSize -= int64(len(entryToEvict.value))
					delete(w.ms.data, keyToEvict)
					actuallyEvicted = true
				}
			}

			if !actuallyEvicted {
				// 정책이 유효하지 않은 키만 계속 반환하는 경우,
				// 무한 루프를 방지하기 위해 중단합니다.
				break
			}
		}
	}

	// Reset the writer and put it back in the pool
	w.buf.Reset()
	w.ms = nil
	w.key = ""
	w.metadata = nil
	writerPool.Put(w)

	return nil
}
