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

// entry holds the value and etag for a single cache item.
type entry struct {
	value []byte
	etag  string
}

// MemStore is a thread-safe, in-memory implementation of the daramjwee.Store interface.
type MemStore struct {
	mu       sync.RWMutex
	data     map[string]entry
	capacity int
	policy   daramjwee.EvictionPolicy
}

// New creates a new, empty in-memory store with a given capacity and eviction policy.
// If capacity is 0 or less, the store has no limit.
// If policy is nil, a no-op policy is used (no eviction).
func New(capacity int, policy daramjwee.EvictionPolicy) *MemStore {
	if policy == nil {
		policy = daramjwee.NewNullEvictionPolicy()
	}
	return &MemStore{
		data:     make(map[string]entry),
		capacity: capacity,
		policy:   policy,
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
		buf:  &bytes.Buffer{},
	}, nil
}

// Delete removes an object from the in-memory map.
func (ms *MemStore) Delete(ctx context.Context, key string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, ok := ms.data[key]; ok {
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
func (w *memStoreWriter) Close() error {
	w.ms.mu.Lock()
	defer w.ms.mu.Unlock()

	finalData := w.buf.Bytes()
	newEntry := entry{
		value: finalData,
		etag:  w.etag,
	}

	w.ms.data[w.key] = newEntry
	// Notify the policy that this key was added, providing its size.
	w.ms.policy.Add(w.key, int64(len(finalData)))

	// Check if eviction is needed.
	if w.ms.capacity > 0 && len(w.ms.data) > w.ms.capacity {
		keysToEvict := w.ms.policy.Evict()
		for _, keyToEvict := range keysToEvict {
			// Eviction is different from explicit deletion,
			// so we don't call policy.Remove here. The policy itself handles its state.
			delete(w.ms.data, keyToEvict)
		}
	}

	return nil
}
