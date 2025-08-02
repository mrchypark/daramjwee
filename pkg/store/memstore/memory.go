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
		New: func() any {
			return &memStoreWriter{
				buf: bytes.NewBuffer(make([]byte, 0, 1024)), // Pre-allocate buffer
			}
		},
	}
	bufferPool = sync.Pool{
		New: func() any {
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
	capacity    int64 // Capacity in bytes
	currentSize int64 // Current total size of stored items in bytes
	policy      daramjwee.EvictionPolicy
}

// New creates a new, empty in-memory store with a given capacity and eviction policy.
// If capacity is 0 or less, the store has no limit.
// If policy is nil, a no-op policy is used (no eviction).
func New(capacity int64, policy daramjwee.EvictionPolicy) *MemStore {
	if policy == nil {
		policy = daramjwee.NewNullEvictionPolicy()
	}
	return &MemStore{
		data:     make(map[string]entry),
		capacity: capacity,
		policy:   policy,
	}
}

// GetStream retrieves an object as a stream from the in-memory map.
func (ms *MemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	ms.mu.Lock() // Use Lock because policy.Touch might modify internal state.
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
	ms.mu.Lock() // Use Lock for policy.Touch
	defer ms.mu.Unlock()

	e, ok := ms.data[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}

	// Access via Stat should also be considered a "touch".
	ms.policy.Touch(key)

	return e.metadata, nil
}

// memStoreWriter is a helper type that satisfies the io.WriteCloser interface.
type memStoreWriter struct {
	ms       *MemStore
	key      string
	metadata *daramjwee.Metadata
	buf      *bytes.Buffer
}

// Write writes the provided data to the internal buffer.
func (w *memStoreWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

// Close is called when the write operation is complete.
// It commits the buffered data to the MemStore and handles eviction if capacity is exceeded.
func (w *memStoreWriter) Close() error {
	w.ms.mu.Lock()
	defer w.ms.mu.Unlock()

	finalData := make([]byte, w.buf.Len())
	copy(finalData, w.buf.Bytes())

	newItemSize := int64(len(finalData))

	// If the item already exists, subtract its old size from currentSize.
	if oldEntry, ok := w.ms.data[w.key]; ok {
		w.ms.currentSize -= int64(len(oldEntry.value))
	}

	newEntry := entry{
		value:    finalData,
		metadata: w.metadata,
	}

	w.ms.data[w.key] = newEntry
	// Add the new item's size to currentSize.
	w.ms.currentSize += newItemSize
	w.ms.policy.Add(w.key, newItemSize)

	// Eviction logic: if capacity is positive and exceeded, evict items.
	if w.ms.capacity > 0 {
		for w.ms.currentSize > w.ms.capacity {
			keysToEvict := w.ms.policy.Evict()
			if len(keysToEvict) == 0 {
				// No more candidates for eviction, break to prevent infinite loop.
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
				// If the policy keeps returning non-existent keys, break to prevent infinite loop.
				break
			}
		}
	}

	// Reset the writer and return it to the pool.
	w.buf.Reset()
	w.ms = nil
	w.key = ""
	w.metadata = nil
	writerPool.Put(w)

	return nil
}
