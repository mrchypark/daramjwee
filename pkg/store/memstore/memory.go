package memstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/mrchypark/daramjwee"
)

var (
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

	return readCloser, cloneMetadata(e.metadata), nil
}

// BeginSet returns a writer that streams data into an in-memory buffer.
// When the writer is closed, the buffered data is committed to the main map.
func (ms *MemStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: begin set: %w", err)
	}
	return ms.beginSet(key, metadata), nil
}

func (ms *MemStore) BeginStagedSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.StagedWriteSink, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: begin staged set: %w", err)
	}
	return ms.beginSet(key, metadata), nil
}

func (ms *MemStore) beginSet(key string, metadata *daramjwee.Metadata) *memStoreSink {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return &memStoreSink{
		ms:       ms,
		key:      key,
		metadata: metadata,
		buf:      buf,
	}
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

	return cloneMetadata(e.metadata), nil
}

// memStoreSink is a helper type that satisfies the daramjwee.WriteSink interface.
type memStoreSink struct {
	mu       sync.Mutex
	ms       *MemStore
	key      string
	metadata *daramjwee.Metadata
	buf      *bytes.Buffer
	done     bool
}

// Write writes the provided data to the internal buffer.
func (w *memStoreSink) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return 0, io.ErrClosedPipe
	}
	return w.buf.Write(p)
}

// Close is called when the write operation is complete.
// It commits the buffered data to the MemStore and handles eviction if capacity is exceeded.
func (w *memStoreSink) Close() error {
	return w.Commit(context.Background())
}

func (w *memStoreSink) Commit(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return nil
	}
	if err := ctx.Err(); err != nil {
		w.done = true
		w.release()
		return fmt.Errorf("memstore: commit: %w", err)
	}

	finalData := bytes.Clone(w.buf.Bytes())
	newItemSize := int64(len(finalData))
	newEntry := entry{
		value:    finalData,
		metadata: cloneMetadata(w.metadata),
	}

	ms := w.ms
	ms.mu.Lock()
	defer w.release()
	defer ms.mu.Unlock()
	if err := ctx.Err(); err != nil {
		w.done = true
		return fmt.Errorf("memstore: commit: %w", err)
	}
	w.done = true

	// If the item already exists, subtract its old size from currentSize.
	if oldEntry, ok := ms.data[w.key]; ok {
		ms.currentSize -= int64(len(oldEntry.value))
	}

	ms.data[w.key] = newEntry
	// Add the new item's size to currentSize.
	ms.currentSize += newItemSize
	ms.policy.Add(w.key, newItemSize)

	// Eviction logic: if capacity is positive and exceeded, evict items.
	if ms.capacity > 0 {
		for ms.currentSize > ms.capacity {
			keysToEvict := ms.policy.Evict()
			if len(keysToEvict) == 0 {
				// No more candidates for eviction, break to prevent infinite loop.
				break
			}

			var actuallyEvicted bool
			for _, keyToEvict := range keysToEvict {
				if entryToEvict, ok := ms.data[keyToEvict]; ok {
					ms.currentSize -= int64(len(entryToEvict.value))
					delete(ms.data, keyToEvict)
					ms.policy.Remove(keyToEvict)
					actuallyEvicted = true
				}
			}

			if !actuallyEvicted {
				// If the policy keeps returning non-existent keys, break to prevent infinite loop.
				break
			}
		}
	}

	return nil
}

func (w *memStoreSink) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return nil
	}
	w.done = true
	w.buf.Reset()
	w.release()
	return nil
}

func (w *memStoreSink) release() {
	if w.buf != nil {
		w.buf.Reset()
		bufferPool.Put(w.buf)
		w.buf = nil
	}
	w.ms = nil
	w.key = ""
	w.metadata = nil
}

func cloneMetadata(meta *daramjwee.Metadata) *daramjwee.Metadata {
	if meta == nil {
		return &daramjwee.Metadata{}
	}
	cloned := *meta
	return &cloned
}
