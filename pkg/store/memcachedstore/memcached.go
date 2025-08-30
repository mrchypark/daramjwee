package memcachedstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
)

const (
	// DefaultMaxItemSize is the default maximum item size for Memcached (1MB).
	DefaultMaxItemSize = 1 << 20
)

var (
	// ErrItemTooLarge is returned when an item exceeds the maximum size allowed by Memcached.
	ErrItemTooLarge = errors.New("memcached: item too large")
)

// memcachedEntry is the structure stored in Memcached. It holds both the
// metadata and the actual data payload.
type memcachedEntry struct {
	Metadata *daramjwee.Metadata `json:"metadata"`
	Data     []byte              `json:"data"`
}

// MemcachedStore is a Memcached-based implementation of the daramjwee.Store.
type MemcachedStore struct {
	client      *memcache.Client
	logger      log.Logger
	maxItemSize int
}

// New creates a new MemcachedStore.
func New(client *memcache.Client, logger log.Logger) daramjwee.Store {
	return &MemcachedStore{
		client:      client,
		logger:      logger,
		maxItemSize: DefaultMaxItemSize,
	}
}

// GetStream retrieves an object and its metadata as a stream from Memcached.
func (ms *MemcachedStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	item, err := ms.client.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	var entry memcachedEntry
	if err := json.Unmarshal(item.Value, &entry); err != nil {
		return nil, nil, err
	}

	return io.NopCloser(bytes.NewReader(entry.Data)), entry.Metadata, nil
}

// SetWithWriter returns a writer that streams data into Memcached.
// NOTE: This implementation buffers the entire object in memory before writing to Memcached.
// This can lead to high memory consumption for large objects.
func (ms *MemcachedStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	w := &memcachedStoreWriter{
		ms:       ms,
		key:      key,
		metadata: metadata,
		buf:      new(bytes.Buffer),
		ctx:      ctx,
	}
	return w, nil
}

// Delete removes an object from Memcached.
func (ms *MemcachedStore) Delete(ctx context.Context, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	err := ms.client.Delete(key)
	if err == memcache.ErrCacheMiss {
		return nil // Deleting a non-existent key is not an error.
	}
	return err
}

// Stat retrieves metadata for an object without its data from Memcached.
// NOTE: This implementation is inefficient as it retrieves the entire entry (data + metadata)
// from Memcached just to return the metadata part. This is a limitation of the
// chosen storage strategy of bundling metadata and data in a single key.
func (ms *MemcachedStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	item, err := ms.client.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}

	var entry memcachedEntry
	if err := json.Unmarshal(item.Value, &entry); err != nil {
		return nil, err
	}

	return entry.Metadata, nil
}

// memcachedStoreWriter is a helper type that satisfies the io.WriteCloser interface.
type memcachedStoreWriter struct {
	ms       *MemcachedStore
	key      string
	metadata *daramjwee.Metadata
	buf      *bytes.Buffer
	ctx      context.Context
}

// Write writes the provided data to the internal buffer.
func (w *memcachedStoreWriter) Write(p []byte) (n int, err error) {
	if w.buf.Len()+len(p) > w.ms.maxItemSize {
		return 0, ErrItemTooLarge
	}
	return w.buf.Write(p)
}

// Close commits the buffered data and metadata to Memcached.
func (w *memcachedStoreWriter) Close() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}

	entry := memcachedEntry{
		Metadata: w.metadata,
		Data:     w.buf.Bytes(),
	}

	entryBytes, err := json.Marshal(entry)
	if err != nil {
		level.Error(w.ms.logger).Log("msg", "failed to marshal entry", "key", w.key, "err", err)
		return err
	}

	if len(entryBytes) > w.ms.maxItemSize {
		return ErrItemTooLarge
	}

	err = w.ms.client.Set(&memcache.Item{Key: w.key, Value: entryBytes})
	if err != nil {
		level.Error(w.ms.logger).Log("msg", "failed to set entry in memcached", "key", w.key, "err", err)
	}

	return err
}