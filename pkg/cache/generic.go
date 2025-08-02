package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mrchypark/daramjwee"
)

// GenericCache provides type-safe operations on top of daramjwee.Cache.
// T is the type of values stored in the cache.
type GenericCache[T any] struct {
	cache daramjwee.Cache
}

// NewGeneric creates a new type-safe cache wrapper.
func NewGeneric[T any](cache daramjwee.Cache) *GenericCache[T] {
	return &GenericCache[T]{
		cache: cache,
	}
}

// GenericFetcher wraps a function that returns a value of type T.
// It's used when the cache needs to fetch data from the origin on cache miss.
type GenericFetcher[T any] func(ctx context.Context, oldMetadata *daramjwee.Metadata) (T, *daramjwee.Metadata, error)

// Fetch implements daramjwee.Fetcher interface.
func (gf GenericFetcher[T]) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	value, metadata, err := gf(ctx, oldMetadata)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %w", err)
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader(data)),
		Metadata: metadata,
	}, nil
}

// Get retrieves a value from the cache with type safety.
// If the key is not found, it uses the fetcher to get the value from origin.
func (gc *GenericCache[T]) Get(ctx context.Context, key string, fetcher GenericFetcher[T]) (T, error) {
	var zero T

	reader, err := gc.cache.Get(ctx, key, fetcher)
	if err != nil {
		return zero, err
	}
	defer reader.Close()

	var value T
	if err := json.NewDecoder(reader).Decode(&value); err != nil {
		return zero, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	return value, nil
}

// Set stores a value in the cache with type safety.
func (gc *GenericCache[T]) Set(ctx context.Context, key string, value T, metadata *daramjwee.Metadata) error {
	// Marshal to a buffer first to avoid writing partial data if encoding fails.
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	writer, err := gc.cache.Set(ctx, key, metadata)
	if err != nil {
		return err
	}

	_, writeErr := writer.Write(data)
	if writeErr != nil {
		// Attempt to close to release resources, but the primary error is the write error.
		_ = writer.Close()
		return fmt.Errorf("failed to write to cache: %w", writeErr)
	}

	// If write is successful, the close error is the one that matters for commit.
	return writer.Close()
}

// Delete removes a key from the cache.
func (gc *GenericCache[T]) Delete(ctx context.Context, key string) error {
	return gc.cache.Delete(ctx, key)
}

// ScheduleRefresh asynchronously refreshes a cache entry.
func (gc *GenericCache[T]) ScheduleRefresh(ctx context.Context, key string, fetcher GenericFetcher[T]) error {
	return gc.cache.ScheduleRefresh(ctx, key, fetcher)
}

// Close gracefully shuts down the underlying cache.
func (gc *GenericCache[T]) Close() {
	gc.cache.Close()
}

// GetOrSet retrieves a value from cache, or sets it using the provided factory function if not found.
// This is a convenience method that combines Get and Set operations.
func (gc *GenericCache[T]) GetOrSet(ctx context.Context, key string, factory func() (T, *daramjwee.Metadata, error)) (T, error) {
	fetcher := GenericFetcher[T](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (T, *daramjwee.Metadata, error) {
		return factory()
	})

	return gc.Get(ctx, key, fetcher)
}

// MustGet is like Get but panics on error. Use when you're confident the operation will succeed.
func (gc *GenericCache[T]) MustGet(ctx context.Context, key string, fetcher GenericFetcher[T]) T {
	value, err := gc.Get(ctx, key, fetcher)
	if err != nil {
panic(fmt.Errorf("MustGet failed: %w", err))
	}
	return value
}

// MustSet is like Set but panics on error. Use when you're confident the operation will succeed.
func (gc *GenericCache[T]) MustSet(ctx context.Context, key string, value T, metadata *daramjwee.Metadata) {
	if err := gc.Set(ctx, key, value, metadata); err != nil {
panic(fmt.Errorf("MustSet failed: %w", err))
	}
}

// GetWithDefault retrieves a value from cache, returning a default value if not found or on error.
// This is useful for graceful degradation when cache operations fail.
func (gc *GenericCache[T]) GetWithDefault(ctx context.Context, key string, defaultValue T, fetcher GenericFetcher[T]) T {
	value, err := gc.Get(ctx, key, fetcher)
	if err != nil {
		return defaultValue
	}
	return value
}
