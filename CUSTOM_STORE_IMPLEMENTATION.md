# Custom Store Implementation Guidelines

## Overview

This document provides comprehensive guidelines for implementing custom Store backends for the daramjwee caching library. A Store provides the storage layer for cached objects and their metadata, supporting various storage mediums like memory, disk, cloud storage, databases, or any other persistent storage system.

## Store Interface Contract

The Store interface defines four core methods that must be implemented:

```go
type Store interface {
    GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
    SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error)
    Delete(ctx context.Context, key string) error
    Stat(ctx context.Context, key string) (*Metadata, error)
}
```

### Method Implementation Requirements

#### GetStream Method

**Purpose**: Retrieve an object and its metadata as a stream.

**Implementation Requirements**:
- Return `daramjwee.ErrNotFound` if the object doesn't exist
- Provide streaming access to avoid loading large objects into memory
- Return accurate metadata that matches the stored object
- Handle context cancellation appropriately
- Ensure the returned stream is properly closeable

**Example Implementation Pattern**:
```go
func (s *CustomStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
    // Check context cancellation
    select {
    case <-ctx.Done():
        return nil, nil, ctx.Err()
    default:
    }
    
    // Check if object exists
    if !s.exists(key) {
        return nil, nil, daramjwee.ErrNotFound
    }
    
    // Open stream to object data
    stream, err := s.openStream(key)
    if err != nil {
        return nil, nil, fmt.Errorf("failed to open stream: %w", err)
    }
    
    // Retrieve metadata
    metadata, err := s.getMetadata(key)
    if err != nil {
        stream.Close()
        return nil, nil, fmt.Errorf("failed to get metadata: %w", err)
    }
    
    return stream, metadata, nil
}
```

#### SetWithWriter Method

**Purpose**: Return a writer for streaming data into the store.

**Implementation Requirements**:
- Provide atomic write operations (object visible only after writer.Close())
- Handle metadata storage alongside object data
- Support context cancellation during write operations
- Implement proper cleanup on write failures
- Ensure thread-safe concurrent access

**Example Implementation Pattern**:
```go
func (s *CustomStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
    // Check context cancellation
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    
    // Create temporary storage for atomic writes
    tempKey := s.generateTempKey(key)
    
    // Create writer with cleanup logic
    writer := &customStoreWriter{
        store:    s,
        key:      key,
        tempKey:  tempKey,
        metadata: metadata,
        ctx:      ctx,
    }
    
    return writer, nil
}

type customStoreWriter struct {
    store    *CustomStore
    key      string
    tempKey  string
    metadata *daramjwee.Metadata
    ctx      context.Context
    buffer   *bytes.Buffer
}

func (w *customStoreWriter) Write(p []byte) (n int, err error) {
    // Check context cancellation
    select {
    case <-w.ctx.Done():
        return 0, w.ctx.Err()
    default:
    }
    
    // Write to temporary storage
    return w.buffer.Write(p)
}

func (w *customStoreWriter) Close() error {
    // Atomic commit: move from temporary to final location
    if err := w.store.atomicCommit(w.tempKey, w.key, w.metadata); err != nil {
        // Cleanup on failure
        w.store.cleanup(w.tempKey)
        return fmt.Errorf("failed to commit write: %w", err)
    }
    
    return nil
}
```

#### Delete Method

**Purpose**: Remove an object from the store.

**Implementation Requirements**:
- Remove both object data and metadata
- Be idempotent (no error for non-existent objects)
- Handle context cancellation
- Maintain consistency across concurrent operations

**Example Implementation Pattern**:
```go
func (s *CustomStore) Delete(ctx context.Context, key string) error {
    // Check context cancellation
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
    }
    
    // Attempt to delete object and metadata
    if err := s.deleteObject(key); err != nil && !s.isNotFoundError(err) {
        return fmt.Errorf("failed to delete object: %w", err)
    }
    
    if err := s.deleteMetadata(key); err != nil && !s.isNotFoundError(err) {
        return fmt.Errorf("failed to delete metadata: %w", err)
    }
    
    return nil
}
```

#### Stat Method

**Purpose**: Retrieve metadata without loading object data.

**Implementation Requirements**:
- Return `daramjwee.ErrNotFound` if object doesn't exist
- Provide fast metadata-only access
- Return consistent metadata with GetStream
- Handle context cancellation

**Example Implementation Pattern**:
```go
func (s *CustomStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
    // Check context cancellation
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    
    // Check if object exists
    if !s.exists(key) {
        return nil, daramjwee.ErrNotFound
    }
    
    // Retrieve metadata only
    metadata, err := s.getMetadata(key)
    if err != nil {
        return nil, fmt.Errorf("failed to get metadata: %w", err)
    }
    
    return metadata, nil
}
```

## Thread Safety Requirements

### Concurrent Access Handling

All Store implementations must be safe for concurrent use across multiple goroutines. This includes:

1. **Read-Write Concurrency**: Multiple readers and writers accessing different keys simultaneously
2. **Same-Key Concurrency**: Multiple operations on the same key (handled by cache-level locking)
3. **Internal State Protection**: Any internal state must be protected with appropriate synchronization

### Locking Strategies

**Fine-Grained Locking**:
```go
type CustomStore struct {
    mu    sync.RWMutex
    data  map[string]*entry
    locks map[string]*sync.RWMutex // Per-key locks
}

func (s *CustomStore) getKeyLock(key string) *sync.RWMutex {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    if lock, exists := s.locks[key]; exists {
        return lock
    }
    
    lock := &sync.RWMutex{}
    s.locks[key] = lock
    return lock
}
```

**Striped Locking** (for high concurrency):
```go
type CustomStore struct {
    stripes []sync.RWMutex
    data    map[string]*entry
}

func (s *CustomStore) getLock(key string) *sync.RWMutex {
    hash := fnv.New32a()
    hash.Write([]byte(key))
    index := hash.Sum32() % uint32(len(s.stripes))
    return &s.stripes[index]
}
```

## Error Handling Patterns

### Standard Error Conditions

1. **Not Found**: Return `daramjwee.ErrNotFound` for missing objects
2. **Context Cancellation**: Respect context cancellation and timeouts
3. **Storage Errors**: Wrap storage-specific errors with descriptive messages
4. **Validation Errors**: Return clear errors for invalid inputs

### Error Handling Examples

```go
// Proper error wrapping
func (s *CustomStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
    stream, metadata, err := s.backend.Get(key)
    if err != nil {
        if s.isNotFoundError(err) {
            return nil, nil, daramjwee.ErrNotFound
        }
        return nil, nil, fmt.Errorf("backend get failed for key %s: %w", key, err)
    }
    return stream, metadata, nil
}

// Context handling
func (s *CustomStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    
    // Implementation continues...
}
```

## Performance Optimization Strategies

### Memory Management

1. **Buffer Pooling**: Reuse buffers to reduce GC pressure
2. **Streaming Operations**: Avoid loading large objects into memory
3. **Lazy Loading**: Load metadata and data only when needed

```go
var bufferPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 32*1024) // 32KB buffers
    },
}

func (s *CustomStore) copyData(dst io.Writer, src io.Reader) (int64, error) {
    buf := bufferPool.Get().([]byte)
    defer bufferPool.Put(buf)
    
    return io.CopyBuffer(dst, src, buf)
}
```

### I/O Optimization

1. **Batch Operations**: Group multiple operations when possible
2. **Async Operations**: Use goroutines for non-blocking operations
3. **Connection Pooling**: Reuse connections for network-based stores

### Caching and Indexing

1. **Metadata Caching**: Cache frequently accessed metadata
2. **Existence Checks**: Optimize existence checking for Stat operations
3. **Index Structures**: Use appropriate data structures for key lookups

## Resource Management

### Cleanup and Lifecycle

```go
type CustomStore struct {
    // ... fields ...
    closed chan struct{}
    wg     sync.WaitGroup
}

func (s *CustomStore) Close() error {
    close(s.closed)
    s.wg.Wait()
    
    // Cleanup resources
    return s.cleanup()
}

func (s *CustomStore) backgroundTask() {
    defer s.wg.Done()
    
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-s.closed:
            return
        case <-ticker.C:
            s.performMaintenance()
        }
    }
}
```

### Connection Management

```go
type NetworkStore struct {
    pool *connectionPool
}

func (s *NetworkStore) getConnection() (*connection, error) {
    return s.pool.Get()
}

func (s *NetworkStore) releaseConnection(conn *connection) {
    s.pool.Put(conn)
}
```

## Complete Custom Store Example

Here's a complete example of a Redis-based store implementation:

```go
package customstore

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "strings"
    "time"
    
    "github.com/go-redis/redis/v8"
    "github.com/mrchypark/daramjwee"
)

// RedisStore implements Store interface using Redis as backend
type RedisStore struct {
    client *redis.Client
    prefix string
    ttl    time.Duration
}

// NewRedisStore creates a new Redis-based store
func NewRedisStore(client *redis.Client, prefix string, ttl time.Duration) *RedisStore {
    return &RedisStore{
        client: client,
        prefix: prefix,
        ttl:    ttl,
    }
}

// GetStream retrieves an object as a stream from Redis
func (r *RedisStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
    // Get both data and metadata in a pipeline
    pipe := r.client.Pipeline()
    dataCmd := pipe.Get(ctx, r.dataKey(key))
    metaCmd := pipe.Get(ctx, r.metaKey(key))
    
    _, err := pipe.Exec(ctx)
    if err != nil {
        if err == redis.Nil {
            return nil, nil, daramjwee.ErrNotFound
        }
        return nil, nil, fmt.Errorf("redis pipeline failed: %w", err)
    }
    
    // Get data
    data, err := dataCmd.Result()
    if err != nil {
        if err == redis.Nil {
            return nil, nil, daramjwee.ErrNotFound
        }
        return nil, nil, fmt.Errorf("failed to get data: %w", err)
    }
    
    // Get metadata
    metaStr, err := metaCmd.Result()
    if err != nil {
        return nil, nil, fmt.Errorf("failed to get metadata: %w", err)
    }
    
    var metadata daramjwee.Metadata
    if err := json.Unmarshal([]byte(metaStr), &metadata); err != nil {
        return nil, nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
    }
    
    // Return data as stream
    reader := strings.NewReader(data)
    return io.NopCloser(reader), &metadata, nil
}

// SetWithWriter returns a writer for streaming data to Redis
func (r *RedisStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
    return &redisWriter{
        store:    r,
        key:      key,
        metadata: metadata,
        ctx:      ctx,
        buffer:   &strings.Builder{},
    }, nil
}

// Delete removes an object from Redis
func (r *RedisStore) Delete(ctx context.Context, key string) error {
    pipe := r.client.Pipeline()
    pipe.Del(ctx, r.dataKey(key))
    pipe.Del(ctx, r.metaKey(key))
    
    _, err := pipe.Exec(ctx)
    if err != nil && err != redis.Nil {
        return fmt.Errorf("redis delete failed: %w", err)
    }
    
    return nil
}

// Stat retrieves metadata without loading data
func (r *RedisStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
    metaStr, err := r.client.Get(ctx, r.metaKey(key)).Result()
    if err != nil {
        if err == redis.Nil {
            return nil, daramjwee.ErrNotFound
        }
        return nil, fmt.Errorf("failed to get metadata: %w", err)
    }
    
    var metadata daramjwee.Metadata
    if err := json.Unmarshal([]byte(metaStr), &metadata); err != nil {
        return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
    }
    
    return &metadata, nil
}

// Helper methods
func (r *RedisStore) dataKey(key string) string {
    return fmt.Sprintf("%s:data:%s", r.prefix, key)
}

func (r *RedisStore) metaKey(key string) string {
    return fmt.Sprintf("%s:meta:%s", r.prefix, key)
}

// redisWriter implements io.WriteCloser for Redis storage
type redisWriter struct {
    store    *RedisStore
    key      string
    metadata *daramjwee.Metadata
    ctx      context.Context
    buffer   *strings.Builder
}

func (w *redisWriter) Write(p []byte) (n int, err error) {
    return w.buffer.Write(p)
}

func (w *redisWriter) Close() error {
    // Serialize metadata
    metaBytes, err := json.Marshal(w.metadata)
    if err != nil {
        return fmt.Errorf("failed to marshal metadata: %w", err)
    }
    
    // Store both data and metadata atomically
    pipe := w.store.client.Pipeline()
    pipe.Set(w.ctx, w.store.dataKey(w.key), w.buffer.String(), w.store.ttl)
    pipe.Set(w.ctx, w.store.metaKey(w.key), string(metaBytes), w.store.ttl)
    
    _, err = pipe.Exec(w.ctx)
    if err != nil {
        return fmt.Errorf("failed to store data: %w", err)
    }
    
    return nil
}
```

## Testing Strategies

### Unit Testing

```go
func TestCustomStore_GetStream(t *testing.T) {
    store := NewCustomStore()
    ctx := context.Background()
    
    // Test not found
    _, _, err := store.GetStream(ctx, "nonexistent")
    assert.Equal(t, daramjwee.ErrNotFound, err)
    
    // Test successful retrieval
    metadata := &daramjwee.Metadata{
        ETag:     "test-etag",
        CachedAt: time.Now(),
    }
    
    writer, err := store.SetWithWriter(ctx, "test-key", metadata)
    require.NoError(t, err)
    
    _, err = writer.Write([]byte("test data"))
    require.NoError(t, err)
    
    err = writer.Close()
    require.NoError(t, err)
    
    stream, meta, err := store.GetStream(ctx, "test-key")
    require.NoError(t, err)
    defer stream.Close()
    
    assert.Equal(t, metadata.ETag, meta.ETag)
    
    data, err := io.ReadAll(stream)
    require.NoError(t, err)
    assert.Equal(t, "test data", string(data))
}
```

### Integration Testing

```go
func TestCustomStore_Integration(t *testing.T) {
    // Test with actual daramjwee cache
    store := NewCustomStore()
    cache, err := daramjwee.New(
        logger,
        daramjwee.WithHotStore(store),
    )
    require.NoError(t, err)
    defer cache.Close()
    
    // Test cache operations
    fetcher := &testFetcher{data: "test data"}
    
    stream, err := cache.Get(context.Background(), "test-key", fetcher)
    require.NoError(t, err)
    defer stream.Close()
    
    data, err := io.ReadAll(stream)
    require.NoError(t, err)
    assert.Equal(t, "test data", string(data))
}
```

### Performance Testing

```go
func BenchmarkCustomStore_GetStream(b *testing.B) {
    store := NewCustomStore()
    ctx := context.Background()
    
    // Setup test data
    setupTestData(store)
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            stream, _, err := store.GetStream(ctx, "test-key")
            if err != nil {
                b.Fatal(err)
            }
            stream.Close()
        }
    })
}
```

## Best Practices

1. **Always implement proper error handling and return `daramjwee.ErrNotFound` for missing objects**
2. **Ensure thread safety through appropriate locking mechanisms**
3. **Implement atomic write operations to prevent partial data visibility**
4. **Use streaming operations to handle large objects efficiently**
5. **Respect context cancellation and timeouts**
6. **Provide comprehensive test coverage including edge cases**
7. **Document any specific requirements or limitations of your implementation**
8. **Consider performance implications and optimize for your use case**
9. **Implement proper resource cleanup and lifecycle management**
10. **Follow Go idioms and conventions for consistent API design**

## Common Pitfalls

1. **Not handling context cancellation properly**
2. **Forgetting to implement atomic writes**
3. **Not returning the correct error types**
4. **Memory leaks from unclosed resources**
5. **Race conditions in concurrent access**
6. **Not testing with the actual cache implementation**
7. **Ignoring metadata consistency between methods**
8. **Poor error messages that don't help with debugging**

This comprehensive guide should help you implement robust, efficient, and well-tested custom Store backends for the daramjwee caching library.