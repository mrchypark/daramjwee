# Extension Testing and Integration Guidelines

## Overview

This document provides comprehensive guidelines for testing custom implementations of daramjwee interfaces (Store, EvictionPolicy, Compressor, Fetcher) and integrating them with the daramjwee caching library. Proper testing ensures reliability, performance, and compatibility with the existing ecosystem.

## Testing Strategy Overview

### Testing Pyramid for Extensions

```
    ┌─────────────────────┐
    │   Integration Tests │  ← Test with daramjwee cache
    │                     │
    ├─────────────────────┤
    │   Component Tests   │  ← Test interface compliance
    │                     │
    ├─────────────────────┤
    │    Unit Tests       │  ← Test individual methods
    │                     │
    └─────────────────────┘
```

### Test Categories

1. **Unit Tests**: Test individual methods and functions
2. **Component Tests**: Test interface compliance and behavior
3. **Integration Tests**: Test with daramjwee cache and other components
4. **Performance Tests**: Benchmark and load testing
5. **Compatibility Tests**: Test with different configurations and scenarios

## Unit Testing Guidelines

### Test Structure Template

```go
package myextension_test

import (
    "context"
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "github.com/mrchypark/daramjwee"
    
    "your-module/myextension"
)

func TestMyExtension_BasicOperations(t *testing.T) {
    // Arrange
    ext := myextension.New(/* parameters */)
    
    // Act & Assert
    // Test basic functionality
}

func TestMyExtension_EdgeCases(t *testing.T) {
    // Test edge cases and error conditions
}

func TestMyExtension_ErrorHandling(t *testing.T) {
    // Test error scenarios
}
```

### Store Unit Testing

```go
func TestCustomStore_GetStream(t *testing.T) {
    store := NewCustomStore()
    ctx := context.Background()
    
    // Test not found
    _, _, err := store.GetStream(ctx, "nonexistent")
    assert.ErrorIs(t, err, daramjwee.ErrNotFound)
    
    // Test successful storage and retrieval
    metadata := &daramjwee.Metadata{
        ETag:     "test-etag",
        CachedAt: time.Now(),
    }
    
    // Store data
    writer, err := store.SetWithWriter(ctx, "test-key", metadata)
    require.NoError(t, err)
    
    testData := []byte("test data content")
    _, err = writer.Write(testData)
    require.NoError(t, err)
    
    err = writer.Close()
    require.NoError(t, err)
    
    // Retrieve data
    stream, retrievedMeta, err := store.GetStream(ctx, "test-key")
    require.NoError(t, err)
    defer stream.Close()
    
    assert.Equal(t, metadata.ETag, retrievedMeta.ETag)
    
    data, err := io.ReadAll(stream)
    require.NoError(t, err)
    assert.Equal(t, testData, data)
}

func TestCustomStore_SetWithWriter(t *testing.T) {
    store := NewCustomStore()
    ctx := context.Background()
    
    metadata := &daramjwee.Metadata{
        ETag:     "test-etag",
        CachedAt: time.Now(),
    }
    
    // Test writer creation
    writer, err := store.SetWithWriter(ctx, "test-key", metadata)
    require.NoError(t, err)
    require.NotNil(t, writer)
    
    // Test writing data
    testData := []byte("test data")
    n, err := writer.Write(testData)
    require.NoError(t, err)
    assert.Equal(t, len(testData), n)
    
    // Test closing writer
    err = writer.Close()
    require.NoError(t, err)
    
    // Verify data was stored
    stream, _, err := store.GetStream(ctx, "test-key")
    require.NoError(t, err)
    defer stream.Close()
    
    stored, err := io.ReadAll(stream)
    require.NoError(t, err)
    assert.Equal(t, testData, stored)
}

func TestCustomStore_Delete(t *testing.T) {
    store := NewCustomStore()
    ctx := context.Background()
    
    // Test deleting non-existent key (should not error)
    err := store.Delete(ctx, "nonexistent")
    assert.NoError(t, err)
    
    // Store data first
    writer, err := store.SetWithWriter(ctx, "test-key", &daramjwee.Metadata{})
    require.NoError(t, err)
    writer.Write([]byte("test"))
    writer.Close()
    
    // Verify data exists
    _, _, err = store.GetStream(ctx, "test-key")
    require.NoError(t, err)
    
    // Delete data
    err = store.Delete(ctx, "test-key")
    require.NoError(t, err)
    
    // Verify data is gone
    _, _, err = store.GetStream(ctx, "test-key")
    assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCustomStore_Stat(t *testing.T) {
    store := NewCustomStore()
    ctx := context.Background()
    
    // Test stat on non-existent key
    _, err := store.Stat(ctx, "nonexistent")
    assert.ErrorIs(t, err, daramjwee.ErrNotFound)
    
    // Store data
    metadata := &daramjwee.Metadata{
        ETag:     "test-etag",
        CachedAt: time.Now(),
    }
    
    writer, err := store.SetWithWriter(ctx, "test-key", metadata)
    require.NoError(t, err)
    writer.Write([]byte("test data"))
    writer.Close()
    
    // Test stat
    statMeta, err := store.Stat(ctx, "test-key")
    require.NoError(t, err)
    assert.Equal(t, metadata.ETag, statMeta.ETag)
}

func TestCustomStore_ContextCancellation(t *testing.T) {
    store := NewCustomStore()
    
    // Test context cancellation
    ctx, cancel := context.WithCancel(context.Background())
    cancel() // Cancel immediately
    
    _, _, err := store.GetStream(ctx, "test-key")
    assert.ErrorIs(t, err, context.Canceled)
}

func TestCustomStore_ConcurrentAccess(t *testing.T) {
    store := NewCustomStore()
    ctx := context.Background()
    
    const numGoroutines = 10
    const numOperations = 100
    
    var wg sync.WaitGroup
    wg.Add(numGoroutines)
    
    // Concurrent writes
    for i := 0; i < numGoroutines; i++ {
        go func(id int) {
            defer wg.Done()
            
            for j := 0; j < numOperations; j++ {
                key := fmt.Sprintf("key-%d-%d", id, j)
                data := fmt.Sprintf("data-%d-%d", id, j)
                
                writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{})
                if err != nil {
                    t.Errorf("SetWithWriter failed: %v", err)
                    return
                }
                
                writer.Write([]byte(data))
                writer.Close()
            }
        }(i)
    }
    
    wg.Wait()
    
    // Verify all data was stored correctly
    for i := 0; i < numGoroutines; i++ {
        for j := 0; j < numOperations; j++ {
            key := fmt.Sprintf("key-%d-%d", i, j)
            expectedData := fmt.Sprintf("data-%d-%d", i, j)
            
            stream, _, err := store.GetStream(ctx, key)
            require.NoError(t, err)
            
            data, err := io.ReadAll(stream)
            stream.Close()
            require.NoError(t, err)
            
            assert.Equal(t, expectedData, string(data))
        }
    }
}
```###
 EvictionPolicy Unit Testing

```go
func TestCustomPolicy_BasicOperations(t *testing.T) {
    policy := NewCustomPolicy()
    
    // Test empty policy
    victims := policy.Evict()
    assert.Empty(t, victims)
    
    // Add items
    policy.Add("key1", 100)
    policy.Add("key2", 200)
    policy.Add("key3", 150)
    
    // Touch items to create access pattern
    policy.Touch("key1")
    policy.Touch("key2")
    // key3 not touched
    
    // Test eviction (should evict least recently used)
    victims = policy.Evict()
    assert.Contains(t, victims, "key3")
    
    // Test remove
    policy.Remove("key1")
    victims = policy.Evict()
    assert.NotContains(t, victims, "key1")
}

func TestCustomPolicy_EdgeCases(t *testing.T) {
    policy := NewCustomPolicy()
    
    // Test operations on empty policy
    policy.Touch("nonexistent")  // Should not panic
    policy.Remove("nonexistent") // Should not panic
    
    victims := policy.Evict()
    assert.Empty(t, victims)
    
    // Test single item
    policy.Add("single", 100)
    victims = policy.Evict()
    assert.Equal(t, []string{"single"}, victims)
    
    // Test duplicate adds
    policy.Add("dup", 100)
    policy.Add("dup", 200) // Should update, not duplicate
    
    victims = policy.Evict()
    assert.Equal(t, []string{"dup"}, victims)
}

func TestCustomPolicy_AccessPatterns(t *testing.T) {
    policy := NewCustomPolicy()
    
    // Create known access pattern
    items := []string{"frequent", "occasional", "rare"}
    for _, item := range items {
        policy.Add(item, 100)
    }
    
    // Create access pattern
    for i := 0; i < 10; i++ {
        policy.Touch("frequent")
    }
    for i := 0; i < 3; i++ {
        policy.Touch("occasional")
    }
    policy.Touch("rare") // Only once
    
    // Test eviction order
    victims := policy.Evict()
    assert.Equal(t, []string{"rare"}, victims)
    
    victims = policy.Evict()
    assert.Equal(t, []string{"occasional"}, victims)
    
    victims = policy.Evict()
    assert.Equal(t, []string{"frequent"}, victims)
}
```

### Compressor Unit Testing

```go
func TestCustomCompressor_BasicOperations(t *testing.T) {
    compressor, err := NewCustomCompressor(5)
    require.NoError(t, err)
    
    // Test data
    original := []byte("Hello, World! This is a test string for compression testing.")
    
    // Test compression
    var compressed bytes.Buffer
    written, err := compressor.Compress(&compressed, bytes.NewReader(original))
    require.NoError(t, err)
    assert.Greater(t, written, int64(0))
    
    // Test decompression
    var decompressed bytes.Buffer
    written, err = compressor.Decompress(&decompressed, &compressed)
    require.NoError(t, err)
    assert.Greater(t, written, int64(0))
    
    // Verify data integrity
    assert.Equal(t, original, decompressed.Bytes())
    
    // Test algorithm and level
    assert.NotEmpty(t, compressor.Algorithm())
    assert.Equal(t, 5, compressor.Level())
}

func TestCustomCompressor_ErrorHandling(t *testing.T) {
    // Test invalid compression level
    _, err := NewCustomCompressor(100)
    assert.ErrorIs(t, err, daramjwee.ErrInvalidCompressionLevel)
    
    compressor, err := NewCustomCompressor(5)
    require.NoError(t, err)
    
    // Test compression with failing writer
    failingWriter := &failingWriter{failAfter: 10}
    _, err = compressor.Compress(failingWriter, strings.NewReader("test data"))
    assert.ErrorIs(t, err, daramjwee.ErrCompressionFailed)
    
    // Test decompression with corrupted data
    var decompressed bytes.Buffer
    corruptedData := bytes.NewReader([]byte("corrupted data"))
    _, err = compressor.Decompress(&decompressed, corruptedData)
    assert.ErrorIs(t, err, daramjwee.ErrDecompressionFailed)
}
```

### Fetcher Unit Testing

```go
func TestCustomFetcher_BasicOperations(t *testing.T) {
    // Create test server
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("ETag", `"test-etag"`)
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("test response data"))
    }))
    defer server.Close()
    
    fetcher := NewHTTPFetcher(server.URL)
    
    // Test successful fetch
    result, err := fetcher.Fetch(context.Background(), nil)
    require.NoError(t, err)
    require.NotNil(t, result)
    
    data, err := io.ReadAll(result.Body)
    result.Body.Close()
    require.NoError(t, err)
    
    assert.Equal(t, "test response data", string(data))
    assert.Equal(t, `"test-etag"`, result.Metadata.ETag)
}

func TestCustomFetcher_ConditionalRequests(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.Header.Get("If-None-Match") == `"test-etag"` {
            w.WriteHeader(http.StatusNotModified)
            return
        }
        
        w.Header().Set("ETag", `"test-etag"`)
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("test data"))
    }))
    defer server.Close()
    
    fetcher := NewHTTPFetcher(server.URL)
    
    // First fetch
    result, err := fetcher.Fetch(context.Background(), nil)
    require.NoError(t, err)
    result.Body.Close()
    
    // Second fetch with metadata (should return ErrNotModified)
    _, err = fetcher.Fetch(context.Background(), result.Metadata)
    assert.ErrorIs(t, err, daramjwee.ErrNotModified)
}

func TestCustomFetcher_ErrorHandling(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusNotFound)
    }))
    defer server.Close()
    
    fetcher := NewHTTPFetcher(server.URL)
    
    // Test 404 response
    _, err := fetcher.Fetch(context.Background(), nil)
    assert.ErrorIs(t, err, daramjwee.ErrCacheableNotFound)
}
```

## Component Testing Guidelines

### Interface Compliance Testing

```go
// TestStoreInterface verifies Store interface compliance
func TestStoreInterface(t *testing.T) {
    store := NewCustomStore()
    
    // Verify store implements Store interface
    var _ daramjwee.Store = store
    
    // Test interface contract
    testStoreContract(t, store)
}

func testStoreContract(t *testing.T, store daramjwee.Store) {
    ctx := context.Background()
    
    // Test GetStream with non-existent key
    _, _, err := store.GetStream(ctx, "nonexistent")
    assert.ErrorIs(t, err, daramjwee.ErrNotFound, "GetStream should return ErrNotFound for non-existent keys")
    
    // Test SetWithWriter and GetStream cycle
    metadata := &daramjwee.Metadata{
        ETag:     "test-etag",
        CachedAt: time.Now(),
    }
    
    writer, err := store.SetWithWriter(ctx, "test-key", metadata)
    require.NoError(t, err, "SetWithWriter should not fail")
    require.NotNil(t, writer, "SetWithWriter should return non-nil writer")
    
    testData := []byte("test data")
    n, err := writer.Write(testData)
    require.NoError(t, err, "Writer.Write should not fail")
    assert.Equal(t, len(testData), n, "Writer.Write should write all data")
    
    err = writer.Close()
    require.NoError(t, err, "Writer.Close should not fail")
    
    // Verify data can be retrieved
    stream, retrievedMeta, err := store.GetStream(ctx, "test-key")
    require.NoError(t, err, "GetStream should not fail after successful write")
    defer stream.Close()
    
    assert.Equal(t, metadata.ETag, retrievedMeta.ETag, "Retrieved metadata should match stored metadata")
    
    retrievedData, err := io.ReadAll(stream)
    require.NoError(t, err, "Reading from stream should not fail")
    assert.Equal(t, testData, retrievedData, "Retrieved data should match stored data")
    
    // Test Stat
    statMeta, err := store.Stat(ctx, "test-key")
    require.NoError(t, err, "Stat should not fail for existing key")
    assert.Equal(t, metadata.ETag, statMeta.ETag, "Stat metadata should match stored metadata")
    
    // Test Delete
    err = store.Delete(ctx, "test-key")
    require.NoError(t, err, "Delete should not fail")
    
    // Verify data is deleted
    _, _, err = store.GetStream(ctx, "test-key")
    assert.ErrorIs(t, err, daramjwee.ErrNotFound, "GetStream should return ErrNotFound after deletion")
    
    _, err = store.Stat(ctx, "test-key")
    assert.ErrorIs(t, err, daramjwee.ErrNotFound, "Stat should return ErrNotFound after deletion")
}
```

## Integration Testing Guidelines

### Testing with Daramjwee Cache

```go
func TestCustomStore_WithDaramjweeCache(t *testing.T) {
    // Create custom store
    store := NewCustomStore()
    
    // Create cache with custom store
    cache, err := daramjwee.New(
        log.NewNopLogger(),
        daramjwee.WithHotStore(store),
    )
    require.NoError(t, err)
    defer cache.Close()
    
    // Test cache operations
    fetcher := &testFetcher{data: "integration test data"}
    
    // First get (cache miss)
    stream, err := cache.Get(context.Background(), "integration-key", fetcher)
    require.NoError(t, err)
    defer stream.Close()
    
    data, err := io.ReadAll(stream)
    require.NoError(t, err)
    assert.Equal(t, "integration test data", string(data))
    
    // Second get (cache hit)
    stream2, err := cache.Get(context.Background(), "integration-key", fetcher)
    require.NoError(t, err)
    defer stream2.Close()
    
    data2, err := io.ReadAll(stream2)
    require.NoError(t, err)
    assert.Equal(t, "integration test data", string(data2))
    
    // Verify fetcher was only called once
    assert.Equal(t, 1, fetcher.callCount, "Fetcher should only be called once due to caching")
}
```

## Performance Testing Guidelines

### Benchmark Testing

```go
func BenchmarkCustomStore_GetStream(b *testing.B) {
    store := NewCustomStore()
    ctx := context.Background()
    
    // Setup test data
    metadata := &daramjwee.Metadata{ETag: "bench-etag"}
    writer, _ := store.SetWithWriter(ctx, "bench-key", metadata)
    writer.Write(make([]byte, 1024)) // 1KB data
    writer.Close()
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            stream, _, err := store.GetStream(ctx, "bench-key")
            if err != nil {
                b.Fatal(err)
            }
            stream.Close()
        }
    })
}

func BenchmarkCustomCompressor_Compress(b *testing.B) {
    compressor, _ := NewCustomCompressor(5)
    
    testCases := []struct {
        name string
        size int
    }{
        {"1KB", 1024},
        {"10KB", 10 * 1024},
        {"100KB", 100 * 1024},
        {"1MB", 1024 * 1024},
    }
    
    for _, tc := range testCases {
        data := make([]byte, tc.size)
        for i := range data {
            data[i] = byte(i % 256)
        }
        
        b.Run(tc.name, func(b *testing.B) {
            b.SetBytes(int64(tc.size))
            b.ResetTimer()
            
            for i := 0; i < b.N; i++ {
                var compressed bytes.Buffer
                _, err := compressor.Compress(&compressed, bytes.NewReader(data))
                if err != nil {
                    b.Fatal(err)
                }
            }
        })
    }
}
```

## Test Utilities

```go
// testFetcher is a simple fetcher for testing
type testFetcher struct {
    data      string
    callCount int
    mu        sync.Mutex
}

func (f *testFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    f.mu.Lock()
    f.callCount++
    f.mu.Unlock()
    
    return &daramjwee.FetchResult{
        Body: io.NopCloser(strings.NewReader(f.data)),
        Metadata: &daramjwee.Metadata{
            ETag:     fmt.Sprintf("etag-%d", f.callCount),
            CachedAt: time.Now(),
        },
    }, nil
}

// failingWriter simulates write failures
type failingWriter struct {
    failAfter int
    written   int
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
    if w.written+len(p) > w.failAfter {
        return 0, errors.New("simulated write failure")
    }
    w.written += len(p)
    return len(p), nil
}

func (w *failingWriter) Close() error {
    return nil
}

// generateRandomData creates random test data
func generateRandomData(size int) []byte {
    data := make([]byte, size)
    rand.Read(data)
    return data
}
```

## Best Practices Summary

### Testing Best Practices

1. **Test Interface Compliance**: Verify your implementations satisfy interface contracts
2. **Test Error Conditions**: Ensure proper error handling and error type returns
3. **Test Concurrency**: Verify thread safety with concurrent operations
4. **Test Edge Cases**: Handle empty data, large data, and boundary conditions
5. **Test Integration**: Verify components work together with daramjwee cache

### Performance Testing

1. **Benchmark Critical Paths**: Focus on frequently called methods
2. **Test Under Load**: Verify behavior under high concurrency
3. **Monitor Resource Usage**: Check for memory leaks and excessive allocations
4. **Test Different Data Sizes**: Verify performance across various data sizes
5. **Profile Your Code**: Use Go's profiling tools to identify bottlenecks

### Integration Guidelines

1. **Follow Interface Contracts**: Implement all required methods correctly
2. **Handle Context Properly**: Respect cancellation and timeouts
3. **Return Correct Errors**: Use appropriate daramjwee error types
4. **Manage Resources**: Properly close streams and clean up resources
5. **Test with Real Cache**: Always test with actual daramjwee cache instance

This comprehensive testing guide should help you create robust, reliable, and well-tested extensions for the daramjwee caching library.