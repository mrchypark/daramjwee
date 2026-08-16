# Performance Optimization

This document describes the performance optimization strategies and current benchmarks in daramjwee.

## Current Benchmarks (Apple M3)

| Metric | Value |
|--------|-------|
| Hot Hit Default TTL | 653–712 ns/op, 1.53 KB, 32 allocs |
| Hot Hit Fresh | 791–806 ns/op, 785 B, 13 allocs |
| Hot Hit Stale | 3.10–3.42 µs/op, ~1.34 KB, 27 allocs |
| Lower-tier Hit | 6.56–11.43 µs/op, 36–38 KB, 71–86 allocs |
| Miss | 14.1–26.1 µs/op, ~128 KB, 74 allocs |

## Optimization Strategies

### 1. sync.Pool Usage

daramjwee uses `sync.Pool` to reduce allocations for frequently allocated objects:

```go
// Metadata pool
var metadataPool = sync.Pool{
    New: func() any { return &Metadata{} },
}

// Buffer pool for MemStore
var bufferPool = sync.Pool{
    New: func() any { return new(bytes.Buffer) },
}

// ByteReadCloser pool for MemStore
var byteReadCloserPool = sync.Pool{
    New: func() any { return &byteReadCloser{} },
}

// Stale refresh callback pool
var staleRefreshCallbackPool = sync.Pool{
    New: func() any { return &staleRefreshCallback{} },
}
```

### 2. Buffer Size Optimization

- **ReadAll buffer**: Increased initial buffer from 512B to 4096B for fewer grow-and-copy cycles
- **Copy buffer**: Fixed 32 KiB buffer for stream copying
- **MemStore buffer pool**: Only returns buffers ≤ 1 MiB to pool

### 3. Lock Optimization

- **MemStore**: Uses `sync.RWMutex` for read operations, separate write lock for `Touch()`
- **FileStore**: Uses striped locking instead of global lock
- **WriteCoordinator**: Uses `sync.RWMutex` for read-only checks on hot path

### 4. Allocation Reduction

- **nopCancelFunc**: Package-level variable eliminates closure allocation on every `Get()` call
- **normalizeEntityTag**: Manual ASCII trim to avoid string allocations
- **ifNoneMatchMatchesCacheTag**: Fast path for single ETag to avoid slice allocation
- **FileStore lenBuf**: Changed from `make([]byte, 4)` to `var lenBuf [4]byte` for stack allocation

### 5. Streaming Optimization

- **io.Copy**: Uses `io.Copy` with pooled 32 KiB buffers
- **streamTeeWriter**: Pooled to reduce allocations
- **fillReadCloser**: Automatic EOF detection and close

## Future Optimization Opportunities

### 1. Zero-Copy Reads

For MemStore, consider returning a read-only view of the data instead of copying:

```go
// Current: copies data
func (ms *MemStore) GetStream(...) (io.ReadCloser, *Metadata, error) {
    // ...
    return &byteReadCloser{data: e.value}, meta, nil
}

// Potential: zero-copy with reference counting
func (ms *MemStore) GetStream(...) (io.ReadCloser, *Metadata, error) {
    // ...
    return &zeroCopyReader{data: &e.value, ref: &e.refCount}, meta, nil
}
```

### 2. Metadata Pool Enhancement

Pool `*Metadata` objects with a `Reset()` method:

```go
var metadataPool = sync.Pool{
    New: func() any { return &Metadata{} },
}

func pooledMetadata() *Metadata {
    return metadataPool.Get().(*Metadata)
}

func releaseMetadata(m *Metadata) {
    if m != nil {
        *m = Metadata{}
        metadataPool.Put(m)
    }
}
```

### 3. Reduce Context Allocation

For hot paths, consider using a pool of contexts:

```go
var contextPool = sync.Pool{
    New: func() any { return new(context.Context) },
}
```

### 4. Optimize Conditional Requests

Cache the result of `ifNoneMatchMatchesCacheTag` for repeated checks:

```go
type cachedRequest struct {
    ifNoneMatch string
    cacheTag    string
    result      bool
}
```

## Profiling

### CPU Profiling

```bash
go test -cpuprofile=cpu.prof -bench=BenchmarkCacheGet_HotHitFresh ./tests
go tool pprof cpu.prof
```

### Memory Profiling

```bash
go test -memprofile=mem.prof -bench=BenchmarkCacheGet_HotHitFresh ./tests
go tool pprof mem.prof
```

### Allocation Profiling

```bash
go test -bench=BenchmarkCacheGet_HotHitFresh -benchmem ./tests
```

## Best Practices

1. **Measure before optimizing**: Always benchmark before and after changes.
2. **Focus on hot paths**: Optimize the most frequently executed code first.
3. **Avoid premature optimization**: Only optimize when measurements show it's needed.
4. **Use appropriate data structures**: Choose the right data structure for the workload.
5. **Minimize allocations**: Use pools and avoid unnecessary allocations.
