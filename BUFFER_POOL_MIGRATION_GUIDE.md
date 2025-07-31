# Buffer Pool Migration Guide

This guide helps you migrate from the default buffer pool to the new adaptive buffer pool with large object optimization.

## Overview

The adaptive buffer pool provides significant performance improvements for large objects (256KB+) while maintaining backward compatibility with existing configurations.

## Migration Options

### Option 1: Automatic Migration (Recommended)

Simply add large object optimization to your existing configuration:

```go
// Before
cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(hotStore),
    daramjwee.WithBufferPool(true, 32*1024),
)

// After - adds large object optimization
cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(hotStore),
    daramjwee.WithBufferPool(true, 32*1024),
    daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
)
```

### Option 2: Full Configuration

Use the comprehensive adaptive buffer pool configuration:

```go
config := daramjwee.BufferPoolConfig{
    // Basic settings (backward compatible)
    Enabled:           true,
    DefaultBufferSize: 32 * 1024,  // 32KB
    MaxBufferSize:     128 * 1024, // 128KB
    MinBufferSize:     4 * 1024,   // 4KB
    
    // Large object optimization (new)
    LargeObjectThreshold:     256 * 1024,  // 256KB
    VeryLargeObjectThreshold: 1024 * 1024, // 1MB
    ChunkSize:                64 * 1024,   // 64KB
    MaxConcurrentLargeOps:    4,
    
    // Monitoring (optional)
    EnableDetailedMetrics: true,
    EnableLogging:         false,
    LoggingInterval:       0,
}

cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(hotStore),
    daramjwee.WithAdaptiveBufferPool(config),
)
```

## Configuration Parameters

### Large Object Thresholds

- **LargeObjectThreshold** (default: 256KB): Objects larger than this use chunked streaming
- **VeryLargeObjectThreshold** (default: 1MB): Objects larger than this use direct streaming
- **ChunkSize** (default: 64KB): Chunk size for streaming operations
- **MaxConcurrentLargeOps** (default: 4): Maximum concurrent large object operations

### Sizing Guidelines

| Use Case | LargeObjectThreshold | VeryLargeObjectThreshold | ChunkSize |
|----------|---------------------|-------------------------|-----------|
| Memory Constrained | 128KB | 512KB | 32KB |
| Balanced | 256KB | 1MB | 64KB |
| High Memory | 512KB | 2MB | 128KB |

## Performance Impact

### Before Migration (Default Buffer Pool)
- Small objects (< 32KB): Excellent performance
- Medium objects (32KB-128KB): Good performance  
- Large objects (> 256KB): **Performance degradation**

### After Migration (Adaptive Buffer Pool)
- Small objects (< 32KB): Excellent performance (unchanged)
- Medium objects (32KB-256KB): Good performance (unchanged)
- Large objects (256KB-1MB): **50-100x performance improvement**
- Very large objects (> 1MB): **Consistent performance**

## Backward Compatibility

### Existing Configurations Continue to Work

```go
// These configurations work unchanged
daramjwee.WithBufferPool(true, 32*1024)
daramjwee.WithBufferPoolAdvanced(config)
```

### API Compatibility

All existing BufferPool interface methods remain unchanged:
- `Get(size int) []byte`
- `Put(buf []byte)`
- `CopyBuffer(dst io.Writer, src io.Reader) (int64, error)`
- `TeeReader(r io.Reader, w io.Writer) io.Reader`
- `GetStats() BufferPoolStats`

### Default Behavior

- Without large object optimization: Uses default buffer pool
- With large object optimization: Uses adaptive buffer pool
- No breaking changes to existing code

## Migration Steps

### Step 1: Assess Current Usage

Identify if your application handles large objects:

```go
// Add metrics to understand your object size distribution
stats := cache.BufferPool.GetStats()
log.Printf("Pool hits: %d, misses: %d", stats.PoolHits, stats.PoolMisses)
```

### Step 2: Enable Large Object Optimization

Add the optimization configuration:

```go
cache, err := daramjwee.New(logger,
    // ... existing options ...
    daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
)
```

### Step 3: Monitor Performance

Enable detailed metrics to monitor the impact:

```go
cache, err := daramjwee.New(logger,
    // ... existing options ...
    daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
    daramjwee.WithBufferPoolMetrics(true, 5*time.Minute),
)
```

### Step 4: Tune Configuration

Adjust thresholds based on your workload:

```go
// For applications with many 128KB objects
daramjwee.WithLargeObjectOptimization(128*1024, 512*1024, 32*1024, 2)

// For applications with very large objects
daramjwee.WithLargeObjectOptimization(512*1024, 2048*1024, 128*1024, 8)
```

## Rollback Procedure

If you need to rollback to the default buffer pool:

```go
// Remove large object optimization options
cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(hotStore),
    daramjwee.WithBufferPool(true, 32*1024), // Back to default
    // Remove: daramjwee.WithLargeObjectOptimization(...)
)
```

## Monitoring and Troubleshooting

### Key Metrics to Monitor

```go
stats := cache.BufferPool.GetStats()

// Basic metrics
fmt.Printf("Total Gets: %d\n", stats.TotalGets)
fmt.Printf("Pool Hits: %d\n", stats.PoolHits)
fmt.Printf("Pool Misses: %d\n", stats.PoolMisses)

// Size category metrics (adaptive pool only)
fmt.Printf("Small Object Ops: %d\n", stats.SmallObjectOps)
fmt.Printf("Medium Object Ops: %d\n", stats.MediumObjectOps)
fmt.Printf("Large Object Ops: %d\n", stats.LargeObjectOps)
fmt.Printf("Very Large Object Ops: %d\n", stats.VeryLargeObjectOps)

// Strategy metrics (adaptive pool only)
fmt.Printf("Pooled Operations: %d\n", stats.PooledOperations)
fmt.Printf("Chunked Operations: %d\n", stats.ChunkedOperations)
fmt.Printf("Direct Operations: %d\n", stats.DirectOperations)
```

### Common Issues and Solutions

#### High Memory Usage
- Reduce `MaxConcurrentLargeOps`
- Increase `LargeObjectThreshold` to use direct streaming sooner
- Reduce `ChunkSize`

#### Poor Performance for Large Objects
- Verify `LargeObjectThreshold` is set correctly
- Check that adaptive buffer pool is being used
- Monitor `ChunkedOperations` and `DirectOperations` metrics

#### Configuration Errors
- Ensure `LargeObjectThreshold > MaxBufferSize`
- Ensure `VeryLargeObjectThreshold > LargeObjectThreshold`
- Verify all threshold values are positive

## Best Practices

1. **Start Conservative**: Begin with default thresholds and adjust based on monitoring
2. **Monitor Metrics**: Use detailed metrics during initial deployment
3. **Test Thoroughly**: Benchmark your specific workload before production deployment
4. **Gradual Rollout**: Deploy to a subset of instances first
5. **Document Changes**: Keep track of configuration changes and their impact

## Support

For issues or questions about the migration:
1. Check the configuration validation errors
2. Review the metrics and logs
3. Consult the benchmark results for your workload
4. Consider reverting to default buffer pool if issues persist