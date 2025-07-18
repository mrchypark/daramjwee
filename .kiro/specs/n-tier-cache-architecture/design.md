# Design Document

## Overview

This design transforms the daramjwee cache from a fixed two-tier architecture (HotStore + ColdStore) to a flexible N-tier system using a slice of Store interfaces. The new architecture maintains all existing performance characteristics while providing unprecedented flexibility for complex caching hierarchies.

The core change replaces separate `HotStore` and `ColdStore` fields with a single `Stores []Store` slice, where `stores[0]` represents the fastest tier and subsequent stores represent progressively slower tiers. This design eliminates the need for nullStore patterns and simplifies the core cache logic significantly.

## Architecture

### Current Architecture Limitations

The existing architecture has several constraints:
- Fixed two-tier limitation (hot + cold only)
- Complex branching logic in Get() method (hot hit, cold hit, miss)
- Requires nullStore pattern when ColdStore is not configured
- Separate handling logic for each tier type

### New N-Tier Architecture

```mermaid
graph TD
    A[Client Request] --> B[Cache.Get()]
    B --> C{Check stores[0]}
    C -->|Hit| D[Serve + Check Staleness]
    C -->|Miss| E{Check stores[1]}
    E -->|Hit| F[Promote to stores[0] + Serve]
    E -->|Miss| G{Check stores[2]}
    G -->|Hit| H[Promote to stores[0,1] + Serve]
    G -->|Miss| I[Fetch from Origin]
    
    D -->|Stale| J[Schedule Background Refresh]
    F --> K[Background Promotion via TeeReader]
    H --> L[Background Promotion via TeeReader]
    I --> M[Cache in stores[0] + Serve]
```

### Store Hierarchy Design

```go
type DaramjweeCache struct {
    Stores           []Store          // Ordered from fastest to slowest
    Logger           log.Logger
    Worker           *worker.Manager
    BufferPool       BufferPool
    DefaultTimeout   time.Duration
    ShutdownTimeout  time.Duration
    PositiveFreshFor time.Duration
    NegativeFreshFor time.Duration
    isClosed         atomic.Bool
}
```

## Components and Interfaces

### Core Cache Interface Changes

The Cache interface remains unchanged to maintain backward compatibility. All changes are internal to the implementation.

### Store Interface

No changes required to the Store interface. All existing Store implementations will work seamlessly with the new architecture.

### Configuration Options

#### New Configuration Options

```go
// WithStores configures multiple cache tiers in order from fastest to slowest
func WithStores(stores ...Store) Option {
    return func(cfg *Config) error {
        if len(stores) == 0 {
            return &ConfigError{"at least one store must be provided"}
        }
        for i, store := range stores {
            if store == nil {
                return &ConfigError{fmt.Sprintf("store at index %d cannot be nil", i)}
            }
        }
        cfg.Stores = stores
        return nil
    }
}
```

#### Backward Compatibility

```go
// Existing options will be internally converted to Stores slice
func WithHotStore(store Store) Option {
    return func(cfg *Config) error {
        if store == nil {
            return &ConfigError{"hot store cannot be nil"}
        }
        if cfg.Stores != nil {
            return &ConfigError{"cannot use WithHotStore with WithStores"}
        }
        cfg.HotStore = store // Temporary field for conversion
        return nil
    }
}

func WithColdStore(store Store) Option {
    return func(cfg *Config) error {
        if cfg.Stores != nil {
            return &ConfigError{"cannot use WithColdStore with WithStores"}
        }
        cfg.ColdStore = store // Temporary field for conversion
        return nil
    }
}
```

### Configuration Validation and Conversion

```go
func (cfg *Config) validate() error {
    // Convert legacy configuration to new format
    if cfg.Stores == nil {
        if cfg.HotStore == nil {
            return &ConfigError{"either WithStores or WithHotStore must be provided"}
        }
        
        // Convert legacy configuration
        if cfg.ColdStore != nil {
            cfg.Stores = []Store{cfg.HotStore, cfg.ColdStore}
        } else {
            cfg.Stores = []Store{cfg.HotStore}
        }
        
        // Clear legacy fields
        cfg.HotStore = nil
        cfg.ColdStore = nil
    }
    
    return nil
}
```

## Data Models

### Cache Entry Metadata

No changes required to the Metadata struct. All existing metadata handling remains the same.

### Configuration Structure

```go
type Config struct {
    // New field
    Stores           []Store          // Replaces HotStore and ColdStore
    
    // Legacy fields (for backward compatibility during transition)
    HotStore         Store            // Deprecated: use WithStores
    ColdStore        Store            // Deprecated: use WithStores
    
    // Existing fields remain unchanged
    WorkerStrategy   string
    WorkerPoolSize   int
    WorkerQueueSize  int
    WorkerJobTimeout time.Duration
    DefaultTimeout   time.Duration
    ShutdownTimeout  time.Duration
    PositiveFreshFor time.Duration
    NegativeFreshFor time.Duration
    BufferPool       BufferPoolConfig
}
```

## Core Algorithm Changes

### Sequential Lookup Algorithm

```go
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
    if c.isClosed.Load() {
        return nil, ErrCacheClosed
    }
    ctx, cancel := c.newCtxWithTimeout(ctx)
    defer cancel()

    // Sequential lookup through all tiers
    for i, store := range c.Stores {
        stream, meta, err := c.getStreamFromStore(ctx, store, key)
        if err == nil {
            if i == 0 {
                // Hit in primary tier
                return c.handlePrimaryHit(ctx, key, fetcher, stream, meta)
            } else {
                // Hit in lower tier - promote and serve
                return c.handleTierHit(ctx, key, i, stream, meta)
            }
        }
        if !errors.Is(err, ErrNotFound) {
            level.Error(c.Logger).Log("msg", "store get failed", "key", key, "tier", i, "err", err)
        }
    }

    // Miss in all tiers - fetch from origin
    return c.handleMiss(ctx, key, fetcher)
}
```

### Promotion Algorithm

```go
func (c *DaramjweeCache) handleTierHit(ctx context.Context, key string, hitTier int, stream io.ReadCloser, meta *Metadata) (io.ReadCloser, error) {
    level.Debug(c.Logger).Log("msg", "cache hit in tier", "key", key, "tier", hitTier)
    
    // Update metadata for promotion
    promoteMeta := &Metadata{}
    if meta != nil {
        *promoteMeta = *meta
    }
    promoteMeta.CachedAt = time.Now()
    
    // Promote to all upper tiers simultaneously
    return c.promoteToUpperTiers(ctx, key, hitTier, promoteMeta, stream)
}

func (c *DaramjweeCache) promoteToUpperTiers(ctx context.Context, key string, fromTier int, meta *Metadata, stream io.ReadCloser) (io.ReadCloser, error) {
    // Create writers for all upper tiers
    var writers []io.WriteCloser
    for i := 0; i < fromTier; i++ {
        writer, err := c.setStreamToStore(ctx, c.Stores[i], key, meta)
        if err != nil {
            level.Error(c.Logger).Log("msg", "failed to create promotion writer", "key", key, "tier", i, "err", err)
            continue
        }
        writers = append(writers, writer)
    }
    
    if len(writers) == 0 {
        // No promotion possible, return original stream
        return stream, nil
    }
    
    // Use MultiWriter for simultaneous promotion
    var teeReader io.Reader
    if c.BufferPool != nil {
        teeReader = c.BufferPool.TeeReader(stream, io.MultiWriter(writers...))
    } else {
        teeReader = io.TeeReader(stream, io.MultiWriter(writers...))
    }
    
    // Combine all closers
    allClosers := append([]io.Closer{stream}, writers...)
    return newMultiCloser(teeReader, allClosers...), nil
}
```

### Write Operations

```go
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
    if c.isClosed.Load() {
        return nil, ErrCacheClosed
    }
    if len(c.Stores) == 0 {
        return nil, &ConfigError{"no stores configured"}
    }
    
    ctx, cancel := c.newCtxWithTimeout(ctx)
    
    if metadata == nil {
        metadata = &Metadata{}
    }
    metadata.CachedAt = time.Now()
    
    // Always write to primary tier (stores[0])
    wc, err := c.setStreamToStore(ctx, c.Stores[0], key, metadata)
    if err != nil {
        cancel()
        return nil, err
    }
    return newCancelWriteCloser(wc, cancel), nil
}
```

### Delete Operations

```go
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
    if c.isClosed.Load() {
        return ErrCacheClosed
    }
    ctx, cancel := c.newCtxWithTimeout(ctx)
    defer cancel()
    
    var firstErr error
    
    // Delete from all tiers
    for i, store := range c.Stores {
        if err := c.deleteFromStore(ctx, store, key); err != nil && !errors.Is(err, ErrNotFound) {
            level.Error(c.Logger).Log("msg", "failed to delete from tier", "key", key, "tier", i, "err", err)
            if firstErr == nil {
                firstErr = err
            }
        }
    }
    
    return firstErr
}
```

## Error Handling

### Store Operation Failures

- **Single store failure**: Continue checking remaining stores
- **Promotion failure**: Serve data successfully, log promotion error
- **Primary store failure**: Return error (cannot serve without primary tier)
- **Delete failures**: Continue with all stores, return first error encountered

### Configuration Errors

- **Empty stores slice**: Return ConfigError
- **Nil store in slice**: Return ConfigError with specific index
- **Mixed legacy/new configuration**: Return ConfigError with clear guidance

### Runtime Error Handling

```go
type TierError struct {
    Tier  int
    Store Store
    Op    string
    Err   error
}

func (e *TierError) Error() string {
    return fmt.Sprintf("daramjwee: tier %d %s operation failed: %v", e.Tier, e.Op, e.Err)
}
```

## Testing Strategy

### Unit Tests

1. **Sequential lookup logic**
   - Test hit in each tier position
   - Test miss in all tiers
   - Test partial store failures

2. **Promotion logic**
   - Test promotion to single upper tier
   - Test promotion to multiple upper tiers
   - Test promotion failure handling

3. **Configuration validation**
   - Test new WithStores option
   - Test backward compatibility
   - Test configuration error cases

4. **Write/Delete operations**
   - Test primary tier targeting
   - Test multi-tier deletion
   - Test error propagation

### Integration Tests

1. **Multi-tier scenarios**
   - Memory → File → Cloud storage hierarchy
   - Different eviction policies per tier
   - Tier failure and recovery scenarios

2. **Performance benchmarks**
   - Compare N-tier vs 2-tier performance
   - Memory allocation profiling
   - Concurrent access patterns

3. **Backward compatibility**
   - Existing configuration continues to work
   - Performance characteristics maintained
   - API behavior unchanged

### Stress Tests

1. **Concurrent promotion**
   - Multiple goroutines promoting same key
   - Race condition detection
   - Resource leak prevention

2. **Store failure scenarios**
   - Random store failures during operation
   - Network partition simulation
   - Recovery behavior validation

## Migration Strategy

### Phase 1: Internal Implementation (v1.x)

- Implement new architecture internally
- Maintain existing API surface
- Convert legacy configuration automatically
- Comprehensive testing and validation

### Phase 2: New API Introduction (v1.x)

- Add WithStores() option
- Document new configuration patterns
- Provide migration examples
- Maintain full backward compatibility

### Phase 3: Deprecation (v2.0)

- Mark legacy options as deprecated
- Provide migration tooling
- Update all documentation and examples
- Remove legacy code paths

### Migration Examples

```go
// v1.x Legacy Configuration
cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(memStore),
    daramjwee.WithColdStore(fileStore),
)

// v2.0 New Configuration
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore, fileStore, cloudStore),
)
```

## Performance Considerations

### Memory Usage

- Elimination of nullStore reduces memory overhead
- Promotion operations use streaming to minimize memory allocation
- Buffer pool integration maintains existing performance characteristics

### CPU Usage

- Sequential lookup adds minimal CPU overhead
- Promotion operations are asynchronous and non-blocking
- Simplified logic reduces branching overhead

### I/O Patterns

- TeeReader enables efficient simultaneous promotion
- Streaming operations maintain constant memory usage
- Background operations prevent blocking client requests

### Scalability

- N-tier support enables more sophisticated caching strategies
- Each tier can be optimized for its specific role
- Better resource utilization across different storage types