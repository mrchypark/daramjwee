# N-Tier Cache Architecture Migration Guide

This guide helps you migrate from the legacy two-tier (HotStore/ColdStore) configuration to the new flexible N-tier architecture in daramjwee.

## Overview

The N-tier architecture provides several benefits over the legacy two-tier system:

- **Flexible Hierarchy:** Support for any number of cache tiers (not limited to 2)
- **Simplified Logic:** Sequential lookup and consistent promotion logic
- **Better Performance:** Optimized for various storage characteristics
- **Backward Compatible:** Existing configurations continue to work

## Migration Scenarios

### 1. Hot Store Only → Single Tier

**Before (Legacy):**
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(memStore),
    // ... other options
)
```

**After (N-Tier):**
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore),
    // ... other options
)
```

### 2. Hot + Cold Store → Two Tier

**Before (Legacy):**
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(memStore),
    daramjwee.WithColdStore(fileStore),
    // ... other options
)
```

**After (N-Tier):**
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore, fileStore),
    // ... other options
)
```

### 3. Extending to Multi-Tier

**New Capability (3+ Tiers):**
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore, fileStore, cloudStore),
    // ... other options
)
```

## Step-by-Step Migration

### Step 1: Identify Current Configuration

Check your current daramjwee configuration:

```go
// Look for these patterns in your code:
daramjwee.WithHotStore(...)
daramjwee.WithColdStore(...)
```

### Step 2: Plan Your N-Tier Hierarchy

Decide on your tier hierarchy based on performance characteristics:

| Tier Position | Typical Storage | Characteristics |
|---------------|----------------|-----------------|
| stores[0] | Memory (MemStore) | Fastest, limited capacity |
| stores[1] | File (FileStore) | Medium speed, larger capacity |
| stores[2] | Cloud (ObjStore) | Slowest, unlimited capacity |

### Step 3: Update Configuration

Replace legacy options with `WithStores()`:

```go
// Before
cache, err := daramjwee.New(logger,
    daramjwee.WithHotStore(hotStore),
    daramjwee.WithColdStore(coldStore),
    daramjwee.WithDefaultTimeout(30*time.Second),
    daramjwee.WithCache(5*time.Minute),
)

// After
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(hotStore, coldStore),
    daramjwee.WithDefaultTimeout(30*time.Second),
    daramjwee.WithCache(5*time.Minute),
)
```

### Step 4: Test Thoroughly

Verify that cache behavior remains identical:

1. **Functional Testing:** Ensure all cache operations work correctly
2. **Performance Testing:** Compare performance metrics
3. **Load Testing:** Verify behavior under high load
4. **Monitoring:** Check cache hit rates and tier utilization

### Step 5: Consider Expansion

Once migrated, consider adding additional tiers:

```go
// Add a third tier for even better cache coverage
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore, fileStore, cloudStore),
    // ... other options
)
```

## Configuration Validation

The new architecture includes automatic validation:

### Valid Configurations

```go
// Single tier
daramjwee.WithStores(memStore)

// Multiple tiers
daramjwee.WithStores(memStore, fileStore, cloudStore)

// Legacy (automatically converted)
daramjwee.WithHotStore(memStore)
daramjwee.WithHotStore(memStore) + daramjwee.WithColdStore(fileStore)
```

### Invalid Configurations

```go
// Empty stores slice
daramjwee.WithStores() // Error: at least one store required

// Nil store
daramjwee.WithStores(memStore, nil, cloudStore) // Error: store cannot be nil

// Mixed legacy and new
daramjwee.WithHotStore(memStore) + daramjwee.WithStores(fileStore) // Error: cannot mix
```

## Behavioral Changes

### Cache Lookup

**Legacy Behavior:**
1. Check HotStore
2. Check ColdStore
3. Fetch from origin

**N-Tier Behavior:**
1. Check stores[0] (primary tier)
2. Check stores[1], stores[2], ... (sequential)
3. Fetch from origin

### Data Promotion

**Legacy Behavior:**
- ColdStore hit → promote to HotStore

**N-Tier Behavior:**
- stores[i] hit → promote to stores[0] through stores[i-1]
- Uses efficient streaming promotion

### Write Operations

**Legacy Behavior:**
- Write to HotStore (and optionally ColdStore)

**N-Tier Behavior:**
- Write to stores[0] (primary tier) only
- Promotion handles distribution to other tiers

## Performance Considerations

### Memory Usage

- N-tier architecture eliminates nullStore patterns
- Streaming promotion minimizes memory allocation
- Buffer pool integration maintains performance

### CPU Usage

- Sequential lookup adds minimal overhead
- Simplified logic reduces branching
- Promotion operations are asynchronous

### I/O Patterns

- TeeReader enables efficient promotion
- Streaming operations maintain constant memory usage
- Background operations prevent blocking

## Monitoring and Debugging

### Enhanced Logging

N-tier architecture provides better observability:

```go
// Tier-specific logging
level.Debug(logger).Log("msg", "cache hit in tier", "key", key, "tier", 2)
level.Debug(logger).Log("msg", "promoting to upper tiers", "key", key, "from_tier", 2)
```

### Error Handling

Improved error context with `TierError`:

```go
var tierErr *daramjwee.TierError
if errors.As(err, &tierErr) {
    log.Printf("Tier %d %s operation failed: %v", 
        tierErr.Tier, tierErr.Operation, tierErr.Err)
}
```

## Common Migration Issues

### Issue 1: Configuration Conflicts

**Problem:** Mixing legacy and new configuration options
```go
// This will fail
daramjwee.WithHotStore(memStore)
daramjwee.WithStores(fileStore)
```

**Solution:** Use only one configuration style
```go
// Use N-tier configuration
daramjwee.WithStores(memStore, fileStore)
```

### Issue 2: Nil Store Validation

**Problem:** Passing nil stores in the slice
```go
// This will fail
daramjwee.WithStores(memStore, nil)
```

**Solution:** Ensure all stores are valid
```go
// Check stores before configuration
if fileStore != nil {
    daramjwee.WithStores(memStore, fileStore)
} else {
    daramjwee.WithStores(memStore)
}
```

### Issue 3: Empty Store Slice

**Problem:** Not providing any stores
```go
// This will fail
daramjwee.WithStores()
```

**Solution:** Always provide at least one store
```go
// Minimum configuration
daramjwee.WithStores(memStore)
```

## Testing Your Migration

### Unit Tests

```go
func TestMigration(t *testing.T) {
    logger := log.NewNopLogger()
    memStore := memstore.New(1024*1024, policy.NewLRUPolicy())
    fileStore, _ := filestore.New("/tmp/test", logger, 10*1024*1024, nil)
    
    // Test legacy configuration
    legacyCache, err := daramjwee.New(logger,
        daramjwee.WithHotStore(memStore),
        daramjwee.WithColdStore(fileStore),
    )
    require.NoError(t, err)
    defer legacyCache.Close()
    
    // Test N-tier configuration
    ntierCache, err := daramjwee.New(logger,
        daramjwee.WithStores(memStore, fileStore),
    )
    require.NoError(t, err)
    defer ntierCache.Close()
    
    // Both should behave identically
    testCacheBehavior(t, legacyCache)
    testCacheBehavior(t, ntierCache)
}
```

### Integration Tests

```go
func TestNTierPromotion(t *testing.T) {
    // Test that data promotion works correctly
    // across multiple tiers
}

func TestNTierPerformance(t *testing.T) {
    // Compare performance between legacy and N-tier
    // configurations
}
```

## Best Practices

### 1. Tier Ordering

Order stores from fastest to slowest:
```go
daramjwee.WithStores(
    memStore,    // Fastest: memory-based
    fileStore,   // Medium: file-based
    cloudStore,  // Slowest: network-based
)
```

### 2. Capacity Planning

Size tiers appropriately:
```go
memStore := memstore.New(100*1024*1024, policy.NewLRUPolicy())  // 100MB
fileStore, _ := filestore.New("/cache", logger, 1024*1024*1024, nil) // 1GB
```

### 3. Monitoring

Monitor tier-specific metrics:
- Hit rates per tier
- Promotion frequency
- Tier utilization
- Error rates per tier

### 4. Gradual Migration

Migrate incrementally:
1. Start with equivalent N-tier configuration
2. Test thoroughly in staging
3. Deploy to production
4. Monitor for issues
5. Consider adding additional tiers

## Support and Troubleshooting

### Getting Help

- Check the [examples](./examples/) directory for working configurations
- Review the [API documentation](./README.md) for detailed option descriptions
- File issues on GitHub for migration-specific problems

### Common Questions

**Q: Do I need to migrate immediately?**
A: No, legacy configurations continue to work and are automatically converted internally.

**Q: Will migration affect performance?**
A: N-tier architecture is designed to maintain or improve performance while providing more flexibility.

**Q: Can I mix legacy and N-tier configurations?**
A: No, you must use either legacy options or the new WithStores() option, not both.

**Q: How many tiers should I use?**
A: Start with 1-2 tiers and add more based on your specific performance requirements and storage characteristics.