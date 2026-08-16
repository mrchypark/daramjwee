# Fill Coordination

This document describes the fill coordination mechanism in daramjwee.

## Overview

When multiple goroutines request the same key simultaneously and it's a cache miss, daramjwee coordinates the fill to prevent duplicate origin fetches.

## Architecture

```
┌── caller A
miss ── fill key ─┼── caller B
                  ├── caller C
                  └── caller D

                    │
                    ▼

                 loader()
                    │
                    ▼
                 publish
                    │
             ┌──────┼──────┐
             ▼      ▼      ▼
             A      B      C
```

## Implementation

### Write Coordinator

The `writeCoordinator` manages fill coordination per key:

```go
type writeCoordinator struct {
    manager     *topWriteManager
    key         string
    // ... other fields
}
```

### Fill Lifecycle

1. **Miss Detection**: Cache detects miss on all tiers
2. **Fill Lease**: Acquire fill lease with generation check
3. **Origin Fetch**: Fetch from origin
4. **Publish**: Publish to top tier with generation validation
5. **Fanout**: Optionally persist to lower tiers

## Guarantees

### 1. Single Loader Per Key

Only one loader runs per key at a time. The write coordinator ensures this:

```go
func (c *writeCoordinator) reserveWithFill(ctx context.Context, expected uint64, fill *topFillSink) (uint64, error) {
    c.stateMu.Lock()
    defer c.stateMu.Unlock()
    
    if c.activeFill != nil {
        return 0, ErrTopWriteInvalidated
    }
    // ...
}
```

### 2. All Waiters Get Result

All waiting goroutines receive the same result through the fill mechanism:

```go
// In streamThroughWithTrace
func (r *fillReadCloser) Read(p []byte) (int, error) {
    // All callers read from the same source
    return r.src.Read(p)
}
```

### 3. Generation Fence

The generation fence prevents stale fills from overwriting newer values:

```go
// Check generation before publish
if c.latestGenerationLocked() != expected {
    return 0, ErrTopWriteInvalidated
}
```

## Failure Scenarios

### A. Loader Returns Error

```go
result, err := c.fetchFromOrigin(ctx, fetcher, oldMetadata)
if err != nil {
    // Error is returned to all waiters
    return nil, err
}
```

**Behavior**:
- All waiters receive the same error
- No value is published to cache
- Callers can retry

### B. Context Cancellation

```go
// If leader's context is cancelled
select {
case <-ctx.Done():
    // Leader cancelled, but waiters continue
    return nil, ctx.Err()
}
```

**Behavior**:
- If the leader is cancelled, waiters continue waiting
- If all waiters are cancelled, loader may be cancelled
- Fill state is properly cleaned up

### C. Loader Panic

```go
func (c *DaramjweeCache) fetchFromOrigin(ctx context.Context, fetcher Fetcher, oldMetadata *Metadata) (*FetchResult, error) {
    defer func() {
        if r := recover(); r != nil {
            // Fill state is released
            // Key is not permanently stuck
        }
    }()
    return fetcher.Fetch(ctx, oldMetadata)
}
```

**Behavior**:
- Fill state is released
- Key is not permanently stuck
- Panic is propagated to caller

### D. Stale Result (Critical Race)

```go
T1: loader A starts (generation=42)
T2: Set(k, newer) (generation=43)
T3: loader A finishes
T4: loader A checks: generation 42 != 43
T5: loader A discards result
```

**Behavior**:
- A fill cannot overwrite a newer value
- The generation fence prevents this
- Caller receives the newer value

## Testing

### Test Scenarios

1. **Concurrent Miss**: Multiple goroutines miss same key, only one loader runs
2. **Loader Error**: All waiters receive error
3. **Context Cancellation**: Leader cancellation doesn't affect waiters
4. **Stale Fill**: Fill is discarded after generation changes
5. **Delete During Fill**: Fill is discarded after delete

### Test Implementation

```go
func TestFillCoordination_ConcurrentMiss(t *testing.T) {
    // Setup
    cache := setupCache()
    
    // Multiple concurrent misses
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            cache.Get(ctx, "key", GetRequest{}, fetcher)
        }()
    }
    wg.Wait()
    
    // Verify only one origin fetch occurred
    require.Equal(t, 1, fetcher.CallCount)
}
```

## Configuration

### Worker Pool

```go
cache, err := daramjwee.New(logger,
    daramjwee.WithWorkers(4),
    daramjwee.WithWorkerQueue(500),
)
```

### Fill Lease Timeout

```go
cache, err := daramjwee.New(logger,
    daramjwee.WithFillLeaseTimeout(5*time.Second),
)
```

## Monitoring

### Key Metrics

- **Fill count**: Number of fills per key
- **Fill latency**: Time spent in fill
- **Fill failures**: Number of failed fills
- **Generation mismatches**: Number of discarded fills

### Logging

```go
c.debugLog("msg", "fill started", "key", key, "generation", generation)
c.debugLog("msg", "fill completed", "key", key, "generation", generation)
c.debugLog("msg", "fill discarded", "key", key, "reason", "generation_mismatch")
```
