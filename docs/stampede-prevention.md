# Stampede Prevention

This document describes the stampede prevention mechanisms in daramjwee.

## Problem

When a popular key expires or becomes stale, many concurrent requests may attempt to refresh it simultaneously, causing a "stampede" or "thundering herd" problem.

```
10,000 requests
    │
    ├── All see stale key
    │
    └── All trigger background refresh
        │
        └── 10,000 origin fetches for the same key
```

## Solution: Background Refresh Coalescing

daramjwee uses a combination of mechanisms to prevent stampedes:

### 1. Worker Pool Limitation

The bounded worker pool limits the number of concurrent background jobs:

```go
// WithWorkers(4) limits to 4 concurrent background jobs
cache, err := daramjwee.New(logger,
    daramjwee.WithWorkers(4),
    daramjwee.WithWorkerQueue(500),
)
```

### 2. Generation-Based Deduplication

Each refresh job captures the current generation. If the generation changes before the refresh completes, the result is discarded:

```go
func (c *DaramjweeCache) scheduleRefreshWithMetadata(...) error {
    expectedGeneration := c.currentTopWriteGeneration(key)
    defer expectedGeneration.release()
    
    job := func(jobCtx context.Context) {
        defer expectedGeneration.release()
        
        // Fetch from origin
        result, err := c.fetchFromOrigin(ctx, fetcher, oldMetadata)
        if err != nil {
            // Handle error
            return
        }
        
        // Attempt to publish with generation check
        writer, err := c.setStreamToTopStoreBestEffortWithGeneration(
            ctx, key, result.Metadata, expectedGeneration)
        if err != nil {
            // Generation changed, discard
            return
        }
        
        // Write and commit
        // ...
    }
    
    return c.runtime.Submit(c.cacheID, JobKindRefresh, job)
}
```

### 3. Stale-While-Revalidate (SWR)

When a stale entry is accessed:
1. Return the stale data immediately to the caller
2. Schedule ONE background refresh
3. Subsequent requests for the same stale key do NOT schedule additional refreshes

```go
func (c *DaramjweeCache) handleTopTierHit(...) (*GetResponse, error) {
    isStale := c.isTierCachedStale(meta, 0)
    
    if isStale {
        // Schedule ONE refresh (not per-request)
        callback := newStaleRefreshCallback(c, requestCtx, key, fetcher, cancel, meta, nil, expectedGeneration)
        return newGetResponse(GetStatusOK, newSafeCloser(stream, callback), meta), nil
    }
    
    return newGetResponse(GetStatusOK, stream, meta), nil
}
```

### 4. Singleflight for Origin Fetches

The cache uses singleflight for origin fetches to coalesce concurrent requests for the same key:

```go
// In the fetcher implementation
type myFetcher struct {
    group singleflight.Group
}

func (f *myFetcher) Fetch(ctx context.Context, key string) (*FetchResult, error) {
    result, err, _ := f.group.Do(key, func() (interface{}, error) {
        // Actual origin fetch
        return fetchFromOrigin(ctx, key)
    })
    if err != nil {
        return nil, err
    }
    return result.(*FetchResult), nil
}
```

## Configuration

### Worker Pool

```go
cache, err := daramjwee.New(logger,
    daramjwee.WithWorkerStrategy("pool"),  // "pool" or "all"
    daramjwee.WithWorkers(4),               // Number of workers
    daramjwee.WithWorkerQueue(500),         // Queue capacity
    daramjwee.WithWorkerTimeout(30*time.Second), // Job timeout
)
```

### CacheGroup Shared Runtime

For multiple caches sharing the same runtime:

```go
group, err := daramjwee.NewGroup(logger,
    daramjwee.WithGroupWorkers(2),
    daramjwee.WithGroupWorkerQueueDefault(8),
)

users, err := group.NewCache("users",
    daramjwee.WithWeight(4),
    daramjwee.WithQueueLimit(16),
)
```

## Monitoring

### Key Metrics

- **Worker pool utilization**: Number of active workers vs. pool size
- **Queue depth**: Number of pending jobs in the queue
- **Refresh coalescing rate**: Number of refresh requests vs. actual origin fetches

### Logging

Refresh scheduling is logged at debug level:

```go
c.debugLog("msg", "scheduling background refresh", "key", key)
c.debugLog("msg", "background refresh completed", "key", key)
```

## Best Practices

1. **Set appropriate worker pool size**: Too few workers may cause queue buildup; too many may overwhelm the origin.

2. **Use CacheGroup for multiple caches**: Share a single runtime across caches to limit total resource usage.

3. **Implement singleflight in fetchers**: For maximum coalescing, implement singleflight in your origin fetcher.

4. **Monitor queue depth**: If the queue is consistently full, consider increasing workers or optimizing origin response time.

5. **Set reasonable timeouts**: Prevent slow origins from blocking the worker pool.

## Failure Stampede Prevention

When origin fails, many concurrent requests may retry simultaneously, causing a "failure stampede" or "retry storm".

### Problem

```
100 requests
    ↓
singleflight
    ↓
origin failure
    ↓
100 retries
    ↓
origin failure
    ↓
... (retry storm)
```

### Solution Strategies

#### 1. Negative Caching

Cache the "not found" or "error" state for a short duration:

```go
// Cache negative results for a short time
if errors.Is(err, ErrCacheableNotFound) {
    negativeMetadata := &Metadata{
        IsNegative: true,
        CachedAt:   time.Now(),
    }
    // Store negative entry
}
```

#### 2. Failure Backoff

Add exponential backoff for repeated failures:

```go
type backoffState struct {
    mu         sync.Mutex
    failures   int
    lastFail   time.Time
    backoff    time.Duration
}

func (b *backoffState) shouldRetry() bool {
    b.mu.Lock()
    defer b.mu.Unlock()
    if b.failures == 0 {
        return true
    }
    return time.Since(b.lastFail) > b.backoff
}

func (b *backoffState) recordFailure() {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.failures++
    b.lastFail = time.Now()
    b.backoff = min(b.backoff*2, 10*time.Second)
}
```

#### 3. Jitter

Add random jitter to prevent synchronized retries:

```go
func jitteredBackoff(base time.Duration) time.Duration {
    jitter := time.Duration(rand.Int63n(int64(base / 2)))
    return base + jitter
}
```

#### 4. Per-Key Retry Suppression

Suppress retries for keys that have recently failed:

```go
type retrySuppressor struct {
    mu        sync.Mutex
    suppressed map[string]time.Time
    window    time.Duration
}

func (r *retrySuppressor) shouldRetry(key string) bool {
    r.mu.Lock()
    defer r.mu.Unlock()
    if t, ok := r.suppressed[key]; ok {
        if time.Since(t) < r.window {
            return false
        }
    }
    return true
}

func (r *retrySuppressor) recordFailure(key string) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.suppressed[key] = time.Now()
}
```

**Integration with Cache**:

```go
// In fetcher or cache layer
func (c *DaramjweeCache) fetchWithSuppression(ctx context.Context, key string, fetcher Fetcher) (*FetchResult, error) {
    if !c.retrySuppressor.shouldRetry(key) {
        return nil, ErrRecentlyFailed
    }
    
    result, err := c.fetchFromOrigin(ctx, fetcher, nil)
    if err != nil {
        c.retrySuppressor.recordFailure(key)
        return nil, err
    }
    
    return result, nil
}
```

**Configuration**:

```go
// WithRetrySuppression enables per-key retry suppression
func WithRetrySuppression(window time.Duration) Option {
    return func(cfg *Config) error {
        cfg.RetrySuppressionWindow = window
        return nil
    }
}
```

**Recommended Values**:
- Window: 5-30 seconds for most workloads
- Shorter window (1-5s) for high-traffic systems
- Longer window (30-60s) for expensive origin fetches

#### 5. Circuit Breaker

Stop retrying after too many consecutive failures:

```go
type circuitBreaker struct {
    mu           sync.Mutex
    failures     int
    threshold    int
    resetTimeout time.Duration
    lastFail     time.Time
    state        string // "closed", "open", "half-open"
}

func (c *circuitBreaker) allow() bool {
    c.mu.Lock()
    defer c.mu.Unlock()
    switch c.state {
    case "closed":
        return true
    case "open":
        if time.Since(c.lastFail) > c.resetTimeout {
            c.state = "half-open"
            return true
        }
        return false
    case "half-open":
        return true
    }
    return false
}
```

### Recommended Default Policy

For most use cases, the following combination works well:

1. **Negative caching**: 5-30 seconds for cacheable misses
2. **Worker pool**: Bounded pool to limit concurrent origin fetches
3. **Timeout**: Reasonable timeout to prevent slow origins from blocking

More aggressive strategies (backoff, jitter, circuit breaker) are operational policies that should be added based on specific workload characteristics.
