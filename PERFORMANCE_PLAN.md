# Daramjwee Performance Innovation Plan

## Profiling Baseline (Apple M3, 8-core)

| Metric | Current | Target |
|--------|---------|--------|
| Hot Hit Default TTL | 850ns, **35 allocs**, 1.9KB | <500ns, **<10 allocs**, <500B |
| Hot Hit Fresh | 870ns, 12 allocs, 657B | <400ns, **<5 allocs**, <200B |
| Stale Hit | 5.5μs, 35 allocs, 39KB | <3μs, <20 allocs, <5KB |
| MemStore R/W | 185ns, 3 allocs | <100ns, **1 alloc** |
| Concurrent Get throughput | baseline | 2-4x on M3 |

---

## Phase 1: Hot Path Allocation Minimization

### 1A. Metadata Pool (target: -8 allocs)

**Problem**: `CloneMetadata` allocates on every tier hit. `memstore.GetStream` line 66 clones metadata for every read.

**Solution**: `sync.Pool` for `*Metadata` with a `Reset()` method.

```go
// cache_pool.go
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

**Changes**:
- `cache_context.go`: `cloneMetadata` uses pool when caller will release
- `memstore/memory.go:66`: Use pool for returned metadata, release on stream close
- `cache_read.go`: `handleTopTierHit` returns metadata via pool, `GetResponse.Close` releases

**Risk**: Low — metadata lifecycle is bounded by response lifetime.

### 1B. Eliminate Default TTL Closure (target: -12 allocs)

**Problem**: `topTierCloseCallback` for stale entries returns `refreshOnCloseCallback(...)` which creates a closure capturing 6 variables. Default TTL (0) means EVERY hit is stale.

**Current flow** (cache_read.go:46-55):
```
topTierCloseCallback → isStale? → refreshOnCloseCallback(requestCtx, key, fetcher, cancel, meta, observedGeneration)
```

**Solution**: Inline the refresh logic into `safeCloser.Close()` using a struct instead of closure.

```go
// cache_read.go — new type
type staleRefreshCallback struct {
    cache              *DaramjweeCache
    requestCtx         context.Context
    key                string
    fetcher            Fetcher
    meta               *Metadata
    observedGeneration uint64
}

func (c *staleRefreshCallback) run() {
    defer c.cache.debugLog("msg", "stale refresh triggered", "key", c.key)
    if err := c.cache.scheduleRefreshWithMetadata(
        detachedValueContext(c.requestCtx), c.key, c.fetcher,
        cloneMetadata(c.meta), nil, &c.observedGeneration,
    ); err != nil {
        c.cache.warnLog("msg", "failed to schedule stale refresh", "key", c.key, "err", err)
    }
}
```

**Changes**:
- `cache_read.go`: `topTierCloseCallback` returns `staleRefreshCallback.run` method value (1 alloc for struct, not 6 for closure)
- Better: pool `staleRefreshCallback` structs

**Risk**: Low — same semantics, different allocation pattern.

### 1C. Skip context.WithTimeout for Hot Hits (target: -2 allocs)

**Problem**: `newCtxWithTimeout` creates `context.WithTimeout` even when parent ctx has a deadline.

**Current** (cache_context.go:78-83):
```go
func (c *DaramjweeCache) newCtxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
    if _, ok := ctx.Deadline(); ok {
        return ctx, func() {}
    }
    return context.WithTimeout(ctx, c.config.opTimeout)
}
```

This is already optimized for deadline case. But `context.WithTimeout` allocates a timer.

**Solution**: For hot hits, we don't need a timeout context at all — the store.GetStream is synchronous and fast.

```go
// cache.go — optimized Get
func (c *DaramjweeCache) Get(ctx context.Context, key string, req GetRequest, fetcher Fetcher) (*GetResponse, error) {
    if c.isClosed.Load() { return nil, ErrCacheClosed }
    if fetcher == nil { return nil, ErrNilFetcher }

    // Fast path: check top tier without timeout context
    topStream, topMeta, err := c.getStreamFromStore(ctx, c.tiers[0], key)
    if err == nil {
        return c.handleTopTierHit(ctx, key, req, fetcher, topStream, topMeta, ...)
    }

    // Slow path: need timeout for lower tiers + origin fetch
    setupCtx, cancel := c.newCtxWithTimeout(ctx)
    // ... rest of logic
}
```

**Risk**: Medium — must ensure top-tier GetStream doesn't block. Store interface contract says it should be fast.

### 1D. Pool GetResponse and safeCloser (target: -2 allocs)

**Problem**: `newGetResponse` and `newSafeCloser` allocate on every successful response.

**Solution**: Pool both with release-on-close.

```go
var getResponsePool = sync.Pool{
    New: func() any { return &GetResponse{} },
}

func pooledGetResponse(status GetStatus, body io.ReadCloser, meta *Metadata) *GetResponse {
    resp := getResponsePool.Get().(*GetResponse)
    resp.Status = status
    resp.Body = body
    if meta != nil { resp.Metadata = *meta }
    return resp
}
```

**Changes**:
- `cache_context.go`: `newGetResponse` → `pooledGetResponse`
- `cache.go`: `safeCloser.Close` calls `releaseMetadata` + `getResponsePool.Put`
- Add `Release()` method to `GetResponse` for explicit control

**Risk**: Low — lifecycle is well-bounded.

---

## Phase 2: Concurrency Scale-up

### 2A. Atomic Generation Counter (target: -50% contention on hot path)

**Problem**: `writeCoordinator.current()` takes `stateMu.Lock()` on every read (line 131-136). This is called from `currentTopWriteGeneration` on every Get().

**Current**:
```go
func (c *writeCoordinator) current() uint64 {
    c.init()
    c.stateMu.Lock()
    defer c.stateMu.Unlock()
    return c.committedGeneration
}
```

**Solution**: Use `atomic.Uint64` for `committedGeneration`.

```go
type writeCoordinator struct {
    // ...
    committedGeneration atomic.Uint64
    // stateMu still protects activeReservations, activeDeletes, etc.
}

func (c *writeCoordinator) current() uint64 {
    c.init()
    return c.committedGeneration.Load()
}
```

**Changes**:
- `write_coordinator.go`: Change `committedGeneration` to `atomic.Uint64`
- All readers use `.Load()`, writers use `.Store()` or `.CompareAndSwap()`
- Keep `stateMu` for `activeReservations` map and `activeDeletes` counter

**Risk**: Medium — must audit all generation access patterns. Some reads need snapshot consistency (read generation + check reservations atomically).

### 2B. RWMutex for Coordinator State (target: -30% contention)

**Problem**: `stateMu sync.Mutex` serializes all state reads, even when only reading `committedGeneration` or checking reservations.

**Solution**: Split into `atomic.Uint64` for generation + `sync.RWMutex` for reservation checks.

```go
type writeCoordinator struct {
    committedGeneration atomic.Uint64  // lock-free reads
    stateMu             sync.RWMutex   // protects reservations map
    activeReservations  map[uint64]struct{}
    activeDeletes       int
    activeDeletesDone   chan struct{}
    // ...
}

// Read-only path (hot)
func (c *writeCoordinator) canAttemptExpectedTopWrite(gen uint64) bool {
    c.stateMu.RLock()
    defer c.stateMu.RUnlock()
    return c.committedGeneration.Load() <= gen
}

// Write path (cold)
func (c *writeCoordinator) reserve(...) { ... }
```

**Risk**: Low — clear read/write separation.

### 2C. Lock-Free activeReservations Check (target: -20% on stale path)

**Problem**: `waitForNoActiveDeletes` uses channel-based signaling which involves goroutine scheduling.

**Solution**: Use `atomic.Int32` for `activeDeletes` counter.

```go
type writeCoordinator struct {
    activeDeletes atomic.Int32
    // ...
}

func (c *writeCoordinator) waitForNoActiveDeletes(ctx context.Context) error {
    for c.activeDeletes.Load() > 0 {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(time.Millisecond):
            // spin with backoff
        }
    }
    return nil
}
```

**Risk**: Low — simpler than channel-based approach.

---

## Phase 3: Bandwidth Maximization

### 3A. streamTeeWriter Pool (target: -1 alloc on WriteTo)

**Problem**: `fillReadCloser.WriteTo` allocates `streamTeeWriter` on every call (line 488).

**Current**:
```go
func (r *fillReadCloser) WriteTo(dst io.Writer) (int64, error) {
    tee := &streamTeeWriter{dst: dst, sink: r.sink}  // allocation!
    // ...
}
```

**Solution**: Pool `streamTeeWriter` objects.

```go
var teeWriterPool = sync.Pool{
    New: func() any { return &streamTeeWriter{} },
}

func (r *fillReadCloser) WriteTo(dst io.Writer) (int64, error) {
    tee := teeWriterPool.Get().(*streamTeeWriter)
    tee.dst = dst
    tee.sink = r.sink
    tee.sinkErr = nil
    defer teeWriterPool.Put(tee)
    // ...
}
```

**Risk**: Low — teeWriter is stack-like in usage.

### 3B. Adaptive Buffer Sizing (target: -50% memory for small payloads)

**Problem**: `streamCopyBufferPool` always uses 32KB buffers, even for 1KB payloads.

**Solution**: Size-class pools.

```go
var (
    smallBufPool = sync.Pool{New: func() any { b := make([]byte, 4*1024); return &b }}
    mediumBufPool = sync.Pool{New: func() any { b := make([]byte, 32*1024); return &b }}
)

func getCopyBuffer(size int) []byte {
    if size <= 4*1024 {
        return *smallBufPool.Get().(*[]byte)
    }
    return *mediumBufPool.Get().(*[]byte)
}
```

**Risk**: Low — pure optimization.

### 3C. Zero-Copy for Small Metadata (target: -1 alloc)

**Problem**: `newGetResponse` copies `*Metadata` into `GetResponse.Metadata` by value (cache_context.go:135-144). For small metadata, this is fine, but we could avoid the pointer dereference.

**Current**:
```go
func newGetResponse(status GetStatus, body io.ReadCloser, meta *Metadata) *GetResponse {
    resp := &GetResponse{Status: status, Body: body}
    if meta != nil {
        resp.Metadata = *meta  // value copy
    }
    return resp
}
```

**Solution**: Already zero-copy for the value. The allocation is the `GetResponse` struct itself, addressed in 1D.

### 3D. fillReadCloser Read Optimization (target: -1 alloc per Read)

**Problem**: `fillReadCloser.Read` calls `writeAll` which may allocate intermediate buffers.

**Current** (streaming.go:420-471):
```go
func (r *fillReadCloser) Read(p []byte) (int, error) {
    r.mu.Lock()          // mutex acquire
    // ...
    r.mu.Unlock()
    n, err := r.src.Read(p)
    if n > 0 {
        writeErr := writeAll(r.sink, p[:n])  // writes to sink
    }
    r.mu.Lock()          // mutex acquire again
    // ...
}
```

**Solution**: Use `atomic.Bool` for `closed` flag instead of mutex for the fast path.

```go
type fillReadCloser struct {
    // ...
    closed atomic.Bool  // fast path check without mutex
}

func (r *fillReadCloser) Read(p []byte) (int, error) {
    if r.closed.Load() {
        return 0, io.ErrClosedPipe
    }
    // ... read and write without mutex on hot path
}
```

**Risk**: Medium — need to ensure Close/Read concurrency is safe.

---

## Implementation Order

| Priority | Task | Expected Impact | Risk |
|----------|------|-----------------|------|
| 1 | 1B: Eliminate Default TTL closure | -12 allocs, -40% | Low |
| 2 | 2A: Atomic generation counter | -50% contention | Medium |
| 3 | 1A: Metadata pool | -8 allocs | Low |
| 4 | 3A: streamTeeWriter pool | -1 alloc on WriteTo | Low |
| 5 | 1C: Skip context for hot hits | -2 allocs | Medium |
| 6 | 1D: Pool GetResponse | -2 allocs | Low |
| 7 | 2B: RWMutex for state | -30% contention | Low |
| 8 | 3B: Adaptive buffer sizing | -50% mem for small | Low |
| 9 | 3D: fillReadCloser optimization | -1 alloc per Read | Medium |
| 10 | 2C: Lock-free activeDeletes | -20% on stale path | Low |

---

## Verification

After each phase:
1. `go build ./...`
2. `go vet ./...`
3. `go test -short -count=1 -timeout 60s ./...`
4. `go test -run='^$' -bench='BenchmarkCacheGet_HotHit' -benchmem -count=5 ./tests/...`
5. `go test -race -short -timeout 120s ./...`

## Expected Final Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Hot Hit Default TTL | 850ns/35 allocs | <400ns/<8 allocs | **2x faster, 4x fewer allocs** |
| Hot Hit Fresh | 870ns/12 allocs | <350ns/<4 allocs | **2.5x faster, 3x fewer allocs** |
| Concurrent Get (8-core) | baseline | 2-4x throughput | RWMutex + atomics |
| Memory per Get | 1.9KB | <400B | **5x reduction** |
