# Store Contract

This document defines the atomic commit contract that all Store implementations must follow.

## Store Interface

```go
type Store interface {
    GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
    BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error)
    Delete(ctx context.Context, key string) error
    Stat(ctx context.Context, key string) (*Metadata, error)
}
```

## Error Classification

### MISS vs ERROR

Store implementations MUST distinguish between MISS and ERROR:

```
GetStream(key)
  ├── HIT(value, metadata) → success
  ├── MISS → ErrNotFound
  └── ERROR → other error
```

**MISS** (`ErrNotFound`):
- Key does not exist in this tier
- Not an error condition
- Cache should continue to next tier or origin

**ERROR** (any other error):
- Storage failure, I/O error, timeout, etc.
- An actual error condition
- Cache should handle as failure, not continue

### Example

```go
// Correct: distinguish MISS from ERROR
reader, meta, err := store.GetStream(ctx, key)
if err != nil {
    if errors.Is(err, daramjwee.ErrNotFound) {
        // MISS: continue to next tier
        continue
    }
    // ERROR: handle failure
    return nil, err
}

// Incorrect: treating MISS as ERROR
if err != nil {
    return nil, err  // WRONG: Miss is not an error
}
```

## Cache Fill Failure vs Data Retrieval Failure

These MUST be treated as separate failure domains:

### Data Retrieval Failure

```
Origin → ERROR
  └── Get returns ERROR to caller
```

This is a failure to retrieve the data. The caller receives an error.

### Cache Fill Failure

```
Origin → SUCCESS(data)
Cache write → ERROR
  └── Get returns data to caller (non-fatal)
```

This is a failure to cache the data. The caller still receives the data.

### Implementation

```go
// Cache fill failure is non-fatal to data retrieval
result, err := c.fetchFromOrigin(ctx, fetcher, oldMetadata)
if err != nil {
    return nil, err  // Data retrieval failure
}

// Attempt cache fill
writer, err := c.setStreamToTopStoreForFill(ctx, key, result.Metadata, expectedGeneration)
if err != nil {
    // Cache fill failure: log and continue, return data
    c.warnLog("msg", "cache fill failed", "key", key, "err", err)
    return newGetResponse(GetStatusOK, result.Body, result.Metadata), nil
}
```

### Policy

> **Cache write failure is non-fatal to a successful source read, unless the caller explicitly requests write-through durability.**

This ensures cache never reduces availability.

## Atomic Commit Contract

### WriteSink Lifecycle

```
BeginSet()
  │
  ├── Write() ──► data streaming
  │
  ├── Close() ──► Commit (atomic publish)
  │
  └── Abort() ──► Discard (cleanup)
```

### Invariants

1. **Visibility**: A key's value MUST NOT be visible to readers until `Close()` succeeds.

2. **Atomicity**: `Close()` MUST be atomic. Either the entire write is committed or nothing changes.

3. **Idempotency**: `Close()` and `Abort()` MUST be safe to call multiple times.

4. **Context Cancellation**: If the context is cancelled before `Close()`, the write MUST be discarded.

5. **No Partial Publish**: A partially written object MUST NEVER be visible as a complete object.

### Streaming Publish Guard

The `fillReadCloser` ensures partial objects are never published:

```go
// Only publish when ALL conditions are met:
publish := r.sawEOF && r.readErr == nil && r.sinkErr == nil

if publish {
    // Commit: source read complete, no errors
    err = r.sink.Close()
} else {
    // Abort: partial read or error
    err = r.sink.Abort()
}
```

**Conditions for publish**:
1. `sawEOF`: Source stream reached EOF (complete read)
2. `readErr == nil`: No read errors
3. `sinkErr == nil`: No write errors

If ANY condition fails, the write is aborted and never visible.

### StagingStore Extension

Stores that implement `StagingStore` provide a two-phase commit:

```go
type StagingStore interface {
    BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error)
}

type StagedWriteSink interface {
    io.Writer
    Commit(ctx context.Context) error
    Abort() error
}
```

**Lifecycle**:
```
BeginStagedSet()
  │
  ├── Write() ──► invisible staging
  │
  ├── Commit() ──► atomic publish (short critical section)
  │
  └── Abort() ──► discard staging
```

## Cache-Level Guarantees

### Generation Fence

Each key has an associated generation counter. The cache uses this to prevent:

1. **Resurrection Race**: Delete increments generation. Concurrent Get captures generation at start. Promotion checks generation before commit.

2. **Stale Promotion**: If generation changes between Get start and promotion attempt, promotion is silently discarded.

### Fill Lease

When the cache fills a tier from a lower tier or origin:

1. A fill lease is acquired with a timeout.
2. If the lease expires, the fill is preempted.
3. Preempted fills are discarded, not partial-committed.

## Implementation Guidelines

### FileStore

- Uses temp file + atomic rename for writes.
- `WithCopyWrite` mode is NOT atomic and cannot be used as tier 0.

### MemStore

- Commits are atomic via map swap.
- Clone data before storing to prevent aliasing.

### ObjectStore

- Uses catalog + segment files.
- Flush is asynchronous but catalog updates are atomic.
- `Close()` flushes pending writes before shutdown.

## Thread Safety

All Store implementations MUST be safe for concurrent use by multiple goroutines.
