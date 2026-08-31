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
  ├── UNCERTAIN → ErrReadStateUncertain
  └── ERROR → other error
```

**MISS** (`ErrNotFound`):
- Key does not exist in this tier
- Not an error condition
- Cache should continue to next tier or origin

**UNCERTAIN** (`ErrReadStateUncertain`):
- This tier owns a newer value, but its body cannot currently be read
- Cache must stop; falling through could return a stale lower-tier value
- The sentinel remains joined with cancellation/deadline errors after ownership is known

**ERROR** (any other error):
- Storage failure, I/O error, timeout, etc.
- An actual error condition
- Cache may continue in degraded mode, but must not promote a lower-tier value

### Example

```go
// Correct: distinguish MISS, an unsafe stale-fallback barrier, and degraded errors
reader, meta, err := store.GetStream(ctx, key)
if err != nil {
    if errors.Is(err, daramjwee.ErrNotFound) {
        // MISS: continue to next tier
        continue
    }
    if errors.Is(err, daramjwee.ErrReadStateUncertain) {
        return nil, err
    }
    // Other errors may continue without promoting a lower-tier hit.
    higherTiersClean = false
    continue
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

### Cache Writer Setup Failure

```
Origin → SUCCESS(data)
Acquire cache writer → ERROR
  └── Get returns data to caller (non-fatal)
```

This happens before response streaming starts, so the caller still receives the
origin data directly.

Once streaming starts, a cache `Write` or finalizing `Close` error is returned
to the caller and the partial fill is aborted. The cache never publishes that
partial object.

### Implementation

```go
// Cache writer setup failure is non-fatal to data retrieval
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

> **Cache writer setup failure is non-fatal. A write or finalization failure
> after streaming begins is reported and prevents publication.**

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
- Remote GC deletes only unreachable payloads that have a terminal GC receipt.
- An active upload intent protects its payload; a missing receipt preserves the payload.
- Terminalization is ordered as terminal intent, receipt, then plan cleanup. Recovery is
  ordered as active intent, receipt removal, then clearing the terminal marker.
- A receipt and a live plan for the same payload is invalid/corrupt state and is not
  produced by supported store formats.

## Thread Safety

All Store implementations MUST be safe for concurrent use by multiple goroutines.
