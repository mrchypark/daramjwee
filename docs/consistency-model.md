# Consistency Model

This document defines the consistency guarantees that daramjwee provides.

## Overview

daramjwee is a **local caching middleware**, not a distributed cache or database. Its consistency model is designed for single-process use with optional remote backing stores.

## Consistency Guarantees

### 1. Single Process

```
Set(k, v) → Get(k) returns v
```

**Guaranteed**: Within a single process, a `Set` followed by a `Get` for the same key will return the set value.

```
Concurrent Set(k, v1) + Set(k, v2) → may not be linearizable
```

**Not Guaranteed**: Concurrent `Set` operations on the same key are last-writer-wins. The order is not guaranteed to be linearizable.

### 2. Multiple Processes (via ObjectStore)

```
Process A: Set(k, v1)
Process B: Get(k) → may not see v1 immediately
```

**Not Guaranteed**: Read-after-write consistency across processes. ObjectStore uses checkpoint caching with TTL, so visibility may be delayed.

### 3. Multiple ObjectStore Writers

```
Writer A: Set(k, v1)
Writer B: Set(k, v2)
→ last-writer-wins
```

**Guaranteed**: Last-writer-wins semantics. External coordination is required for same-key ordering.

## Fill Coordination

When multiple goroutines request the same key simultaneously:

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

### Fill Guarantees

1. **Single Loader**: Only one loader runs per key at a time
2. **All Waiters Get Result**: All waiting goroutines receive the same result
3. **Generation Fence**: Published value must match the generation observed when fill started

### Fill Failure Scenarios

#### A. Loader Returns Error

```go
loader() → error
```

- All waiters receive the same error
- No value is published to cache
- Callers can retry

#### B. Context Cancellation

```go
A: loader(ctx) → cancelled
B: wait → continues waiting
C: wait → continues waiting
```

- If the leader is cancelled, waiters continue waiting
- If all waiters are cancelled, loader may be cancelled

#### C. Loader Panic

```go
loader() → panic
```

- Fill state is released
- Key is not permanently stuck

#### D. Stale Result (Critical Race)

```go
T1: loader A starts (generation=42)
T2: Set(k, newer) (generation=43)
T3: loader A finishes
T4: loader A checks: generation 42 != 43
T5: loader A discards result
```

**Guaranteed**: A fill cannot overwrite a newer value. The generation fence prevents this.

## Deletion Semantics

### Single Process Delete

```
Delete(k) → Get(k) returns ErrNotFound
```

**Guaranteed**: After `Delete` returns, subsequent `Get` operations will not return the deleted value.

### Concurrent Delete and Get

```
T1: Get(k) starts
T2: Delete(k)
T3: Get(k) returns old value (already in progress)
T4: Get(k) starts (new request)
T5: Get(k) returns ErrNotFound
```

**Behavior**: Already-in-progress `Get` operations may return the old value. New `Get` operations after `Delete` returns will not.

### Delete During Fill

```
T1: Get(k) starts, captures generation=42
T2: Fill begins
T3: Delete(k), generation → 43
T4: Fill completes, checks generation 42 != 43
T5: Fill discards result
```

**Guaranteed**: A fill that started before a `Delete` cannot publish the old value.

## Tier Promotion Semantics

### Promotion is Conditional

```go
Promote(key, value, observedGeneration)
  │
  ├── Check: observedGeneration == currentGeneration
  │   ├── Yes → commit promotion
  │   └── No → discard promotion
  │
  └── Return data to caller regardless
```

**Guaranteed**: Promotion only succeeds if the generation has not changed since the value was read.

### Promotion Failure is Non-Fatal

If promotion fails (due to generation mismatch, disk full, etc.):
- Caller still receives the data
- Cache is not populated
- This is a cache degradation, not a data retrieval failure

## ObjectStore Consistency

### Local State

- Catalog is atomically updated via temp file + rename
- Segment files are append-only
- Flush is asynchronous but catalog updates are synchronous

### Remote State

- Checkpoint visibility may be delayed by TTL
- Concurrent writers use last-writer-wins
- External coordination required for same-key ordering

### Recovery

- On startup, local state is recovered from catalog
- Missing segments are repaired from remote
- Stale local generations are rejected

## Testing

### Required Test Scenarios

1. **Set then Get**: Verify read-after-write within single process
2. **Concurrent Sets**: Verify last-writer-wins behavior
3. **Delete then Get**: Verify deletion is visible
4. **Concurrent Delete and Get**: Verify in-progress gets can return old value
5. **Fill then Delete**: Verify fill is discarded after delete
6. **Generation Fence**: Verify stale fills are discarded
7. **Loader Error**: Verify all waiters receive error
8. **Loader Panic**: Verify fill state is released

### Stress Testing

Run with race detector:

```bash
go test -race ./...
```

Run stress tests:

```bash
go test -run TestCrashConsistency -count=100
```
