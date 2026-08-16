# Tier Promotion Semantics

This document defines the visibility semantics for tier promotion in daramjwee.

## Core Invariant

> **A value may be promoted into a higher tier only if the generation observed when the value was retrieved is still current at promotion commit time.**

## Promotion Three-Phase Model

### Overview

Every tier promotion follows three distinct phases:

```
Phase 1: Read Visibility
    │
    ▼
Phase 2: Promotion Eligibility
    │
    ▼
Phase 3: Promotion Commit
```

### Phase 1: Read Visibility

Data is read from a lower tier or origin. The reader captures the current generation at this point.

```
Read(key) from Tier N
  │
  ├── value, metadata
  ├── observedGeneration = currentGeneration(key)
  │
  └── return (value, observedGeneration)
```

**Key Points**:
- Generation is captured at read time, not at promotion time
- This snapshot is used later to validate eligibility

### Phase 2: Promotion Eligibility

Before promotion can proceed, eligibility is checked:

```
Eligible(value, observedGeneration)
  │
  ├── Check: no active delete (activeDeletes == 0)
  ├── Check: observedGeneration == currentGeneration
  │
  └── return eligible (true/false)
```

**Key Points**:
- If generation has changed (e.g., due to Delete), promotion is NOT eligible
- If there's an active delete, promotion is NOT eligible
- This check happens BEFORE any write begins

### Phase 3: Promotion Commit

If eligible, the value is atomically published to the higher tier:

```
Commit(value)
  │
  ├── Write to higher tier
  ├── Advance generation
  │
  └── visible to future readers
```

**Key Points**:
- Commit is atomic
- Generation is advanced after successful commit
- If commit fails, generation is NOT advanced

## Implementation in Code

### Write Coordinator

```go
func (c *writeCoordinator) reserveWithFill(ctx context.Context, expected uint64, fill *topFillSink) (uint64, error) {
    c.stateMu.Lock()
    defer c.stateMu.Unlock()
    
    // Phase 2: Eligibility check
    if c.activeDeletes > 0 || c.fillPreemptions > 0 {
        return 0, ErrTopWriteInvalidated
    }
    if c.latestGenerationLocked() != expected {
        return 0, ErrTopWriteInvalidated
    }
    
    // Phase 3: Reserve generation
    generation := c.reserveGenerationLocked()
    // ...
}
```

### Cache Read Path

```go
func (c *DaramjweeCache) Get(ctx context.Context, key string, req GetRequest, fetcher Fetcher) (*GetResponse, error) {
    // Phase 1: Capture generation
    topGenerationAtStart := c.currentTopWriteGeneration(key)
    defer topGenerationAtStart.release()
    
    // ... lookup tiers ...
    
    // Phase 2 & 3: Eligibility check and commit happen in promotion functions
    return c.promotePositiveLowerTierHit(ctx, setupCtx, key, tierIndex, src, meta, metaToPromote, cancel, topGenerationAtStart)
}
```

## Concurrency Scenarios

### Scenario 1: Delete During Promotion

```
Time    Goroutine A (Get)         Goroutine B (Delete)
─────   ─────────────────────     ──────────────────────
T1      Get(X) starts
T2      Capture generation=5      [Phase 1: Read Visibility]
T3      Read from Tier 1
T4                                Delete(X) starts
T5                                generation → 6
T6                                Delete Tier 1
T7      Attempt promotion         [Phase 2: Eligibility]
T8      Check: gen 5 != 6
T9      Discard promotion
T10     Return data to caller
```

**Result**: Data is returned to caller, but promotion is discarded. The deleted key stays deleted.

### Scenario 2: Delete During Fill

```
Time    Goroutine A (Get)         Goroutine B (Delete)
─────   ─────────────────────     ──────────────────────
T1      Get(X) starts
T2      Capture generation=5      [Phase 1: Read Visibility]
T3      Miss all tiers
T4      Fetch from origin
T5                                Delete(X) starts
T6                                generation → 6
T7      Fetch completes
T8      Attempt publish           [Phase 2: Eligibility]
T9      Check: gen 5 != 6
T10     Discard publish
```

**Result**: Origin data is not published after delete.

### Scenario 3: Concurrent Promotions

```
Time    Goroutine A (Get)         Goroutine B (Get)
─────   ─────────────────────     ──────────────────────
T1      Get(X) starts             Get(X) starts
T2      Capture generation=5      Capture generation=5    [Phase 1]
T3      Read from Tier 1          Read from Tier 1
T4      Attempt promotion         Attempt promotion       [Phase 2]
T5      Acquire write lease       Wait for write lease
T6      Check: gen 5 == 5
T7      Commit promotion                                   [Phase 3]
T8      generation → 6
T9                                Acquire write lease
T10                               Check: gen 5 != 6
T11                               Discard promotion
```

**Result**: Only one promotion succeeds.

### Scenario 4: Cache Fill Failure

```
Time    Goroutine A (Get)
─────   ─────────────────────
T1      Get(X) starts
T2      Capture generation=5
T3      Miss all tiers
T4      Fetch from origin → SUCCESS(data)
T5      Attempt cache write → ERROR
T6      Log warning
T7      Return data to caller (non-fatal)
```

**Result**: Caller receives data, cache is not populated.

## Guarantees

1. **No Partial Visibility**: A key is never visible with partially written data.

2. **No Resurrection**: A deleted key cannot be resurrected by a concurrent promotion.

3. **No Double Promotion**: Only one promotion can succeed for a given generation.

4. **Caller Always Gets Data**: Even if promotion fails, the caller receives the requested data.

5. **Fill Failure is Non-Fatal**: Cache fill failure does not cause data retrieval failure.

## Testing

The following test scenarios verify promotion semantics:

1. **Concurrent Get and Delete**: Verify that Delete prevents promotion.
2. **Concurrent Promotions**: Verify that only one promotion succeeds.
3. **Stale Promotion**: Verify that stale entries can be promoted and later refreshed.
4. **Generation Mismatch**: Verify that mismatched generations cause promotion failure.
5. **Cache Fill Failure**: Verify that fill failure does not affect data retrieval.
6. **Delete During Fill**: Verify that delete during origin fetch prevents publish.
