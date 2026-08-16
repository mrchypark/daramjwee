# Generation Invariant

This document defines the generation-based invariant that ensures correctness in concurrent Delete/Get/promotion operations.

## Core Invariant

> **A value may be promoted into a higher tier only if the generation observed when the value was retrieved is still current at promotion commit time.**

This invariant ensures that:
1. A deleted key cannot be resurrected by a concurrent promotion
2. Stale data from before a delete cannot be promoted after the delete completes

## Generation Lifecycle

```
Key X
  │
  ├── generation = 41 (initial)
  │
  ├── Get(X) starts
  │   └── captures generation = 41
  │
  ├── Delete(X)
  │   └── generation → 42
  │
  ├── Get attempts promotion
  │   └── checks: observedGeneration(41) == currentGeneration(42)?
  │       ├── No → discard promotion
  │       └── Yes → commit promotion
  │
  └── Result: stale data is never promoted
```

## Implementation

### Delete Operation

```go
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
    coord := c.topWrites.coordinator(key)
    
    // Step 1: Begin delete (increments generation)
    if err := coord.beginDelete(ctx); err != nil {
        coord.releaseReference()
        return err
    }
    
    // Step 2: Physical delete (bottom-up)
    defer coord.finishDelete(topDeleteSucceeded)
    
    // ... delete from all tiers ...
}
```

### Get Operation

```go
func (c *DaramjweeCache) Get(ctx context.Context, key string, req GetRequest, fetcher Fetcher) (*GetResponse, error) {
    // Capture current generation at start
    topGenerationAtStart := c.currentTopWriteGeneration(key)
    defer topGenerationAtStart.release()
    
    // ... lookup tiers ...
    // Pass topGenerationAtStart to promotion functions
}
```

### Promotion Check

```go
func (c *writeCoordinator) reserveWithFill(ctx context.Context, expected uint64, fill *topFillSink) (uint64, error) {
    c.stateMu.Lock()
    defer c.stateMu.Unlock()
    
    // Check if generation matches expected
    if c.latestGenerationLocked() != expected {
        return 0, ErrTopWriteInvalidated
    }
    
    // Reserve new generation
    generation := c.reserveGenerationLocked()
    // ...
}
```

## Concurrency Scenarios

### Scenario 1: Delete During Get

```
Time    Goroutine A (Get)         Goroutine B (Delete)
─────   ─────────────────────     ──────────────────────
T1      Get(X) starts
T2      Capture generation=5
T3      Read from Tier 1
T4                                Delete(X) starts
T5                                generation → 6
T6                                Delete Tier 1
T7      Attempt promotion
T8      Check: gen 5 != 6
T9      Discard promotion
T10     Return data to caller
```

**Result**: Data is returned to caller, but promotion is discarded.

### Scenario 2: Delete During Fill

```
Time    Goroutine A (Get)         Goroutine B (Delete)
─────   ─────────────────────     ──────────────────────
T1      Get(X) starts
T2      Capture generation=5
T3      Miss all tiers
T4      Fetch from origin
T5                                Delete(X) starts
T6                                generation → 6
T7      Fetch completes
T8      Attempt publish
T9      Check: gen 5 != 6
T10     Discard publish
```

**Result**: Origin data is not published after delete.

### Scenario 3: Concurrent Promotions

```
Time    Goroutine A (Get)         Goroutine B (Get)
─────   ─────────────────────     ──────────────────────
T1      Get(X) starts             Get(X) starts
T2      Capture generation=5      Capture generation=5
T3      Read from Tier 1          Read from Tier 1
T4      Attempt promotion         Attempt promotion
T5      Acquire write lease       Wait for write lease
T6      Check: gen 5 == 5
T7      Commit promotion
T8      generation → 6
T9                                Acquire write lease
T10                               Check: gen 5 != 6
T11                               Discard promotion
```

**Result**: Only one promotion succeeds.

## Testing

### Required Test Scenarios

1. **Delete/Get race**: Get reads old value, Delete increments generation, promotion is discarded
2. **Delete during fill**: Origin fetch completes after delete, publish is discarded
3. **Concurrent promotions**: Only one promotion succeeds per generation
4. **Generation mismatch**: Promotion with stale generation is rejected

### Test Implementation

```go
func TestGenerationFence_DeleteDuringPromotion(t *testing.T) {
    // Setup
    top := memstore.New(0, nil)
    lower := memstore.New(0, nil)
    
    // Seed lower tier
    // ...
    
    // Start Get (captures generation)
    // Delete (increments generation)
    // Verify promotion is discarded
}
```

## Verification

To verify the invariant is maintained:

1. Check that `reserveWithFill` validates generation before committing
2. Check that `finishDelete` increments generation
3. Check that promotion functions pass `expectedGeneration` to write operations
4. Check that tests cover all concurrency scenarios
