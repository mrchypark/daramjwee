# Daramjwee Codebase Analysis (v0.9.2, commit 5771bae)

## 1. Write Coordination — Three Sink Types

### Sink Types in `write_coordinator_sinks.go`

| Sink | Lease Held | Commit Model | Use Case |
|------|------------|--------------|----------|
| `coordinatedTopWriteSink` | **Yes** (entire writer lifetime) | Single-phase `Close()` | Non-staging stores (`Store.BeginSet` only) |
| `coordinatedStagedTopWriteSink` | **No** (only short commit phase) | Two-phase `Commit(ctx)` | `StagingStore` implementations (objectstore) |
| `conditionalGenerationWriteSink` | **No** | Single-phase `Close()` | Fanout persistence to lower tiers |

### Shared Patterns

All three sinks share:
- **Generation validation** at `Close()`: check `committedGeneration > generation` → invalidate
- **Post-close wait** for `activeDeletes == 0` (prevents delete-from-top racing persist)
- **`onInvalidated` callback** for cleanup (deletes key from destination on invalidation)
- **`sync.Once`** for terminal operations

### Divergence Points

| Aspect | Top (coordinated) | Staged | Conditional (fanout) |
|--------|-------------------|--------|----------------------|
| Lease acquisition | `acquireWrite()` held to Close | `lockCommitWhenNoActiveDeletes()` only for commit | None |
| Reservation unregister | On Close/Abort + detachForFillPreempt | On Close/Abort + detachForFillPreempt | Never (generation borrowed from parent) |
| State mutation on success | Advances `committedGeneration`, prunes | Advances `committedGeneration`, prunes | **Read-only** — never advances generation |
| `Abort()` behavior | Unregisters reservation | Unregisters reservation | No-op on coordinator |

### Unification Path — **Real but Partial**

**Common base interface** extractable:
```go
type generationSink interface {
    io.WriteCloser
    Abort() error
    generation() uint64
    coordinator() *writeCoordinator
    waitTimeout() time.Duration
    onInvalidated() func() error
    setError(error)
}
```

**Shared `closeCore` function** possible for the validation/wait/cleanup sequence (≈60 lines duplicated across 3 sinks).

**Cannot fully unify** because:
- Lease semantics fundamentally differ (held vs short-lived vs none)
- `conditionalGenerationWriteSink` is **read-only** on coordinator — no state mutation
- Staged vs non-staged commit protocol mismatch

**Recommendation**: Extract `closeCore` helper + `generationSink` interface. Keep 3 concrete types.

---

## 2. Read Path — `handleLowerTierHit` Decision Logic

### Current Structure (cache_read.go:73-101)

`handleLowerTierHit` mixes **decision** (which action to take) with **execution** (how to do it):

```go
func handleLowerTierHit(p lowerTierHitParams) (*GetResponse, error) {
    // Decision tree:
    if !higherTiersClean     -> serveLowerTierWithoutPromotion
    else if conditional hit  -> handleConditionalLowerTierHit
    else if negative entry   -> promoteNegative... / handleStale...
    else if stale            -> handleStaleLowerTierHit
    else                     -> promotePositiveLowerTierHit
}
```

Each branch:
1. Makes policy decision (serve stale? promote? conditional?)
2. Acquires writers (`setStreamToTopStore...`)
3. Copies data (`io.Copy`, `streamThroughWithTrace`)
4. Handles errors / invalidation

### Separation Opportunity

**Decision → Execution separation** would yield:

```go
type lowerTierAction int
const (
    actionServeDirect lowerTierAction = iota
    actionServeConditional
    actionPromoteNegative
    actionPromotePositive
    actionServeStaleWithRefresh
)

type lowerTierDecision struct {
    action       lowerTierAction
    isStale      bool
    canConditional bool
    meta         *Metadata
    // ... params needed for execution
}

func (c *DaramjweeCache) decideLowerTierHit(p lowerTierHitParams) lowerTierDecision
func (c *DaramjweeCache) executeLowerTierDecision(ctx, setupCtx, p, decision) (*GetResponse, error)
```

**Benefits**:
- Decision logic becomes pure, testable without store mocks
- Execution paths share common writer acquisition / stream copy / error handling
- Eliminates duplicate `cancel()` / `src.Close()` / error wrapping patterns

**Risk**: Medium — execution paths have subtle differences (e.g., `streamThroughWithTrace` vs `newCancelOnCloseReadCloser` vs direct copy). But the decision tree itself is a clear separable concern.

---

## 3. ObjectStore — Minimal Interface for Cache Core

### What Cache Core Actually Uses from `Store` (daramjwee.go:233-267)

```go
type Store interface {
    GetStream(ctx, key) (io.ReadCloser, *Metadata, error)
    BeginSet(ctx, key, *Metadata) (WriteSink, error)
    Delete(ctx, key) error
    Stat(ctx, key) (*Metadata, error)
}
```

### Optional Extensions (capability detection via type assertion)

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `GetStreamUsesContext` | cache_context.go:85-88 | Determines if returned reader needs request ctx vs setup ctx |
| `BeginSetUsesContext` | cache_context.go:90-93 | Determines if sink needs request ctx vs setup ctx |
| `StagingStore` | write_coordinator.go:574,615,660 | Enables two-phase commit, avoids holding write lease |
| `TierValidator` | options.go:404-408 | Validates tier position at init |

### ObjectStore Provides Beyond Minimal

| Feature | In Store? | Used by Cache Core? |
|---------|-----------|---------------------|
| `BlockCache` / `PageCache` | No (internal) | No |
| `Catalog` (local manifest index) | No | No |
| `GC` / `Compaction` / `Flush` | No | No |
| `CheckpointCache` | No | No |
| `Paged/Whole` layout selection | No | No |

**Conclusion**: The cache core needs **only the 4-method `Store` interface + 3 optional capability interfaces**. All objectstore internals (segments, catalog, block/page cache, GC, compaction, layout logic) are correctly encapsulated.

**No interface change needed** — current boundary is clean.

---

## 4. Cache Core — Config vs Runtime State

### `cacheConfig` (cache.go:32-40) — **Immutable after construction**

| Field | Source | Mutable? |
|-------|--------|----------|
| `opTimeout` | `WithOpTimeout` | No |
| `closeTimeout` | `WithCloseTimeout` | No |
| `fillLeaseTimeout` | `WithFillLeaseTimeout` | No |
| `positiveFreshness` | `WithFreshness` | No |
| `negativeFreshness` | `WithFreshness` | No |
| `tierFreshnessOverrides` | `WithTierFreshness` | No |
| `loggingDisabled` | Derived from logger | No |

### `DaramjweeCache` fields (cache.go:43-53) — **Runtime state**

| Field | Category |
|-------|----------|
| `tiers []Store` | Config (but mutable slice — should be immutable) |
| `logger` | Config |
| `runtime` | Config (set at construction) |
| `cacheID` | Config |
| `config cacheConfig` | Config |
| `closeHook func()` | Runtime (set post-construction) |
| `isClosed atomic.Bool` | Runtime |
| `topWrites topWriteManager` | Runtime (per-key coordinators) |
| `fanoutWrites fanoutWriteManager` | Runtime (per-key/dest locks) |

### Issues Found

1. **`tiers` slice is mutable** — no protection against post-construction modification
2. **`closeHook`** is the only post-construction config — should move to options
3. **`fillLeaseTimeout`** stored in `Config` but copied to `cacheConfig` — redundant
4. **Worker config** (`WorkerStrategy`, `Workers`, etc.) only used during `buildCacheConfig` → not stored in cache at all (correct)

---

## 5. Interface Boundaries — Cache Core ↔ Store

### What Cache Core Needs (Actual Usage)

| Method | Call Sites | Context Used |
|--------|------------|--------------|
| `GetStream` | cache.go:71, cache_read.go:172, cache_persist.go:62, cache_refresh.go:172, 237 | `getStreamContextForStore` (request vs setup) |
| `BeginSet` | write_coordinator.go:597, 638, 707 | `beginSetContextForStore` (request vs setup) |
| `BeginStagedSet` | write_coordinator.go:579, 620, 672 | Same as above |
| `Delete` | cache.go:166, 178, cache_persist.go:85, cache_refresh.go:163 | `newCtxWithTimeout` (bounded) |
| `Stat` | cache.go:282, cache_refresh.go:42, 136, 149, 198 | `statFromStore` (with opTimeout) |

### What Store Provides (But Cache Core Doesn't Use)

- Nothing — the `Store` interface is **minimal and sufficient**

### What Cache Core Implicitly Assumes (Not in Interface)

| Assumption | Where Enforced |
|------------|----------------|
| `GetStream` returns non-nil `*Metadata` on success | cache_context.go:152-156 |
| `BeginSet` keeps current value readable until Close/Abort | StagingStore docs + write_coordinator logic |
| `Delete` doesn't wait for uncommitted `BeginSet` on same key | StagingStore docs + Delete implementation |
| `WriteSink.Close()` publishes atomically | coordinatedTopWriteSink logic |

**Gap**: These are **documented in interface comments** but not enforced. Could add `Validate()` method to `TierValidator` or runtime checks in `New()`.

---

## Refactoring Plan

### Files to Create

| File | Purpose |
|------|---------|
| `write_coordinator_sink_base.go` | Shared `closeCore` logic + `generationSink` interface |
| `cache_read_decision.go` | `decideLowerTierHit` + `lowerTierDecision` type |
| `cache_read_execution.go` | `executeLowerTierDecision` + shared writer acquisition |

### Files to Modify

| File | Changes |
|------|---------|
| `write_coordinator_sinks.go` | Refactor 3 sinks to use `closeCore`; implement `generationSink` |
| `cache_read.go` | Split `handleLowerTierHit` → decision + execution |
| `cache.go` | Make `tiers` immutable (copy on config); remove `closeHook` field |
| `cache_context.go` | Move `closeHook` to Option pattern |
| `options.go` | Add `WithCloseHook` option |

### Interface Changes

| Change | Type | Risk |
|--------|------|------|
| `generationSink` interface | New internal | Low (internal only) |
| `closeCore` helper | New internal | Low |
| `lowerTierDecision` type | New internal | Low |
| `Store` interface | **None** | — |

### Risk Assessment

| Area | Risk | Mitigation |
|------|------|------------|
| Write sink unification | **Medium** — subtle lease differences | Keep 3 types; only extract shared close logic |
| Read path separation | **Medium** — execution paths differ | Extract decision only; keep execution per-action |
| Config immutability | **Low** — simple field changes | Add `tiers` copy in `newCacheFromConfig` |
| Test coverage | **High** — behavior must not change | Existing tests in `write_coordinator_test.go`, `cache_read_test.go` cover all paths |

### Estimated Complexity Reduction

| Metric | Before | After | Reduction |
|--------|--------|-------|-----------|
| `write_coordinator_sinks.go` lines | 297 | ~220 | ~25% (shared closeCore) |
| `handleLowerTierHit` cyclomatic complexity | ~18 | ~6 (decision) + ~8 (execution) | Separated concerns |
| Duplicated validation/wait/cleanup code | 3× (~60 lines) | 1× | ~120 lines removed |
| Config/runtime field confusion | 10 fields mixed | Clear separation | Documentation + immutability |

---

## Priority Order

1. **Write sink `closeCore` extraction** — highest ROI, lowest risk, ~120 lines deduplicated
2. **Read decision/execution separation** — improves testability, enables future policy changes
3. **Config immutability** — defensive, prevents bugs
4. **Tier slice copy** — one-line fix in `newCacheFromConfig`

All changes are **internal refactors** with no public API impact.