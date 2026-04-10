# CacheGroup Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `CacheGroup` as a shared background-runtime owner for multiple caches with different tier chains while preserving cache-local policy and close semantics.

**Architecture:** Keep `Cache` responsible for tiers, freshness, fanout, and fetch behavior. Add a narrow background runtime abstraction, implement a bounded shared group runtime with per-cache queues and weighted dequeue fairness, and make both standalone caches and group-attached caches satisfy the same cache-local shutdown contract.

**Tech Stack:** Go, `go test`, `testify`, `go-kit/log`, existing `internal/worker` package

---

## File Map

### New Files

- `group.go`
  - public `CacheGroup` interface
  - `NewGroup(...)`
  - group constructor wiring
- `group_options.go`
  - `GroupConfig`
  - `GroupOption`
  - group-only option helpers such as `WithGroupWorkers(...)`, `WithGroupWorkerQueueDefault(...)`, and `WithGroupCloseTimeout(...)`
- `background_runtime.go`
  - `backgroundRuntime` interface
  - `JobKind`
  - `CacheRuntimeConfig`
  - shared runtime helper types
- `background_runtime_standalone.go`
  - standalone runtime wrapper used by `New(...)`
- `background_runtime_group.go`
  - bounded shared group runtime
  - per-cache queue registration
  - weighted dequeue scheduler
- `background_runtime_test.go`
  - root-package tests for runtime registration, queue limits, fairness, and shutdown
- `tests/cache_group_integration_test.go`
  - external API coverage for `NewGroup(...)`, shared-runtime caches, and group lifecycle
- `tests/cache_group_close_test.go`
  - external coverage for cache-local close semantics, queued-job dropping, and in-flight cancellation

### Existing Files To Modify

- `daramjwee.go`
  - keep `New(...)` behavior intact
  - create standalone runtime instead of constructing a worker manager directly in the cache
  - add or move public `CacheGroup` declarations if kept here instead of `group.go`
- `cache.go`
  - replace direct worker ownership with runtime ownership
  - add per-cache runtime identity and close behavior
  - preserve cache-local quiescence contract
- `cache_refresh.go`
  - submit refresh work through `backgroundRuntime`
- `cache_persist.go`
  - submit persist work through `backgroundRuntime`
- `options.go`
  - reject group-only options in standalone cache construction
  - add cache-runtime options `WithWeight(...)` and `WithQueueLimit(...)`
  - keep existing option validation clear and explicit
- `options_test.go`
  - extend constructor validation for standalone vs group-attached option mixes
- `constructor_validation_external_test.go`
  - external constructor compatibility coverage for `New(...)` and `NewGroup(...)`
- `README.md`
  - document `NewGroup(...)`
  - clarify standalone vs shared runtime ownership
- `CHANGELOG.md`
  - document the new `CacheGroup` API and option split

### Existing Files To Reuse As References

- `internal/worker/worker.go`
  - existing worker-manager boundary to reuse for standalone runtime
- `internal/worker/pool_strategy.go`
  - bounded worker behavior that group runtime should mirror at a higher level
- `tests/cache_regression_test.go`
  - existing refresh/persist semantics and queue pressure patterns
- `tests/cache_worker_stress_test.go`
  - existing close-under-pressure behavior to preserve for standalone caches

## Chunk 1: Runtime and API Boundary

### Task 1: Lock Down Constructor and Option Rules First

**Files:**
- Modify: `options_test.go`
- Modify: `constructor_validation_external_test.go`
- Test: `options_test.go`
- Test: `constructor_validation_external_test.go`

- [ ] **Step 1: Write failing standalone/group option typing and validation tests**

```go
func TestNew_RejectsGroupRuntimeOptions(t *testing.T) {
    _, err := New(nil, WithTiers(&optionsTestMockStore{id: 1}), WithWeight(2))
    require.Error(t, err)
    require.Contains(t, err.Error(), "group-attached cache option")
}

func TestNewGroup_UsesGroupOptionSurface(t *testing.T) {
    _, err := NewGroup(nil, WithGroupWorkers(1), WithGroupWorkerQueueDefault(8))
    require.NoError(t, err)
}

func TestNewCache_RejectsStandaloneWorkerOptions(t *testing.T) {
    group, err := NewGroup(nil, WithGroupWorkers(1))
    require.NoError(t, err)
    defer group.Close()

    _, err = group.NewCache("bad", WithTiers(&optionsTestMockStore{id: 1}), WithWorkers(2))
    require.Error(t, err)
    require.Contains(t, err.Error(), "standalone worker option")
}
```

- [ ] **Step 2: Run targeted tests to verify failure**

Run: `go test ./... -run 'Test(New_|Option).*'`
Expected: FAIL because `NewGroup`, `WithGroupWorkers`, `WithWeight`, and group/cache option separation do not exist yet

- [ ] **Step 3: Add minimal option and constructor surface**

```go
type GroupOption func(*GroupConfig) error

func WithGroupWorkers(count int) GroupOption {
    ...
}

func WithWeight(weight int) Option {
    return func(cfg *Config) error {
        if weight <= 0 {
            return &ConfigError{"cache weight must be positive"}
        }
        cfg.RuntimeWeight = weight
        return nil
    }
}
```

- [ ] **Step 4: Implement explicit invalid-mix validation where the type system cannot enforce it**

```go
if cfg.RuntimeWeight != 0 {
    return nil, &ConfigError{"WithWeight is only valid for group-attached caches"}
}
```

- [ ] **Step 5: Re-run targeted tests**

Run: `go test ./... -run 'Test(New_|Option).*'`
Expected: PASS for new validation coverage and existing option tests

- [ ] **Step 6: Commit**

```bash
git add options.go group_options.go options_test.go constructor_validation_external_test.go daramjwee.go
git commit -m "refactor: split cache and group option validation"
```

### Task 2: Introduce the Background Runtime Abstraction

**Files:**
- Create: `background_runtime.go`
- Create: `background_runtime_standalone.go`
- Modify: `cache.go`
- Modify: `daramjwee.go`
- Test: `background_runtime_test.go`

- [ ] **Step 1: Write failing runtime-shape tests**

```go
func TestStandaloneRuntime_CloseWaitsForRunningJobs(t *testing.T) {
    rt := newStandaloneRuntime(...)
    // submit blocking job, close, unblock, assert close returns
}
```

- [ ] **Step 2: Run the runtime test to verify failure**

Run: `go test ./... -run 'TestStandaloneRuntime_'`
Expected: FAIL because the runtime abstraction does not exist

- [ ] **Step 3: Add the narrow runtime interface and job kinds**

```go
type backgroundRuntime interface {
    Register(cacheID string, cfg CacheRuntimeConfig) error
    Submit(cacheID string, kind JobKind, job worker.Job) bool
    CloseCache(cacheID string, timeout time.Duration) error
    Shutdown(timeout time.Duration) error
}
```

- [ ] **Step 4: Wrap the existing worker manager as a standalone runtime**

```go
type standaloneRuntime struct {
    manager *worker.Manager
    ...
}
```

- [ ] **Step 5: Replace direct `worker.Manager` ownership in `DaramjweeCache`**
- [ ] **Step 5: Add runtime ownership to `DaramjweeCache` without deleting `c.worker` yet**

```go
type DaramjweeCache struct {
    worker  *worker.Manager // temporary bridge until refresh/persist switch to runtime
    runtime backgroundRuntime
    cacheID string
    ...
}
```

- [ ] **Step 6: Re-run runtime and cache constructor tests**

Run: `go test ./... -run 'Test(StandaloneRuntime_|New_)'`
Expected: PASS with a compiling tree and no standalone behavior change

- [ ] **Step 7: Commit**

```bash
git add background_runtime.go background_runtime_standalone.go cache.go daramjwee.go background_runtime_test.go
git commit -m "refactor: introduce background runtime abstraction"
```

### Task 3: Implement the Shared Group Runtime and Public Group API

**Files:**
- Create: `group.go`
- Create: `background_runtime_group.go`
- Modify: `daramjwee.go`
- Test: `background_runtime_test.go`
- Test: `tests/cache_group_integration_test.go`

- [ ] **Step 1: Write failing tests for group registration and queue isolation**

```go
func TestGroupRuntime_QueueIsolation(t *testing.T) {
    group, err := NewGroup(nil, WithGroupWorkers(1), WithGroupWorkerQueueDefault(1))
    require.NoError(t, err)
    ...
}
```

- [ ] **Step 2: Run targeted group tests to verify failure**

Run: `go test ./... -run 'Test(GroupRuntime_|CacheGroup_)'`
Expected: FAIL because `NewGroup(...)`, `WithGroupWorkers(...)`, and the group runtime do not exist

- [ ] **Step 3: Add `CacheGroup` and `NewGroup(...)`**

```go
type CacheGroup interface {
    NewCache(name string, opts ...Option) (Cache, error)
    Close()
}
```

- [ ] **Step 4: Implement bounded per-cache queues plus weighted dequeue**

```go
type groupRuntime struct {
    workers int
    caches  map[string]*registeredCache
    ...
}
```

- [ ] **Step 5: Ensure group runtime is pool-only and uses only group-prefixed runtime options**

```go
if cfg.GroupWorkerStrategy != "" {
    return nil, &ConfigError{"group runtime uses a bounded shared pool only"}
}
```

- [ ] **Step 6: Re-run group runtime and API tests**

Run: `go test ./... -run 'Test(GroupRuntime_|CacheGroup_|NewGroup_)'`
Expected: PASS for registration, queue isolation, and constructor validation

- [ ] **Step 7: Commit**

```bash
git add group.go group_options.go background_runtime_group.go background_runtime_test.go tests/cache_group_integration_test.go
git commit -m "feat: add shared cache group runtime"
```

## Chunk 2: Cache Integration, Lifecycle, and Docs

### Task 4: Switch Refresh and Persist Paths to the Runtime and Remove the Worker Bridge

**Files:**
- Modify: `cache_refresh.go`
- Modify: `cache_persist.go`
- Modify: `cache.go`
- Test: `tests/cache_regression_test.go`
- Test: `tests/cache_group_integration_test.go`

- [ ] **Step 1: Add failing external tests for shared-cache refresh and persist**

```go
func TestCacheGroup_ScheduleRefresh_PersistsAcrossSharedRuntime(t *testing.T) {
    // build group, create hot+cold cache, schedule refresh, verify top and lower-tier behavior
}
```

- [ ] **Step 2: Run the targeted integration tests to verify failure**

Run: `go test ./... -run 'Test(CacheGroup_|ScheduleRefresh_)'`
Expected: FAIL because refresh/persist paths still submit directly through cache-local worker ownership

- [ ] **Step 3: Replace direct submission in refresh flow**

```go
if !c.runtime.Submit(c.cacheID, JobKindRefresh, job) {
    return ErrBackgroundJobRejected
}
```

- [ ] **Step 4: Replace direct submission in persist flow**

```go
if !c.runtime.Submit(c.cacheID, JobKindPersist, job) {
    c.warnLog("msg", "background set rejected", "key", key, "dest_tier", destTierIndex)
}
```

- [ ] **Step 5: Re-run regression and shared-runtime integration tests**

Run: `go test ./... -run 'Test(CacheGroup_|ScheduleRefresh_|Cache_CloseDuringRefreshQueuePressureStress)'`
Expected: PASS for both standalone and group-attached caches

- [ ] **Step 6: Remove the temporary `c.worker` bridge once callers are switched**

```go
type DaramjweeCache struct {
    runtime backgroundRuntime
    cacheID string
    ...
}
```

- [ ] **Step 7: Commit**

```bash
git add cache_refresh.go cache_persist.go cache.go tests/cache_group_integration_test.go tests/cache_regression_test.go
git commit -m "refactor: route background cache jobs through runtime"
```

### Task 5: Implement Cache-Local Close Quiescence on Top of Shared Runtime

**Files:**
- Modify: `cache.go`
- Modify: `background_runtime_group.go`
- Modify: `background_runtime_standalone.go`
- Test: `tests/cache_group_close_test.go`
- Test: `tests/cache_worker_stress_test.go`
- Test: `background_runtime_test.go`

- [ ] **Step 1: Write failing close semantics tests first**

```go
func TestCacheGroup_CloseCache_DropsQueuedWorkAndWaitsForRunningJobs(t *testing.T) {}
func TestCacheGroup_Close_StopsFutureDequeuesAndTimesOutBestEffort(t *testing.T) {}
func TestCacheGroup_CloseOrdering_CacheThenGroupIsSafe(t *testing.T) {}
func TestCacheGroup_CloseOrdering_GroupThenCacheIsSafe(t *testing.T) {}
func TestCacheGroup_Close_IsIdempotent(t *testing.T) {}
func TestCacheGroup_CloseCache_IsIdempotent(t *testing.T) {}
func TestCacheGroup_Close_ConcurrentSubmitIsSafe(t *testing.T) {}
func TestCacheGroup_CloseCache_DequeueRaceRechecksClosedState(t *testing.T) {}
func TestCacheGroup_Close_CancelsRunningJobContext(t *testing.T) {}
```

- [ ] **Step 2: Run the targeted close tests to verify failure**

Run: `go test ./... -run 'Test(CacheGroup_Close|Cache_CloseDuringRefreshQueuePressureStress|StandaloneRuntime_Close)'`
Expected: FAIL because shared runtime cache-close quiescence does not exist yet

- [ ] **Step 3: Add per-cache cancellation and close waiting**

```go
func (r *groupRuntime) CloseCache(cacheID string, timeout time.Duration) error {
    // mark closed, cancel cache context, drop queued jobs, wait for in-flight jobs
}
```

- [ ] **Step 4: Add group shutdown behavior**

```go
func (g *cacheGroup) Close() {
    _ = g.runtime.Shutdown(g.closeTimeout)
}
```

- [ ] **Step 5: Re-run close, stress, and regression coverage**

Run: `go test ./... -run 'Test(CacheGroup_Close|Cache_CloseDuringRefreshQueuePressureStress|ScheduleRefresh_|StandaloneRuntime_Close)'`
Expected: PASS for clean shutdown, ordering, idempotency, concurrent close-vs-submit, and timeout-path behavior

- [ ] **Step 6: Commit**

```bash
git add cache.go background_runtime_group.go background_runtime_standalone.go tests/cache_group_close_test.go tests/cache_worker_stress_test.go background_runtime_test.go
git commit -m "feat: preserve cache-local close semantics in shared runtime"
```

### Task 6: Finish Documentation and Compatibility Coverage

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `options_test.go`
- Test: `tests/cache_group_integration_test.go`

- [ ] **Step 1: Add failing doc-adjacent tests if constructor examples changed**

```go
func TestNewGroup_StandaloneWorkerOptionsRejected(t *testing.T) {
    ...
}
```

- [ ] **Step 2: Update README and changelog**

```md
- `New(...)` still creates a self-contained cache.
- `NewGroup(...)` creates multiple caches that share one bounded background runtime.
```

- [ ] **Step 3: Run the full test suite**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 4: Spot-check package docs and examples**

Run: `rg -n "WithWorkerStrategy|NewGroup|WithWeight|WithQueueLimit" README.md CHANGELOG.md examples tests`
Expected: only valid standalone references to `WithWorkerStrategy("all")`; group docs should describe pool-only shared runtime

- [ ] **Step 5: Commit**

```bash
git add README.md CHANGELOG.md options_test.go tests/cache_group_integration_test.go docs/plans/2026-04-10-cache-group-design.md
git commit -m "docs: document cache group runtime behavior"
```

## Execution Notes

- Keep `Cache` interface unchanged in v1.
- Do not bring the standalone `"all"` worker strategy into `CacheGroup`.
- Prefer additive files over overloading `cache.go` further.
- Preserve the observable `Cache.Close()` contract for both standalone and group-attached caches.
- Keep predictive warming out of this implementation slice.

## Verification Checklist

- `New(...)` remains backward-compatible for existing callers
- `NewGroup(...)` can create multiple caches with different tier chains
- one cache queue filling up does not block submissions to another cache
- `Cache.Close()` on a group-attached cache drops queued jobs for that cache and waits for in-flight work up to timeout
- `CacheGroup.Close()` shuts down the shared runtime without leaving accepted jobs running indefinitely
- cache close then group close is safe
- group close then cache close is safe
- repeated close calls are idempotent
- concurrent close vs submit does not race into invalid state
- a job that has been dequeued but not yet started re-checks closed state before executing
- an in-flight job observes cache/group cancellation through its context and exits via cleanup
- existing standalone worker regression and stress tests still pass

## Handoff

Plan complete and saved to `docs/plans/2026-04-10-cache-group-implementation.md`. Ready to execute?
