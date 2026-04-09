# Daramjwee Maintainability Refactor Plan

## Goal

Reduce regression risk in the core cache orchestration path while making the codebase easier to reason about, test, and evolve.

## Current Assessment

### 1. Style Consistency

- Production Go code is mostly consistent in naming, doc comments, and API shape.
- Test code is less consistent: `require/assert` and stdlib assertions are mixed freely, and comments switch between English and Korean.
- Terminology is mostly coherent, but `top/hot`, `lower/cold`, `regular/persist` appear interchangeably across files and tests.

### 2. Orthogonal Design Boundaries

- The main design problem is concentration, not abstraction count.
- [`cache.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/cache.go) mixes public API entrypoints, tier traversal, freshness policy, origin fetch handling, promotion/fanout logic, and stream lifecycle helpers.
- [`write_coordinator.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/write_coordinator.go) correctly isolates generation coordination, but the core cache path still reaches into it at many semantic layers.
- Store packages are generally better scoped. `objectstore` is large, but it is already split into supporting files more effectively than the root package.

### 3. Maintainability / Readability

- The biggest readability risk is branching density in `Get`, stale refresh flows, and generation-aware publish logic.
- Helper wrappers were under-tested relative to how much correctness they carry.
- Internal objectstore helpers such as page/block caches and segment staging were simple but previously lacked direct regression tests.

### 4. Comment / Code Alignment

- Production comments are generally aligned with implementation.
- The main risk is not incorrect comments; it is that some functions are too broad for comments alone to keep intent obvious.
- A few comments are underspecified rather than wrong, for example wrappers whose correctness depends on idempotent close/abort behavior but did not say so explicitly.

## Refactor Priorities

### Slice 1: Split Core Cache Orchestration

Goal: reduce cognitive load in the root package without changing behavior.

Files:
- Modify: [`cache.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/cache.go)
- Create: `cache_get.go`
- Create: `cache_refresh.go`
- Create: `cache_persist.go`
- Create: `cache_context.go`

Steps:
1. Move freshness and context-selection helpers into a focused file.
2. Move refresh/promotion fallback paths into a dedicated refresh file.
3. Move persist/fanout scheduling into a dedicated persist file.
4. Keep `Get`, `Set`, `Delete`, and `Close` as thin entrypoints.

Verification:
- `go test ./...`
- No behavior change in existing integration tests.

### Slice 2: Narrow Internal Surface Area

Goal: make invariants explicit and reduce accidental misuse.

Files:
- Modify: [`cache.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/cache.go)
- Modify: [`daramjwee.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/daramjwee.go)

Steps:
1. Review which `DaramjweeCache` fields need to remain exported.
2. Prefer private state unless an external consumer truly depends on direct field access.
3. Add short invariant comments around generation-sensitive publish paths.

Verification:
- API compatibility review before changing exported fields.
- `go test ./...`

### Slice 3: Normalize Test Conventions

Goal: make future regression tests easier to read and cheaper to maintain.

Files:
- Modify: high-churn test files under `/tests`, root package tests, and store package tests

Steps:
1. Standardize on one assertion style per file.
2. Normalize test naming around behavior and outcome.
3. Remove mixed-language comments in frequently edited tests unless domain-specific Korean is intentional.

Verification:
- `go test ./...`
- Spot-check top-level test files for consistent structure.

### Slice 4: Continue Closing Targeted Test Gaps

Goal: protect simple but correctness-critical helpers before refactors land.

Already added in this session:
- [`response_and_cancel_test.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/response_and_cancel_test.go)
- [`cache_test.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/pkg/store/objectstore/internal/pagecache/cache_test.go)
- [`cache_test.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/pkg/store/objectstore/internal/blockcache/cache_test.go)
- [`segment_test.go`](/Users/cypark/.codex/worktrees/00ba/daramjwee/pkg/store/objectstore/internal/segment/segment_test.go)

Next tests to add:
1. Direct root-package tests for generation-invalidated publish/abort edge cases that currently live only in broad integration coverage.
2. Direct tests for `GetStatus.String()` and null eviction policy trivial helpers if coverage targets matter.
3. Additional `segment` failure-path tests by stubbing `syncDir`/filesystem failures if those paths become churn-heavy.
