# Daramjwee V2 Tiering Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the fixed hot/cold model with n-tier cache plus optional durable tier, and promote objectstore into a first-party durable backend.

**Architecture:** Keep the current `Store` / `WriteSink` streaming contracts. Generalize cache orchestration to a regular tier slice plus a separate durable tier. Move objectstore out of `adapter/` and rebuild it around immutable blobs, manifest publish, and optional local page-cache backed range reads.

**Tech Stack:** Go, go-kit/log, thanos-io/objstore, existing daramjwee worker/background pipeline, current test and benchmark harness.

---

### Phase 1: Public API and Config Model

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/daramjwee.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/options.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/options_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/README.md`

**Intent:**
- Replace `HotStore` / `ColdStore` in config with `Tiers []Store` and `DurableTier Store`
- Add `WithTiers(...)` and `WithDurableTier(...)`
- Decide fate of `WithHotStore` / `WithColdStore`

**Tasks:**
1. Write failing config tests for:
   - zero tiers rejected
   - duplicate tier rejected
   - durable tier duplicated in tier list rejected
   - objectstore in `WithTiers(...)` rejected
2. Add new config fields and new options.
3. Keep deprecated shims only if strictly needed for migration tests.
4. Update README examples to new config style.
5. Run `go test ./...`

### Phase 2: Cache Core Generalization

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/streaming.go`
- Add: `/Users/cypark/Documents/project/daramjwee/tests/tiering_integration_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/api_contract_test.go`

**Intent:**
- Replace fixed hot/cold orchestration with `tiers + durable`
- Preserve pure streaming semantics

**Tasks:**
1. Add failing tests for:
   - lower-tier hit promotes to upper tiers
   - durable-tier hit backfills top tier
   - origin miss fills top tier and async persists downward
   - partial read still aborts publish
2. Refactor cache core helper names:
   - “top tier” instead of “hot store”
   - “regular tier” instead of “cold store”
   - “durable tier” explicitly separate
3. Generalize lookup order over `[]Store`.
4. Generalize background persistence fan-out.
5. Keep `streamThrough(...)` as the single miss/fill lifecycle.
6. Run `go test ./...`

### Phase 3: Promote Objectstore Backend

**Files:**
- Move/Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore`
- Migrate from: `/Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore.go`
- Add tests under: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore`

**Intent:**
- Replace thin adapter with first-party durable backend

**Tasks:**
1. Create new package skeleton:
   - `store.go`
   - `writer.go`
   - `reader.go`
   - `options.go`
   - `validation.go`
2. Port current tests into new package without behavior change.
3. Implement immutable blob + manifest publish model.
4. Keep initial layout support to `whole` only.
5. Keep hot-tier validation reject path.
6. Update imports/examples.
7. Run package tests and full `go test ./...`

### Phase 4: Add Paged Layout

**Files:**
- Add: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/manifest`
- Add: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/layout`
- Add: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/rangeio`
- Add tests in matching package tree

**Intent:**
- Support `layout=whole` and `layout=paged`

**Tasks:**
1. Add failing tests for:
   - large object chooses paged layout
   - paged reader returns full logical stream
   - abort before publish leaves no visible manifest
2. Implement manifest schema and codec.
3. Add layout decision options:
   - whole threshold
   - page size
4. Implement `GetRange`-based paged reader.
5. Run `go test ./...`

### Phase 5: Local Page Cache

**Files:**
- Add: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/pagecache`
- Add tests in same package
- Add benchmarks in `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore`

**Intent:**
- Add backend-internal local acceleration for paged reads

**Tasks:**
1. Add failing tests for:
   - page cache hit avoids remote range read
   - same-page concurrent misses coalesce
2. Implement in-memory page cache first.
3. Add singleflight or equivalent request coalescing.
4. Add cold vs warm read benchmarks.
5. Run `go test ./...`

### Phase 6: Durable-Tier Integration Policy

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/tiering_integration_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/pure_streaming_benchmark_test.go`

**Intent:**
- Lock down how regular tiers interact with durable tier

**Tasks:**
1. Add tests for:
   - tier hit only promotes upward
   - durable hit backfills regular tiers
   - direct `Set` only writes tier 0
   - `Delete` removes durable visibility too
2. Keep v2 fan-out policy intentionally simple.
3. Update benchmark naming from hot/cold to tier/durable terminology where needed.
4. Run `go test ./...`

### Phase 7: Migration and Docs

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/cache/README.md`
- Add/Modify examples under `/Users/cypark/Documents/project/daramjwee/examples`

**Intent:**
- Make the new model understandable and usable

**Tasks:**
1. Add new examples:
   - `WithTiers(mem, file)`
   - `WithTiers(mem, redis) + WithDurableTier(objectstore)`
2. Document that `objectstore` is a first-party durable backend.
3. Document that `objectstore` is not yet a supported regular tier.
4. Add migration notes from v1.
5. Run `go test ./...`

### Phase 8: Release Readiness

**Files:**
- Modify release notes/changelog files if present
- Update docs/plans if final design changed during implementation

**Intent:**
- Prepare a breaking-change release with evidence

**Tasks:**
1. Run:
   - `go test ./...`
   - `go test -race ./...`
   - tiering/objectstore benchmark suite
2. Compare against current v1 baseline where meaningful.
3. Summarize:
   - API breakage
   - migration path
   - performance deltas
4. Draft PR body and release notes.
