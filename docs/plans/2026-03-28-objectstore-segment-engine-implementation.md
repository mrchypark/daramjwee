# Objectstore Segment-Engine Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the durable-tier branch with an n-tier-only cache model and rebuild `pkg/store/objectstore` as a local-ingest, remote-segment store that preserves pure streaming while reducing remote object count.

**Architecture:** Remove special top-level treatment for durable storage. Keep `Store` / `WriteSink` as the only storage contract. Implement objectstore as a composite backend: local append-only ingest and catalog for publish semantics, background segment flush to remote object storage, and remote segment reads with optional block caching.

**Tech Stack:** Go, `thanos-io/objstore`, existing `WriteSink` streaming contract, local filesystem, current daramjwee worker/background pipeline, existing tests and benchmark harness.

---

### Task 1: Replace top-level durable-tier API with ordered tiers only

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/options.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/daramjwee.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/options_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/api_contract_test.go`

**Step 1: Write the failing tests**

Add tests that assert:
- `WithTiers(a, b)` is valid
- empty tier list is rejected
- duplicate tier instance is rejected
- `WithDurableTier` no longer exists or no longer participates in config
- `ValidateRegularTier` is no longer part of top-level validation

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./... -run 'TestNew_|TestOptions_'
```

Expected: compile failures or assertion failures referencing removed durable-tier assumptions.

**Step 3: Write minimal implementation**

- Remove `DurableTier` from config
- Remove `WithDurableTier(...)`
- Collapse freshness config to `WithTierFreshness(...)`
- Remove role-based validator use from `New(...)`

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./... -run 'TestNew_|TestOptions_'
```

Expected: PASS

**Step 5: Commit**

```bash
git add options.go daramjwee.go options_test.go tests/api_contract_test.go
git commit -m "refactor: remove durable-tier config model"
```

### Task 2: Generalize cache orchestration to n-tier only

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/streaming.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/tiering_integration_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/pure_streaming_integration_test.go`

**Step 1: Write the failing tests**

Add or rewrite tests for:
- lower-tier hit synchronously fills only tier 0
- lower-tier hit asynchronously fans out to remaining higher tiers only after publish
- direct `Set` writes to tier 0
- delete removes from all tiers
- partial read still aborts publish across tier fills

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./tests -run 'TestCache_'
```

Expected: FAIL with durable-tier-specific assumptions or missing tier-chain behavior.

**Step 3: Write minimal implementation**

- Replace durable-specific branches with ordered-tier traversal
- Keep `streamThrough(...)` as the only synchronous fill path
- Make tier 0 the only sync sink target
- Keep fan-out to other tiers async/best-effort

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./tests -run 'TestCache_'
```

Expected: PASS

**Step 5: Commit**

```bash
git add cache.go streaming.go tests/tiering_integration_test.go tests/pure_streaming_integration_test.go
git commit -m "refactor: simplify cache core to ordered tiers"
```

### Task 3: Strip current objectstore down to a clean backend shell

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/store.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/options.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/store_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/benchmark_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore.go`

**Step 1: Write the failing tests**

Add tests that lock the new baseline:
- objectstore is usable as ordinary `Store`
- no regular-tier rejection path remains
- no per-key manifest assumption remains in tests

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./pkg/store/objectstore ./pkg/store/adapter
```

Expected: FAIL due to current manifest/page-cache oriented assumptions.

**Step 3: Write minimal implementation**

- Remove `ValidateRegularTier` special rejection
- Mark current adapter shim deprecated and point examples at `pkg/store/objectstore`
- Reduce store tests to interface-level expectations only

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./pkg/store/objectstore ./pkg/store/adapter
```

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/store/objectstore pkg/store/adapter/objstore.go
git commit -m "refactor: make objectstore an ordinary store backend"
```

### Task 4: Add local ingest segment writer and local publish catalog

**Files:**
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/segment`
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/catalog`
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/catalog.go`
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/writer.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/store.go`
- Add test: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/ingest_test.go`

**Step 1: Write the failing tests**

Add tests for:
- `BeginSet` writes into local segment and is not visible before `Close`
- `Close` publishes through local catalog
- `Abort` leaves no visible entry
- restart/reopen can recover published local entries

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(BeginSet|Abort|Recover)'
```

Expected: FAIL because local ingest/catalog does not exist yet.

**Step 3: Write minimal implementation**

- add active segment append writer
- add local catalog entry format
- make `Close` publish locally
- make `Abort` discard unpublished extent

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(BeginSet|Abort|Recover)'
```

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/store/objectstore
git commit -m "feat: add local ingest and catalog publish to objectstore"
```

### Task 5: Add remote packed-segment flush and checkpointing

**Files:**
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/flusher.go`
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/shard`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/store.go`
- Add test: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/flush_test.go`

**Step 1: Write the failing tests**

Add tests for:
- sealed local segment is uploaded as one remote segment object
- multiple keys share one remote segment
- remote checkpoint is shard-scoped, not key-scoped
- no key-by-key remote manifest objects are created

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(Flush|Checkpoint)'
```

Expected: FAIL because remote flush/checkpoint path does not exist.

**Step 3: Write minimal implementation**

- add segment sealing
- add async flusher
- add shard checkpoint writer
- keep direct large-blob path unimplemented or feature-gated for now

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(Flush|Checkpoint)'
```

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/store/objectstore
git commit -m "feat: flush objectstore segments to remote checkpoints"
```

### Task 6: Add remote read path for packed segments

**Files:**
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/reader.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/store.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/store_test.go`
- Add test: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/read_test.go`

**Step 1: Write the failing tests**

Add tests for:
- local published hit reads from local segment
- remote-only hit resolves through shard checkpoint
- packed record read returns exact logical object stream
- delete tombstone hides older packed record

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(GetStream|Delete)'
```

Expected: FAIL because packed-segment remote reads are not implemented.

**Step 3: Write minimal implementation**

- resolve key through local catalog first
- fallback to shard checkpoint
- open packed segment reader by offset/length
- wire delete tombstones into lookup

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(GetStream|Delete)'
```

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/store/objectstore
git commit -m "feat: read packed segment entries from objectstore"
```

### Task 7: Add large-object direct path and threshold split

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/options.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/store.go`
- Add test: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/large_test.go`

**Step 1: Write the failing tests**

Add tests for:
- payload below threshold uses packed segment path
- payload above threshold uses direct blob path
- large object still respects pure streaming publish/abort

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(Packed|Large)'
```

Expected: FAIL because size-threshold split is not implemented.

**Step 3: Write minimal implementation**

- add `WithPackedObjectThreshold(...)`
- route large payloads to direct remote blob path
- keep catalog entry format common across packed and direct paths

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(Packed|Large)'
```

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/store/objectstore
git commit -m "feat: split packed and large object paths"
```

### Task 8: Add block cache and same-block request coalescing

**Files:**
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/internal/blockcache`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/reader.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/options.go`
- Add test: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/blockcache_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/benchmark_test.go`

**Step 1: Write the failing tests**

Add tests for:
- warm block hit avoids remote range read
- concurrent same-block misses coalesce
- cache key uses segment id + block index, not logical key

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(BlockCache|Coalesce)'
```

Expected: FAIL because block cache/coalescing is not implemented.

**Step 3: Write minimal implementation**

- add in-memory block cache
- add singleflight around block loads
- wire packed-segment range reader through block cache

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(BlockCache|Coalesce)'
```

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/store/objectstore
git commit -m "feat: add objectstore block cache and coalescing"
```

### Task 9: Add reclaim and compaction

**Files:**
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/compactor.go`
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/recovery.go`
- Add test: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/compaction_test.go`

**Step 1: Write the failing tests**

Add tests for:
- tombstoned or superseded records are reclaimed during compaction
- old remote checkpoints can be pruned after replacement
- compaction does not break visible reads

**Step 2: Run tests to verify failure**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(Compact|Reclaim)'
```

Expected: FAIL because reclaim/compaction is missing.

**Step 3: Write minimal implementation**

- compact local segments conservatively
- prune obsolete remote checkpoint references
- keep reclaim safety simple and generation-based

**Step 4: Run tests to verify pass**

Run:

```bash
go test ./pkg/store/objectstore -run 'TestStore_(Compact|Reclaim)'
```

Expected: PASS

**Step 5: Commit**

```bash
git add pkg/store/objectstore
git commit -m "feat: add conservative objectstore compaction"
```

### Task 10: Migrate docs, examples, and benchmarks

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/cache/README.md`
- Modify examples under: `/Users/cypark/Documents/project/daramjwee/examples`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/pure_streaming_benchmark_test.go`

**Step 1: Write the failing checks**

List expected updates:
- examples no longer mention durable tier
- objectstore is documented as ordinary `Store` with internal segment engine
- filestore remains an example/simple tier
- benchmark names and scenarios use ordered tiers terminology

**Step 2: Run verification to establish current mismatch**

Run:

```bash
go test ./... && go test ./tests -run '^$' -bench 'BenchmarkCacheGet_' -benchmem
```

Expected: tests may pass, but docs/examples will be outdated relative to new API and architecture.

**Step 3: Write minimal implementation**

- update README and package docs
- migrate examples
- keep filestore example as simple whole-object tier
- add objectstore benchmarks for packed/local/warm remote paths

**Step 4: Run verification**

Run:

```bash
go test ./...
go test -race ./...
go test ./tests -run '^$' -bench 'BenchmarkCacheGet_' -benchmem
go test ./pkg/store/objectstore -run '^$' -bench 'BenchmarkStore_' -benchmem
```

Expected: all pass and benchmark suite runs cleanly.

**Step 5: Commit**

```bash
git add README.md pkg/cache/README.md examples tests/pure_streaming_benchmark_test.go pkg/store/objectstore
git commit -m "docs: align examples and benchmarks with objectstore engine"
```
