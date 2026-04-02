# Pure Streaming Cache Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert `daramjwee` cold-hit and origin-miss paths to true stream-through caching with a smaller design: `Close == publish`, explicit `Abort == discard`, fail-fast rejection for unsupported hot-store modes, passthrough only for transient sink-acquisition failure, and no cleanup logic in the semantic acceptance surface.

**Architecture:** Replace `Store.BeginSet` with `Store.BeginSet` returning a `WriteSink` (`io.WriteCloser` + `Abort`). Change `Cache.Set` to return the same `WriteSink` directly. Refactor `cache.go` around a dedicated `fillReadCloser` plus a single `streamThrough(...)` helper used by both cold-hit and miss. Keep pure-streaming guarantees only for `memstore`, `filestore` rename mode, and `redisstore`. Reject unsupported hot-store modes at construction/options validation, and allow passthrough only when `BeginSet(...)` fails transiently at request time. Treat orphan cleanup and out-of-scope stores (`objstore`, `WithCopyAndTruncate()`) as separate concerns, not second runtime modes inside cache core.

**Tech Stack:** Go 1.26, stdlib `io`/`sync`, existing Go test suite, Redis client, file-backed store, in-memory store.

---

### Phase 1: Cut The Contract Down To `Close+Abort`, Fail Fast On Unsupported Hot Stores, And Keep The Repo Green

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/daramjwee.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/nullstore.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/options_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/api_contract_test.go`
- Modify: any remaining `BeginSet` compile site found by `rg -n "BeginSet|Commit\\(|SupportsStreamingFill" /Users/cypark/Documents/project/daramjwee`

**Step 1: Write the failing tests**

Pin the smaller boundary:

- `Store.BeginSet` exists and returns abortable sink semantics
- `Cache.Set` returns the same abortable sink model
- `Cache.Set` publishes on `Close()`
- `Cache.Set` discards on `Abort()`
- unsupported hot-store store/mode is rejected at construction/options validation
- nil fetcher behavior remains unchanged

```go
func TestCache_SetPublishesOnClose(t *testing.T) {}
func TestCache_SetDiscardsOnAbort(t *testing.T) {}
func TestCache_RejectsUnsupportedHotStore(t *testing.T) {}
func TestCache_GetRejectsNilFetcher(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run:

```bash
go test ./tests -run 'TestCache_(Set|GetRejectsNilFetcher)' -v
go test ./...
```

Expected: build failures because the old writer contract still exists.

**Step 3: Write minimal implementation**

- Add `WriteSink` as `io.WriteCloser + Abort`
- Replace `Store.BeginSet` with `Store.BeginSet`
- Change `Cache.Set` to return `WriteSink`
- Remove `Commit` and `SupportsStreamingFill` from the public design surface
- Add fail-fast validation for unsupported hot-store store/mode
- Sweep compile sites in one phase so the repo builds again
- Give all accepted stores (`MemStore`, `FileStore` rename mode, `RedisStore`) working staged `Close/Abort` semantics before this phase ends
- If an out-of-scope store needs mechanical adaptation to compile, do only the minimum needed here; if it cannot satisfy the new `Close/Abort` contract, reject/disable it instead of pretending support

**Step 4: Run test to verify it passes**

Run:

```bash
go test ./tests -run 'TestCache_(Set|GetRejectsNilFetcher)' -v
go test ./...
```

Expected: PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "refactor: shrink store contract to close and abort"
```

### Phase 2: Refactor Cache Core To One Stream-Through Path

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Create: `/Users/cypark/Documents/project/daramjwee/streaming_reader_test.go`
- Create: `/Users/cypark/Documents/project/daramjwee/tests/pure_streaming_integration_test.go`

**Step 1: Write the failing tests**

Core reader tests:

- full read + close publishes
- full read without close does not publish
- partial read + close aborts
- source error aborts
- short write becomes `io.ErrShortWrite`
- publish error surfaces from `Close()`

Streaming proof tests:

- miss first byte arrives before source EOF
- cold-hit first byte arrives before source EOF
- no hot reread in stream-through path
- transient `BeginSet` failure falls back to passthrough without caching
- miss full read + close publishes to hot and schedules cold persist
- publish failure does not schedule cold persist
- partial miss/cold-hit read publishes nothing

```go
func TestFillReadCloser_FullReadAndClosePublishes(t *testing.T) {}
func TestFillReadCloser_FullReadWithoutCloseDoesNotPublish(t *testing.T) {}
func TestCache_MissStreamsBeforeSourceEOF(t *testing.T) {}
func TestCache_ColdHitStreamsBeforeSourceEOF(t *testing.T) {}
func TestCache_MissBeginSetFailureFallsBackToPassthrough(t *testing.T) {}
func TestCache_PublishFailureDoesNotScheduleColdPersist(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run:

```bash
go test . -run 'TestFillReadCloser_' -v
go test ./tests -run 'TestCache_(Miss|ColdHit)' -v
```

Expected: FAIL because cache core still does write-then-read.

**Step 3: Write minimal implementation**

- Implement `writeAll`
- Implement dedicated `fillReadCloser`
- Remove cache-core buffering/coalescing helpers
- Remove runtime fallback for unsupported hot-store modes
- Introduce one helper like `streamThrough(src, sink, onPublish)`
- Refactor both cold-hit and miss to use the same helper
- Keep `Close()` as the only publish point
- Preserve passthrough only for transient `BeginSet` acquisition failure

**Step 4: Run test to verify it passes**

Run:

```bash
go test . -run 'TestFillReadCloser_' -v
go test ./tests -run 'TestCache_(Miss|ColdHit)' -v
go test ./pkg/cache -run 'TestGenericCache_' -v
go test ./...
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/cache.go \
        /Users/cypark/Documents/project/daramjwee/streaming_reader_test.go \
        /Users/cypark/Documents/project/daramjwee/tests/pure_streaming_integration_test.go
git commit -m "refactor: unify stream-through cache path"
```

### Phase 3: Add One Shared Conformance Suite And Refine In-Scope Stores

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/memstore/memory.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/redisstore/redis.go`
- Create: `/Users/cypark/Documents/project/daramjwee/pkg/store/storetest/writesink_conformance.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/memstore/memory_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/redisstore/redis_test.go`

**Step 1: Write the failing tests**

Shared conformance cases for each accepted store:

- `Close()` publishes
- `Abort()` discards
- partial write + abort leaves no published entry
- repeated close / abort is safe
- `AbortThenCloseDoesNotPublish`
- `CloseThenAbortDoesNotUnpublish`
- terminal writes fail

Keep only unique store tests outside the shared suite:

- `FileStore` rename atomicity and lock window
- `RedisStore` publish script behavior, active-stream staged-key survival, and optional internal coalescing behavior

```go
func RunWriteSinkConformance(t *testing.T, factory func(t *testing.T) daramjwee.Store) {}
```

**Step 2: Run test to verify it fails**

Run:

```bash
go test ./pkg/store/... -v
go test ./...
```

Expected: FAIL because the shared conformance suite and store-specific invariants have not been wired up yet, even though the accepted stores already expose basic `Close/Abort` semantics from Phase 1.

**Step 3: Write minimal implementation**

- `MemStore`: staged buffer, `Close` publish, `Abort` discard
- `FileStore` rename mode: temp file, `Close` rename publish, `Abort` delete temp
- `RedisStore`: temp key, `Close` publish, `Abort` discard
- Keep backend-specific buffering inside `RedisStore`, not cache core
- Keep orphan cleanup/TTL/scavenging as store-local hygiene, but test Redis active-stream staged-key survival as an accepted invariant

**Step 4: Run test to verify it passes**

Run:

```bash
go test ./pkg/store/... -v
go test ./tests -run 'TestCache_(Miss|ColdHit)' -v
go test ./...
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/pkg/store/memstore \
        /Users/cypark/Documents/project/daramjwee/pkg/store/filestore \
        /Users/cypark/Documents/project/daramjwee/pkg/store/redisstore \
        /Users/cypark/Documents/project/daramjwee/pkg/store/storetest
git commit -m "refactor: convert in-scope stores to pure streaming sink model"
```

### Phase 4: Docs, Mechanical Out-Of-Scope Cleanup, And Final Verification

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/cache/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/examples/main.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/options.go`
- Modify: any remaining docs/examples referring to `BeginSet`, `Commit`, fallback hot-store behavior, or old `Cache.Set` return type
- Modify if needed for build only: `/Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore.go`

**Step 1: Write the failing tests / checks**

Verification checklist:

- docs/examples reflect `Close()` as a correctness requirement
- docs reflect `Cache.Set` returning abortable sink
- docs reflect no runtime fallback path
- docs reflect fail-fast rejection of unsupported hot-store store/mode
- docs reflect passthrough only for transient `BeginSet` acquisition failure
- out-of-scope modes (`objstore`, `WithCopyAndTruncate()`) are clearly documented as excluded

If the final review finds a real regression, add one focused failing test for that issue before fixing it.

**Step 2: Run verification before implementation**

Run:

```bash
go test ./...
```

Expected: FAIL if docs/examples or remaining compile sites still assume the old contract.

**Step 3: Write minimal implementation**

- Update docs and examples
- Make out-of-scope boundaries explicit
- If `objstore` needs mechanical changes to compile against the new surface, do only the minimum and do not claim hot-tier semantic support
- Ensure any mechanically adapted out-of-scope store either satisfies the `Close/Abort` contract for the paths it still exposes, or is explicitly rejected/disabled

**Step 4: Run final verification**

Run:

```bash
go test ./...
go test -race ./...
```

Expected: PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "docs: finalize smaller pure streaming design"
```
