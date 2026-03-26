# Pure Streaming Cache Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert `daramjwee` cold-hit and origin-miss paths to true stream-through caching with explicit `Commit`/`Abort` semantics.

**Architecture:** Replace `Store.SetWithWriter` with `Store.BeginSet` returning a `WriteSink`. Refactor `cache.go` around a `fillReadCloser` that copies bytes from source to caller and hot sink in one lifecycle, committing only after successful EOF and `Close()`. Convert built-in `memstore`, `filestore`, and `redisstore` to the new contract; keep `objstore` limited to contract adaptation in this batch.

**Tech Stack:** Go 1.26, stdlib `io`/`sync`, existing Go test suite, Redis client, file-backed store, in-memory store.

---

### Task 1: Cut The Public Write Contract

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/daramjwee.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/nullstore.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Test: `/Users/cypark/Documents/project/daramjwee/tests/api_contract_test.go`

**Step 1: Write the failing test**

Add a contract test that references `BeginSet` through a tiny fake store and verifies `Get` still rejects nil fetchers after the interface cut.

```go
type fakeSink struct{}

func (s *fakeSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *fakeSink) Commit() error               { return nil }
func (s *fakeSink) Abort() error                { return nil }
```

**Step 2: Run test to verify it fails**

Run: `go test ./tests -run TestCache_GetRejectsNilFetcher -v`
Expected: build failure because `Store` still exposes `SetWithWriter`.

**Step 3: Write minimal implementation**

- Add `WriteSink` to `daramjwee.go`
- Replace `SetWithWriter` with `BeginSet`
- Update `nullstore` and `cache.go` wrappers to compile against `BeginSet`

```go
type WriteSink interface {
    Write(p []byte) (int, error)
    Commit() error
    Abort() error
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./tests -run TestCache_GetRejectsNilFetcher -v`
Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/daramjwee.go \
        /Users/cypark/Documents/project/daramjwee/nullstore.go \
        /Users/cypark/Documents/project/daramjwee/cache.go \
        /Users/cypark/Documents/project/daramjwee/tests/api_contract_test.go
git commit -m "refactor: introduce writesink contract"
```

### Task 2: Add A Dedicated Streaming Reader State Machine

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Create: `/Users/cypark/Documents/project/daramjwee/streaming_reader_test.go`

**Step 1: Write the failing test**

Create unit tests for a new internal `fillReadCloser`.

```go
func TestFillReadCloser_CommitsOnFullRead(t *testing.T) {}
func TestFillReadCloser_AbortsOnEarlyClose(t *testing.T) {}
func TestFillReadCloser_AbortsOnSinkWriteError(t *testing.T) {}
func TestFillReadCloser_CloseReturnsCommitError(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run: `go test . -run 'TestFillReadCloser_' -v`
Expected: FAIL because `fillReadCloser` does not exist.

**Step 3: Write minimal implementation**

Implement `fillReadCloser` in `cache.go` or a small adjacent file in the root package.

```go
type fillReadCloser struct {
    src      io.ReadCloser
    sink     WriteSink
    onCommit func()
    // state fields...
}
```

`Close()` must:
- commit only after EOF + no read/sink error
- otherwise abort
- run exactly once

**Step 4: Run test to verify it passes**

Run: `go test . -run 'TestFillReadCloser_' -v`
Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/cache.go \
        /Users/cypark/Documents/project/daramjwee/streaming_reader_test.go
git commit -m "refactor: add stream-through reader state machine"
```

### Task 3: Refactor Cache Miss And Cold-Hit Paths To Use `fillReadCloser`

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/cache_regression_test.go`
- Create: `/Users/cypark/Documents/project/daramjwee/tests/pure_streaming_integration_test.go`

**Step 1: Write the failing test**

Add integration tests for:
- miss returns first bytes without hot reread
- cold hit returns first bytes without hot reread
- partial read leaves no hot entry

```go
func TestCache_MissPartialReadDoesNotPublishHotEntry(t *testing.T) {}
func TestCache_ColdHitPartialReadDoesNotPublishHotEntry(t *testing.T) {}
```

Use fake stores/fetchers that count `GetStream` calls on hot store so the test can prove reread is gone.

**Step 2: Run test to verify it fails**

Run: `go test ./tests -run 'TestCache_(Miss|ColdHit)PartialRead' -v`
Expected: FAIL because current code still does write-then-read and publishes on partial consumption.

**Step 3: Write minimal implementation**

- Change `handleColdHit` to `BeginSet` a hot sink and return `fillReadCloser`
- Change `handleMiss` to `BeginSet` a hot sink and return `fillReadCloser`
- Move cold persist scheduling to `onCommit`
- Keep passthrough fallback when `BeginSet` fails

**Step 4: Run test to verify it passes**

Run: `go test ./tests -run 'TestCache_(Miss|ColdHit)PartialRead' -v`
Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/cache.go \
        /Users/cypark/Documents/project/daramjwee/tests/cache_regression_test.go \
        /Users/cypark/Documents/project/daramjwee/tests/pure_streaming_integration_test.go
git commit -m "refactor: stream through miss and cold-hit paths"
```

### Task 4: Convert `MemStore` To `WriteSink`

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/memstore/memory.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/memstore/memory_test.go`

**Step 1: Write the failing test**

Add tests for:
- `Abort()` leaves no entry
- `Commit()` publishes entry
- `Commit()` and `Abort()` are idempotent

```go
func TestMemStoreSink_AbortDiscardsEntry(t *testing.T) {}
func TestMemStoreSink_CommitPublishesEntry(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/store/memstore -run 'TestMemStoreSink_' -v`
Expected: FAIL because store still exposes `SetWithWriter`.

**Step 3: Write minimal implementation**

- Replace `SetWithWriter` with `BeginSet`
- Rename writer type to sink type
- Move publish logic to `Commit`
- Make `Abort` reset buffer and release pooled writer without touching store state

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/store/memstore -run 'TestMemStoreSink_' -v`
Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/pkg/store/memstore/memory.go \
        /Users/cypark/Documents/project/daramjwee/pkg/store/memstore/memory_test.go
git commit -m "refactor: convert memstore to writesink"
```

### Task 5: Convert `FileStore` To `WriteSink`

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage_test.go`

**Step 1: Write the failing test**

Add tests for:
- `Abort()` removes temp file and does not publish final path
- `Commit()` publishes final file
- `Abort()` after partial write leaves no visible entry

```go
func TestFileStoreSink_AbortRemovesTempFile(t *testing.T) {}
func TestFileStoreSink_PartialWriteAbortLeavesNoEntry(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/store/filestore -run 'TestFileStoreSink_' -v`
Expected: FAIL because file store still commits on `Close()`.

**Step 3: Write minimal implementation**

- Replace `lockedWriteCloser` with `fileSink`
- `BeginSet` creates temp file and stores lock ownership in sink
- `Commit` finalizes temp file and updates accounting
- `Abort` closes/removes temp file and unlocks
- Preserve legacy-path read/delete compatibility work already in tree

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/store/filestore -run 'TestFileStoreSink_' -v`
Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage.go \
        /Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage_test.go
git commit -m "refactor: convert filestore to writesink"
```

### Task 6: Convert `RedisStore` To `WriteSink`

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/redisstore/redis.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/redisstore/redis_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/redis_cache_integration_test.go`

**Step 1: Write the failing test**

Add tests for:
- `Abort()` deletes temp key
- partial write + abort leaves no final data/meta
- commit still supports zero-byte negative cache entries

```go
func TestRedisStoreSink_AbortDeletesTempKey(t *testing.T) {}
func TestRedisStoreSink_PartialWriteAbortLeavesNoFinalKey(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/store/redisstore ./tests -run 'Test(RedisStoreSink_|RedisCache)' -v`
Expected: FAIL because redis store still commits on `Close()`.

**Step 3: Write minimal implementation**

- Replace `redisStoreWriter.Close` with `Commit`
- Add explicit `Abort` to delete temp key
- Make commit/abort idempotent
- Keep Lua commit path for atomic final publish

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/store/redisstore ./tests -run 'Test(RedisStoreSink_|RedisCache)' -v`
Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/pkg/store/redisstore/redis.go \
        /Users/cypark/Documents/project/daramjwee/pkg/store/redisstore/redis_test.go \
        /Users/cypark/Documents/project/daramjwee/tests/redis_cache_integration_test.go
git commit -m "refactor: convert redis store to writesink"
```

### Task 7: Keep `objstore` Compiling Against The New Contract

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore_test.go`

**Step 1: Write the failing test**

Add a compile/runtime test that uses `BeginSet`, writes bytes, and calls `Commit`.

```go
func TestObjstoreSink_CommitPublishesEntry(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/store/adapter -run TestObjstoreSink_CommitPublishesEntry -v`
Expected: FAIL because adapter still exposes `SetWithWriter`.

**Step 3: Write minimal implementation**

- Replace `SetWithWriter` with `BeginSet`
- Map current upload finalization to `Commit`
- Add explicit `Abort` that closes pipe, waits upload goroutine, and best-effort cleans temporary state
- Do not claim full pure-streaming publish semantics in docs yet

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/store/adapter -run TestObjstoreSink_CommitPublishesEntry -v`
Expected: PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore.go \
        /Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore_test.go
git commit -m "refactor: adapt objstore to writesink contract"
```

### Task 8: Update Generic Cache, Docs, And Full Verification

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/cache/generic.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/cache/generic_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/cache/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/examples/main.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/options.go`

**Step 1: Write the failing test**

Add/adjust integration tests that assert:
- miss/cold-hit semantics are now pure streaming
- docs/examples compile against `BeginSet` contract

```go
func TestGenericCache_StreamThroughMissCommitsOnClose(t *testing.T) {}
```

**Step 2: Run test to verify it fails**

Run: `go test ./...`
Expected: FAIL because generic cache/docs/examples still assume old write contract or old flow.

**Step 3: Write minimal implementation**

- Update generic cache internals if helper wrappers changed
- Rewrite README flow description to reflect real pure streaming
- Update examples and option docs
- Remove stale comments referring to `SetWithWriter`

**Step 4: Run test to verify it passes**

Run:

```bash
go test ./...
go test -race ./...
```

Expected: both commands PASS

**Step 5: Commit**

```bash
git add /Users/cypark/Documents/project/daramjwee/pkg/cache/generic.go \
        /Users/cypark/Documents/project/daramjwee/pkg/cache/generic_test.go \
        /Users/cypark/Documents/project/daramjwee/README.md \
        /Users/cypark/Documents/project/daramjwee/pkg/cache/README.md \
        /Users/cypark/Documents/project/daramjwee/examples/main.go \
        /Users/cypark/Documents/project/daramjwee/options.go
git commit -m "docs: update pure streaming contract"
```

### Task 9: Final Review Pass For Maintainability

**Files:**
- Review: `/Users/cypark/Documents/project/daramjwee/cache.go`
- Review: `/Users/cypark/Documents/project/daramjwee/pkg/store/memstore/memory.go`
- Review: `/Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage.go`
- Review: `/Users/cypark/Documents/project/daramjwee/pkg/store/redisstore/redis.go`
- Review: `/Users/cypark/Documents/project/daramjwee/tests/pure_streaming_integration_test.go`

**Step 1: Write the failing test**

Write a checklist-style review note in the PR description draft or local notes:

- no reread in miss/cold-hit
- abort correctness
- idempotent commit/abort
- commit error surfaced from close
- no deadlocks around file/redis sinks

**Step 2: Run test to verify it fails**

Run: `go test ./... && go test -race ./...`
Expected: if any issue remains, stop and add a targeted regression test before touching implementation.

**Step 3: Write minimal implementation**

Only if review finds a real issue:
- add a focused failing regression test
- implement the smallest fix
- rerun affected package tests

**Step 4: Run test to verify it passes**

Run:

```bash
go test ./...
go test -race ./...
```

Expected: PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "test: close remaining pure streaming gaps"
```
