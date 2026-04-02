# Objectstore Spool Reclaim Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `pkg/store/objectstore` treat local disk as spool-only storage and reclaim sealed local segments after remote commit, while keeping open local readers safe.

**Architecture:** Add runtime segment refcount tracking to `Store`, wrap local readers so refcounts are released on `Close()`, and reclaim sealed segments once remote upload, checkpoint publish, and local catalog remote-path updates all succeed. Recovery keeps remote-backed entries readable even when local files are gone.

**Tech Stack:** Go, local filesystem segments, Thanos `objstore`, existing ordered-tier cache core, testify tests.

---

### Task 1: Add runtime segment refcount tracking

**Files:**
- Modify: `pkg/store/objectstore/store.go`

**Step 1: Add runtime bookkeeping fields**

Add a small runtime-only structure on `Store`:
- `segmentRefs map[string]int`
- `segmentRefsMu sync.Mutex`

**Step 2: Add helper methods**

Implement helpers:
- `acquireSegment(path string)`
- `releaseSegment(path string) int`
- `segmentRefCount(path string) int`

**Step 3: Keep helpers no-op for empty paths**

Guard empty `SegmentPath` so reclaim paths stay simple.

**Step 4: Run targeted objectstore tests**

Run: `go test ./pkg/store/objectstore`

**Step 5: Commit**

`git commit -am "feat: add objectstore segment ref tracking"`

### Task 2: Wrap local readers with refcounted closers

**Files:**
- Modify: `pkg/store/objectstore/store.go`

**Step 1: Replace plain local reader wrapper**

Change `openLocalEntry(...)` so it becomes a store method or receives `Store`, acquires the segment ref before returning the reader, and releases it on `Close()`.

**Step 2: Keep file closing and ref release coupled**

The closer must:
- close the file
- release the segment ref exactly once

**Step 3: Preserve existing read behavior**

Do not change offsets, metadata, or read semantics.

**Step 4: Run targeted tests**

Run: `go test ./pkg/store/objectstore -run 'TestStore_GetStream_'`

**Step 5: Commit**

`git commit -am "feat: protect objectstore local readers with refs"`

### Task 3: Add reclaim helper for remote-committed segments

**Files:**
- Modify: `pkg/store/objectstore/catalog.go`
- Modify: `pkg/store/objectstore/flusher.go`

**Step 1: Add reclaim candidate scan**

Implement a helper that, given current catalog entries, returns sealed local segment paths that:
- are not missing
- are not empty
- are no longer needed by any live local-only entry
- are referenced only by entries with non-empty `RemotePath`

**Step 2: Add reader-count gate**

Only reclaim a candidate path when `segmentRefCount(path) == 0`.

**Step 3: Reclaim after successful remote commit**

In `flushShard`, after:
- remote upload succeeds
- checkpoint publish succeeds
- `commitFlushUpdates(...)` succeeds

attempt reclaim of eligible local segments for that shard.

**Step 4: Keep reclaim best-effort**

Failure to delete a reclaimable local segment should not roll back remote commit. Log and continue.

**Step 5: Run targeted tests**

Run: `go test ./pkg/store/objectstore -run 'TestStore_(Flush|Delete|Overwrite|Reopen)'`

**Step 6: Commit**

`git commit -am "feat: reclaim remote-committed objectstore spool segments"`

### Task 4: Add tests for reclaim semantics

**Files:**
- Modify: `pkg/store/objectstore/ingest_test.go`
- Modify: `pkg/store/objectstore/read_test.go`

**Step 1: Add reclaim-after-flush test**

Test that:
- local segment exists after `Close()`
- `flushPending(...)` completes
- local segment is removed when no reader is open

**Step 2: Add open-reader protection test**

Test that:
- a local reader is opened before flush reclaim
- flush commits remote state
- local segment remains until reader `Close()`
- segment is deleted after final release

**Step 3: Add overwrite reclaim test**

Test that:
- overwrite publishes a new local segment
- older local segment is removed once no longer referenced

**Step 4: Add delete reclaim safety test**

Test that delete still removes local segment and keeps remote tombstone semantics intact.

**Step 5: Run tests**

Run: `go test ./pkg/store/objectstore`

**Step 6: Commit**

`git commit -am "test: cover objectstore spool reclaim behavior"`

### Task 5: Verify recovery semantics still work

**Files:**
- Modify only if needed: `pkg/store/objectstore/recovery.go`
- Test: `pkg/store/objectstore/ingest_test.go`

**Step 1: Re-run missing-local remote-backed scenarios**

Ensure recovery continues to repair missing local paths into remote-backed entries.

**Step 2: Add regression if needed**

If reclaim changes recovery behavior, add a regression covering:
- empty local disk
- remote checkpoint still serving data

**Step 3: Run targeted tests**

Run: `go test ./pkg/store/objectstore -run 'TestStore_(MissingLocalSegment|Reopen|Recover)'`

**Step 4: Commit**

`git commit -am "test: preserve recovery semantics with spool reclaim"`

### Task 6: Full verification

**Files:**
- No code changes expected

**Step 1: Run package tests**

Run: `go test ./pkg/store/objectstore ./tests`

**Step 2: Run full test suite**

Run: `go test ./...`

**Step 3: Run race tests**

Run: `go test -race ./...`

**Step 4: Check worktree**

Run: `git status --short`
Expected: clean

**Step 5: Commit any final fixups**

`git commit -am "chore: finalize objectstore spool reclaim"`
