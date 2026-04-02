# Prod-like Store Benchmark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add reproducible prod-like benchmark harnesses and a cross-version runner for `filestore` and `objectstore`.

**Architecture:** Keep the maintained benchmark code in the current branch under package tests. Use an env gate so normal test runs skip the harnesses. For `v0.3.10`, generate temporary harness files inside a detached worktree and run them with the same workload and Azurite configuration.

**Tech Stack:** Go tests, shell runner script, Azurite, git worktrees.

---

### Task 1: Add current `filestore` compare harness

**Files:**
- Create: `pkg/store/filestore/prodlike_compare_test.go`

**Steps:**
1. Write an env-gated test that skips unless `DJ_RUN_PRODLIKE_COMPARE=1`.
2. Build the prod-like workload in code.
3. Measure write, cold read, and warm read phases.
4. Emit one JSON line prefixed with `DJ_PRODLIKE_COMPARE=`.

### Task 2: Add current `objectstore` Azurite compare harness

**Files:**
- Create: `pkg/store/objectstore/prodlike_compare_test.go`

**Steps:**
1. Write an env-gated test that creates an Azurite-backed bucket.
2. Wrap the bucket to count upload/get/get_range/list operations and bytes.
3. Measure write, cold read, and warm read using the production preset.
4. Emit one JSON line prefixed with `DJ_PRODLIKE_COMPARE=`.

### Task 3: Add runner script for `current` vs `v0.3.10`

**Files:**
- Create: `scripts/run_prodlike_store_compare.sh`

**Steps:**
1. Start Azurite if it is not already listening.
2. Create a detached `v0.3.10` worktree.
3. Inject temporary version-specific benchmark files into the worktree.
4. Run current and `v0.3.10` compare commands.
5. Store logs under a temp directory and clean up worktree/processes.

### Task 4: Document execution and interpretation

**Files:**
- Modify: `pkg/store/filestore/README.md`
- Modify: `pkg/store/objectstore/README.md`

**Steps:**
1. Add the prod-like compare commands.
2. Define each reported metric.
3. Point to the runner script for cross-version comparison.

### Task 5: Verify and commit

**Steps:**
1. Run `go test ./pkg/store/filestore ./pkg/store/objectstore`.
2. Run the env-gated harnesses.
3. Run `scripts/run_prodlike_store_compare.sh`.
4. Stage and commit the benchmark files and docs.
