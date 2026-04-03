# Option Naming Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current public option naming surface with shorter, clearer names whose behavior is understandable directly from call sites.

**Architecture:** Update root `daramjwee`, `filestore`, and `objectstore` option names together so the public API uses one coherent naming model. Remove old option names instead of keeping aliases, migrate all internal call sites, examples, and docs in the same change, and preserve behavior unless the redesign explicitly changes API shape.

**Tech Stack:** Go, testify, current package-level option constructors, README and example code.

---

### Task 1: Root option API rename

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/options.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/daramjwee.go`
- Test: `/Users/cypark/Documents/project/daramjwee/options_test.go`

**Step 1: Write the failing test**

Add or update tests to require:

- `WithFreshness(...)`
- `WithTierFreshness(index, positive, negative)`
- `WithOpTimeout(...)`
- `WithCloseTimeout(...)`
- `WithWorkerStrategy(...)`
- `WithWorkers(...)`
- `WithWorkerQueue(...)`
- `WithWorkerTimeout(...)`

And remove expectations for deleted names.

**Step 2: Run test to verify it fails**

Run: `go test ./... -run 'TestNew_OptionValidation|TestOptionOverrides'`

Expected: FAIL due to missing renamed root options.

**Step 3: Write minimal implementation**

Implement renamed root option functions and migrate internal config fields as needed.

**Step 4: Run test to verify it passes**

Run: `go test ./... -run 'TestNew_OptionValidation|TestOptionOverrides'`

Expected: PASS

### Task 2: Migrate freshness and timeout call sites

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/tiering_integration_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/cache_regression_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/pure_streaming_integration_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/stress_test.go`
- Modify other root-package tests/examples that still use removed names

**Step 1: Write the failing test**

Migrate a small focused set first so the tree does not compile until all renamed root option call sites are updated.

**Step 2: Run test to verify it fails**

Run: `go test ./tests/...`

Expected: FAIL due to removed root option names.

**Step 3: Write minimal implementation**

Update all root option call sites to the renamed API.

**Step 4: Run test to verify it passes**

Run: `go test ./tests/...`

Expected: PASS

### Task 3: filestore option rename

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/filestore/storage.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/filestore/*_test.go`
- Modify examples/docs referencing filestore options

**Step 1: Write the failing test**

Update tests and references to require:

- `WithCopyWrite()`
- `WithEviction(...)`

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/store/filestore/...`

Expected: FAIL due to removed filestore option names.

**Step 3: Write minimal implementation**

Rename filestore option functions and update references.

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/store/filestore/...`

Expected: PASS

### Task 4: objectstore option rename

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/options.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/*_test.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/examples/file_objstore/main.go`
- Modify: `/Users/cypark/Documents/project/daramjwee/examples/file_objstore_provider/main.go`
- Modify objectstore docs and examples

**Step 1: Write the failing test**

Update tests and references to require:

- `WithDir(...)`
- `WithGCGrace(...)`
- `WithPackThreshold(...)`
- `WithPagedThreshold(...)`
- `WithPageCache(...)`
- `WithBlockCache(...)`
- `WithCheckpointCache(...)`
- `WithCheckpointTTL(...)`

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/store/objectstore/...`

Expected: FAIL due to removed objectstore option names.

**Step 3: Write minimal implementation**

Rename objectstore option functions and update references.

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/store/objectstore/...`

Expected: PASS

### Task 5: Docs, examples, and full verification

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/store/objectstore/README.md`
- Modify: `/Users/cypark/Documents/project/daramjwee/pkg/cache/README.md`
- Modify examples still using removed option names

**Step 1: Update docs**

Rewrite all public examples to the renamed API only.

**Step 2: Run focused build/test verification**

Run: `go test ./...`

Expected: PASS

**Step 3: Commit**

```bash
git add options.go daramjwee.go options_test.go pkg/store/filestore/storage.go pkg/store/objectstore/options.go README.md pkg/store/objectstore/README.md pkg/cache/README.md tests examples docs/plans/2026-04-03-option-naming-redesign-design.md docs/plans/2026-04-03-option-naming-redesign.md
git commit -m "refactor: redesign option naming"
```
