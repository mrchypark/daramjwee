# Per-Tier Freshness Override Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add per-tier freshness overrides on top of the current chain-wide freshness defaults.

**Architecture:** Keep the ordered `n-tier` cache API unchanged and preserve `WithTierFreshness(...)` as the default. Add two opt-in override options keyed by tier index, then route stale checks through a tier-aware freshness resolver so tier 0 and lower-tier reads can use different positive and negative windows.

**Tech Stack:** Go, testify, existing ordered-tier cache orchestration and option parsing.

---

### Task 1: Add failing option-level tests for per-tier overrides

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/options_test.go`

**Step 1: Write the failing tests**

Add tests covering:

- `WithTierPositiveFreshness(1, -time.Second)` rejects with a config error
- `WithTierNegativeFreshness(1, -time.Second)` rejects with a config error
- applying per-tier overrides stores the values on config/cache state

**Step 2: Run test to verify it fails**

Run: `go test ./... -run 'TestWithTier(Positive|Negative)Freshness|TestOptions_PerTierFreshnessOverrides'`

Expected: FAIL because the new options and state do not exist yet.

**Step 3: Write minimal implementation**

Implement:

- per-tier override fields in config
- the two new option helpers with validation

**Step 4: Run test to verify it passes**

Run: `go test ./... -run 'TestWithTier(Positive|Negative)Freshness|TestOptions_PerTierFreshnessOverrides'`

Expected: PASS

### Task 2: Add failing cache behavior tests for tier-specific stale decisions

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/tests/tiering_integration_test.go`

**Step 1: Write the failing tests**

Add tests showing:

- lower-tier positive hit can remain fresh because of a tier override even when chain default would mark it stale immediately
- lower-tier negative hit can remain fresh because of a tier override even when chain default would mark it stale immediately

**Step 2: Run test to verify it fails**

Run: `go test ./tests -run 'TestCache_Get_LowerTier(Positive|Negative)FreshnessOverride'`

Expected: FAIL because stale resolution still uses the chain-wide values only.

**Step 3: Write minimal implementation**

Implement:

- tier-aware freshness resolution in cache
- replace chain-wide stale checks with tier-index-based stale checks

**Step 4: Run test to verify it passes**

Run: `go test ./tests -run 'TestCache_Get_LowerTier(Positive|Negative)FreshnessOverride'`

Expected: PASS

### Task 3: Update docs and run focused verification

**Files:**
- Modify: `/Users/cypark/Documents/project/daramjwee/README.md`

**Step 1: Update docs**

Document:

- chain-wide default freshness
- per-tier positive/negative overrides
- tier index mapping to `WithTiers(...)`

**Step 2: Run focused verification**

Run: `go test ./... -run 'TestWithTier(Positive|Negative)Freshness|TestOptions_PerTierFreshnessOverrides|TestCache_Get_LowerTier(Positive|Negative)FreshnessOverride'`

Expected: PASS

**Step 3: Run broader regression verification**

Run: `go test ./...`

Expected: PASS

**Step 4: Commit**

```bash
git add options.go daramjwee.go cache.go options_test.go tests/tiering_integration_test.go README.md docs/plans/2026-04-03-per-tier-freshness-design.md docs/plans/2026-04-03-per-tier-freshness.md
git commit -m "feat: add per-tier freshness overrides"
```
