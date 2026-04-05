# v0.6.0 CacheTag Get Redesign Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the public read API with a request/response `Get` contract that exposes decision-consistent metadata, renames internal metadata `ETag` to `CacheTag`, and updates the package surface to match the new cache-validator semantics.

**Architecture:** Keep the existing tiered decision engine, but route every read through a richer response object that can represent `OK`, `NotModified`, and `NotFound` without forcing callers to infer semantics from side-channel store metadata. Rename `Metadata.ETag` to `Metadata.CacheTag` across the package so internal semantics describe cache-owned validators rather than origin-owned HTTP validators.

**Tech Stack:** Go, testify, existing daramjwee cache/store packages

---

## Chunk 1: Public API and Core Read Path

### Task 1: Add failing API-contract tests for richer `Get`

**Files:**
- Modify: `tests/api_contract_test.go`
- Modify: `tests/tiering_integration_test.go`

- [ ] Add tests that expect `Cache.Get(ctx, key, GetRequest{}, fetcher)` to return `*GetResponse`.
- [ ] Add tests that expect `GetResponse.Status`, `GetResponse.Body`, and `GetResponse.Metadata` on positive hits.
- [ ] Add tests that expect `IfNoneMatch` to produce `GetStatusNotModified` on positive hits.
- [ ] Add tests that expect stale `NotModified` responses to schedule background refresh without requiring a body close.
- [ ] Run targeted tests and confirm compile/test failure on the old API.

### Task 2: Implement new public API in root package

**Files:**
- Modify: `daramjwee.go`
- Modify: `cache.go`
- Modify: `tests/api_contract_test.go`

- [ ] Add `GetRequest`, `GetStatus`, and `GetResponse` types to the public API.
- [ ] Rename `Metadata.ETag` to `Metadata.CacheTag`.
- [ ] Change `Cache.Get` signature to `Get(ctx, key, req, fetcher) (*GetResponse, error)`.
- [ ] Refactor internal read helpers so they return response objects instead of bare streams.
- [ ] Preserve current stream-through semantics for `GetStatusOK`.
- [ ] Make conditional `NotModified` stale responses schedule refresh immediately.
- [ ] Keep runtime errors in `error`; use `GetStatusNotFound` for negative decisions.

## Chunk 2: Wrappers, Helpers, and Test Fixtures

### Task 3: Update wrappers and mocks to the new contract

**Files:**
- Modify: `pkg/cache/generic.go`
- Modify: `pkg/cache/generic_test.go`
- Modify: test helpers under `tests/`

- [ ] Update `GenericCache` to consume `GetResponse`.
- [ ] Preserve typed convenience methods while exposing metadata where appropriate.
- [ ] Rename all test fixture metadata fields from `ETag` to `CacheTag`.
- [ ] Run wrapper-focused tests and confirm green.

### Task 4: Update store-level tests and fixtures for metadata rename

**Files:**
- Modify: `pkg/store/**/**/*_test.go`
- Modify: `pkg/store/storetest/*.go`

- [ ] Replace `ETag` assertions and fixture setup with `CacheTag`.
- [ ] Keep serialized metadata compatibility logic intact where legacy on-disk formats are intentionally read.
- [ ] Run store package tests and confirm green.

## Chunk 3: Docs, Examples, and Release Surface

### Task 5: Migrate examples and docs to v0.6.0 semantics

**Files:**
- Modify: `README.md`
- Modify: `pkg/cache/README.md`
- Modify: `examples/**/*.go`

- [ ] Rewrite examples to use `GetRequest` and `GetResponse`.
- [ ] Document `CacheTag` as the cache-owned validator that apps may map to HTTP `ETag`.
- [ ] Document `GetStatusNotModified` and `GetStatusNotFound`.
- [ ] Update any wording that still implies `Metadata.ETag` is the public/internal canonical name.

### Task 6: Final verification

**Files:**
- Modify as needed from previous tasks

- [ ] Run focused tests for root package, wrappers, and affected integration suites.
- [ ] Run the full test suite if feasible.
- [ ] Summarize breaking changes as v0.6.0 release notes in the final response.
