# Changelog

## v0.14.2

### 🔒 Correctness

*   **Miss-leader lifecycle fix**: Miss-leader registration now releases when the streaming fill completes (body close), not when the response is returned. Callers arriving while the leader's stream is open join as waiters instead of becoming new leaders and duplicating the origin fetch.
*   **Eager generation semantics restored**: The top-write generation is now snapshotted before the tier-0 read (non-creating), preserving the original invalidation semantics for stale refreshes. Writes committed after the snapshot still invalidate the refresh.
*   **Skip coalescing without fill**: Misses with `higherTiersClean=false` skip coalescing entirely, since waiting would only add latency without benefit.

### ✅ Testing

*   **End-to-end test suite** (`tests/e2e`): 18 scenarios exercising the public API through a real HTTP origin + HTTP front proxy + real store backends (memstore, filestore, objectstore).
    - Basic: cold/hot hit, conditional 304, negative caching, delete invalidation, origin failure, stale-while-revalidate
    - Streaming: partial read no-publish, leader body open waiter arrival
    - Concurrency: concurrent cold-request coalescing (20 callers), concurrent delete+get burst, concurrent PUT same key, stale refresh coalescing
    - Tiers: multi-tier restart promotion, file→objectstore chain with remote-durability probe
    - Runtime: cache-group isolation and shutdown, promotion probation
    - Edge cases: special characters in keys, PUT/GET round trip
*   Added `TestMissCoalescing_NoDuplicateFetchWhileLeaderStreams` race test (unit)

## v0.14.1

### ⚡ Performance

*   **Miss coalescing**: Concurrent misses for the same key now coalesce into a single origin fetch. The first caller becomes the miss leader and fills the top tier; waiters are served from the top tier once the fill publishes, falling back to their own fetch after a bounded wait. This eliminates cold-key stampedes for fast origins while preserving stream-through latency.
*   **Hot-hit allocation reduction**: Reduced fresh top-tier hit allocations from 13 to 5 per operation (786 B → 288 B).
    *   Top-write generation snapshots are now captured lazily, only when a stale entry needs a background refresh. Fresh hits no longer touch the write coordinator at all.
    *   `writeCoordinator` state (reservation map, delete-done channel) is now allocated lazily on first write/delete, keeping read-only keys allocation-free.
    *   `safeCloser` handlers are plain functions instead of boxed interfaces; fresh hits share the no-op cancel function.
    *   `debugLog` calls on hot paths are guarded so disabled logging causes no variadic packing allocations.
*   **MemStore large-payload copy elimination**: Large payloads (>1 MiB) transfer buffer ownership instead of cloning the full payload again on commit, halving large-object miss allocation (~2.1 MB → ~1.05 MB for 1 MiB objects).
*   **MemStore policy lock separation**: Eviction policy mutations now use a dedicated lock, so concurrent map readers never contend with LRU/S3-FIFO/SIEVE touches.
*   **Refresh deduplication**: Background refreshes are deduplicated per key; while one refresh is in flight, later stale hits reuse it instead of queueing duplicate jobs.

### ✨ Features

*   **2-hit promotion probation** (`WithPromotionProbation(maxEntries)`): When enabled, the first lower-tier hit for a key is served without promoting to the top tier; only the second hit promotes. Prevents one-hit wonders from polluting the hot tier. Deleting a key resets its probation state.

### 🔒 Correctness

*   **Fixed setup-context cancellation race**: A tier-0 entry published by a concurrent fill between the fast-path miss and the slow-path retry is now handled as a proper top-tier hit instead of continuing with a canceled setup context (previously returned `context canceled` and leaked the stream).

### ✅ Testing

*   **End-to-end test suite** (`tests/e2e`): HTTP origin + HTTP front proxy exercising the public API with real stores. Covers cold/hot hits, conditional 304, negative caching, delete invalidation, origin failure propagation, stale-while-revalidate, partial-read no-publish, concurrent cold-request coalescing, multi-tier restart promotion, file→objectstore chains, cache-group isolation, promotion probation, and PUT/GET round trips.
*   Added miss-coalescing tests (concurrent misses share one origin fetch, slow-leader fallback).
*   Added promotion-probation tests (second-hit promotion, delete reset).
*   Added refresh deduplication test (single in-flight refresh per key).

## v0.14.0

### 🔒 Correctness & Consistency

*   **Redis store removed**: Removed Redis store and dependencies to simplify the codebase and reduce external dependencies.
*   **streamTeeWriter pool lifetime fix**: Fixed use-after-free bug where `streamTeeWriter` was read after being returned to pool.
*   **Multi-tier delete bottom-up order**: Changed delete order from top-down to bottom-up to prevent value resurrection.
*   **Close/Shutdown completion barrier**: Added `sync.Once` and completion channel to ensure all callers wait for shutdown to complete.
*   **objectstore Close method**: Added `Close()` method to flush pending writes and clean up resources.
*   **MemStore buffer pool size limit**: Added 1 MiB limit for buffer pool returns to prevent memory retention.

### 📚 Documentation

*   **Consistency model**: Documented single-process, multi-process, and multi-writer consistency guarantees.
*   **Fill coordination**: Documented loader error, context cancellation, panic, and stale result scenarios.
*   **ObjectStore architecture**: Documented WAL/spool, segment writer, manifest/checkpoint, reader, compactor, GC, and remote adapter.
*   **Tier promotion semantics**: Documented 3-phase model (Read Visibility → Promotion Eligibility → Promotion Commit).
*   **Generation invariant**: Documented "observedGeneration == currentGeneration" enforcement.
*   **Store contract**: Documented MISS/ERROR separation and cache fill failure handling.
*   **Stampede prevention**: Documented per-key retry suppression and failure backoff strategies.
*   **Metrics abstraction**: Documented `MetricsCollector` interface (documentation-only).
*   **Recommended presets**: Documented configuration presets for common use cases.

### ✅ Testing

*   **Crash consistency tests**: Added 12 tests for concurrent delete/promotion, partial writes, generation fence, and close behavior.
*   **Eviction correctness tests**: Added 5 tests for LRU, S3-FIFO, and SIEVE eviction policies.
*   **Lock tuning benchmarks**: Added 9 benchmarks for concurrent read/write/delete performance.

### ✅ Verification

*   All unit tests pass
*   Race detector tests pass
*   ChatGPT Deep Reasoner review: P0-P2 all resolved

## v0.13.0

### ⚡ Performance

*   **Hot path allocation reduction**: Reduced allocations on the hot path by 3-9 allocs per operation through targeted pooling and elimination of unnecessary allocations.
    *   **`staleRefreshCallback` pooling**: Introduced `sync.Pool` for `staleRefreshCallback` structs, reducing allocations on stale cache hits.
    *   **`byteReadCloser` for memstore**: Replaced `bytes.NewReader` + `io.NopCloser` with a custom pooled `byteReadCloser`, eliminating 2 allocations per memstore `GetStream` call.
    *   **`nopCancelFunc` package-level variable**: Eliminated closure allocation on every `Get()` call by sharing a single no-op cancel function.
    *   **`ReadAll` buffer optimization**: Increased initial buffer from 512B to 4096B for fewer grow-and-copy cycles.
    *   **`normalizeEntityTag` manual trim**: Replaced `strings.TrimSpace` with manual ASCII trim to avoid string allocations.
    *   **`ifNoneMatchMatchesCacheTag` fast path**: Added fast path for single ETag (no commas) to avoid slice allocation.
    *   **Filestore `lenBuf` stack allocation**: Changed `make([]byte, 4)` to `var lenBuf [4]byte` for stack allocation.

### 🔒 Concurrency

*   **`writeCoordinator` RWMutex**: Changed `stateMu` from `sync.Mutex` to `sync.RWMutex`, allowing concurrent read-only checks on the hot path (`canAttemptExpectedTopWrite`).

### 📊 Benchmark Results (Apple M3, 8-core)

| Metric | v0.11.1 | v0.13.0 | Improvement |
|--------|---------|---------|-------------|
| Hot Hit Default TTL | 850ns, 35 allocs, 1.9KB | 675-684ns, 32 allocs, 1.53KB | **20% faster, -3 allocs** |
| Hot Hit Fresh | 870ns, 12 allocs, 657B | 820-827ns, 13 allocs, 785B | **6% faster** |
| Hot Hit Stale | 5.5μs, 35 allocs, 39KB | 3.3-3.5μs, 26 allocs, 1.23KB | **37% faster, -9 allocs** |

### ✅ Verification

*   All unit tests pass
*   Race detector tests pass
*   Benchmarks confirm 20-37% improvement on hot path

## v0.12.0

### 🧰 Internal Improvements

*   **golangci-lint v2 migration**: Migrated `.golangci.yml` from v1 to v2 format using `golangci-lint migrate`.
*   **Lint issue resolution**: Resolved all 108 lint issues across 101 files.
    *   Fixed errcheck: added `_ =` for go-kit/log calls and sync.Pool type assertions.
    *   Fixed errorlint: replaced `err ==` with `errors.Is()`, `%v` with `%w` format verbs.
    *   Removed unused code: `promoteLowerTierHitToTop`, `readMetadata`, `updateLocalEntries`, `deleteLocalEntry`, `manifestRoot`, `blobTimestampFromPath`, `registerActiveFill`.
    *   Fixed staticcheck: nil context → `context.Background()`, embedded field selector simplification, `reflect.Ptr` → `reflect.Pointer`.
    *   Fixed goimports: auto-formatted example files.
    *   Fixed misspell: `copywrite` → `copyright` in test names.
    *   Fixed ineffassign, unconvert, prealloc, nilerr, gosec issues.

### ✅ Verification

*   All unit tests pass
*   Race detector tests pass
*   golangci-lint reports 0 issues

## v0.11.1

### ⚡ Performance

*   **MemStore read lock optimization**: switched to `sync.RWMutex` for read operations, with separate write lock for `Touch()` calls to improve read parallelism.
*   **SIEVE eviction latency bound**: limited eviction scan to at most 2 full passes, preventing O(n) worst-case latency when all items have been recently accessed.
*   **FileStore initialization**: switched from `filepath.WalkDir` to `os.ReadDir` for faster cold-start, with separate handling for encoded key directory.
*   **FileStore copy strategy**: introduced `sync.Pool` for 32KB buffer reuse, reducing memory allocations during file copy operations.

### 🐛 Bug Fixes

*   **S3-FIFO eviction**: bounded main queue scan to half the queue size, preventing excessive scanning when many items have `wasHit` flag set.
*   **Boundary test stability**: fixed `TestIsCachedStale_ExactBoundary` timing issue by using a past timestamp.

### ✅ Verification

*   All unit tests pass
*   Race detector tests pass
*   Benchmarks confirm no performance regression

## v0.9.2

### 🧰 Internal Improvements

*   **Deduplicated `cloneMetadata`**: extracted to root package as exported `CloneMetadata`, removing duplication across `memstore` and `objectstore`.
*   **Shared striped lock manager**: extracted `internal/stripedlock` package, replacing duplicated implementations in `filestore` and `objectstore`.
*   **Split `write_coordinator.go`**: moved 3 coordinated sink wrappers (`coordinatedTopWriteSink`, `coordinatedStagedTopWriteSink`, `conditionalGenerationWriteSink`) to `write_coordinator_sinks.go`, reducing the main file from 1054 to 770 lines.
*   **Refactored `cache_read.go`**: introduced `lowerTierHitParams` struct to replace 9-parameter `handleLowerTierHit` and extracted `serveLowerTierWithoutPromotion` for cleaner branching.
*   **Extracted `cacheConfig` struct**: moved configuration-related fields from `DaramjweeCache` into a dedicated `cacheConfig` type.

### ⚡ Performance

*   **RedisStore `GetStream` pipelining**: combined 3 serial Redis round trips (GET meta, EXISTS data, STRLEN data) into a single pipeline.
*   **ObjectStore `publishManifest`**: replaced `strings.NewReader(string(bytes))` with `bytes.NewReader(bytes)` to avoid unnecessary allocation.
*   **FileStore initialization**: switched from `filepath.Walk` to `filepath.WalkDir` for faster cold-start on large caches.

### 🧹 Cleanup

*   Removed dead `chunkSize` constant from `redisstore`.
*   Added `.golangci.yml` with production-appropriate linter settings.
*   Extracted shared test helpers to `tests/testutil_test.go`.

### ✅ Verification

*   `go test -short ./...`
*   `go vet ./...`

## v0.9.1

### ⚠️ Breaking Changes & API Updates

*   **The experimental SQLite store package is not part of the current public surface**: `pkg/store/sqlitestore` is not included in this release line.

### 🧰 Migration Notes

*   If you tested `github.com/mrchypark/daramjwee/pkg/store/sqlitestore` from a branch or non-canonical tag snapshot, remove that import before upgrading on this release line.

### 🐛 Bug Fixes & Refinements

*   **Objectstore seal-failure cleanup is stricter**: failed segment commits now abort active and sealed staging paths, preserve seal and cleanup errors with commit context, sync successfully removed segment directories independently, and avoid redundant directory syncs when a staged file is already absent.
*   **Segment cleanup regressions are pinned by focused tests**: added coverage for sealed staging cleanup, joined cleanup errors, root-safe Unix permission behavior, and Windows test compilation safety.

### 📚 Documentation & Examples

*   **Objectstore tier examples now handle local tier initialization errors**: README examples assign and check `filestore.New(...)` before passing the tier into `WithTiers(...)`.

### ✅ Verification

*   `go test ./...`
*   `GOOS=windows go test -c ./pkg/store/objectstore/internal/segment`

## v0.8.0

### ⚠️ Breaking Changes & API Updates

*   **`DaramjweeCache` runtime fields are no longer part of the public surface**: the concrete cache type remains exported, but `Tiers`, `Worker`, logger state, timeout fields, and freshness fields are now internal implementation details instead of exported struct fields.
*   **Cache construction now has a standalone/group split**: `New(...)` creates a self-contained cache with its own background runtime, while `NewGroup(...)` creates a `CacheGroup` whose caches share one bounded background runtime.
*   **Group and cache runtime options are separated**: group construction uses the `WithGroup...` option surface, while group-attached caches use `WithWeight(...)` and `WithQueueLimit(...)` for per-cache runtime tuning. Standalone caches continue to use `WithWorkers(...)`, `WithWorkerQueue(...)`, `WithWorkerTimeout(...)`, and `WithWorkerStrategy(...)`.
*   **Unknown worker strategies now fail fast**: `WithWorkerStrategy(...)` accepts only `"pool"` and `"all"`, and invalid values now return a configuration error instead of silently falling back to `"pool"`.
*   **Objectstore tier initialization is now validated during `daramjwee.New(...)`**: misconfigured `objectstore` tiers fail cache construction immediately instead of deferring the failure until first store operation.

### 🧰 Migration Notes

*   Stop reading or mutating runtime fields on `*DaramjweeCache` directly. Treat `daramjwee.New(...)` as the construction boundary and keep concrete cache state internal to the package.
*   Use `daramjwee.New(...)` for a self-contained cache, or `daramjwee.NewGroup(...)` when several caches should share one bounded background runtime.
*   Keep the option surfaces separate: `WithGroupWorkers(...)`, `WithGroupWorkerTimeout(...)`, `WithGroupWorkerQueueDefault(...)`, and `WithGroupCloseTimeout(...)` configure the group itself, while `WithWeight(...)` and `WithQueueLimit(...)` configure caches created from that group.
*   Audit any custom configuration that relied on unknown worker strategy strings being accepted. Use `WithWorkerStrategy("pool")` or `WithWorkerStrategy("all")` explicitly.
*   If you build `objectstore` tiers dynamically, expect `daramjwee.New(...)` to return initialization errors earlier when the local directory or other objectstore prerequisites are invalid.

### 🐛 Bug Fixes & Refinements

*   **Cache orchestration is split into focused units**: stale reads, background refresh, persistence fanout, and context policy now live in dedicated files instead of one monolithic `cache.go`, making maintenance and review substantially easier.
*   **Background jobs now preserve request-scoped values without inheriting request cancellation**: refresh and persist work keep caller context values available to context-sensitive stores and fetchers, while still running under worker-managed deadlines.
*   **Invalidated fanout cleanup regained best-effort semantics**: destination-tier cleanup after generation invalidation now runs under a fresh timeout context again, so worker shutdown or timeout races do not leave stale persisted objects behind.
*   **Constructor and helper regressions are pinned by direct tests**: response/cancel wrappers, lower-tier promotion cleanup, objectstore init failures, and internal block/page/segment caches now have explicit regression coverage.
*   **New runnable `CacheGroup` example**: added a local example under `examples/cache_group` showing shared-runtime construction, per-cache weights, and mixed tier layouts without requiring external services.
*   **New runnable local objectstore examples**: added `examples/file_objstore_gcs_vind` and `examples/file_objstore_s3_vind` to smoke-test the ordered `FileStore -> objectstore` flow against local GCS and S3-compatible emulators on the `vcluster` Docker driver, and clarified that the older GCS examples target real cloud buckets.

### ✅ Verification

*   `go test ./...`
*   `(cd examples/file_objstore_gcs_vind && ./verify.sh)`
*   `(cd examples/file_objstore_s3_vind && ./verify.sh)`

## v0.6.2

### 🐛 Bug Fixes & Refinements

*   **Top-tier writes now coordinate by committed generation**: same-key foreground writes, deletes, stale refreshes, and promotions no longer let late closes overwrite newer visible state in tier 0.
*   **Lower-tier fanout and conditional promotion were hardened**: stale lower-tier promotion no longer leaks false `304 Not Modified` decisions, and same-destination fanout now avoids stale cleanup deleting a newer persisted value.
*   **Store write/delete contracts are now explicit and enforced**: built-in stores are covered by contract tests that require `BeginSet(...)` to keep the previously visible value intact until `Close()`/`Abort()`, and require `Delete(...)` not to wait on pending staged writes.

### 🚀 Performance & Validation

*   **Objectstore packed cold reads are cheaper without a block cache**: no-cache packed reads now use a single logical range read instead of block-by-block materialization, reducing cold-read latency and allocations.
*   **Concurrency verification coverage expanded substantially**: added write coordinator regression tests, store contract tests, background-worker stress tests, race tests, fuzz targets, and benchmark fixtures that separate cache-core costs from mock I/O overhead.

### 🧰 Notes

*   This release keeps the v0.6 public API surface, but custom `Store` implementations must preserve the currently visible value after `BeginSet(...)` until the returned sink is terminally closed or aborted.
*   Same-key write/delete timing is stricter than in `v0.6.1`; callers that relied on stale late closes winning races should expect newer visible state to win consistently instead.

### ✅ Verification

*   `go test ./...`
*   `go test ./... -race`
*   `go test ./pkg/store/objectstore -run '^$' -bench 'BenchmarkStore_ReadRemotePacked(Cold|Warm)$' -benchmem -cpu=1 -benchtime=2s -count=5`

## v0.6.1

### 🐛 Bug Fixes & Refinements

*   **Writer-lifetime key/path locks removed from `filestore` and `objectstore`**: same-key writers now stage concurrently without holding a long-lived key/path lock for their full lifetime, eliminating the main orphan-lock failure mode when a `WriteSink` is never terminally closed.
*   **Generation-based publish arbitration for staged writes**: visible state changes now happen only during publish, and stale late closes are discarded locally instead of reviving data over newer writes or deletes.
*   **Delete/write race handling hardened**: `filestore` and `objectstore` now keep newer tombstones ahead of stale writers, and stale objectstore deletes no longer remove remote manifest visibility when they lose the generation race.
*   **Expanded race and contention regression coverage**: added same-key overlap, stale-close, delete-race, and local contention test coverage across `filestore` and `objectstore`, plus a local race harness for lock-contention verification.

### 🧰 Notes

*   This release does not add cross-process coordination. The new generation arbitration remains process-local.

### ✅ Verification

*   `go test ./...`

## v0.6.0

### ⚠️ Breaking Changes & API Updates

*   **Public read API redesigned around request/response semantics**: `Cache.Get(...)` now accepts a `GetRequest` and returns a `GetResponse`, exposing `GetStatusOK`, `GetStatusNotModified`, and `GetStatusNotFound` without forcing callers to infer behavior from side-channel store reads.
*   **Metadata validator renamed to `CacheTag`**: Public and internal metadata now describe the cache-owned representation validator as `Metadata.CacheTag` instead of `Metadata.ETag`.

### 🚀 Features & Enhancements

*   **Decision-consistent conditional reads**: `IfNoneMatch` can now be evaluated directly against the cache's current representation, including stale-hit paths that trigger background refresh immediately when returning `GetStatusNotModified`.
*   **`GetResponse` implements `io.ReadCloser`**: Body-bearing responses still work naturally with `io.ReadAll`, `io.Copy`, and `defer resp.Close()` patterns.
*   **Backward-compatible metadata decoding**: Stored metadata can still decode legacy serialized `ETag` fields while writing the new `CacheTag` name.

### 🧰 Migration Notes

*   Update `cache.Get(ctx, key, fetcher)` to the new request/response form and handle `resp.Status` explicitly. For example:

    ```go
    resp, err := cache.Get(ctx, key, daramjwee.GetRequest{
        IfNoneMatch: clientETag,
    }, fetcher)
    if err != nil {
        return err
    }
    defer resp.Close()

    switch resp.Status {
    case daramjwee.GetStatusOK:
        // stream resp.Body
    case daramjwee.GetStatusNotModified:
        // return 304 / skip body work
    case daramjwee.GetStatusNotFound:
        // return 404 / handle negative cache
    }
    ```
*   Replace `Metadata.ETag` reads/writes with `Metadata.CacheTag`.

### ✅ Verification

*   `go test ./...`

## v0.5.0

### ⚠️ Breaking Changes & API Updates

*   **Option naming redesigned across the public API**: Root cache options and store-specific option surfaces were renamed for shorter, more readable call sites.
    *   Root cache configuration now centers on `WithFreshness(...)`, `WithTierFreshness(...)`, `WithOpTimeout(...)`, `WithCloseTimeout(...)`, `WithWorkers(...)`, `WithWorkerQueue(...)`, `WithWorkerTimeout(...)`, and `WithWorkerStrategy(...)`.
    *   `filestore` options now use the simplified `WithCopyWrite(...)` and `WithEviction(...)` names.
    *   `objectstore` options now use the simplified `WithDir(...)`, `WithGCGrace(...)`, `WithPackThreshold(...)`, `WithPagedThreshold(...)`, `WithPageCache(...)`, `WithBlockCache(...)`, `WithCheckpointCache(...)`, and `WithCheckpointTTL(...)` names.

### 🚀 Features & Enhancements

*   **Per-tier freshness overrides**: Added `WithTierFreshness(index, positive, negative)` so ordered tier chains can override freshness on a specific tier while keeping a chain-wide default via `WithFreshness(...)`.
*   **Expanded migration coverage**: Updated examples, READMEs, and tests across the repository to use the redesigned option names consistently.
*   **Design documentation for the API redesign**: Added design and implementation notes covering the per-tier freshness work and the option naming redesign.

### 🐛 Bug Fixes & Refinements

*   **Worker strategy preserved during the rename**: Restored configurable worker strategy support through `WithWorkerStrategy(...)` so the naming cleanup does not silently hardcode the background execution mode.
*   **Cache concrete type compatibility retained**: Restored exported `DaramjweeCache` fields to avoid breaking downstream code that type-asserts to the concrete cache type for inspection or tests.
*   **Objectstore write lifecycle now follows request context semantics**: `objectstore.Store` now explicitly marks `BeginSet(...)` as context-bound, and objectstore write sinks keep using the request context through `Write` and `Close`.
*   **Additional validation hardening**: Tightened per-tier freshness override validation, deduplicated freshness validation logic, and aligned objectstore internal threshold naming.

### ✅ Verification

*   `go test ./...`

## v0.4.2

### 🚀 Features & Enhancements

*   **Prod-like store compare harness**: Added a reusable prod-like comparison workload, baseline templates, and an azurite-aware runner for `filestore` and `objectstore` store validation.
*   **Expanded store comparison docs**: Documented the new comparison workflow in the `filestore` and `objectstore` READMEs so perf and correctness checks are easier to reproduce.

### 🐛 Bug Fixes & Refinements

*   **More stable stale-refresh integration coverage**: Tightened tiering integration timing so stale-close behavior and no-promotion expectations are asserted more reliably.

### 🧰 Maintenance

*   **Go toolchain and workflow refresh**: Updated CI to Go `1.26.1` and refreshed module dependencies for the current toolchain.
*   **Security dependency update**: Bumped indirect `golang.org/x/crypto` from `0.39.0` to `0.45.0`.

### ✅ Verification

*   `go test ./...`

## v0.4.1

### 🚀 Features & Enhancements

*   **Objectstore local spool reclaim**: `objectstore` now treats local disk as a flush spool and reclaims sealed local segments after remote commit, instead of retaining them as an implicit local read cache.
*   **Checkpoint metadata memory cache**: Added an in-memory shard checkpoint cache for remote `latest.json` snapshots to reduce repeated metadata fetch and decode cost on hot read paths.
*   **Expanded objectstore docs and examples**: Added more detailed `objectstore` tuning guidance and aligned the file/objectstore examples around the current GCS-backed presets and tiered deployment model.

### 🐛 Bug Fixes & Refinements

*   **Safer local/reclaim read races**: Hardened `objectstore` local-open retry logic so reclaim, overwrite, and disappearing-local races fall back safely without surfacing stale or incorrect generations.
*   **Correct multi-writer checkpoint merges**: Flushes now build their shard checkpoint merge base from a fresh remote checkpoint instead of a cached snapshot, avoiding cross-writer key loss when checkpoint caching is enabled.

### ✅ Verification

*   `go test ./...`
*   `go test -race ./pkg/store/objectstore ./tests`
*   `go test ./pkg/store/objectstore -run '^$' -bench 'BenchmarkStore_(ReadRemotePackedCold|ReadRemotePackedWarm)$' -benchmem -cpu=1 -benchtime=2s -count=5`

## v0.4.0

### ⚠️ Breaking Changes & API Updates

*   **Ordered tier configuration replaces hot/cold wiring**: The cache is now configured with `WithTiers(...)` and ordered tier terminology (`tier 0`, `tier 1`, ...). The old hot/cold-specific wiring and documentation have been removed.
*   **Store write API changed to staged publish semantics**: `Store.SetWithWriter(...)` has been replaced by `Store.BeginSet(...)`, which returns a `WriteSink`.
    *   `WriteSink.Close()` publishes the new value.
    *   `WriteSink.Abort()` discards the in-flight value.
*   **Pure streaming is now the default fill model**: lower-tier hits and misses stream directly to callers while filling tier 0, instead of fully writing and reopening cached data first.

### 🚀 Features & Enhancements

*   **Ordered multi-tier cache flow**: The cache now treats stores as an ordered tier chain, promoting lower-tier hits back into tier 0 and fanning out background persistence where appropriate.
*   **First-party objectstore backend**: Added a first-party `objectstore` backend with local ingest, packed remote segments, recovery, and block-cache-backed read paths.
*   **Expanded benchmark coverage**: Added benchmark variants that separate cache-core overhead from fixture costs, including prebuilt fetcher and direct sink scenarios for large miss paths.

### 🐛 Bug Fixes & Refinements

*   **304 / stale refresh correctness**: Stale revalidation paths now preserve stream lifecycle correctly, avoid self-deadlocks, and repopulate tier 0 / refresh `CachedAt` after `304 Not Modified` responses.
*   **Safer store edge cases**: Tightened filestore namespace ownership and reindexing behavior, hardened memstore sink lifecycle handling, and preserved legacy objstore adapter compatibility.
*   **Objectstore recovery and read-path hardening**: Fixed stale catalog overwrite hazards, protected recovery against remote-backed entry loss, and rejected truncated packed/range reads with explicit EOF errors instead of looping.

### 📚 Documentation & Examples

*   Updated the main README and examples to use ordered-tier terminology consistently.
*   Switched objectstore examples from Azure placeholders to Google Cloud Storage placeholders.

### ✅ Verification

*   `go test ./...`
*   `go test -race ./...`
*   Added targeted cache/objectstore benchmarks and miss-path fixture decomposition benchmarks.

## v0.2.0

### 🚀 Features & Enhancements

*   **`memstore` Performance Boost**: Significantly improved the performance of the in-memory store (`memstore`) by integrating `sync.Pool` for `memStoreWriter` and `bytes.Buffer`. This optimization drastically reduces memory allocations and garbage collection overhead, leading to faster write operations, especially under high concurrency.
    *   **Time per operation:** ~3% faster
    *   **Memory per operation:** ~58% reduction
    *   **Allocations per operation:** 66% reduction (from 3 to 1 allocs/op)
*   **Robust Worker Shutdown**: The worker shutdown process is now more robust. The `Shutdown` method now accepts a timeout, ensuring that the cache can terminate gracefully without waiting indefinitely for long-running jobs.
*   **Graceful Job Handling**: The worker pool now gracefully handles full queues by dropping new jobs instead of blocking, preventing backpressure issues. A warning is logged when a job is dropped.
*   **Improved Eviction Logic**: The `memstore` eviction loop is now protected against infinite loops that could occur if an `EvictionPolicy` consistently returns non-existent keys.

### ⚠️ Breaking Changes & API Updates

*   **Configuration Options Renamed for Clarity**:
    *   `WithGracePeriod` has been renamed to `WithCache` to more clearly represent its function of setting the freshness lifetime for positive cache entries.
    *   `WithNegativeCache`'s `gracePeriod` parameter is now named `freshFor`.
*   **New `ShutdownTimeout` Option**: A new `WithShutdownTimeout` option has been added to configure the graceful shutdown period for the worker manager.
*   **Error Handling on Closed Cache**: Calling `Get`, `Set`, or `Delete` on a closed cache instance now immediately returns an `ErrCacheClosed` error, providing clearer feedback.

### 🐛 Bug Fixes & Refinements

*   **Safer Concurrent Deletion**: The `Delete` method in `DaramjweeCache` now deletes from the hot and cold stores sequentially to prevent potential deadlocks that could arise from using `errgroup` with nested locking.
*   **Correct `CachedAt` Timestamping**: The `CachedAt` metadata field is now correctly set at the time of caching for all scenarios, including direct `Set` calls, cold cache promotions, and background refreshes.

### ✅ Testing

*   **Enhanced Race Condition Detection**: Added a new chaos test (`TestCache_Chaos_RaceCondition`) specifically designed to be run with the `-race` flag to detect data races under heavy concurrent load.
*   **Goroutine Leak Prevention**: Added tests to ensure that background goroutines (e.g., for object storage uploads) are properly terminated when the context is canceled, preventing goroutine leaks.
*   **Deterministic Tests**: Replaced `time.Sleep` with channel-based synchronization in several tests to create more reliable and deterministic test cases.
*   **Edge Case Validation**: Added new tests to validate behavior in edge cases, such as:
    *   Eviction loops with misbehaving policies.
    *   Orphan file creation on metadata write failures in `filestore`.
    *   Correct error propagation in `multiCloser`.

### 🗑️ Removed

*   Removed the `pkg/picker` and `pkg/transport` packages, which were related to a distributed store implementation that is no longer part of the core focus.
