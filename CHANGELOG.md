# Changelog

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
