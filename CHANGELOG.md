# Changelog

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
*   **New `ShutdownTimeout` Option**: A new `WithCloseTimeout` option has been added to configure the graceful shutdown period for the worker manager.
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
