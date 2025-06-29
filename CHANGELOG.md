# Changelog

## v0.2.0 (Unreleased)

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
