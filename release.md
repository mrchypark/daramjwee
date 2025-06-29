# Release v0.1.1

This is a patch release focused on enhancing the performance and efficiency of the in-memory storage backend (`memstore`).

## 📈 Performance Improvements

- **Optimized `memstore` for lower memory usage and faster performance:** Implemented `sync.Pool` for `memStoreWriter` and `bytes.Buffer` objects. This change significantly reduces memory allocations and lowers the garbage collector's workload, especially in high-concurrency scenarios involving frequent write operations.

### Benchmark Comparison

The improvements are demonstrated by the following benchmark results for the `BenchmarkMemStore_ConcurrentReadWrite` test:

**Before (v0.1.0):**
- **Time per operation:** `130.6 ns/op`
- **Memory per operation:** `136 B/op`
- **Allocations per operation:** `3 allocs/op`

**After (v0.1.1):**
- **Time per operation:** `126.6 ns/op` (~3% faster)
- **Memory per operation:** `56 B/op` (~58% reduction)
- **Allocations per operation:** `1 allocs/op` (66% reduction)

## What's Changed

*   perf(memstore): Reuse writers and buffers with sync.Pool to reduce memory allocations.

**Full Changelog**: `https://github.com/mrchypark/daramjwee/compare/v0.1.0...v0.1.1`
