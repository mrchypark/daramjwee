# Lock Implementations Benchmarks

This directory contains various lock implementations used within `daramjwee` and their performance benchmarks.

## Benchmark Results

Here's a comparison of the performance (ns/op, lower is better) of different locking mechanisms under varying read/write ratios. Benchmarks were run on an Apple M1 chip.

| Lock Implementation | Read 90% (Read-heavy) | Read 50% (Mixed) | Read 10% (Write-heavy) |
| :------------------ | :-------------------- | :--------------- | :--------------------- |
| `MutexLock`         | ~32.84 ns/op          | ~57.17 ns/op     | ~105.9 ns/op           |
| `StripeLock`        | ~20.53 ns/op          | ~23.73 ns/op     | ~22.64 ns/op           |

**Analysis:**

*   **`StripeLock`** consistently shows the **best performance** across all scenarios. This is because it effectively reduces contention by distributing lock requests across multiple mutexes.
*   **`MutexLock`** performs well, but is generally slower than `StripeLock` as contention increases.
