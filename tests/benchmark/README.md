# Benchmark Tests

Performance measurement and regression testing.

## Test Categories

- **Cache Performance**: `cache_benchmark_test.go`
- **Buffer Pool Performance**: `adaptive_buffer_pool_benchmark_test.go`
- **Large Data Handling**: `large_data_performance_test.go`
- **N-tier Performance**: `n_tier_benchmark_test.go`
- **Performance Analysis**: `performance_analysis_test.go`
- **Regression Testing**: `performance_regression_test.go`

## Running Benchmark Tests

```bash
# Run all benchmarks
go test -bench=. ./tests/benchmark/...

# Run specific benchmark
go test -bench=BenchmarkCacheGet ./tests/benchmark/...

# Save results to file
go test -bench=. ./tests/benchmark/... > bench_results.txt
```

## Benchmark Guidelines

- Run benchmarks multiple times for consistency
- Use `-benchmem` flag to measure memory allocations
- Compare results across different configurations