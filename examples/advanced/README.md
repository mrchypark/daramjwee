# Advanced Features and Performance Test Examples

This example demonstrates advanced features and performance testing of daramjwee cache. It tests various aspects including compression, benchmarks, concurrency, memory usage, and more.

## Included Tests

1. **Compression Functionality Tests**
   - Comparison of various compression algorithms (None, Gzip)
   - Compression ratio measurement for different data sizes
   - Compression/decompression integrity verification

2. **Performance Benchmark Tests**
   - Performance comparison across different cache policies
   - Throughput measurement (ops/sec)
   - Real performance measurement after warmup

3. **Concurrency Tests**
   - Performance measurement at various concurrency levels
   - Parallel request processing capability testing
   - Consistency verification under concurrent access

4. **Memory Usage Tests**
   - Memory usage measurement when caching large amounts of data
   - GC execution count monitoring
   - Memory efficiency analysis

5. **Cache Policy Comparison Tests**
   - Comparison of LRU, SIEVE, S3-FIFO policies
   - Hit rate measurement
   - Processing time comparison

6. **Lock Strategy Performance Comparison**
   - Performance comparison between Mutex vs Stripe locks
   - Stripe lock performance measurement with various slot counts
   - Performance testing under contention scenarios

7. **TTL and Expiration Tests**
   - Cache item expiration testing
   - Performance comparison before and after expiration
   - Background refresh verification

8. **Error Handling and Recovery Tests**
   - Error situation simulation
   - Recovery mechanism testing
   - Cached data consistency verification

## How to Run

```bash
go run examples/advanced/main.go
```