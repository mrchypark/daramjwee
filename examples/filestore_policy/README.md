# FileStore Policy Examples

This directory contains examples demonstrating how to use various eviction policies with FileStore.

## 📁 File Structure

```
examples/filestore_policy/
├── README.md                    # This file
├── main.go                     # Simple basic demo
├── filestore_policy_test.go    # Functional tests
├── config.yaml                 # Configuration example
├── demo/
│   └── policy_comparison.go    # Visual policy comparison demo
└── benchmark/
    └── policy_benchmark_test.go # Performance benchmark tests
```

## 🚀 Usage

### 1. Basic Demo (main.go)

Demonstrates basic FileStore operations with LRU policy.

```bash
go run examples/filestore_policy/main.go
```

**Features:**
- Simple and intuitive demo
- Basic LRU policy behavior verification
- Easy to understand with visual output

### 2. Policy Comparison Demo

Visually compares the behavior of three different policies.

```bash
go run examples/filestore_policy/demo/policy_comparison.go
```

**Example Output:**
```
🔍 FileStore Policy Comparison Demo
===================================

📋 Test 1: LRU Policy
   Least Recently Used - evicts oldest accessed items

   📝 Writing 3 files (200 bytes each)...
   👆 Accessing file1 to make it 'recently used'...
   📝 Writing large file (400 bytes) to trigger eviction...
   📊 Results for LRU:
      Initial state: file1✅ file2✅ file3✅
      After access:  file1✅ file2✅ file3✅
      After eviction: file1✅ file2❌ file3❌ large_file✅
      🗑️  Evicted: [file2 file3]
```

### 3. Functional Tests

Tests the basic functionality of each policy.

```bash
go test ./examples/filestore_policy/
```

**Test Coverage:**
- LRU policy behavior verification
- S3-FIFO policy basic functionality
- SIEVE policy basic functionality
- No-eviction scenario testing

### 4. Performance Benchmarks

Quantitatively measures the performance of each policy.

```bash
# Run all benchmarks
go test -bench=. ./examples/filestore_policy/benchmark/

# Benchmark specific policy only
go test -bench=BenchmarkFileStorePolicy_LRU ./examples/filestore_policy/benchmark/

# Include memory allocation information
go test -bench=. -benchmem ./examples/filestore_policy/benchmark/

# Adjust benchmark time (for more accurate results)
go test -bench=. -benchtime=10s ./examples/filestore_policy/benchmark/
```

**Benchmark Types:**
- `BenchmarkFileStorePolicy_LRU`: LRU policy performance
- `BenchmarkFileStorePolicy_S3FIFO`: S3-FIFO policy performance  
- `BenchmarkFileStorePolicy_SIEVE`: SIEVE policy performance
- `BenchmarkFileStorePolicy_NoEviction`: No eviction case
- `BenchmarkFileStorePolicyMixed_*`: Mixed read/write workloads

**Example Benchmark Results (Apple M3):**
```
BenchmarkFileStorePolicy_LRU-8          6844    170695 ns/op    2360 B/op    32 allocs/op
BenchmarkFileStorePolicy_S3FIFO-8       6925    175500 ns/op    2376 B/op    32 allocs/op
BenchmarkFileStorePolicy_SIEVE-8        7423    181920 ns/op    2460 B/op    32 allocs/op
BenchmarkFileStorePolicy_NoEviction-8   6891    179795 ns/op    2352 B/op    30 allocs/op

BenchmarkFileStorePolicyMixed_LRU-8     18472   67279 ns/op     1202 B/op    18 allocs/op
BenchmarkFileStorePolicyMixed_S3FIFO-8  18327   73283 ns/op     1213 B/op    18 allocs/op
BenchmarkFileStorePolicyMixed_SIEVE-8   14644   79488 ns/op     1215 B/op    18 allocs/op
```

## 📊 Policy Characteristics Comparison

| Policy | Time Complexity | Space Complexity | Features | Suitable Use Cases |
|--------|-----------------|------------------|----------|-------------------|
| **LRU** | O(1) | O(n) | Predictable, simple implementation | General caching, strong temporal locality workloads |
| **S3-FIFO** | O(1) | O(n) | High hit rate, frequency-aware | Web caching, diverse access patterns |
| **SIEVE** | O(1) | O(n) | Low overhead, balanced performance | High-performance requirements, memory-constrained environments |

## 🔧 Customization

### 1. Testing New Policies

```go
// Create custom policy
customPolicy := policy.NewS3FIFO(2*1024*1024, 15) // 2MB, 15% small queue

store, err := filestore.New(
    "/path/to/cache",
    logger,
    filestore.WithCapacity(2*1024*1024),
    filestore.WithEvictionPolicy(customPolicy),
)
```

### 2. Adding Benchmark Scenarios

You can add new benchmark functions to `policy_benchmark_test.go`:

```go
func BenchmarkCustomScenario(b *testing.B) {
    // Custom benchmark logic
}
```

### 3. Configuration-based Testing

You can create configuration file-based tests by referring to `config.yaml`.

## 📈 Performance Optimization Tips

1. **Capacity Settings**: Too small causes frequent eviction, too large wastes memory
2. **Policy Selection**: Choose policy that matches your workload patterns
3. **NFS Environment**: Use `WithCopyAndTruncate()` option
4. **Monitoring**: Adjust log levels to minimize performance impact

## 🐛 Troubleshooting

### Common Issues

1. **Permission denied**: Check cache directory permissions
2. **Disk full**: Verify capacity settings and actual disk space  
3. **Performance issues**: Use benchmarks to identify bottlenecks

### Debugging

```go
// Enable debug logging
logger := level.NewFilter(logger, level.AllowDebug())

// Check detailed eviction logs
```

## 📚 Additional Resources

- [daramjwee Main Documentation](../../README.md)
- [Policy Package Documentation](../../pkg/policy/)
- [FileStore Documentation](../../pkg/store/filestore/)