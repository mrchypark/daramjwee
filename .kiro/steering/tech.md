# Technology Stack

## Language & Runtime
- **Go 1.24.1** - Modern Go with latest features
- Requires Go modules support

## Key Dependencies
- **github.com/go-kit/log** - Structured logging throughout the application
- **github.com/thanos-io/objstore** - Cloud object store integration (S3, GCS, Azure)
- **github.com/stretchr/testify** - Testing framework with assertions and mocking
- **github.com/zeebo/xxh3** - Fast hashing for internal operations
- **golang.org/x/sync** - Extended synchronization primitives

## Build System
Uses standard Go toolchain with Make for common tasks:

### Common Commands
```bash
# Run tests with coverage
make coverage          # Generate HTML coverage report
make coverage-text     # Show coverage in terminal

# Run benchmarks
make bench            # Run all benchmarks, save to bench_results.txt

# Standard Go commands
go test ./...         # Run all tests
go build ./cmd/daramjwee  # Build CLI binary
go mod tidy          # Clean up dependencies
```

## Architecture Patterns
- **Interface-driven design**: Core components (Store, EvictionPolicy, Fetcher) are interfaces
- **Stream-based processing**: All I/O through io.Reader/io.Writer for memory efficiency
- **Worker pool pattern**: Background task processing with configurable strategies
- **Null object pattern**: Default no-op implementations (nullEvictionPolicy, nullStore)
- **Option pattern**: Functional configuration with validation
- **Striped locking**: Reduces contention for concurrent operations

## Testing Strategy
- Comprehensive unit tests with mocks
- Integration tests for multi-tier scenarios  
- Stress tests for concurrent operations
- Benchmark tests for performance validation
- Race condition detection with `-race` flag
- go test with timeout all the time

## Code
- code first and import with ide and check correct