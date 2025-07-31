# Test Organization

This directory contains all test files organized by category for better maintainability and clarity.

## Directory Structure

### `/unit/` - Unit Tests
Individual component testing with isolated functionality:
- Core cache operations
- Configuration validation
- Buffer pool management
- Individual store implementations
- Helper functions and utilities

### `/integration/` - Integration Tests
Multi-component interaction testing:
- Multi-tier cache scenarios
- Cross-component data flow
- Backward compatibility
- End-to-end workflows

### `/benchmark/` - Performance Tests
Performance measurement and regression testing:
- Throughput benchmarks
- Memory usage analysis
- Latency measurements
- Performance regression detection

### `/stress/` - Stress & Concurrency Tests
High-load and concurrent access testing:
- Race condition detection
- Concurrent safety validation
- Large-scale operation handling
- Resource exhaustion scenarios

## Running Tests

```bash
# Run all tests
go test ./tests/...

# Run specific category
go test ./tests/unit/...
go test ./tests/integration/...
go test ./tests/benchmark/...
go test ./tests/stress/...

# Run with race detection
go test -race ./tests/...

# Run benchmarks
go test -bench=. ./tests/benchmark/...
```

## Test Guidelines

- Unit tests should be fast and isolated
- Integration tests may require setup/teardown
- Benchmark tests should be consistent and repeatable
- Stress tests should validate concurrent safety