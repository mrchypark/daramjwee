# Stress & Concurrency Tests

High-load and concurrent access testing.

## Test Categories

- **Race Conditions**: `race_test.go`
- **Concurrent Safety**: `concurrent_safety_test.go`
- **Large Operations**: `large_operation_manager_test.go`
- **General Stress**: `stress_test.go`

## Running Stress Tests

```bash
# Run with race detection (recommended)
go test -race ./tests/stress/...

# Run stress tests with timeout
go test -timeout=30s ./tests/stress/...

# Run with verbose output
go test -v ./tests/stress/...
```

## Important Notes

- Always run with `-race` flag to detect race conditions
- These tests may take longer and use more resources
- Some tests may require specific system configurations