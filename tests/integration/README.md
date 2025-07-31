# Integration Tests

Multi-component interaction testing.

## Test Categories

- **Multi-tier Operations**: `multi_tier_promotion_test.go`, `n_tier_integration_test.go`
- **Cache Integration**: `cache_integration_test.go`
- **Buffer Pool Integration**: `adaptive_buffer_pool_integration_test.go`
- **Compatibility**: `backward_compatibility_test.go`

## Running Integration Tests

```bash
go test ./tests/integration/...
```

These tests may require more setup and take longer to run than unit tests.