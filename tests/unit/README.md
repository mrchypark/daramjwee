# Unit Tests

Individual component testing with isolated functionality.

## Test Categories

- **Cache Core**: `cache_test.go`, `cache_struct_test.go`
- **Configuration**: `config_validation_test.go`, `configuration_validation_test.go`, `options_test.go`
- **Buffer Management**: `buffer_pool_test.go`, `buffer_lifecycle_test.go`, `adaptive_buffer_pool_test.go`
- **Streaming**: `chunked_streaming_test.go`
- **Compression**: `compression_test.go`
- **Utilities**: `helpers_test.go`, `debug_test.go`
- **Logging**: `logging_test.go`, `buffer_pool_logging_test.go`
- **Memory**: `memory_efficiency_test.go`
- **Error Handling**: `tier_error_test.go`
- **Options**: `with_stores_option_test.go`

## Running Unit Tests

```bash
go test ./tests/unit/...
```