# daramjwee Examples Collection

This directory contains various usage examples for the daramjwee cache library.

## How to Run Examples

You can run all examples using the unified runner:

```bash
go run examples/cmd/main.go <command>
```

### Available Commands

- `basic` - Run basic HTTP server example
- `scenarios` - Run scenario-based configuration examples
- `advanced` - Run advanced features and performance test examples
- `http` - Run HTTP server integration examples
- `config` - Run comprehensive configuration examples
- `all` - Run all examples (excluding servers)

### Usage Examples

```bash
# Run basic HTTP server example
go run examples/cmd/main.go basic

# Run scenario-based configuration examples
go run examples/cmd/main.go scenarios

# Run advanced features and performance test examples
go run examples/cmd/main.go advanced

# Run HTTP server integration examples
go run examples/cmd/main.go http

# Run comprehensive configuration examples
go run examples/cmd/main.go config

# Run all examples (excluding servers)
go run examples/cmd/main.go all
```

### Running Individual Examples Directly

Each example can also be run independently:

```bash
go run examples/basic/main.go
go run examples/scenarios/main.go
go run examples/advanced/main.go
go run examples/http/main.go
go run examples/config/main.go
```

## Example Descriptions

### 1. Basic Example (basic)

Shows how to use daramjwee cache with a basic HTTP server. Uses file-based store and handles ETag-based conditional requests.

### 2. Scenario-based Examples (scenarios)

Shows daramjwee cache configurations tailored to various real-world usage scenarios:

- Web proxy cache
- API response cache
- Image/media cache
- Database query cache
- CDN edge cache
- Microservice inter-communication cache
- Development/test environment cache
- High-performance read-only cache

### 3. Advanced Features Examples (advanced)

Demonstrates advanced features and performance testing of daramjwee:

- Compression functionality tests
- Performance benchmark tests
- Concurrency tests
- Memory usage tests
- Cache policy comparison tests
- Lock strategy performance comparison
- TTL and expiration tests
- Error handling and recovery tests

### 4. HTTP Server Integration Examples (http)

Shows practical examples of integrating daramjwee cache with HTTP servers:

- Web proxy server
- API caching server
- Static file server
- Database caching server

### 5. Comprehensive Configuration Examples (config)

Comprehensive examples showing all configuration options of daramjwee cache:

- Basic memory cache
- Advanced memory cache
- File store cache
- Hybrid multi-tier cache
- Worker strategy configurations
- Cache policy configurations
- Lock strategy configurations
- Compression configurations
- Timeout and TTL configurations