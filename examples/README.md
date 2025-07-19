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

Shows how to use daramjwee cache with a basic HTTP server using single-tier N-tier configuration. Demonstrates file-based store and ETag-based conditional requests.

### 2. Multi-Tier Cache Example (multi_tier_cache)

**NEW**: Comprehensive demonstration of N-tier cache architecture:

- Single, two, and three-tier configurations
- Memory → File → Cloud storage hierarchy
- Automatic tier promotion and performance comparison
- Legacy configuration migration examples
- Interactive HTTP server with tier-aware logging

### 3. Scenario-based Examples (scenarios)

Shows daramjwee cache configurations tailored to various real-world usage scenarios:

- Web proxy cache with N-tier setup
- API response cache with memory + file tiers
- Image/media cache with multi-tier storage
- Database query cache with promotion logic
- CDN edge cache with distributed tiers
- Microservice inter-communication cache
- Development/test environment cache
- High-performance read-only cache

### 4. Advanced Features Examples (advanced)

Demonstrates advanced features and performance testing with N-tier architecture:

- Compression functionality tests
- Performance benchmark tests (single vs multi-tier)
- Concurrency tests with tier-aware locking
- Memory usage tests with tier promotion
- Cache policy comparison tests
- Lock strategy performance comparison
- TTL and expiration tests
- Error handling and recovery tests

### 5. HTTP Server Integration Examples (http)

Shows practical examples of integrating daramjwee cache with HTTP servers:

- Web proxy server with N-tier caching
- API caching server with automatic promotion
- Static file server with memory + file tiers
- Database caching server with tier optimization

### 6. Comprehensive Configuration Examples (config)

Comprehensive examples showing all configuration options of daramjwee cache:

- Basic memory cache (single-tier)
- Advanced memory cache with file backup
- Multi-tier cache configurations
- N-tier vs legacy configuration comparison
- Worker strategy configurations
- Cache policy configurations
- Lock strategy configurations
- Compression configurations
- Timeout and TTL configurations