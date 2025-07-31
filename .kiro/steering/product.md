# Product Overview

**daramjwee** is a pragmatic and lightweight hybrid caching middleware for Go applications.

## Core Purpose
Acts as a caching layer between applications and origin data sources (databases, APIs) to provide efficient, stream-based hybrid caching with high throughput at low cost in cloud-native environments.

## Key Features
- **Stream-based API**: All data processed through `io.Reader`/`io.Writer` interfaces for memory efficiency
- **Hybrid multi-tier caching**: Hot tier (fast access) + Cold tier (larger capacity) 
- **Modular architecture**: Pluggable storage backends, eviction policies, and worker strategies
- **Advanced eviction algorithms**: LRU, S3-FIFO, and SIEVE policies
- **ETag-based optimization**: Avoids unnecessary data transfer with conditional requests
- **Negative caching**: Caches "not found" states to prevent repeated wasteful requests
- **Stale-while-revalidate**: Serves stale data while refreshing in background

## Target Use Cases
- High-throughput proxy/caching scenarios
- Cloud-native applications requiring efficient resource usage
- Applications needing sophisticated caching strategies beyond simple LRU
- Systems requiring atomic write guarantees and concurrent access patterns