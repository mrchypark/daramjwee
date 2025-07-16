# Comprehensive Configuration Examples

This example includes comprehensive examples showing all configuration options of daramjwee cache. It demonstrates the flexibility of the cache through various configuration combinations.

## Included Configuration Examples

1. **Basic Memory Cache (Minimal Configuration)**
   - Simplest memory cache configuration
   - Default policy (no eviction)
   - Uses only minimal options

2. **Advanced Memory Cache (All Options)**
   - Uses LRU policy and stripe locks
   - Worker pool configuration
   - Timeout and TTL settings

3. **File Store Cache (Basic)**
   - Basic file store configuration
   - Uses temporary directory
   - Default policy

4. **File Store Cache (Advanced Options)**
   - Uses S3-FIFO policy
   - Uses hashed keys
   - Copy-and-Truncate for NFS compatibility
   - Uses Mutex locks

5. **Hybrid Multi-tier Cache**
   - Memory hot store (SIEVE policy)
   - File cold store (LRU policy)
   - Worker pool configuration
   - Various timeout settings

6. **Worker Strategy Configurations**
   - Pool strategy (default)
   - All strategy (execute all tasks immediately)
   - Various worker pool sizes and queue sizes

7. **Cache Policy Configurations**
   - LRU policy
   - S3-FIFO policy
   - SIEVE policy
   - No policy (unlimited)

8. **Lock Strategy Configurations**
   - Mutex locks
   - Stripe locks (various slot counts)
   - Compare lock strategies with same capacity and policy

9. **Compression Configuration Examples**
   - No compression
   - Gzip compression (various levels)
   - LZ4 compression (not implemented)
   - Zstd compression (not implemented)

10. **Timeout and TTL Configurations**
    - Fast response settings
    - Standard settings
    - Long cache settings
    - Immediate expiration settings

## How to Run

```bash
go run examples/config/main.go
```