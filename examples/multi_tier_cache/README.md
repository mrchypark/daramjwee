# Multi-Tier Cache Example

This example demonstrates the N-tier cache architecture capabilities of daramjwee, showing how to configure and use multiple cache tiers for optimal performance and cost efficiency.

## Features Demonstrated

- **N-tier Configuration:** Memory → File → Cloud storage hierarchy
- **Automatic Promotion:** Data promotion from lower to upper tiers
- **Performance Comparison:** Single vs. multi-tier performance
- **Tier-specific Logging:** Detailed logging for each cache tier
- **Migration Examples:** Legacy to N-tier configuration migration

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Memory Tier   │    │   File Tier     │    │   Cloud Tier    │
│   (stores[0])   │    │   (stores[1])   │    │   (stores[2])   │
│                 │    │                 │    │                 │
│ • Fastest       │    │ • Medium Speed  │    │ • Slowest       │
│ • Limited Size  │    │ • Larger Size   │    │ • Unlimited     │
│ • Primary Tier  │    │ • Persistent    │    │ • Distributed   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Cache Behavior

1. **Lookup:** Sequential check from Memory → File → Cloud
2. **Hit in Memory:** Serve immediately (fastest path)
3. **Hit in File:** Serve + promote to Memory
4. **Hit in Cloud:** Serve + promote to Memory and File
5. **Miss:** Fetch from origin + cache in Memory

## Running the Example

```bash
go run examples/multi_tier_cache/main.go
```

## Configuration Examples

### Single Tier
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore),
)
```

### Two Tier
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore, fileStore),
)
```

### Three Tier
```go
cache, err := daramjwee.New(logger,
    daramjwee.WithStores(memStore, fileStore, cloudStore),
)
```

## Performance Characteristics

| Tier | Access Speed | Capacity | Persistence | Cost |
|------|-------------|----------|-------------|------|
| Memory | ~1ms | Limited | No | High |
| File | ~10ms | Large | Yes | Medium |
| Cloud | ~100ms | Unlimited | Yes | Low |

## Use Cases

- **Web Applications:** Memory + File for session and content caching
- **CDN Systems:** Memory + File + Cloud for global content distribution
- **Data Processing:** Memory + File for intermediate results caching
- **Microservices:** Memory + Distributed cache for service communication