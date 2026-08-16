# Recommended Presets

This document provides recommended configuration presets for common use cases.

## Basic Cache

For simple caching needs with a single tier:

```go
cache, err := daramjwee.New(logger,
    daramjwee.WithTiers(memstore.New(0, nil)),
    daramjwee.WithFreshness(5*time.Minute, 30*time.Second),
    daramjwee.WithOpTimeout(5*time.Second),
)
```

## File-Based Cache

For persistent local caching:

```go
fileStore, err := filestore.New("/var/lib/cache", logger)
if err != nil {
    return err
}

cache, err := daramjwee.New(logger,
    daramjwee.WithTiers(fileStore),
    daramjwee.WithFreshness(10*time.Minute, 1*time.Minute),
    daramjwee.WithOpTimeout(10*time.Second),
)
```

## Multi-Tier Cache

For hot/cold tier architecture:

```go
hot := memstore.New(100*1024*1024, policy.NewLRU()) // 100MB
cold, err := filestore.New("/var/lib/cache", logger)
if err != nil {
    return err
}

cache, err := daramjwee.New(logger,
    daramjwee.WithTiers(hot, cold),
    daramjwee.WithFreshness(1*time.Minute, 30*time.Second),
    daramjwee.WithTierFreshness(0, 5*time.Minute, 1*time.Minute),
    daramjwee.WithOpTimeout(5*time.Second),
)
```

## ObjectStore Backend

For durable remote caching:

```go
bucket, err := objstore.NewBucket(ctx, "s3://my-bucket", logger)
if err != nil {
    return err
}

objectStore := objectstore.New(bucket, logger,
    objectstore.WithDir("/var/lib/objectstore"),
    objectstore.WithPrefix("prod/api-cache"),
    objectstore.WithPackThreshold(1<<20), // 1 MiB
    objectstore.WithPageSize(256<<10),    // 256 KiB
    objectstore.WithBlockCache(64<<20),   // 64 MiB
    objectstore.WithCheckpointCache(16<<20), // 16 MiB
    objectstore.WithCheckpointTTL(2*time.Second),
)

cache, err := daramjwee.New(logger,
    daramjwee.WithTiers(objectStore),
    daramjwee.WithFreshness(5*time.Minute, 1*time.Minute),
    daramjwee.WithOpTimeout(30*time.Second),
)
```

## Multi-Tier with ObjectStore

For hot local + cold remote architecture:

```go
hot := memstore.New(100*1024*1024, policy.NewLRU()) // 100MB
cold := objectstore.New(bucket, logger,
    objectstore.WithDir("/var/lib/objectstore"),
    objectstore.WithPrefix("prod/cache"),
    objectstore.WithPackThreshold(1<<20),
)

cache, err := daramjwee.New(logger,
    daramjwee.WithTiers(hot, cold),
    daramjwee.WithFreshness(1*time.Minute, 30*time.Second),
    daramjwee.WithOpTimeout(5*time.Second),
)
```

## CacheGroup

For multiple caches sharing a runtime:

```go
group, err := daramjwee.NewGroup(logger,
    daramjwee.WithGroupWorkers(2),
    daramjwee.WithGroupWorkerQueueDefault(8),
)
if err != nil {
    return err
}

users, err := group.NewCache("users",
    daramjwee.WithTiers(memstore.New(0, nil)),
    daramjwee.WithWeight(4),
    daramjwee.WithQueueLimit(16),
)
if err != nil {
    return err
}

posts, err := group.NewCache("posts",
    daramjwee.WithTiers(memstore.New(0, nil)),
    daramjwee.WithWeight(2),
    daramjwee.WithQueueLimit(8),
)
if err != nil {
    return err
}
```

## Eviction Policies

### LRU

```go
policy.NewLRU()
```

Best for: General-purpose caching with temporal locality.

### S3-FIFO

```go
policy.NewS3FIFO(totalCapacity, 10) // 10% small queue
```

Best for: High-throughput workloads with scan resistance.

### SIEVE

```go
policy.NewSieve()
```

Best for: Workloads with strong frequency skew.

## Freshness Configuration

### Default Freshness

```go
daramjwee.WithFreshness(positive, negative)
```

- `positive`: How long positive entries are fresh
- `negative`: How long negative entries are fresh

### Per-Tier Freshness

```go
daramjwee.WithTierFreshness(index, positive, negative)
```

Override freshness for specific tiers.

## Worker Configuration

### Standalone Cache

```go
daramjwee.WithWorkers(4)
daramjwee.WithWorkerQueue(500)
daramjwee.WithWorkerTimeout(30*time.Second)
```

### CacheGroup

```go
daramjwee.WithGroupWorkers(2)
daramjwee.WithGroupWorkerQueueDefault(8)
```

## Timeout Configuration

### Operation Timeout

```go
daramjwee.WithOpTimeout(5*time.Second)
```

### Close Timeout

```go
daramjwee.WithCloseTimeout(10*time.Second)
```

### Fill Lease Timeout

```go
daramjwee.WithFillLeaseTimeout(5*time.Second)
```
