# ObjectStore Lifecycle

This document describes the background flush lifecycle and shutdown behavior of the objectstore backend.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ObjectStore                           │
│                                                         │
│  ┌─────────┐    ┌─────────┐    ┌─────────────────┐     │
│  │ Catalog │    │ Segment │    │ Block/Page Cache │     │
│  └────┬────┘    └────┬────┘    └────────┬────────┘     │
│       │              │                  │               │
│       └──────────────┴──────────────────┘               │
│                      │                                  │
│                      ▼                                  │
│              ┌──────────────┐                           │
│              │ Flush Queue  │                           │
│              └──────┬───────┘                           │
│                     │                                   │
│                     ▼                                   │
│              ┌──────────────┐                           │
│              │ Remote Bucket│                           │
│              └──────────────┘                           │
└─────────────────────────────────────────────────────────┘
```

## Write Flow

1. **BeginSet/BeginStagedSet**: Creates a local catalog entry and segment file.
2. **Write**: Streams data to the local segment file.
3. **Close/Commit**: Marks the catalog entry as committed and enqueues a flush.
4. **Background Flush**: Uploads segment data to the remote bucket.

## Flush Lifecycle

### Auto-Flush

When `autoFlush` is enabled (default), the store automatically schedules flushes:

```
Write completes
    │
    ▼
enqueueFlush(key)
    │
    ▼
scheduleFlushLocked()
    │
    ▼
flushPending() [debounced]
    │
    ├── Success → reset retry delay
    │
    └── Failure → increase retry delay, reschedule
```

### Flush Retry

On failure, the flush is rescheduled with exponential backoff:

- Minimum delay: 20ms
- Maximum delay: 1s
- Reset on success

### Pending Shards

Flushes are organized by shard for efficiency:

```go
type pendingFlushRecord struct {
    key   string
    entry localCatalogEntry
}
```

## Shutdown Behavior

### Close()

When `Close()` is called:

1. **Disable auto-flush**: Prevents new flushes from being scheduled.
2. **Final flush**: Attempts to flush all pending writes.
3. **Return**: Returns any error from the final flush.

```go
func (s *Store) Close() error {
    s.closeOnce.Do(func() {
        s.isClosed.Swap(true)
        s.autoFlush = false
        err := s.flushPending(context.Background())
        // ...
    })
    return s.closeErr
}
```

### Idempotency

`Close()` is safe to call multiple times. Only the first call performs the shutdown.

### In-Flight Writes

Writes that are in progress when `Close()` is called will complete normally. The final flush will attempt to upload their data.

## Recovery

On startup, the store recovers local state:

1. **Catalog Recovery**: Reads the catalog snapshot file.
2. **Segment Recovery**: Identifies segments with pending flushes.
3. **Auto-Flush**: If `autoFlush` is enabled, schedules flushes for pending shards.

```go
func (s *Store) recoverLocalState() error {
    // 1. Read catalog snapshot
    // 2. Identify pending segments
    // 3. Schedule flushes if autoFlush enabled
}
```

## Configuration

### WithDir

The `dataDir` is a local workspace for the store:

- Stores catalog state
- Stores flush spool data
- NOT a persistent local read-cache tier

If the pod restarts with an empty directory, already-flushed remote entries can still be served from the remote checkpoint/segment state.

### WithPrefix

Adds a prefix to all remote object paths:

```go
objectstore.WithPrefix("prod/api-cache")
```

### WithPackThreshold

Controls whether small objects are packed into shared segments:

- **Small objects** (< threshold): Packed into shared remote segment objects
- **Large objects** (≥ threshold): Uploaded as direct remote blobs

Recommended starting points:
- `512 KiB ~ 1 MiB` for objectstore-only tiers
- `1 MiB ~ 2 MiB` when FileStore is in front

## Monitoring

### Key Metrics

- `pendingShards`: Number of shards with pending flushes
- `flushRetryDelay`: Current retry delay
- `flushScheduled`: Whether a flush is currently scheduled

### Logging

Flush operations are logged at appropriate levels:

```go
_ = level.Warn(s.logger).Log("msg", "objectstore flush failed", "err", err)
_ = level.Info(s.logger).Log("msg", "objectstore flush completed", "shard", shardID)
```
