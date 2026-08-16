# ObjectStore Architecture

This document describes the internal architecture of the objectstore backend.

## Overview

ObjectStore is a first-party object storage backend that provides durable caching with remote backing. It uses a segment-based storage model with checkpoint-based visibility.

## Architecture

```
ObjectStore
 ├── WAL/spool
 ├── segment writer
 ├── manifest/checkpoint
 ├── reader
 ├── compactor
 ├── GC
 └── remote adapter
```

## Components

### 1. WAL/Spool

Local workspace for staging writes before remote publication.

**Location**: `dataDir/ingest/`

**Purpose**:
- Stage writes before remote upload
- Buffer small writes for batching
- Provide local visibility for recent writes

**Structure**:
```
dataDir/
└── ingest/
    ├── active/
    │   └── <shard>/
    │       └── <segment>.seg
    └── sealed/
        └── <shard>/
            └── <segment>.seg
```

### 2. Segment Writer

Writes data to segment files.

**Interface**:
```go
type segmentWriter interface {
    Write(key string, data []byte, metadata *Metadata) error
    Seal() error
    Close() error
}
```

**Behavior**:
- Appends records to segment files
- Seals segment when full
- Handles concurrent writes

### 3. Manifest/Checkpoint

Tracks which keys are stored in which segments.

**Manifest**: Per-key metadata pointing to segment + offset
```
key → {
    segment_path: "segments/ab/segment-123.seg",
    offset: 1024,
    length: 512,
    metadata: {...}
}
```

**Checkpoint**: Per-shard snapshot of all keys
```
checkpoints/<shard>/latest.json
```

**Atomicity**:
- Checkpoint updates are atomic via temp file + rename
- Manifest updates are atomic via temp file + rename

### 4. Reader

Reads data from segments or remote storage.

**Interface**:
```go
type reader interface {
    Read(key string) (io.ReadCloser, *Metadata, error)
}
```

**Read Path**:
1. Check local catalog
2. If local, read from segment file
3. If remote, load from checkpoint/manifest
4. If paged, load pages on demand

### 5. Compactor

Merges small segments into larger ones.

**Interface**:
```go
type compactor interface {
    Compact(shardID string) error
}
```

**Behavior**:
- Scans for small segments
- Merges into larger segments
- Updates checkpoint atomically

### 6. GC (Garbage Collector)

Removes orphaned segments and old data.

**Interface**:
```go
type gc interface {
    Collect() error
}
```

**Behavior**:
- Identifies orphaned segments
- Removes segments not referenced by any checkpoint
- Respects grace period

### 7. Remote Adapter

Interfaces with remote object storage.

**Interface**:
```go
type remoteAdapter interface {
    Upload(ctx context.Context, path string, data io.Reader) error
    Download(ctx context.Context, path string) (io.ReadCloser, error)
    Delete(ctx context.Context, path string) error
    List(ctx context.Context, prefix string) ([]string, error)
}
```

**Supported Backends**:
- S3 (via thanos-io/objstore)
- GCS
- Azure Blob Storage
- Any objstore-compatible backend

## Data Flow

### Write Path

```
BeginSet()
    │
    ▼
Write to segment file (local)
    │
    ▼
Update catalog (local)
    │
    ▼
Close() → enqueue flush
    │
    ▼
Background: upload segment to remote
    │
    ▼
Background: update checkpoint (remote)
```

### Read Path

```
GetStream()
    │
    ▼
Check local catalog
    │
    ├── Local hit → read from segment file
    │
    └── Local miss → load from remote
            │
            ├── Manifest hit → read from remote blob
            │
            └── Checkpoint hit → read from remote segment
```

## Configuration

### Pack Threshold

```go
objectstore.WithPackThreshold(1 << 20) // 1 MiB
```

- **Small objects** (< threshold): Packed into shared segment objects
- **Large objects** (≥ threshold): Uploaded as direct remote blobs

### Page Size

```go
objectstore.WithPageSize(256 << 10) // 256 KiB
```

- Controls page size for paged layout
- Larger pages reduce metadata overhead

### Block Cache

```go
objectstore.WithBlockCache(64 << 20) // 64 MiB
```

- In-process cache for packed remote reads
- Reduces remote fetches for hot data

### Checkpoint Cache

```go
objectstore.WithCheckpointCache(16 << 20) // 16 MiB
```

- In-process cache for decoded shard checkpoints
- Reduces metadata fetches

## Consistency

### Local State

- Catalog is atomically updated
- Segment files are append-only
- Flush is asynchronous but catalog updates are synchronous

### Remote State

- Checkpoint visibility may be delayed by TTL
- Concurrent writers use last-writer-wins
- External coordination required for same-key ordering

### Recovery

- On startup, local state is recovered from catalog
- Missing segments are repaired from remote
- Stale local generations are rejected

## Monitoring

### Key Metrics

- **Pending shards**: Number of shards with pending flushes
- **Flush latency**: Time to flush a shard
- **Compaction latency**: Time to compact segments
- **GC latency**: Time to collect garbage

### Logging

```go
_ = level.Info(s.logger).Log("msg", "objectstore flush completed", "shard", shardID)
_ = level.Warn(s.logger).Log("msg", "objectstore flush failed", "err", err)
```
