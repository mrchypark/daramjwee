# objectstore

`objectstore` is a first-party `Store` for [`thanos-io/objstore`](https://github.com/thanos-io/objstore) providers such as S3, GCS, and Azure Blob Storage.

It fits into the same ordered-tier API as the other backends:

```go
cache, err := daramjwee.New(
    logger,
    daramjwee.WithTiers(
        filestore.New("/var/lib/daramjwee/tier0", log.With(logger, "tier", "0")),
        objectstore.New(
            bucket,
            log.With(logger, "tier", "1"),
            objectstore.WithDir("/var/lib/daramjwee/objectstore"),
            objectstore.WithPrefix("prod/api-cache"),
            objectstore.WithPackThreshold(1<<20),
            objectstore.WithPageSize(256<<10),
            objectstore.WithBlockCache(64<<20),
            objectstore.WithCheckpointCache(16<<20),
            objectstore.WithCheckpointTTL(2*time.Second),
        ),
    ),
)
```

## Mental Model

`objectstore` is best thought of as a durable remote tier with a local write spool.

- `WithDir(...)` is a local workspace for ingest segments and catalog state.
- It is not a user-visible `FileStore` replacement and not a persistent read-cache tier.
- If the local directory is empty but the remote checkpoint and segments exist, `objectstore` can still serve those remote entries.
- If you want local filesystem cache hits after the first remote hit, place `FileStore` ahead of `objectstore` in `WithTiers(...)`.

That means these two setups behave differently:

- `WithTiers(objectstore.New(...))`
  - Remote cache can be served directly.
  - Local disk is used as spool/catalog workspace.
- `WithTiers(filestore.New(...), objectstore.New(...))`
  - First request after a cold start can be a remote `objectstore` hit.
  - That request repopulates `FileStore`, so later requests can hit local disk in tier 0.

## Important Options

### `WithDir(dir string)`

Configures the local working directory.

What it stores:

- ingest segments before or during remote flush
- local catalog snapshot

What it does not promise:

- a persistent local read-cache layer equivalent to `FileStore`
- sharing safely between multiple live instances

Recommendations:

- use one stable path per pod or instance
- do not point multiple writers at the same directory
- expect remote-backed entries to remain readable even if this directory is lost

### `WithPrefix(prefix string)`

Scopes all remote object paths under one bucket prefix.

Use this to isolate:

- service A from service B
- staging from production
- independent cache namespaces in the same bucket

Recommended pattern:

- `prod/api-cache`
- `staging/search-cache`

### `WithPackThreshold(threshold int64)`

This is the main cost/performance knob in the current implementation.

Behavior:

- entries `<= threshold` are packed into shared remote segment objects
- entries `> threshold` are uploaded as direct remote blobs
- `threshold <= 0` keeps everything on the packed path

Tradeoff:

- lower threshold
  - more direct blobs
  - better large-object cold-read behavior
  - worse object count and remote PUT/LIST cost
- higher threshold
  - fewer remote objects
  - better small-object packing efficiency
  - worse read amplification for larger packed objects

Recommended starting values:

- `objectstore` only: `512 KiB ~ 1 MiB`
- `FileStore -> objectstore`: `1 MiB ~ 2 MiB`
- large media-heavy workload: `256 KiB ~ 512 KiB`

If you are unsure, start with `1 MiB`.

### `WithPageSize(size int64)`

Configures the block size used for packed remote range reads.

Tradeoff:

- smaller pages
  - less over-read
  - more range requests
- larger pages
  - fewer range requests
  - more over-read on sparse or short reads

Recommended starting value:

- `256 KiB`

### `WithBlockCache(capacityBytes int64)`

Enables in-process block caching for packed remote reads.

Properties:

- speeds up repeated packed remote reads
- does not survive restart
- complements ordered tiers, but does not replace them

Recommended starting values:

- `32 MiB` when `FileStore` is already in front
- `64 MiB ~ 128 MiB` when `objectstore` serves more remote hits directly

### `WithCheckpointCache(capacityBytes int64)`

Enables an in-process cache for decoded shard checkpoints such as `checkpoints/<shard>/latest.json`.

Properties:

- caches checkpoint metadata, not payload blocks
- avoids repeated remote GET + JSON decode for hot shards
- does not survive restart
- is most useful when many requests repeatedly touch the same shard set

Recommended starting values:

- `8 MiB ~ 16 MiB` for moderate shard churn
- `16 MiB ~ 32 MiB` when key count per hot shard is high

If your keys are very fine-grained (for example, plant/day time-series objects), do not try to cache every shard indefinitely. Keep this cache bounded and rely on the TTL.

### `WithCheckpointTTL(ttl time.Duration)`

Controls how long an in-memory shard checkpoint stays valid before the next read reloads it from remote storage.

Properties:

- same-process checkpoint publishes refresh the cache immediately
- external writers are observed after TTL expiry
- shorter TTL reduces staleness; longer TTL reduces remote metadata traffic

Recommended starting values:

- `2s` when the same shard may be updated by multiple writers
- `5s` when writes are rare and read fan-out is high

### `WithPageCache(capacityBytes int64)`

This is mainly relevant for paged manifest-backed reads.

For the current packed/direct checkpoint path, `WithPackThreshold(...)` and `WithBlockCache(...)` are the knobs that usually matter first.

### `WithGCGrace(grace time.Duration)`

Controls the grace period used by conservative remote GC sweeps.

This is about remote object cleanup, not local spool size budgeting.

## Suggested Presets

### API / general web workload

```go
objectstore.New(
    bucket,
    logger,
    objectstore.WithDir("/var/lib/daramjwee/objectstore"),
    objectstore.WithPrefix("prod/api-cache"),
    objectstore.WithPackThreshold(1<<20), // 1 MiB
    objectstore.WithPageSize(256<<10),            // 256 KiB
    objectstore.WithBlockCache(64<<20),     // 64 MiB
    objectstore.WithCheckpointCache(16<<20), // 16 MiB
    objectstore.WithCheckpointTTL(2*time.Second),
)
```

### Tiered local + remote cache

```go
objectstore.New(
    bucket,
    logger,
    objectstore.WithDir("/var/lib/daramjwee/objectstore"),
    objectstore.WithPrefix("prod/cache"),
    objectstore.WithPackThreshold(2<<20), // 2 MiB
    objectstore.WithPageSize(256<<10),
    objectstore.WithBlockCache(32<<20),
    objectstore.WithCheckpointCache(8<<20), // 8 MiB
    objectstore.WithCheckpointTTL(2*time.Second),
)
```

### Large-object heavy workload

```go
objectstore.New(
    bucket,
    logger,
    objectstore.WithDir("/var/lib/daramjwee/objectstore"),
    objectstore.WithPrefix("prod/media-cache"),
    objectstore.WithPackThreshold(512<<10), // 512 KiB
    objectstore.WithPageSize(256<<10),
    objectstore.WithBlockCache(128<<20),
    objectstore.WithCheckpointCache(16<<20), // 16 MiB
    objectstore.WithCheckpointTTL(5*time.Second),
)
```

## Operational Notes

- Concurrent same-key writes across multiple distributed writers are still last-writer-wins unless coordinated externally.

## Prod-like Compare Harness

`objectstore` has an env-gated Azurite compare harness that measures the production preset against a prod-like workload.

Run only the current `objectstore + Azurite` harness:

```bash
DJ_RUN_PRODLIKE_COMPARE=1 \
DJ_AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=https://127.0.0.1:10000/devstoreaccount1;" \
go test ./pkg/store/objectstore -run TestObjstoreProdLikeCompareHarness -count=1 -v
```

This emits a single JSON line prefixed with `DJ_PRODLIKE_COMPARE=`.

Reported metrics:

- `write.duration_ms`: write plus explicit flush time
- `cold_read.duration_ms`: remote read after reopening with a fresh data dir
- `warm_read.duration_ms`: repeated read on the same reopened instance
- `upload_calls`: remote object uploads
- `get_calls`: remote full-object reads, including checkpoint fetches
- `get_range_calls`: remote range reads for packed segments or direct blobs
- `iter_calls`: list operations
- `bytes_sent`: bytes uploaded to the bucket
- `bytes_received`: bytes read back from the bucket

To compare `current` against `v0.3.10` for both `filestore` and `objectstore + Azurite`, run:

```bash
scripts/run_prodlike_store_compare.sh
```
- Losing the local `dataDir` does not lose already-flushed remote cache entries.
- Losing the local `dataDir` does remove the local spool/catalog state, so the next requests may read from remote until higher tiers are rewarmed.
