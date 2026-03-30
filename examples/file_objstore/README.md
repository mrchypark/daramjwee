# Ordered `FileStore -> objectstore` Example

This example shows a practical ordered-tier deployment:

- **tier 0**: `FileStore`
- **tier 1**: `objectstore` backed by Google Cloud Storage

It demonstrates the main intended split of responsibilities:

- `FileStore` is the user-visible local filesystem cache tier.
- `objectstore` is the larger remote backing tier.
- `objectstore.WithDataDir(...)` is a local workspace for ingest/catalog state, not a replacement for `FileStore`.

## Why this layout is useful

This is the recommended starting point when you want:

- fast local disk hits after the first request
- a larger durable remote backing store
- a cache that can still serve remote-flushed entries after tier 0 is wiped

In this example:

1. First request misses both tiers and fetches from origin.
2. The response is published into tier 0 and flushed toward tier 1.
3. Second request hits tier 0 directly.
4. The example then deletes tier 0 to simulate a restart or cache loss.
5. The next request hits tier 1 (`objectstore`) and repopulates tier 0.

## Run

Run from this directory:

```bash
go run .
```

The example expects a `config.yaml` in this directory.

## Configuration

This example uses the generic `thanos-io/objstore/client` format:

```yaml
type: GCS
config:
  bucket: "<YOUR_GCS_BUCKET>"
  service_account: |-
    {
      "type": "service_account",
      "project_id": "<YOUR_GCP_PROJECT>",
      "private_key_id": "<YOUR_PRIVATE_KEY_ID>",
      "private_key": "-----BEGIN PRIVATE KEY-----\\n<YOUR_PRIVATE_KEY>\\n-----END PRIVATE KEY-----\\n",
      "client_email": "<YOUR_SERVICE_ACCOUNT_EMAIL>",
      "client_id": "<YOUR_CLIENT_ID>",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "<YOUR_CLIENT_CERT_URL>"
    }
```

Replace the placeholders before running.

## objectstore tuning used here

The example uses these `objectstore` options:

```go
objectstore.New(
    bucket,
    logger,
    objectstore.WithDataDir("/tmp/.../workspace"),
    objectstore.WithPrefix("examples/file-objstore"),
    objectstore.WithPackedObjectThreshold(1<<20), // 1 MiB
    objectstore.WithPageSize(256<<10),            // 256 KiB
    objectstore.WithMemoryBlockCache(64<<20),     // 64 MiB
)
```

What they mean:

- `WithDataDir(...)`
  - local workspace for ingest and catalog state
  - not a local `FileStore` replacement
- `WithPrefix(...)`
  - isolates this example's remote objects under one bucket namespace
- `WithPackedObjectThreshold(1<<20)`
  - packs objects up to 1 MiB into shared remote segments
  - larger objects use direct remote blobs
- `WithPageSize(256<<10)`
  - packed remote reads use 256 KiB blocks
- `WithMemoryBlockCache(64<<20)`
  - repeated packed remote reads are cached in process memory

## When to use a different layout

If you want a simpler setup with no local file tier:

```go
daramjwee.WithTiers(
    objectstore.New(...),
)
```

That still allows remote cache hits, but it does **not** behave like a persistent local filesystem cache. The first remote hit after a cold start will be served directly from `objectstore` rather than from `FileStore`.
