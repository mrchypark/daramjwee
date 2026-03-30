# Daramjwee Google Cloud Storage Objectstore Example

This example demonstrates how to configure `daramjwee` with ordered tiers:

- **Tier 0**: `fileStore`, using the local filesystem for fast access.
- **Tier 1**: `objectstore`, using Google Cloud Storage as a larger backing tier.

This setup is a practical pattern for an ordered-tier cache. Frequently accessed data resides on fast local disk, while less frequent data can be served from Google Cloud Storage and promoted back into tier 0.

It also illustrates an important `objectstore` rule:

- `FileStore` is the actual local filesystem cache tier.
- `objectstore.WithDataDir(...)` is only the backend's local workspace for ingest/catalog state.
- If tier 0 is wiped, tier 1 can still serve remote-flushed entries and repopulate tier 0 on demand.

## Run

Run the example from this directory:

```bash
go run .
```

The example logs three scenarios:

1. first request misses both tiers
2. second request hits tier 0
3. tier 0 is deleted, then tier 1 serves the object and repopulates tier 0

## GCS Configuration (`config.yaml`)

The example uses a `config.yaml` file to configure the connection to Google Cloud Storage, following the standard format used by [Thanos](https://thanos.io/tip/thanos/storage.md/#gcs-google-cloud-storage).

Run the example from this directory and replace the placeholder values with your own GCS bucket and service account JSON.

```yaml
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

## objectstore tuning used in the example

The example configures `objectstore` like this:

```go
objectstore.New(
    bucket,
    logger,
    objectstore.WithDataDir("/tmp/.../workspace"),
    objectstore.WithPrefix("examples/file-objstore-provider"),
    objectstore.WithPackedObjectThreshold(1<<20), // 1 MiB
    objectstore.WithPageSize(256<<10),            // 256 KiB
    objectstore.WithMemoryBlockCache(64<<20),     // 64 MiB
)
```

Why these values:

- `WithPackedObjectThreshold(1<<20)`
  - good starting point for a `FileStore -> objectstore` layout
  - reduces object count while keeping larger objects on a direct path
- `WithPageSize(256<<10)`
  - reasonable default block size for packed remote reads
- `WithMemoryBlockCache(64<<20)`
  - helps repeated packed remote reads inside the same process
- `WithPrefix(...)`
  - keeps example objects in an isolated namespace
- `WithDataDir(...)`
  - gives `objectstore` a stable local workspace without pretending it is a regular cache tier

If you use `objectstore` without a `FileStore` in front of it, consider a lower packed threshold such as `512 KiB ~ 1 MiB` because remote reads become more directly user-facing.
