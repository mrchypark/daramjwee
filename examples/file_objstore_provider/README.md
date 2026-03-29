# Daramjwee Google Cloud Storage Objectstore Example

This example demonstrates how to configure `daramjwee` with ordered tiers:

- **Tier 0**: `fileStore`, using the local filesystem for fast access.
- **Tier 1**: `objectstore`, using Google Cloud Storage as a larger backing tier.

This setup is a practical pattern for an ordered-tier cache. Frequently accessed data resides on fast local disk, while less frequent data can be served from Google Cloud Storage and promoted back into tier 0.

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
