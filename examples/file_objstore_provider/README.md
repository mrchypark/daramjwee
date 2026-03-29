# Daramjwee Azure Objectstore Example

This example demonstrates how to configure `daramjwee` with ordered tiers:

-   **Tier 0**: `fileStore`, using the local filesystem for fast access.
-   **Tier 1**: `objectstore`, using Azure Blob Storage as a larger backing tier.

This setup is a practical pattern for an ordered-tier cache. Frequently accessed data resides on fast local disk, while less frequent data can be served from Azure Blob Storage and promoted back into tier 0.

## Azure Configuration (`config.yaml`)

The example uses a `config.yaml` file to configure the connection to Azure Blob Storage, following the standard format used by [Thanos](https://thanos.io/tip/thanos/storage.md/#azure).

**You must create this file and populate it with your own Azure Storage account details.**

```yaml
type: AZURE
config:
  # Your Azure Storage account name.
  storage_account: "<YOUR_AZURE_STORAGE_ACCOUNT>"

  # Choose ONE of the following authentication methods.
  # 1. Using Storage Account Key (less recommended for production)
  storage_account_key: "<YOUR_AZURE_STORAGE_KEY>"

  # 2. Using a Connection String (more flexible, can use SAS tokens)
  # storage_connection_string: "<YOUR_AZURE_CONNECTION_STRING>"

  # 3. Using Managed Identity (recommended for Azure resources)
  # user_assigned_id: "<YOUR_USER_ASSIGNED_MANAGED_IDENTITY_CLIENT_ID>"
  # msi_resource: "https://<YOUR_AZURE_STORAGE_ACCOUNT>.blob.core.windows.net"

  # The name of the blob container to use.
  container: "<YOUR_BLOB_CONTAINER_NAME>"

  # Optional: Create the container if it does not exist.
  # Be cautious with this in production environments.
  storage_create_container: true
