# daramjwee 🐿️ `/dɑːrɑːmdʒwiː/`

In Korean, *daramjwee* means **squirrel**.

Like its namesake, `daramjwee` is an efficient and lightweight hybrid caching middleware for Go. It is built with a pragmatic, cloud-native philosophy: to be **low-cost, highly efficient, and "good enough"** for demanding workloads, without the bloat of over-engineering.

`daramjwee` sits between your application and your original data source, providing a two-tier cache (a fast local disk cache and a vast cloud object store cache) to reduce latency and load on your origin.

**Key Features:**

* 🐿️ **True Caching Middleware:** Intelligently caches data from any original source (DB, API, etc.).
* 🔥 **Hybrid Tiering:** Combines a local disk (Hot Tier) and a cloud store (Cold Tier) for optimal performance and cost.
* 💰 **Cost-Effective by Design:** The API encourages efficient data fetching patterns (`CheckFn`, `UpdateFn`) to minimize cloud egress costs.
* 🔄 **Negative Caching:** Caches "not found" results to prevent repeated lookups for non-existent objects.
* 🧩 **Pluggable Eviction:** The local cache uses a pluggable eviction policy (e.g., LRU) to manage its finite capacity.
* ⚙️ **Background Refresh:** Asynchronously checks for content updates and refreshes the cache using flexible worker strategies (`pool` or `all`).
* ☁️ **Cloud-Native:** Uses the battle-tested `thanos-io/objstore` for robust cloud connectivity.