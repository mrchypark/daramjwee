# Stampede Prevention

daramjwee limits duplicate work in two places:

1. **Cold misses** use the per-key miss coordinator described in
   [fill-coordination.md](fill-coordination.md). A waiter re-reads the top tier
   after the leader publishes, with a bounded fallback to its own fetch.
2. **Stale refreshes** use a per-key in-flight marker. Only the first stale hit
   schedules a refresh until that job runs or is discarded.

Background concurrency and queue size are additionally bounded by the selected
runtime (`WithWorkerStrategy`, `WithWorkers`, and `WithWorkerQueue`, or the
corresponding `CacheGroup` options). Generation fences reject refreshes and
fills whose top-tier snapshot became obsolete while the origin was being read.

The cache does not use `singleflight` for origin fetches and does not broadcast
one response body or error to all callers. Applications may still coalesce
inside a fetcher when they need different timing or cross-cache semantics.

Negative caching can reduce repeated not-found fetches. Origin error backoff is
an application concern; daramjwee does not implement a retry/backoff policy.

See `tests/miss_coalescing_test.go`, `cache_refresh_test.go`, and the end-to-end
concurrency tests for the executable behavior.
