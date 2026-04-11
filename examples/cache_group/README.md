# CacheGroup Example

This example shows how to create multiple caches that share one bounded
background runtime through `daramjwee.NewGroup(...)`.

It demonstrates:

- one shared `CacheGroup`
- two caches with different tier layouts
- group-level runtime options via `WithGroup...`
- per-cache runtime tuning via `WithWeight(...)` and `WithQueueLimit(...)`

## Run

From this directory:

```bash
go run .
```

Expected flow:

1. create one shared runtime with `WithGroupWorkers(2)`
2. create a `users` cache with `mem -> file`
3. create a `reports` cache with `file` only
4. fetch one value through each cache
5. schedule background refresh on both caches

The example uses only local temporary directories and does not require any
external service.
