# Daramjwee CacheGroup Design

**Date:** 2026-04-10

## Goal

Add a `CacheGroup` runtime container that allows multiple caches with different tier layouts to share one background worker runtime without collapsing cache-local policy boundaries.

The target use case is:

- cache A: `mem -> file`
- cache B: `mem -> objectstore`
- cache C: `file -> objectstore`

Those caches should remain independently configured, but their background refresh and persist work should run through one shared worker system with basic fairness controls.

## Problem

Current `DaramjweeCache` instances are self-contained.

- each cache owns its own worker manager
- each cache owns its own worker queue semantics
- `Close()` shuts down that cache's workers directly
- refresh and persist submission paths are hard-coupled to the cache-local worker manager

That model is simple for a single cache, but it becomes wasteful and operationally awkward when an application needs several caches with different tier chains:

- each cache brings its own worker pool and queue
- one busy cache cannot be coordinated with another
- there is no group-level fairness or capacity control
- later features such as predictive warming would have no shared runtime surface to build on

## Design Principles

### 1. Keep Cache Policy Local

`Cache` remains the owner of:

- tier chain
- freshness policy
- fanout behavior
- write coordination
- fetch contract

`CacheGroup` must not become a policy layer that understands tier topology, freshness semantics, or origin rules.

### 2. Move Runtime Ownership Up

`CacheGroup` owns:

- shared worker pool
- shared scheduling runtime
- per-cache queue registration
- basic fairness
- shared shutdown

This keeps the boundary crisp:

```text
CacheGroup = shared runtime owner
Cache      = cache policy owner
```

### 3. Preserve Existing Single-Cache Behavior

The existing `New(...)` constructor should keep creating a self-contained cache with private worker ownership.

Shared-runtime behavior must be opt-in through a new group construction path rather than silently changing the semantics of `New(...)`.

### 4. Preserve Cache-Local Close Semantics

Regardless of ownership model, `Cache.Close()` must still mean:

- this cache stops accepting new background work
- queued background work for this cache will not run
- in-flight background work for this cache is driven to quiescence before `Close()` returns or times out

Callers should not need to know whether a cache is self-contained or group-attached in order to rely on `Cache.Close()` as the cache-local shutdown boundary.

### 5. Keep v1 Small

The first version should solve only:

- shared runtime ownership
- per-cache bounded fairness
- lifecycle separation

It should not try to solve predictive warming, dependency graphs, or distributed scheduling yet.

## Rejected Approaches

### Global Queue Only

Rejected because a single FIFO queue does not provide meaningful isolation between caches. One cache can still saturate the shared runtime and starve others.

### Group-Owned Cache Policy

Rejected because it would make `CacheGroup` understand tier ordering, freshness, fanout, and cache-local coordination semantics. That would blur boundaries and make the design hard to evolve.

### Full Per-Cache Worker Stacks

Rejected for v1 because it recreates most of the current complexity while adding a coordinating layer on top. It improves isolation but does not meaningfully simplify runtime ownership.

## Chosen Design

Use a shared bounded worker pool with per-cache bounded queues.

- `CacheGroup` owns one runtime and one worker pool.
- Each cache registered with the group gets its own queue and weight.
- The group scheduler selects work from cache queues using a simple weighted round-robin style policy.
- `Cache` continues to generate refresh and persist jobs, but job submission goes through the group runtime instead of a cache-private worker manager.

This is the smallest design that preserves both:

- different tier chains per cache
- basic anti-starvation control

## Public API Direction

The existing `Cache` interface remains unchanged.

```go
type Cache interface {
	Get(ctx context.Context, key string, req GetRequest, fetcher Fetcher) (*GetResponse, error)
	Set(ctx context.Context, key string, metadata *Metadata) (WriteSink, error)
	Delete(ctx context.Context, key string) error
	ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error
	Close()
}
```

Add a new group-level construction surface:

```go
type CacheGroup interface {
	NewCache(name string, opts ...Option) (Cache, error)
	Close()
}

func New(logger log.Logger, opts ...Option) (Cache, error)
func NewGroup(logger log.Logger, opts ...GroupOption) (CacheGroup, error)
```

Rationale:

- `New(...)` remains backward-compatible and self-contained
- `NewGroup(...)` makes shared runtime ownership explicit
- `Cache.Close()` keeps the same cache-local quiescence contract in both ownership models
- only the underlying runtime owner changes

## Configuration Model

Separate cache-local options from group-runtime options.

### Cache Options

These remain cache-local and continue to use the existing `Option` namespace:

- `WithTiers(...)`
- `WithFreshness(...)`
- `WithTierFreshness(...)`
- `WithOpTimeout(...)`
- `WithCloseTimeout(...)`

Add cache-runtime knobs at cache creation time:

- `WithWeight(int)`
- `WithQueueLimit(int)`

Those settings describe how this cache participates in the shared runtime. They do not change cache semantics.

For group-attached caches, `WithCloseTimeout(...)` is a per-cache quiescence timeout used by `Cache.Close()`. It does not control shared runtime shutdown.

### Group Options

Move shared runtime knobs to group configuration:

- `WithGroupWorkers(...)`
- `WithGroupWorkerTimeout(...)`
- `WithGroupWorkerQueueDefault(...)`
- `WithGroupCloseTimeout(...)`

If `WithQueueLimit(...)` is omitted for a cache, the group default applies.

### Option Validation Rules

The constructors must reject invalid mixes explicitly with `ConfigError`. No option may be silently ignored.

- `New(...)` accepts existing standalone worker options and rejects `WithWeight(...)` / `WithQueueLimit(...)`.
- `NewGroup(...)` accepts only `GroupOption`.
- `CacheGroup.NewCache(...)` accepts cache-local `Option` values and rejects standalone worker ownership options such as `WithWorkers(...)`, `WithWorkerQueue(...)`, `WithWorkerTimeout(...)`, and `WithWorkerStrategy(...)`.

This keeps migration incremental without allowing reused option slices to misconfigure shared caches accidentally.

## Internal Runtime Shape

The internal runtime boundary should be narrow.

```go
type JobKind int

const (
	JobKindRefresh JobKind = iota
	JobKindPersist
)

type CacheRuntimeConfig struct {
	Weight     int
	QueueLimit int
}

type backgroundRuntime interface {
	Register(cacheID string, cfg CacheRuntimeConfig) error
	Submit(cacheID string, kind JobKind, job worker.Job) bool
	CloseCache(cacheID string, timeout time.Duration) error
	Shutdown(timeout time.Duration) error
}
```

`Cache` should only need to:

- register itself with a cache ID and runtime config
- submit background jobs with its cache ID
- close its own runtime handle on close

The runtime should not need to know tier details or fetch semantics.

## Fairness Model

v1 fairness should stay simple.

- one shared worker pool
- one bounded queue per cache
- one scheduler that iterates non-empty cache queues
- each cache receives dequeue opportunities according to its configured weight

This gives useful operational behavior without turning the runtime into a complex scheduler.

### What Fairness Means In v1

Fairness is queue-selection fairness, not execution-time fairness.

That distinction matters because jobs are not equal:

- a refresh job may be short
- a persist job may stream or copy much more data

So v1 only guarantees:

- one cache cannot fill another cache's queue
- one cache cannot monopolize dequeue opportunities forever

It does not guarantee equal CPU time, wall time, or bytes processed.

That limitation should be documented rather than hidden behind misleading "weighted fairness" language.

## Lifecycle Semantics

This is the most important compatibility boundary.

### `New(...)`

- creates a self-contained cache
- private runtime ownership
- `Cache.Close()` shuts down that cache's own runtime

### `NewGroup(...).NewCache(...)`

- creates a cache attached to a shared runtime
- `Cache.Close()` shuts down that cache's runtime handle inside the group
- `CacheGroup.Close()` shuts down the shared runtime

### Pending Jobs

After a cache is closed:

- new submissions from that cache are rejected
- queued jobs for that cache are dropped before execution
- execution re-checks whether the cache is closed before running the job body

This avoids "ghost job" behavior where a cache is closed but old queued closures still mutate it later.

### In-Flight Jobs

The runtime should create a cancelable child context per registered cache.

- jobs submitted for cache A run with cache A's runtime context
- `Cache.Close()` cancels that cache context, rejects future submissions, drops queued work, and waits up to that cache's `WithCloseTimeout(...)` for already-running jobs to observe cancellation and finish cleanup
- `CacheGroup.Close()` marks the group as closing, stops new dequeue work, cancels all cache contexts plus the group root context, and waits up to `WithGroupCloseTimeout(...)` for in-flight jobs to finish cleanup

If the timeout expires, shutdown completes as a best-effort close and logs the timeout condition. The intent is not to force-kill goroutines, but to make job cancellation and drain behavior explicit.

### Runtime Strategy

Shared groups are pool-based only in v1.

- `New(...)` may continue supporting existing standalone worker strategies
- `NewGroup(...)` uses a bounded shared pool runtime
- queue fairness claims in this document apply only to that bounded shared pool runtime

The unbounded `"all"` strategy is not part of `CacheGroup` v1 because it cannot provide meaningful queue-based fairness.

## Error Semantics

The public API can continue returning `ErrBackgroundJobRejected` for submission failures.

Internally, rejection reasons should be separated:

- group shutting down
- cache already closed
- cache queue full

That split keeps the public surface stable while preserving useful operational signals.

## Observability

Shared runtime without cache identity will be hard to operate.

At minimum, logs and metrics should carry:

- `cache_id`
- `job_kind`
- queue depth per cache
- rejected job count
- rejection reason

This is especially important once more than one cache shares the runtime.

## Migration Strategy

The migration should be additive.

1. Keep `New(...)` behavior unchanged.
2. Introduce `CacheGroup` and a shared runtime implementation.
3. Route refresh and persist job submission through an internal runtime interface.
4. Make group-attached caches use the shared runtime.
5. Keep all tier traversal, freshness, fanout, and write coordination logic inside `Cache`.

This avoids a broad refactor of cache semantics while still making runtime ownership pluggable.

## Testing

Add focused coverage for the new ownership and fairness boundaries.

### Constructor and Lifecycle

- `New(...)` still creates a self-contained cache
- `NewGroup(...).NewCache(...)` creates a shared-runtime cache
- closing one shared cache does not stop the group runtime
- closing the group stops all future submissions
- invalid option combinations are rejected explicitly

### Queue Isolation

- one cache filling its queue does not consume another cache's queue capacity
- queue full for cache A does not prevent cache B submission

### Scheduling Behavior

- weighted round-robin eventually services multiple active caches
- a non-empty lower-weight cache still makes progress while a higher-weight cache is busy

### Closed-Cache Safety

- queued work for a closed cache does not execute
- execution re-checks the cache closed state before running job bodies
- in-flight work observes cache or group cancellation and exits through normal cleanup paths

### Shutdown Ordering

- cache close then group close
- group close then cache close
- concurrent close vs submit
- idempotent repeated close calls
- timeout-expiry behavior for cache close and group close

### Backward Compatibility

- existing single-cache worker tests still pass through `New(...)`
- public `Cache` interface consumers do not need code changes

## v1 Non-Goals

The first version should explicitly exclude:

- predictive warming
- request tree or request DAG modeling
- cross-cache dependency graphs
- dynamic weight auto-tuning
- byte-based or time-based fairness
- cross-cache deduplication of jobs
- group-level freshness or tier policy
- changes to the public `Cache` interface
- bringing the standalone `"all"` worker strategy into shared groups
- distributed scheduling across processes or nodes

## Follow-Up Work

Once `CacheGroup` exists as a clean shared runtime owner, later work can attach to it more safely:

- predictive warm planner
- request-graph driven background jobs
- cache-class defaults
- richer runtime metrics

Those should remain follow-up layers on top of the runtime, not reasons to let `CacheGroup` absorb cache-local policy.
