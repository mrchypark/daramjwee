# Option Naming Redesign Design

**Date:** 2026-04-03

## Goal

Rebuild the public option naming surface so code reads clearly at call sites without relying on hidden context, while keeping names short.

## Design Principles

- Prefer short names over exhaustive names.
- Preserve scope in the name when scope changes behavior.
- Avoid duplicate aliases for the same meaning.
- Keep one option responsible for one concern.
- Avoid implementation-detail words unless they change external behavior.

## Current Problems

### Root package

- `WithCache(...)` and `WithNegativeCache(...)` are legacy names that no longer describe what they do.
- `WithTierFreshness(...)` means chain-wide default freshness, while `WithTierPositiveFreshness(...)` and `WithTierNegativeFreshness(...)` mean per-tier overrides. The shared `WithTier...` stem hides the scope difference.
- `WithDefaultTimeout(...)` sounds generic, but the code applies it to setup-stage operation contexts, not to all end-to-end streaming time.
- `WithWorker(...)` hides four settings inside one function and makes call sites hard to scan.

### filestore

- `WithCopyAndTruncate()` is an implementation-detail name, not a user-level behavior name.
- `WithEvictionPolicy(...)` is more verbose than needed.

### objectstore

- `WithDataDir(...)` can be shortened because the backend context is already `objectstore`.
- `WithDefaultGCGrace(...)` uses `Default` even though there is no nearby non-default GC grace option.
- `WithPackedObjectThreshold(...)` and `WithWholeObjectThreshold(...)` are asymmetric and hard to infer at a glance.
- `WithMemoryPageCache(...)`, `WithMemoryBlockCache(...)`, and `WithMemoryCheckpointCache(...)` repeat `Memory` even though these are constructor-local caches already.
- `WithCheckpointCacheTTL(...)` is more verbose than the meaning it exposes.

## Chosen Naming Model

### Root package

Keep:

```go
WithTiers(...)
```

Rename:

```go
WithFreshness(positive, negative)
WithTierFreshness(index, positive, negative)

WithOpTimeout(d)
WithCloseTimeout(d)

WithWorkers(n)
WithWorkerQueue(n)
WithWorkerTimeout(d)
```

Remove:

```go
WithTierFreshness(...)
WithTierPositiveFreshness(...)
WithTierNegativeFreshness(...)
WithCache(...)
WithNegativeCache(...)
WithDefaultTimeout(...)
WithShutdownTimeout(...)
WithWorker(...)
```

Rationale:

- `WithFreshness(...)` is short and correctly reads as the chain default.
- `WithTierFreshness(...)` makes the per-tier override explicit by including the tier index.
- `WithOpTimeout(...)` is closer to actual code semantics than `WithDefaultTimeout(...)`.
- Worker settings become individually readable at call sites.

### filestore

Keep:

```go
WithCapacity(...)
```

Rename:

```go
WithCopyWrite()
WithEviction(...)
```

Remove:

```go
WithCopyAndTruncate()
WithEvictionPolicy(...)
```

Rationale:

- `WithCopyWrite()` describes the user-visible write path choice without exposing low-level mechanics.
- `WithEviction(...)` is sufficient in `filestore` context.

### objectstore

Rename:

```go
WithDir(...)
WithPrefix(...)
WithGCGrace(...)

WithPackThreshold(...)
WithPagedThreshold(...)
WithPageSize(...)

WithPageCache(...)
WithBlockCache(...)
WithCheckpointCache(...)
WithCheckpointTTL(...)
```

Remove:

```go
WithDataDir(...)
WithDefaultGCGrace(...)
WithPackedObjectThreshold(...)
WithWholeObjectThreshold(...)
WithMemoryPageCache(...)
WithMemoryBlockCache(...)
WithMemoryCheckpointCache(...)
WithCheckpointCacheTTL(...)
```

Rationale:

- `WithDir(...)` and `WithGCGrace(...)` drop redundant qualifiers.
- `WithPackThreshold(...)` and `WithPagedThreshold(...)` describe the behavior split more directly.
- cache-related options no longer repeat `Memory`.
- `WithCheckpointTTL(...)` keeps the important noun and the timing concept.

## API Shape Changes

### Root config

Replace separate positive/negative per-tier options with one combined override option:

```go
func WithTierFreshness(index int, positive, negative time.Duration) Option
```

Internally this can still support optional positive/negative values if needed, but the public naming model should expose one clear concept.

### Worker options

Replace:

```go
func WithWorker(strategyType string, poolSize int, queueSize int, jobTimeout time.Duration) Option
```

With:

```go
func WithWorkers(count int) Option
func WithWorkerQueue(size int) Option
func WithWorkerTimeout(timeout time.Duration) Option
```

The strategy input is removed from public API unless a second strategy is still a real supported product surface.

## Migration Policy

This redesign is intentionally breaking.

- old option names are removed, not deprecated
- README and examples must migrate in the same change
- tests must migrate in the same change

## Non-Goals

- Do not change `New(...)` constructor names unless the option redesign exposes a concrete naming conflict.
- Do not redesign backend runtime behavior in this change.
- Do not change semantics of existing options unless required by the naming cleanup.
