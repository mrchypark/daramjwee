# Per-Tier Freshness Override Design

**Date:** 2026-04-03

**Status:** Superseded by `2026-04-03-option-naming-redesign-design.md`. This document captures the first per-tier freshness design pass; the final public API on this branch is the renamed option surface.

## Goal

Keep the ordered `n-tier` cache model from `v0.4.x`, but allow selected tiers to override the default positive and negative freshness policy.

## Problem

Current `v0.4.x` freshness is chain-wide:

- `WithFreshness(positive, negative)` sets one default for every tier.
- `cache.go` uses the same stale check for tier 0 hits, lower-tier hits, and refresh scheduling.

This is too coarse for mixed layouts such as:

- `tier 0 = memstore`
- `tier 1 = filestore`
- `tier 2 = objectstore`

Those tiers often need different staleness windows even though they all participate in the same ordered chain.

## Requirements

- Preserve the existing `n-tier` public model.
- Preserve current behavior when no overrides are configured.
- Resolve freshness by tier index because cache orchestration already reasons about tier positions.

## Rejected Approaches

### Replace defaults with a single per-tier map only

Rejected because it makes simple one-tier and common-chain setups more verbose and weakens backward compatibility.

### Move freshness policy into `Store`

Rejected because freshness is a top-level cache orchestration concern, not backend-local behavior. Pushing it into `Store` would blur responsibilities and complicate backend reuse.

### Override by backend type

Rejected because the same backend type may appear in different tier positions with different freshness requirements.

## Chosen Design

Keep chain-wide defaults and add per-tier overrides.

### Public API

Final API on this branch:

```go
func WithFreshness(positive, negative time.Duration) Option
func WithTierFreshness(index int, positive, negative time.Duration) Option
```

Rationale:

- one short option sets the chain default
- one short option overrides a specific tier
- defaults remain concise
- option composition stays predictable

## Internal Model

Add a config/cache-level override table keyed by tier index.

Example shape:

```go
type TierFreshnessOverride struct {
	Positive time.Duration
	Negative time.Duration
}
```

The cache resolves effective freshness as:

1. start from chain-wide default
2. replace both freshness values when a tier-specific override exists

## Cache Flow Changes

Replace the current generic stale helper with tier-aware resolution.

Current:

```go
isCachedStale(meta, c.TierPositiveFreshFor, c.TierNegativeFreshFor)
```

New direction:

```go
isTierCachedStale(meta, tierIndex)
```

That resolver will be used in:

- tier 0 hit path
- lower-tier hit path
- refresh decision paths that re-check cached metadata

## Backward Compatibility

- No override configured: behavior remains identical to the chain-default freshness model.
- The later naming redesign intentionally removed the earlier `WithCache` / `WithNegativeCache` naming in favor of `WithFreshness`.
- The historical partial-override proposal in this document was not carried into the final renamed API.

## Testing

Add coverage for:

- option validation for negative per-tier freshness values
- positive override on a lower tier while default remains immediate-stale
- negative override on a lower tier while default remains immediate-stale
- no-override path remaining unchanged

## Documentation

Update README to clarify:

- `WithFreshness(...)` is the chain-wide default
- `WithTierFreshness(...)` overrides a specific tier
- per-tier overrides are indexed by tier position in `WithTiers(...)`
