# Per-Tier Freshness Override Design

**Date:** 2026-04-03

## Goal

Keep the ordered `n-tier` cache model from `v0.4.x`, but allow selected tiers to override the default positive and negative freshness policy.

## Problem

Current `v0.4.x` freshness is chain-wide:

- `WithTierFreshness(positive, negative)` sets one default for every tier.
- `cache.go` uses the same stale check for tier 0 hits, lower-tier hits, and refresh scheduling.

This is too coarse for mixed layouts such as:

- `tier 0 = memstore`
- `tier 1 = filestore`
- `tier 2 = objectstore`

Those tiers often need different staleness windows even though they all participate in the same ordered chain.

## Requirements

- Preserve the existing `n-tier` public model.
- Preserve current behavior when no overrides are configured.
- Allow overriding positive freshness without forcing a negative override.
- Allow overriding negative freshness without forcing a positive override.
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

Keep:

```go
func WithTierFreshness(positive, negative time.Duration) Option
func WithCache(freshFor time.Duration) Option
func WithNegativeCache(freshFor time.Duration) Option
```

Add:

```go
func WithTierPositiveFreshness(index int, freshFor time.Duration) Option
func WithTierNegativeFreshness(index int, freshFor time.Duration) Option
```

Rationale:

- callers can override only the positive path
- callers can override only the negative path
- defaults remain concise
- option composition stays predictable

## Internal Model

Add a config/cache-level override table keyed by tier index.

Example shape:

```go
type TierFreshnessOverride struct {
	Positive *time.Duration
	Negative *time.Duration
}
```

The cache resolves effective freshness as:

1. start from chain-wide default
2. apply per-tier positive override if present
3. apply per-tier negative override if present

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

- No override configured: behavior remains identical to current `v0.4.x`.
- Existing callers using `WithCache`, `WithNegativeCache`, or `WithTierFreshness` continue to work unchanged.
- Existing tests should keep passing unless they assert exact option internals.

## Testing

Add coverage for:

- option validation for negative per-tier freshness values
- positive override on a lower tier while default remains immediate-stale
- negative override on a lower tier while default remains immediate-stale
- no-override path remaining unchanged

## Documentation

Update README to clarify:

- `WithTierFreshness(...)` is the chain-wide default
- `WithTierPositiveFreshness(...)` and `WithTierNegativeFreshness(...)` are selective overrides
- per-tier overrides are indexed by tier position in `WithTiers(...)`
