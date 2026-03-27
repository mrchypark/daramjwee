# Daramjwee V2 Tiering Design

**Date:** 2026-03-27

## Goal

`daramjwee`를 `hot/cold` 2-tier 고정 모델에서 `n-tier cache + optional durable tier` 모델로 올린다.

핵심 목표:

- `mem/file/redis` 같은 regular cache tier를 여러 개 둘 수 있어야 한다.
- `objectstore`는 일반 cache tier가 아니라 마지막 backing layer인 `durable tier`로 정의한다.
- current pure streaming contract는 유지한다.
- `objectstore`는 thin adapter가 아니라 first-party backend로 승격한다.
- `objectstore`는 현재 제품 정책으로는 durable tier에서만 공식 지원하지만, 내부 설계는 tier-agnostic해야 한다.

## Why This Change Exists

현재 구조는 [`HotStore`](/Users/cypark/Documents/project/daramjwee/daramjwee.go) + [`ColdStore`](/Users/cypark/Documents/project/daramjwee/daramjwee.go) 두 칸에 의미가 섞여 있다.

- `mem/file/redis`는 latency-oriented cache tier다.
- `objectstore`는 capacity/durability-oriented backing store다.
- 둘을 모두 `Store` 두 칸으로만 보면 역할이 흐려진다.

특히 `objectstore`는 long-lived immutable blob, manifest publish, range read, local page cache 같은 특성을 가지게 되므로 ordinary cold cache와는 성격이 다르다.

## Final Mental Model

최종 모델은 이렇다.

```text
Tier[0] -> Tier[1] -> ... -> Tier[n-1] -> DurableTier? -> Origin
```

- `Tier[]`
  - regular cache tiers
  - 승격과 backfill 대상
  - 예: memory, file(rename mode), redis
- `DurableTier`
  - optional terminal durable store
  - regular cache tier가 아님
  - backing source / terminal sink
  - 예: objectstore

즉 `n-tier cache`와 `durable tier`는 구분된다.

## Contract Boundaries

### Keep

- `Fetcher`
- `WriteSink`
- `Store`
- pure streaming `Get` miss/cold-hit semantics

즉 object-store-aware block/page 개념은 public `Store` API 위로 끌어올리지 않는다.

### Change

public configuration surface는 `HotStore` / `ColdStore` 대신 tier list와 durable tier로 바뀐다.

개념 초안:

```go
type CacheConfig struct {
    Tiers       []Store
    DurableTier Store
}
```

public option 초안:

```go
func WithTiers(stores ...Store) Option
func WithDurableTier(store Store) Option
```

compatibility layer:

- `WithHotStore(x)` -> `WithTiers(x)`
- `WithColdStore(y)` -> v2에서 제거 또는 deprecated compatibility shim

## Invariants

### Regular Tiers

- regular tier는 `Store` contract를 따르는 whole-object cache tier다.
- hit 시 더 위 tier들로 promotion할 수 있다.
- miss 시 더 아래 tier 또는 durable tier에서 fill할 수 있다.
- partial read/source error/cancel 시 publish되면 안 된다.

### Durable Tier

- durable tier는 backing source이자 terminal sink다.
- durable tier는 regular tier chain의 일부가 아니다.
- durable tier 아래에 또 다른 regular tier를 둘 수 없다.
- durable tier는 optional이다.
- durable tier hit는 상위 regular tiers로 backfill할 수 있다.

### Objectstore

- `objectstore`는 first-party durable tier backend다.
- current product policy로는 `WithDurableTier(objectstore.New(...))`만 공식 지원한다.
- `WithTiers(objectstore.New(...))`는 초기 v2에서는 config error다.
- 다만 구현은 future hot-tier 가능성을 막지 않게 한다.

## Objectstore Backend Design

### Package

현재 [`pkg/store/adapter/objstore`](/Users/cypark/Documents/project/daramjwee/pkg/store/adapter/objstore.go)는 승격한다.

새 패키지:

```text
pkg/store/objectstore
```

internal subpackages:

```text
pkg/store/objectstore/internal/manifest
pkg/store/objectstore/internal/layout
pkg/store/objectstore/internal/pagecache
pkg/store/objectstore/internal/rangeio
pkg/store/objectstore/internal/gc
pkg/store/objectstore/internal/shard
```

### Data Model

remote object model:

- immutable blob
- manifest pointer

paths:

- `manifests/<shard>/<encoded-key>.json`
- `blobs/<shard>/<encoded-key>/<version>.data`

manifest fields:

- `version`
- `blob_path`
- `layout`
  - `whole`
  - `paged`
  - `negative`
- `size`
- `page_size`
- `etag`
- `cached_at`
- `checksum`

publish rule:

- write blob first
- publish by writing manifest last

delete rule:

- delete manifest first
- blob cleanup is best-effort / GC

### Layout Strategy

`objectstore`는 dual layout를 가진다.

- `whole`
  - small object
  - full-blob read/write
- `paged`
  - large immutable object
  - remote blob은 still single immutable blob
  - read path만 `GetRange` + local page cache 사용

이 설계에서 remote object를 page object 여러 개로 쪼개지 않는다.

이유:

- write path를 단순하게 유지
- manifest/generation 모델을 단순하게 유지
- GC를 blob 단위로 유지
- provider round-trip 폭발을 피함

### Read Path

1. manifest lookup
2. `layout=negative` -> negative metadata 처리
3. `layout=whole` -> `bucket.Get`
4. `layout=paged` -> `GetRange` based reader
   - page cache lookup
   - same-page request coalescing
   - miss 시 remote range fetch 후 page fill

### Write Path

`BeginSet`

- new version 생성
- new blob path 결정
- streaming upload 시작

`Close`

- upload 완료 대기
- manifest write
- success == publish

`Abort`

- upload cancel
- new blob best-effort cleanup
- manifest untouched

### Why Foyer/Cachey Influence Stops Here

`foyer`/`cachey`에서 가져올 축은 backend 내부의 page cache engine이다.

가져오지 않는 것:

- public API를 block-oriented로 바꾸는 것
- top-level cache orchestration을 block engine으로 바꾸는 것

즉 `daramjwee`는 object cache API를 유지하고, `objectstore` backend 내부만 blob/page engine을 가진다.

## Top-Level Cache Flow

조회:

1. `Tier[0]`부터 순차 조회
2. first hit 발견 시:
   - caller에게 stream 반환
   - hit tier보다 위쪽 tiers에 promotion
3. regular tier 모두 miss면 `DurableTier` 조회
4. durable hit면 상위 tiers에 fill
5. durable도 miss면 origin fetch

쓰기:

- direct `Set`은 `Tier[0]`에만 write
- miss/origin fetch 성공 시 `Tier[0]` pure streaming fill
- publish 후 lower regular tiers / durable tier로 async persist 가능

삭제:

- regular tiers 삭제
- durable tier delete

## Promotion / Persistence Policy

v2 initial policy:

- read hit on lower regular tier -> promote to all higher tiers
- durable hit -> fill higher regular tiers
- origin miss fill -> write top tier first, then async fan-out downward

이 정책은 conservative하고 이해하기 쉽다.

후속 최적화로 selective persistence를 둘 수 있지만 v2 initial goal은 아니다.

## Validation Rules

config validation:

- `len(Tiers) >= 1`
- no nil tier
- no duplicate store instance
- `DurableTier` must not also appear in `Tiers`
- current v2: `objectstore` in `Tiers` is rejected
- current v2: `FileStore.WithCopyAndTruncate()` in `Tiers` is rejected

## Migration Strategy

v2 is a breaking change.

public migration:

- old: `WithHotStore`, `WithColdStore`
- new: `WithTiers`, `WithDurableTier`

recommended mapping:

- old hot -> first tier
- old cold objectstore -> durable tier
- old cold non-objectstore -> second regular tier if still desired

## Testing Requirements

### Cache Core

- n-tier lookup order
- lower-tier promotion
- durable-tier backfill
- origin miss fill semantics
- partial read abort semantics still hold

### Objectstore

- manifest publish
- abort leaves no published generation
- overwrite creates new generation
- delete removes manifest visibility
- negative layout
- paged layout reads
- page cache hit/miss
- same-page coalescing

### Benchmarks

- tier traversal overhead
- durable-tier hit latency
- objectstore whole small-object
- objectstore paged large-object
- page cache warm vs cold

## Non-Goals For Initial V2

- objectstore as supported regular hot tier
- remote blob physically split into per-page objects
- multi-durable-tier
- policy DSL for arbitrary fan-out graphs
- provider-specific tuning beyond a minimal option surface

## Recommendation

v2 should land in two architecture layers:

1. top-level cache model becomes `n-tier + durable`
2. `objectstore` becomes a first-party terminal durable backend with internal page-cache support

이 순서면 objectstore semantics와 top-level orchestration을 동시에 올바르게 만들 수 있고, pure streaming work도 그대로 재사용된다.
