# Daramjwee Objectstore Segment-Engine Design

**Date:** 2026-03-28

**Supersedes:** `/Users/cypark/Documents/project/daramjwee/docs/plans/2026-03-27-v2-tiering-design.md`

## Goal

`daramjwee`를 다시 **범용 n-tier cache engine**으로 정렬한다.

핵심 목표:

- top-level API는 backend 종류를 몰라야 한다.
- `mem/file/redis/objectstore` 모두 ordered tier chain 안에 들어갈 수 있어야 한다.
- pure streaming `Store` / `WriteSink` contract는 유지한다.
- `objectstore` 비용 최적화는 top-level special tier가 아니라 backend 내부 엔진으로 해결한다.
- object count/PUT/LIST 비용을 먼저 줄이고, 그 다음 egress/GET-range 비용을 줄인다.

## Why The Durable-Tier Direction Was Rejected

`DurableTier`는 구현상 성립했지만, 패키지 목표와 충돌했다.

- top-level orchestration이 backend 의미를 알게 된다.
- `objectstore`가 ordinary `Store`가 아니라 special slot을 요구하게 된다.
- `objectstore` 내부 page cache와 top-level durable tier가 겹치면서 계층 모델이 이중화된다.
- 결과적으로 “여러 backend를 여러 tier로 조합한다”는 목표보다 “특정 backend를 위한 special engine”에 가까워진다.

그래서 최종 공개 모델은 다시 단순해야 한다.

```text
Tier[0] -> Tier[1] -> ... -> Tier[n-1] -> Origin
```

여기서 `objectstore`는 special durable slot이 아니라, 내부 구현이 강한 ordinary `Store`다.

## Public API

top-level config는 다시 ordered tiers 하나로 단순화한다.

```go
type Config struct {
    Tiers []Store
}
```

public option:

```go
func WithTiers(stores ...Store) Option
```

remove:

- `WithDurableTier(...)`
- `WithDurableTierFreshness(...)`
- `ValidateRegularTier()` based role distinction

freshness는 initial v2에서 다시 chain-wide policy로 둔다.

```go
func WithTierFreshness(positive, negative time.Duration) Option
```

향후 필요하면 `WithPerTierFreshness(...)`를 추가할 수 있지만 initial scope는 아니다.

## Filestore Position

`filestore`는 제거하지 않는다.

- 유지 목적:
  - simple whole-object disk tier
  - example backend
  - tests and comparisons
- 내려놓는 역할:
  - objectstore cost-optimized path의 core building block으로 top-level에 계속 노출되는 것

즉 `filestore`는 first-party `Store`로 남지만, objectstore 최적화의 중심축은 아니다.

## Objectstore Becomes A First-Class Store Engine

`pkg/store/objectstore`는 thin adapter가 아니라 **composite cache engine**이 된다.

publicly:

- 그냥 `Store`
- pure streaming write semantics 유지
- tier chain 어디에나 둘 수 있음

internally:

- local ingest plane
- remote flush plane
- local/remote read plane
- compaction / reclaim plane

이 설계에서 핵심은 `remote object storage`를 write path의 직접 sink로 보지 않는 것이다.

object storage는 append-friendly하지 않으므로, pure streaming을 유지하면서 object count를 줄이려면 local mutable layer가 필요하다.

## Internal Architecture

### 1. Ingest Plane

`BeginSet`의 실제 목적지는 remote object가 아니라 **local active segment**다.

- key/value record를 local disk segment에 append
- metadata와 record header를 같이 기록
- `Abort()` 시 unpublished extent를 버린다
- `Close()` 시 local catalog에 publish 한다

이 의미는 중요하다.

- `Close()==nil`이면 **이 node에서 cache hit 가능**
- `Close()==nil`이 곧 remote bucket visibility를 뜻하지는 않는다

즉 objectstore backend는 cache semantics를 가진 write-back store다.

### 2. Catalog Plane

per-key remote manifest는 버린다.

대신:

- local mutable shard catalog
- periodic remote shard checkpoint

를 둔다.

lookup 정보는 key당 object가 아니라 shard index 안에 들어간다.

catalog entry가 가리키는 대상은:

- local staged segment + offset + length
- remote packed segment + offset + length
- remote direct large blob
- tombstone

### 3. Flush Plane

local sealed segment를 remote object storage로 비동기 업로드한다.

기본 전략:

- 작은/중간 객체: packed remote segment
- 큰 객체: direct blob path

이 split은 비용 우선순위 때문이다.

- 작은 객체를 key별 object로 올리면 object count가 폭증한다
- 큰 객체는 packed segment에 넣는 이득이 작고 range/read amplification이 커질 수 있다

### 4. Read Plane

read는 세 단계다.

1. local catalog lookup
2. local resident hit면 local segment read
3. remote location이면 range reader 사용

remote packed segment read에서는:

- block cache
- same-block request coalescing

를 사용한다.

large direct blob read에서도 필요한 경우 range read를 사용할 수 있지만, packed segment path가 최우선 최적화 대상이다.

## Data Model

### Local

```text
local/
  ingest/
    active/<shard>/<segment-id>.seg
    sealed/<shard>/<segment-id>.seg
  catalog/
    <shard>.wal
    <shard>.snapshot
  cache/
    blocks/...
```

### Remote

```text
remote/
  segments/<shard>/<segment-id>.seg
  large/<shard>/<key-hash>/<version>.blob
  checkpoints/<shard>/<checkpoint-id>.json
```

remote object count rule:

- key마다 object를 만들지 않는다
- remote object의 기본 단위는 segment다
- large object만 예외적으로 direct blob을 허용한다

## Write Lifecycle

`BeginSet(key, metadata)`

- shard를 계산한다
- active local segment writer를 얻는다
- record header를 준비한다
- returned sink는 payload를 local segment에 순차 기록한다

`Write(p)`

- local segment append
- caller backpressure는 local disk/io에 의해 결정된다

`Close()`

- record를 seal 한다
- local shard catalog에 publish 한다
- flush queue에 segment 또는 record를 enqueue 한다
- success면 entry는 local cache hit 가능 상태가 된다

`Abort()`

- unpublished extent를 버린다
- catalog는 건드리지 않는다

pure streaming invariant:

- caller가 EOF까지 읽고 `Close()`해야 publish 된다
- partial read / source error / caller close early면 publish 되면 안 된다

## Read Lifecycle

`GetStream(key)`

1. local catalog lookup
2. latest visible location 확인
3. location kind에 따라:
   - local segment reader
   - remote packed segment range reader
   - remote direct blob reader
   - tombstone => not found

packed segment read에서는 block cache key에 최소한 아래가 들어간다.

- segment id
- block index
- segment generation if reused

logical key alone로 block cache/coalescing을 하는 것은 금지한다.

## Delete Lifecycle

delete는 key를 즉시 remote에서 purge하려고 하지 않는다.

- local catalog에 tombstone publish
- future read는 not found
- background compaction/reclaim이 obsolete records를 정리

즉 delete correctness는 catalog visibility로 정의한다.

## Recovery Model

restart 후 recovery는 local catalog와 local segment를 기준으로 한다.

- cleanly published local entries는 recovery 후 다시 보인다
- unsealed / aborted extents는 무시한다
- flush queue는 local metadata에서 재구성한다

initial scope에서 허용하는 손실:

- remote flush 전 crash 시, 아직 remote에 반영되지 않은 cache entry가 사라질 수 있다

이건 cache이므로 허용한다. 원본 데이터의 source of truth는 origin이다.

## Cost Model

### Priority 1: Object Count / PUT / LIST

가장 중요한 규칙:

- small/medium object는 per-key remote object 금지
- shard checkpoint는 key별 manifest를 대체
- remote는 segment 위주로만 보인다

### Priority 2: Egress / GET-Range

- packed segment read는 range 기반
- hot block은 local block cache로 흡수
- concurrent same-block miss는 coalescing

### Priority 3: Local Disk / Memory

- active segment 수 제한
- block cache capacity 제한
- sealed-but-unflushed segment budget 제한

## Package Structure

최종 구조 초안:

```text
pkg/store/objectstore
  store.go
  options.go
  writer.go
  reader.go
  catalog.go
  flusher.go
  compactor.go
  recovery.go
  stats.go
  internal/shard
  internal/segment
  internal/catalog
  internal/blockcache
  internal/rangeio
```

현재 `internal/pagecache`, `internal/rangeio`는 이 구조에 맞게 재배치하거나 대체한다.

## Non-Goals For Initial Version

- multi-node globally visible write-through durability
- remote objectstore를 즉시 authoritative source로 만드는 것
- distributed shared writer
- per-tier freshness override
- direct replacement of every current objectstore code path in one patch without migration

## Recommended Migration

1. remove durable-tier API from top level
2. keep `objectstore` as ordinary `Store`
3. first implement local ingest + local catalog + packed remote segment flush
4. keep remote large-object direct path simple
5. add block cache only after segment path correctness is stable

## Decision Summary

최종 설계는 이렇다.

- top-level은 `n-tier only`
- `filestore`는 예시용/단순 tier용 first-party store로 유지
- `objectstore`는 first-party store이지만 thin adapter가 아니라 local-disk-backed segment engine
- pure streaming publish는 local ingest/catalag publish 기준으로 유지
- 비용 최적화는 remote per-key object를 없애는 방향으로 해결한다
