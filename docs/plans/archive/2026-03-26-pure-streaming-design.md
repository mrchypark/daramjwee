# Pure Streaming Cache Design

**Date:** 2026-03-26

## Goal

`daramjwee`의 cold-hit / origin-miss 경로를 true stream-through로 바꾼다.

- caller는 source에서 첫 바이트를 즉시 받아야 한다.
- 동일 바이트가 hot tier에도 동시에 기록되어야 한다.
- caller가 스트림을 끝까지 읽고 `Close()`까지 호출한 경우에만 hot tier에 publish되어야 한다.
- partial read, source error, caller cancel에서는 publish되면 안 된다.

## Why The Previous Design Was Too Heavy

이전 설계가 커진 이유는 correctness 자체보다 optionality를 같이 들고 갔기 때문이다.

- pure streaming을 mandatory requirement로 두면서도 unsupported mode를 runtime fallback으로 남겼다.
- direct write와 stream-through read가 다른 terminal model을 가졌다.
- cleanup/TTL/scavenging을 semantic contract처럼 끌어올렸다.
- cache core와 store 구현이 같은 lifecycle state machine을 중복으로 가질 가능성이 높았다.

이 문서는 그 축을 잘라낸 더 작은 설계다.

## Scope And Boundary

이 변경은 **flag-day breaking change**다.

- 공개 `Store` 계약과 `Cache.Set` 시그니처가 바뀐다.
- 외부 custom store 구현은 없다고 가정한다.
- additive transition path는 두지 않는다.
- repo는 각 phase 끝마다 `go test ./...` 가능한 상태를 유지한다.

이번 배치의 semantic acceptance 범위:

- 포함: `MemStore`, `FileStore` 기본 rename 모드, `RedisStore`
- 제외: `FileStore.WithCopyAndTruncate()`, `objstore` hot-tier true streaming

중요한 점은 이 제외 범위를 **runtime fallback으로 지원하지 않는다는 것**이다.

- 이 배치는 pure-streaming hot store를 줄이는 게 아니라 고정한다.
- unsupported hot-store mode는 “느리게라도 동작”하는 second path를 가지지 않는다.
- out-of-scope store/mode는 follow-up 또는 mechanical adaptation 대상으로만 취급한다.
- cache construction/options validation은 unsupported hot-store store/mode를 **fail-fast**로 거부해야 한다.

## Current State

현재 [`cache.go`](/Users/cypark/Documents/project/daramjwee/cache.go) 기준 동작:

- hot hit: hot stream 그대로 반환
- cold hit: cold 전체 복사 -> hot commit -> hot reread -> caller 반환
- miss: origin 전체 복사 -> hot commit -> hot reread -> caller 반환

이 구조는 partial publish를 막는 대신:

- first-byte latency가 hot tier write 완료에 묶이고
- source -> caller 와 source -> hot tier lifecycle이 분리되고
- large object 경로가 사실상 `write-then-read`가 된다.

## Simplified Contracts

### WriteSink

```go
type WriteSink interface {
    io.WriteCloser
    Abort() error
}
```

의미는 단순하다.

- `Write`는 staged write에 데이터를 추가한다.
- `Close`는 publish다.
- `Abort`는 discard다.

`Commit`은 별도 메서드로 두지 않는다. 기존 `Close == commit`은 direct write 쪽에서는 충분했고, pure streaming에 부족했던 건 `Abort`가 없었다는 점이다.

### Store

```go
type Store interface {
    GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
    BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error)
    Delete(ctx context.Context, key string) error
    Stat(ctx context.Context, key string) (*Metadata, error)
}
```

### Cache

`Cache.Set`도 같은 terminal model을 쓴다.

```go
type Cache interface {
    Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, *Metadata, error)
    Set(ctx context.Context, key string, metadata *Metadata) (WriteSink, error)
    Delete(ctx context.Context, key string) error
    Exists(ctx context.Context, key string) (bool, error)
    ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error
    Close() error
}
```

즉 `Cache.Set`은 더 이상 wrapper가 아니다. `HotStore.BeginSet(...)`이 돌려준 abortable sink를 그대로 surface한다.

## Contract Rules

- `BeginSet`은 아직 외부에 보이지 않는 staged write를 시작한다.
- `GetStream` 성공 시 metadata는 반드시 non-nil이어야 한다.
- `WriteSink.Write`는 표준 `io.Writer` semantics를 따른다.
- cache core는 partial write를 직접 처리하기 위해 `writeAll` helper를 사용한다.
- `Close()`가 `nil`이면 새 entry는 publish되어야 한다.
- `Close()`가 error면 새 valid published entry는 존재하면 안 된다.
- `Abort()`가 `nil`이면 새 published entry는 존재하면 안 된다.
- caller/core는 terminal action으로 `Close` 또는 `Abort` 중 하나를 선택해야 한다.
- repeated `Close()` 또는 repeated `Abort()`는 safe no-op 또는 terminal error여야 한다.
- `Close()` 후 `Abort()`나 `Abort()` 후 `Close()`는 published state를 바꾸면 안 된다.
- out-of-scope store/mode가 새 surface에 mechanically adapted되더라도, `WriteSink`를 구현하는 이상 `Close/Abort` contract 자체는 지켜야 한다.

이 contract는 이전안보다 좁다. 중요한 건 “publish”와 “discard” 두 결과뿐이고, 그 선택은 core 또는 direct caller가 한다.

## Close Is Part Of Correctness

`Get()`가 반환한 reader는 `Close()`가 correctness-critical하다.

- full read만 하고 `Close()`하지 않으면 publish가 보장되지 않는다.
- partial read 후 `Close()`하면 `Abort()`되어야 한다.
- 이건 resource cleanup이 아니라 caller-facing semantic contract다.

문서/예제/테스트는 이를 전제로 맞춘다.

## Cache Core Design

핵심은 `fillReadCloser` 하나다.

```go
type fillReadCloser struct {
    src       io.ReadCloser
    sink      WriteSink
    cancel    context.CancelFunc
    onPublish func()

    mu      sync.Mutex
    sawEOF  bool
    readErr error
    writeErr error
    closed  bool
}
```

### Read Path

- `Read(p)`는 `src.Read(p)`를 수행한다.
- `n > 0`이면 읽은 바이트를 caller에게 그대로 반환하고, 같은 바이트를 `writeAll(sink, p[:n])`로 staged write에 기록한다.
- source가 `io.EOF`를 반환하면 `sawEOF = true`로 표시한다.
- source read error 또는 sink write error가 발생하면 이후 terminal path는 `Abort`다.

### Close Path

`Close()`가 유일한 종료 지점이다.

- `sawEOF == true`이고 read/write error가 없으면 `sink.Close()`로 publish
- 그 외에는 `sink.Abort()`로 discard
- 마지막에 `src.Close()`와 `cancel()` 정리
- publish 성공 후에만 `onPublish()` 실행

### Concurrency Rule

반환된 reader는 concurrent use를 지원하지 않는다.

- `Read()`와 `Close()`는 동시에 호출하지 않는 것을 기본 contract로 둔다.
- `Close()`는 blocked `Read()`를 강제로 깨우는 메커니즘으로 쓰지 않는다.
- cancellation은 source가 받은 context를 통해 전파되는 것으로 가정한다.
- 이 제약을 문서와 테스트에 명시해서 state machine을 작게 유지한다.

### Deliberate Simplifications

- core에 `bufferedSink`를 두지 않는다.
- backend-aware batching은 store sink 내부 책임이다.
- stream-through path에 auto-close wrapper를 겹쳐 쓰지 않는다.
- `fillReadCloser`가 source close, cancel, publish/discard를 직접 소유한다.

## Unified Stream-Through Flow

cold hit과 miss는 같은 helper로 처리한다.

```go
streamThrough(src io.ReadCloser, sink WriteSink, onPublish func()) io.ReadCloser
```

### Hot Hit

기존 semantics 유지.

- fresh hit: hot stream 그대로 반환
- stale hit: hot stream 반환 + background refresh 예약
- negative hit: `ErrNotFound`

### Cold Hit

- cold stream 확보
- hot sink `BeginSet(...)`
- `BeginSet(...)`이 성공하면 `streamThrough(coldStream, hotSink, nil)` 반환
- `BeginSet(...)`이 transient error면 cold stream passthrough를 반환하고 hot fill은 건너뛴다

### Miss

- `fetcher.Fetch(...)`
- `ErrCacheableNotFound`, `ErrNotModified` semantics 유지
- hot sink `BeginSet(...)`
- `BeginSet(...)`이 성공하면 `streamThrough(result.Body, hotSink, scheduleColdPersist)` 반환
- `BeginSet(...)`이 transient error면 origin stream passthrough를 반환하고 hot fill은 건너뛴다

### No Runtime Fallback

이번 설계는 unsupported hot-store mode를 런타임에 품지 않는다.

- pure streaming은 accepted hot-store set의 hard requirement다.
- unsupported mode는 request path에서 degrade하지 않고 config/build 단계에서 거부한다.
- 단, `BeginSet(...)`의 transient acquisition failure에 대해서는 availability를 위해 source passthrough를 허용한다.

## Store Semantics

### MemStore

- `BeginSet`: in-memory buffer sink 생성
- `Close`: map publish + eviction
- `Abort`: buffer 폐기

### FileStore

- 대상은 rename mode만이다.
- `BeginSet`: temp file 생성
- `Close`: final-path lock 획득 -> rename publish -> accounting
- `Abort`: temp file close/remove

`WithCopyAndTruncate()`는 이 변경의 hot-tier pure-streaming 대상이 아니다.

### RedisStore

- `BeginSet`: temp key 기반 sink 생성
- `Write`: 필요하면 내부 coalescing 후 append
- `Close`: Lua 기반 final publish
- `Abort`: temp key discard
- healthy active stream 동안 staged temp key는 terminal `Close`/`Abort`까지 살아 있어야 한다.
- buffering, temp key TTL, orphan cleanup 방식 자체는 store-local operability detail이다.

### Objstore

이 변경의 semantic acceptance 범위 밖이다.

- hot-tier pure-streaming 설계 대상이 아니다.
- mechanical compile adaptation이 필요하면 별도 follow-up으로 처리한다.

## Cleanup And Operability

temp-state cleanup은 semantic correctness가 아니라 hygiene다.

보장하는 것:

- partial read / source error / caller abort에서 published entry는 생기지 않는다.
- accepted Redis stream에서는 active staged data가 terminal action 전까지 사라지지 않는다.

보장하지 않는 것:

- orphan temp file/key가 언제 회수되는지
- cleanup이 모든 backend에서 같은 방식으로 동작하는지

즉 scavenging, TTL, orphan recovery는 store-local best effort다. 필요하면 후속 hardening으로 다룬다.

## Error Handling Rules

- source read failure: `Abort`, read error 반환
- sink write failure: `Abort`, write error 반환
- publish failure: caller는 `Close()`에서 error를 받음
- abort failure: original error에 join
- double close: no-op 또는 terminal error

## Testing Strategy

### 1. Core Reader Tests

`fillReadCloser`에 대해:

- full read + close => publish
- full read without close => no publish
- partial read + close => abort
- source error => abort
- sink short write => `writeAll`이 `io.ErrShortWrite`
- publish error surfaces from close

### 2. Shared WriteSink Conformance Suite

같은 테스트를 `MemStore`, `FileStore` rename mode, `RedisStore`에 공통 적용한다.

- `Close()` publishes
- `Abort()` discards
- partial write + abort leaves no entry
- repeated close / abort is safe
- `Abort()` 후 `Close()`가 publish하지 않는다
- `Close()` 후 `Abort()`가 published entry를 되돌리지 않는다
- terminal write fails

### 3. Streaming Proof Suite

cache integration에서 딱 이 속성만 증명한다.

- miss first byte arrives before source EOF
- cold-hit first byte arrives before source EOF
- no hot reread in stream-through path
- full read + close publishes to hot
- miss full read + close schedules cold persist
- publish failure does not schedule cold persist
- partial miss/cold-hit read publishes nothing
- stale refresh, negative cache, `ErrNotModified` semantics preserved

### 4. Store-Specific Unique Tests Only

공통 conformance로 커버되는 것은 각 store에서 반복하지 않는다.

- `FileStore`: rename atomicity, final-lock window
- `RedisStore`: unique publish script behavior, optional coalescing behavior

## Non-Goals

- `objstore` true-streaming hot-tier support
- `FileStore.WithCopyAndTruncate()` hot-tier support
- orphan temp-state hardening
- stale refresh strategy 변경
- negative cache semantics 변경

## Main Risks

- caller가 `Close()`하지 않으면 publish가 보장되지 않는다.
- sink write latency가 miss/cold-hit read path에 직접 걸린다.
- cold persist는 여전히 async라 queue pressure 시 durability가 늦거나 누락될 수 있다.
- orphan temp state cleanup은 backend마다 best effort다.

## Recommendation

가장 작은 설계는 `Close+Abort` 모델이다.

- `Commit`을 새 개념으로 들이지 않는다.
- `SupportsStreamingFill()`와 runtime fallback을 제거한다.
- cache core는 `fillReadCloser + streamThrough` 한 경로만 가진다.
- 공통 conformance suite로 store duplication을 줄인다.

이 구성이 pure streaming 보장을 유지하면서 가장 많은 설계 복잡성을 삭제한다.
