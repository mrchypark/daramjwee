# Pure Streaming Cache Design

**Date:** 2026-03-26

## Goal

`daramjwee`의 cold-hit / origin-miss 경로를 진짜 stream-through로 바꾼다. caller가 첫 바이트를 source에서 바로 받으면서, 같은 바이트가 hot tier에 동시에 기록되고, 스트림이 정상 종료된 경우에만 hot tier에 publish되도록 만든다.

## Current State

현재 [`cache.go`](/Users/cypark/Documents/project/daramjwee/cache.go) 기준 동작은 다음과 같다.

- hot hit: hot stream을 그대로 반환
- cold hit: cold stream 전체를 hot store에 복사하고 commit한 뒤, hot store를 다시 열어 caller에게 반환
- miss: origin body 전체를 hot store에 복사하고 commit한 뒤, hot store를 다시 열어 caller에게 반환

이 구조는 partial object publish를 피하는 데는 유리하지만 다음 문제가 있다.

- cold hit / miss에서 first-byte latency가 hot tier commit 완료에 묶인다.
- source -> caller 와 source -> hot store가 한 스트림 lifecycle을 공유하지 않는다.
- README가 originally 의도했던 pure streaming과 구현이 다르다.

## Why The Current Interface Blocks Pure Streaming

현재 `Store` 계약은 `SetWithWriter(...) -> io.WriteCloser`이고, `Close()`가 곧 commit이다.

이 계약으로는 다음 상황을 안전하게 표현할 수 없다.

- caller가 일부만 읽고 연결을 닫음
- source stream이 중간에 실패함
- sink write는 일부 성공했지만 publish는 하면 안 됨

즉 "정상 완료라서 publish"와 "실패/취소라서 discard"를 분리할 수 없기 때문에, cache core는 write-then-read 외의 안전한 구현을 할 수 없다.

## New Public Contract

`Store`는 이제 `BeginSet`과 `WriteSink`를 제공한다.

```go
type WriteSink interface {
    Write(p []byte) (n int, err error)
    Commit() error
    Abort() error
}

type Store interface {
    GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error)
    BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error)
    Delete(ctx context.Context, key string) error
    Stat(ctx context.Context, key string) (*Metadata, error)
}
```

### Contract Rules

- `BeginSet`은 아직 외부에 보이지 않는 in-progress write를 시작한다.
- `Commit` 성공 전까지 partial data는 published state에 나타나면 안 된다.
- `Abort` 후에는 partial data가 보이면 안 된다.
- `Commit` / `Abort`는 둘 다 idempotent해야 한다.
- `GetStream` 성공 시 metadata는 반드시 non-nil이어야 한다.

## Cache Core Design

핵심은 `fillReadCloser`다. 이 reader는 source에서 읽은 바이트를 caller에게 돌려주면서 동시에 hot sink에도 기록한다.

```go
type fillReadCloser struct {
    src      io.ReadCloser
    sink     WriteSink
    onCommit func()

    mu      sync.Mutex
    sawEOF  bool
    readErr error
    sinkErr error
    closed  bool
}
```

### Read Path

- `Read(p)`는 `src.Read(p)`를 수행한다.
- `n > 0`이면 같은 바이트를 `sink.Write(p[:n])`에도 전달한다.
- source가 `io.EOF`를 반환하면 `sawEOF = true`로 표시한다.
- source read 에러 또는 sink write 에러가 발생하면 이후 종료는 `Abort` 경로로 간다.

### Close Path

`Close()`가 유일한 종료 지점이다.

- `sawEOF == true`이고 read/sink 에러가 없으면 `Commit`
- 그 외에는 `Abort`
- 마지막에 source를 닫고 context cancel을 정리

이렇게 하면 partial read, source error, client disconnect 모두 cache abort로 수렴한다.

## Cache Flow After Refactor

### Hot Hit

기존 semantics 유지.

- fresh hit: hot stream 그대로 반환
- stale hit: hot stream 반환 + background refresh 예약
- negative hit: `ErrNotFound`

### Cold Hit

- cold stream 확보
- hot sink `BeginSet(...)`
- 성공 시 `fillReadCloser{src=coldStream, sink=hotSink}` 반환
- `BeginSet` 실패 시 availability를 위해 cold stream passthrough fallback 허용

### Miss

- `fetcher.Fetch(...)`
- `ErrCacheableNotFound`, `ErrNotModified` semantics 유지
- 성공 시 hot sink `BeginSet(...)`
- 성공 시 `fillReadCloser{src=result.Body, sink=hotSink, onCommit=scheduleColdPersist}` 반환
- `BeginSet` 실패 시 origin body passthrough fallback 허용

### Cold Persist Timing

지금과 달리 cold persist는 miss 시점에 바로 예약하면 안 된다.

- source stream이 EOF까지 완료됨
- hot sink `Commit()` 성공
- 그 다음에만 cold persist 예약

## Store-Specific Semantics

### MemStore

- `BeginSet`: buffer만 가진 sink 생성
- `Commit`: map 반영 + eviction
- `Abort`: buffer 폐기

### FileStore

- `BeginSet`: temp file + metadata header
- `Commit`: temp close -> rename/copy -> accounting
- `Abort`: temp close -> temp remove

### RedisStore

- `BeginSet`: temp key 생성
- `Write`: `APPEND tempKey`
- `Commit`: Lua commit (rename + metadata set)
- `Abort`: `DEL tempKey`

### Objstore

이번 리팩터의 pure streaming correctness 기준에서는 제외한다.

- 새 `WriteSink` 계약에는 적응해야 한다.
- 그러나 final object visibility를 metadata와 temp object copy로 어떻게 안전하게 publish할지는 별도 설계로 다룬다.

## Error Handling Rules

- source read failure: `Abort`, read error 반환
- sink write failure: `Abort`, 이후 stream 실패 상태 고정
- `Commit` failure: caller는 `Close()`에서 에러를 받음
- `Abort` failure: 원래 에러에 join해서 반환
- double close: no-op

## Testing Strategy

### Core

- full read + close => commit
- partial read + close => abort
- source error => abort
- sink write error => abort
- commit error surfaces from close
- double close idempotent

### Integration

- cold hit no longer performs hot reread before first byte
- miss no longer performs hot reread before first byte
- partial miss/cold read leaves no hot entry
- stale refresh, negative cache, `ErrNotModified` semantics preserved

### Store Contracts

- `Commit` before publish only
- `Abort` removes temp state
- idempotent `Commit`/`Abort`

## Non-Goals For This Change

- objstore true-streaming publish semantics
- changing stale refresh strategy
- changing negative cache semantics

## Risks

- `Close()`를 호출하지 않는 caller는 이제 commit/abort를 끝내지 못한다.
- sink write latency가 miss/cold-hit read path에 직접 걸린다.
- Redis/FileStore 구현이 상태 머신을 잘못 가지면 temp leak 가능성이 있다.

## Recommendation

breaking change로 `Store`를 새 계약으로 일괄 전환하고, `cache.go`를 `fillReadCloser` 중심으로 단순화하는 것이 맞다. 이 구조가 partial object correctness와 true stream-through를 동시에 만족하는 가장 작은 변경이다.
