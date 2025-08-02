# filestore

FileStore는 디스크 기반의 daramjwee.Store 구현체입니다. 파일 시스템을 사용하여 캐시 데이터를 저장하며, 다양한 eviction policy를 지원합니다.

## Features

- **Disk-based storage**: 파일 시스템을 사용한 영구 저장
- **Eviction policies**: LRU, S3-FIFO, SIEVE 등 다양한 eviction 정책 지원
- **Capacity management**: 최대 용량 설정 및 자동 eviction
- **Thread-safe**: 동시 접근을 위한 파일 락 메커니즘
- **NFS compatibility**: copy-and-truncate 전략 지원

## Usage

### Basic Usage

```go
import (
    "github.com/mrchypark/daramjwee/pkg/store/filestore"
    "github.com/mrchypark/daramjwee/pkg/policy"
)

// Create a basic FileStore
store, err := filestore.New("/path/to/cache", logger)

// Create FileStore with capacity and LRU policy
store, err := filestore.New(
    "/path/to/cache",
    logger,
    filestore.WithCapacity(1024*1024), // 1MB capacity
    filestore.WithEvictionPolicy(policy.NewLRU()),
)
```

### Available Options

- `WithCapacity(capacity int64)`: 최대 용량 설정 (바이트 단위)
- `WithEvictionPolicy(policy daramjwee.EvictionPolicy)`: eviction 정책 설정
- `WithCopyAndTruncate()`: NFS 호환성을 위한 copy-and-truncate 전략 사용

### Supported Eviction Policies

- **LRU**: Least Recently Used - 가장 오래 사용되지 않은 항목을 먼저 제거
- **S3-FIFO**: Second-Chance FIFO - 빈도 기반 eviction과 FIFO를 결합
- **SIEVE**: 낮은 오버헤드의 eviction 알고리즘

```go
// LRU Policy
lruPolicy := policy.NewLRU()

// S3-FIFO Policy (total capacity, small queue ratio as percentage)
s3fifoPolicy := policy.NewS3FIFO(1024*1024, 10) // 1MB total, 10% for small queue

// SIEVE Policy
sievePolicy := policy.NewSievePolicy()
```

## Examples

자세한 사용 예제는 `examples/filestore_policy/` 디렉토리를 참조하세요:

- `main.go`: 기본적인 policy 사용법
- `policy_comparison.go`: 다양한 policy 비교
- `config.yaml`: 설정 예제

## Benchmarks

```
$ go test -bench=. -benchmem ./pkg/store/filestore/
goos: linux
goarch: arm64
pkg: github.com/mrchypark/daramjwee/pkg/store/filestore
BenchmarkFileStore_Set_RenameStrategy-8            26770             39491 ns/op            1418 B/op         25 allocs/op
BenchmarkFileStore_Set_CopyStrategy-8              24439             48586 ns/op            1450 B/op         32 allocs/op
BenchmarkFileStore_Get_RenameStrategy-8           137865              8158 ns/op            1402 B/op         20 allocs/op
BenchmarkFileStore_Get_CopyStrategy-8             152445              7706 ns/op            1402 B/op         20 allocs/op
PASS
ok      github.com/mrchypark/daramjwee/pkg/store/filestore      10.002s

$ go test -bench=. -benchmem ./pkg/store/filestore/
goos: linux
goarch: arm64
pkg: github.com/mrchypark/daramjwee/pkg/store/filestore
BenchmarkFileStore_Set_RenameStrategy-8            38035             27859 ns/op            1193 B/op         22 allocs/op
BenchmarkFileStore_Set_CopyStrategy-8              32852             42279 ns/op            1049 B/op         26 allocs/op
BenchmarkFileStore_Get_RenameStrategy-8           267121              4379 ns/op             549 B/op         16 allocs/op
BenchmarkFileStore_Get_CopyStrategy-8             269330              4312 ns/op             549 B/op         16 allocs/op
PASS
ok      github.com/mrchypark/daramjwee/pkg/store/filestore      6.558s
```