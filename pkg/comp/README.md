# Compression Package (comp)

daramjwee의 압축 기능을 제공하는 패키지입니다. 스트림 기반의 압축/해제 인터페이스를 통해 메모리 효율적인 압축 처리를 지원합니다.

## 지원하는 압축 알고리즘

### ✅ 구현 완료
- **Gzip**: 표준 gzip 압축 (RFC 1952)
- **LZ4**: 고속 압축 알고리즘 (pierrec/lz4 라이브러리 사용)
- **Zstd**: Facebook의 고성능 압축 알고리즘 (klauspost/compress 라이브러리 사용)
- **None**: 압축하지 않는 pass-through 구현

## 사용법

### Gzip 압축기

```go
package main

import (
    "bytes"
    "strings"
    "github.com/mrchypark/daramjwee/pkg/comp"
)

func main() {
    // 기본 압축기 생성
    compressor := comp.NewDefaultGzipCompressor()
    
    // 또는 특정 압축 레벨로 생성
    compressor, err := comp.NewGzipCompressor(gzip.BestCompression)
    if err != nil {
        panic(err)
    }
    
    // 압축
    testData := "Hello, World! This is test data."
    var compressed bytes.Buffer
    src := strings.NewReader(testData)
    compressedBytes, err := compressor.Compress(&compressed, src)
    if err != nil {
        panic(err)
    }
    
    // 압축 해제
    var decompressed bytes.Buffer
    decompressedBytes, err := compressor.Decompress(&decompressed, &compressed)
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Original: %s\n", testData)
    fmt.Printf("Compressed bytes: %d\n", compressedBytes)
    fmt.Printf("Decompressed: %s\n", decompressed.String())
}
```

### None 압축기 (Pass-through)

```go
// 압축하지 않는 구현 (테스트나 개발 시 유용)
compressor := daramjwee.NewNoneCompressor()

var output bytes.Buffer
src := strings.NewReader("test data")
written, err := compressor.Compress(&output, src)
// 데이터가 그대로 복사됨
```

## 인터페이스

모든 압축기는 `daramjwee.Compressor` 인터페이스를 구현합니다:

```go
type Compressor interface {
    // 압축: 입력 스트림을 압축하여 출력 스트림에 쓰기
    Compress(dst io.Writer, src io.Reader) (int64, error)
    
    // 압축 해제: 압축된 입력 스트림을 해제하여 출력 스트림에 쓰기
    Decompress(dst io.Writer, src io.Reader) (int64, error)
    
    // 압축 알고리즘 이름 반환
    Algorithm() string
    
    // 압축 레벨 반환
    Level() int
}
```

## 압축 레벨

### Gzip
- `gzip.HuffmanOnly` (-2): 허프만 코딩만 사용
- `gzip.BestSpeed` (1): 최고 속도 우선
- `gzip.DefaultCompression` (-1): 기본 압축 (권장)
- `gzip.BestCompression` (9): 최고 압축률 우선

### LZ4
- 1-12: 1이 가장 빠름, 12가 가장 높은 압축률

### Zstd
- 1-22: 1이 가장 빠름, 22가 가장 높은 압축률

## 성능 특성

벤치마크 결과 (Apple M3 기준):

```
BenchmarkAllCompressors/gzip_compress-8            11324    118944 ns/op    863375 B/op    22 allocs/op
BenchmarkAllCompressors/lz4_compress-8             75553     13482 ns/op     54188 B/op    12 allocs/op
BenchmarkAllCompressors/zstd_compress-8             9387    123911 ns/op   2395960 B/op    50 allocs/op
BenchmarkAllCompressors/none_compress-8           383479      3088 ns/op     49233 B/op     3 allocs/op

BenchmarkGzipCompressor_Decompress-8               60190     21155 ns/op    171843 B/op    15 allocs/op
BenchmarkLZ4Compressor_Decompress-8                 3620    276809 ns/op   8447332 B/op    13 allocs/op
BenchmarkZstdCompressor_Decompress-8               34785     39325 ns/op    195161 B/op    39 allocs/op
```

### 압축 성능 (빠른 순서)
1. **LZ4**: 가장 빠른 압축 (13,482 ns/op) - 실시간 처리에 적합
2. **Gzip**: 균형잡힌 성능 (118,944 ns/op) - 범용적 사용
3. **Zstd**: 높은 압축률 (123,911 ns/op) - 저장 공간 절약 우선
4. **None**: 압축하지 않음 (3,088 ns/op) - 참조용

### 압축 해제 성능 (빠른 순서)
1. **Gzip**: 빠른 해제 (21,155 ns/op)
2. **Zstd**: 중간 성능 (39,325 ns/op)
3. **LZ4**: 상대적으로 느린 해제 (276,809 ns/op)

## 에러 처리

압축 관련 에러들:

```go
// 압축 실패
daramjwee.ErrCompressionFailed

// 압축 해제 실패
daramjwee.ErrDecompressionFailed

// 지원하지 않는 알고리즘
daramjwee.ErrUnsupportedAlgorithm

// 잘못된 압축 레벨
daramjwee.ErrInvalidCompressionLevel

// 손상된 압축 데이터
daramjwee.ErrCorruptedData
```

## 테스트

```bash
# 모든 테스트 실행
go test ./pkg/comp -v

# 벤치마크 테스트
go test ./pkg/comp -bench=. -benchmem

# 커버리지 확인
go test ./pkg/comp -cover
```

현재 테스트 커버리지: **90.0%**

## 향후 계획

1. **압축 메타데이터**: 압축률, 원본 크기 등 자동 계산
2. **스트리밍 최적화**: 더 큰 데이터에 대한 메모리 사용량 최적화
3. **압축 알고리즘 자동 선택**: 데이터 특성에 따른 최적 알고리즘 추천
4. **병렬 압축**: 대용량 데이터에 대한 병렬 처리 지원