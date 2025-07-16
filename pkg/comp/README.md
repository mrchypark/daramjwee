# Compression Package (comp)

daramjwee의 압축 기능을 제공하는 패키지입니다. 스트림 기반의 압축/해제 인터페이스를 통해 메모리 효율적인 압축 처리를 지원합니다.

## 지원하는 압축 알고리즘

### ✅ 구현 완료
- **Gzip**: 표준 gzip 압축 (RFC 1952)
- **None**: 압축하지 않는 pass-through 구현

### 🚧 구현 예정
- **LZ4**: 고속 압축 알고리즘 (스켈레톤 구현됨)
- **Zstd**: Facebook의 고성능 압축 알고리즘 (스켈레톤 구현됨)

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

### LZ4 (구현 예정)
- 1-12: 1이 가장 빠름, 12가 가장 높은 압축률

### Zstd (구현 예정)
- 1-22: 1이 가장 빠름, 22가 가장 높은 압축률

## 성능 특성

벤치마크 결과 (Apple M3 기준):

```
BenchmarkGzipCompressor_Compress-8                 10000    108663 ns/op    863375 B/op    22 allocs/op
BenchmarkGzipCompressor_Decompress-8               60969     21422 ns/op    171843 B/op    15 allocs/op
BenchmarkAllCompressors/none_compress-8           406546      3095 ns/op     49232 B/op     3 allocs/op
```

- **Gzip**: 압축률이 좋지만 CPU 사용량이 높음
- **None**: 압축하지 않으므로 매우 빠름

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

1. **LZ4 구현**: `github.com/pierrec/lz4` 라이브러리 통합
2. **Zstd 구현**: `github.com/klauspost/compress/zstd` 라이브러리 통합
3. **압축 메타데이터**: 압축률, 원본 크기 등 자동 계산
4. **스트리밍 최적화**: 더 큰 데이터에 대한 메모리 사용량 최적화