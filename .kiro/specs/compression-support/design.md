# 설계 문서

## 개요

daramjwee 캐시 시스템에 압축 기능을 추가하여 저장 효율성을 향상시키고 메모리 사용량을 줄입니다. 이 설계는 기존 스트림 기반 아키텍처를 유지하면서 투명한 압축/해제 기능을 제공합니다.

## 아키텍처

### 압축 계층 구조

```
Application Layer
       ↓
Cache Interface (daramjwee.Cache)
       ↓
Compression Layer (새로 추가)
       ↓
Store Interface (daramjwee.Store)
       ↓
Storage Backend (MemStore/FileStore)
```

### 핵심 설계 원칙

1. **투명성**: 기존 Cache 인터페이스는 변경하지 않음
2. **스트림 기반**: io.Reader/io.Writer 패턴 유지
3. **선택적 활성화**: 압축 기능은 옵션으로 제공
4. **메타데이터 확장**: 압축 정보를 Metadata에 추가

## 컴포넌트 및 인터페이스

### 1. 압축 인터페이스

```go
// Compressor는 압축/해제 기능을 정의하는 인터페이스
type Compressor interface {
    // Compress는 입력 스트림을 압축하여 출력 스트림에 씁니다
    Compress(dst io.Writer, src io.Reader) (int64, error)
    
    // Decompress는 압축된 입력 스트림을 해제하여 출력 스트림에 씁니다
    Decompress(dst io.Writer, src io.Reader) (int64, error)
    
    // Algorithm은 압축 알고리즘 이름을 반환합니다
    Algorithm() string
    
    // Level은 압축 레벨을 반환합니다
    Level() int
}

// CompressionType은 지원하는 압축 알고리즘을 정의합니다
type CompressionType string

const (
    CompressionGzip CompressionType = "gzip"
    CompressionZstd CompressionType = "zstd"
    CompressionLZ4  CompressionType = "lz4"
    CompressionNone CompressionType = "none"
)
```

### 2. 압축 메타데이터 확장

```go
// CompressionMetadata는 압축 관련 메타데이터를 저장합니다
type CompressionMetadata struct {
    Algorithm        string `json:"algorithm"`         // 압축 알고리즘
    Level           int    `json:"level"`             // 압축 레벨
    OriginalSize    int64  `json:"original_size"`     // 원본 크기
    CompressedSize  int64  `json:"compressed_size"`   // 압축된 크기
    CompressionRatio float64 `json:"compression_ratio"` // 압축 비율
}

// Metadata 구조체에 압축 정보 추가
type Metadata struct {
    ETag        string               `json:"etag,omitempty"`
    IsNegative  bool                 `json:"is_negative,omitempty"`
    CachedAt    time.Time            `json:"cached_at"`
    Compression *CompressionMetadata `json:"compression,omitempty"` // 새로 추가
}
```

### 3. 압축 설정 옵션

```go
// CompressionConfig는 압축 설정을 정의합니다
type CompressionConfig struct {
    Enabled         bool            // 압축 활성화 여부
    Algorithm       CompressionType // 압축 알고리즘
    Level          int             // 압축 레벨
    MinSize        int64           // 압축 최소 크기 임계값 (기본: 1KB)
    ReturnCompressed bool          // 압축된 상태로 반환할지 여부
}

// WithCompression은 압축 설정을 추가하는 옵션 함수입니다
func WithCompression(config CompressionConfig) Option {
    return func(cfg *Config) error {
        if config.MinSize <= 0 {
            config.MinSize = 1024 // 기본 1KB
        }
        cfg.Compression = config
        return nil
    }
}
```

### 4. 압축 구현체

```go
// gzipCompressor는 gzip 압축을 구현합니다
type gzipCompressor struct {
    level int
}

// zstdCompressor는 zstd 압축을 구현합니다  
type zstdCompressor struct {
    level int
}

// lz4Compressor는 lz4 압축을 구현합니다
type lz4Compressor struct {
    level int
}

// NewCompressor는 지정된 알고리즘과 레벨로 압축기를 생성합니다
func NewCompressor(algorithm CompressionType, level int) (Compressor, error)
```

### 5. 압축 래퍼 스토어

```go
// CompressedStore는 기존 Store를 래핑하여 압축 기능을 추가합니다
type CompressedStore struct {
    store      Store               // 원본 스토어
    compressor Compressor          // 압축기
    config     CompressionConfig   // 압축 설정
    logger     log.Logger          // 로거
}

// NewCompressedStore는 압축 기능이 추가된 스토어를 생성합니다
func NewCompressedStore(store Store, config CompressionConfig, logger log.Logger) (*CompressedStore, error)
```

## 데이터 모델

### 압축된 데이터 저장 형식

```
[메타데이터 길이: 4바이트] [메타데이터: JSON] [압축된 데이터]
```

메타데이터에는 압축 정보가 포함되어 압축 해제 시 필요한 정보를 제공합니다.

### 압축 결정 로직

```go
func shouldCompress(size int64, config CompressionConfig) bool {
    return config.Enabled && size >= config.MinSize
}
```

## 에러 처리

### 압축 관련 에러

```go
var (
    ErrCompressionFailed   = errors.New("compression failed")
    ErrDecompressionFailed = errors.New("decompression failed")
    ErrUnsupportedAlgorithm = errors.New("unsupported compression algorithm")
    ErrCorruptedData       = errors.New("corrupted compressed data")
)
```

### Fallback 전략

1. **압축 실패 시**: 원본 데이터를 압축하지 않고 저장
2. **해제 실패 시**: 에러를 반환하고 캐시에서 해당 항목 제거
3. **알고리즘 불일치**: 지원하지 않는 알고리즘의 경우 에러 반환

## 테스트 전략

### 단위 테스트

1. **압축기 테스트**
   - 각 알고리즘별 압축/해제 정확성
   - 압축 레벨별 성능 및 압축률
   - 에러 케이스 처리

2. **압축 스토어 테스트**
   - 압축 임계값 테스트
   - 메타데이터 저장/조회
   - 압축/비압축 데이터 혼재 시나리오

### 통합 테스트

1. **캐시 통합 테스트**
   - 압축 활성화/비활성화 시나리오
   - Hot/Cold 스토어 간 데이터 이동
   - 백그라운드 리프레시 동작

2. **성능 테스트**
   - 압축률 vs 성능 트레이드오프
   - 메모리 사용량 개선 효과
   - 다양한 데이터 타입별 압축 효과

### 벤치마크 테스트

```go
func BenchmarkCompressionAlgorithms(b *testing.B)
func BenchmarkCompressedVsUncompressed(b *testing.B)
func BenchmarkCompressionLevels(b *testing.B)
```

## 구현 세부사항

### 압축 스트림 처리

```go
// compressedWriter는 데이터를 압축하면서 스토어에 저장합니다
type compressedWriter struct {
    originalWriter io.WriteCloser
    compressor     Compressor
    buffer         *bytes.Buffer
    metadata       *Metadata
    config         CompressionConfig
}

// compressedReader는 압축된 데이터를 읽어서 해제합니다
type compressedReader struct {
    originalReader io.ReadCloser
    decompressor   Compressor
    buffer         *bytes.Buffer
    metadata       *Metadata
}
```

### 메타데이터 처리

압축 메타데이터는 기존 Metadata 구조체에 추가되어 스토어에 함께 저장됩니다. 이를 통해 압축 해제 시 필요한 정보를 제공하고 압축 효율성을 모니터링할 수 있습니다.

### 성능 최적화

1. **버퍼 풀링**: 압축/해제 시 사용하는 버퍼를 풀링하여 GC 압력 감소
2. **스트리밍 압축**: 전체 데이터를 메모리에 로드하지 않고 스트림 방식으로 처리
3. **압축 레벨 최적화**: 기본값으로 속도와 압축률의 균형점 제공

### 호환성 고려사항

1. **기존 데이터**: 압축되지 않은 기존 캐시 데이터와의 호환성 유지
2. **점진적 도입**: 압축 기능을 점진적으로 활성화할 수 있는 구조
3. **버전 관리**: 압축 메타데이터에 버전 정보 포함으로 향후 확장성 확보