# FileStore Policy Examples

이 디렉토리는 FileStore에서 다양한 eviction policy를 사용하는 방법을 보여주는 예제들을 포함합니다.

## 📁 파일 구조

```
examples/filestore_policy/
├── README.md                    # 이 파일
├── main.go                     # 간단한 기본 데모
├── filestore_policy_test.go    # 기능 테스트
├── config.yaml                 # 설정 예제
├── demo/
│   └── policy_comparison.go    # 시각적 policy 비교 데모
└── benchmark/
    └── policy_benchmark_test.go # 성능 벤치마크 테스트
```

## 🚀 실행 방법

### 1. 기본 데모 (main.go)

LRU policy를 사용한 기본적인 FileStore 동작을 확인합니다.

```bash
go run examples/filestore_policy/main.go
```

**특징:**
- LRU policy와 1KB 용량 제한
- 파일 쓰기/읽기/삭제 작업
- Eviction 동작 확인
- 상세한 로그 출력

### 2. Policy 비교 데모

세 가지 policy의 동작을 시각적으로 비교합니다.

```bash
go run examples/filestore_policy/demo/policy_comparison.go
```

**출력 예시:**
```
🔍 FileStore Policy Comparison Demo
===================================

📋 Test 1: LRU Policy
   Least Recently Used - evicts oldest accessed items

   📝 Writing 3 files (200 bytes each)...
   👆 Accessing file1 to make it 'recently used'...
   📝 Writing large file (400 bytes) to trigger eviction...
   📊 Results for LRU:
      Initial state: file1✅ file2✅ file3✅
      After access:  file1✅ file2✅ file3✅
      After eviction: file1✅ file2❌ file3❌ large_file✅
      🗑️  Evicted: [file2 file3]
```

### 3. 성능 벤치마크 (policy_benchmark_test.go)

각 policy의 성능을 정량적으로 측정합니다.

```bash
# 모든 벤치마크 실행
go test -bench=. ./examples/filestore_policy/benchmark/

# 특정 policy만 벤치마크
go test -bench=BenchmarkFileStorePolicy_LRU ./examples/filestore_policy/benchmark/

# 메모리 할당 정보 포함
go test -bench=. -benchmem ./examples/filestore_policy/benchmark/

# 벤치마크 시간 조정 (더 정확한 결과)
go test -bench=. -benchtime=10s ./examples/filestore_policy/benchmark/
```

**벤치마크 종류:**
- `BenchmarkFileStorePolicy_LRU`: LRU policy 성능
- `BenchmarkFileStorePolicy_S3FIFO`: S3-FIFO policy 성능  
- `BenchmarkFileStorePolicy_SIEVE`: SIEVE policy 성능
- `BenchmarkFileStorePolicy_NoEviction`: Eviction 없는 경우
- `BenchmarkFileStorePolicyMixed_*`: 읽기/쓰기 혼합 워크로드

**예시 벤치마크 결과 (Apple M3):**
```
BenchmarkFileStorePolicy_LRU-8          6844    170695 ns/op    2360 B/op    32 allocs/op
BenchmarkFileStorePolicy_S3FIFO-8       6925    175500 ns/op    2376 B/op    32 allocs/op
BenchmarkFileStorePolicy_SIEVE-8        7423    181920 ns/op    2460 B/op    32 allocs/op
BenchmarkFileStorePolicy_NoEviction-8   6891    179795 ns/op    2352 B/op    30 allocs/op

BenchmarkFileStorePolicyMixed_LRU-8     18472   67279 ns/op     1202 B/op    18 allocs/op
BenchmarkFileStorePolicyMixed_S3FIFO-8  18327   73283 ns/op     1213 B/op    18 allocs/op
BenchmarkFileStorePolicyMixed_SIEVE-8   14644   79488 ns/op     1215 B/op    18 allocs/op
```

## 📊 Policy 특성 비교

| Policy | 시간 복잡도 | 공간 복잡도 | 특징 | 적합한 사용 사례 |
|--------|-------------|-------------|------|------------------|
| **LRU** | O(1) | O(n) | 예측 가능, 구현 단순 | 일반적인 캐시, 시간 지역성이 강한 워크로드 |
| **S3-FIFO** | O(1) | O(n) | 높은 hit rate, 빈도 인식 | 웹 캐시, 다양한 접근 패턴 |
| **SIEVE** | O(1) | O(n) | 낮은 오버헤드, 균형잡힌 성능 | 고성능 요구사항, 메모리 제약 환경 |

## 🔧 커스터마이징

### 1. 새로운 Policy 테스트

```go
// 커스텀 policy 생성
customPolicy := policy.NewS3FIFO(2*1024*1024, 15) // 2MB, 15% small queue

store, err := filestore.New(
    "/path/to/cache",
    logger,
    filestore.WithCapacity(2*1024*1024),
    filestore.WithEvictionPolicy(customPolicy),
)
```

### 2. 벤치마크 시나리오 추가

`policy_benchmark_test.go`에 새로운 벤치마크 함수를 추가할 수 있습니다:

```go
func BenchmarkCustomScenario(b *testing.B) {
    // 커스텀 벤치마크 로직
}
```

### 3. 설정 기반 테스트

`config.yaml`을 참고하여 설정 파일 기반의 테스트를 만들 수 있습니다.

## 📈 성능 최적화 팁

1. **용량 설정**: 너무 작으면 빈번한 eviction, 너무 크면 메모리 낭비
2. **Policy 선택**: 워크로드 패턴에 맞는 policy 선택
3. **NFS 환경**: `WithCopyAndTruncate()` 옵션 사용
4. **모니터링**: 로그 레벨을 조정하여 성능 영향 최소화

## 🐛 문제 해결

### 일반적인 문제들

1. **Permission denied**: 캐시 디렉토리 권한 확인
2. **Disk full**: 용량 설정과 실제 디스크 공간 확인  
3. **Performance issues**: 벤치마크로 병목 지점 파악

### 디버깅

```go
// 디버그 로그 활성화
logger := level.NewFilter(logger, level.AllowDebug())

// 상세한 eviction 로그 확인
```

## 📚 추가 자료

- [daramjwee 메인 문서](../../README.md)
- [Policy 패키지 문서](../../pkg/policy/)
- [FileStore 문서](../../pkg/store/filestore/)