# 프로젝트 구조

## 루트 레벨 파일들
- `daramjwee.go` - 메인 캐시 인터페이스와 핵심 타입 정의
- `options.go` - 설정 옵션과 함수형 설정 패턴
- `cache.go` - DaramjweeCache 구현체 (실제 캐시 로직)
- `nullstore.go` - Null Object 패턴의 기본 스토어 구현
- `*_test.go` - 각 모듈별 테스트 파일들

## 패키지 구조

### `/cmd/daramjwee/`
- CLI 애플리케이션 진입점
- 현재는 기본 구조만 있음

### `/pkg/` - 재사용 가능한 라이브러리 컴포넌트
- **`/pkg/store/`** - 스토리지 백엔드 구현체들
  - `memstore/` - 인메모리 스토어 (LRU, S3-FIFO, SIEVE 정책 지원)
  - `filestore/` - 파일시스템 기반 스토어 (원자적 쓰기 보장)
  - `adapter/` - 외부 스토리지 어댑터 (thanos objstore 통합)

- **`/pkg/policy/`** - 캐시 제거 정책들
  - `lru.go` - 전통적인 LRU 알고리즘
  - `s3fifo.go` - 현대적인 S3-FIFO 알고리즘  
  - `sieve.go` - SIEVE 알고리즘

- **`/pkg/lock/`** - 동시성 제어
  - `mutex.go` - 기본 뮤텍스 락
  - `stripe.go` - 스트라이프 락 (성능 최적화)

### `/internal/` - 내부 전용 패키지
- **`/internal/worker/`** - 백그라운드 작업 처리
  - `worker.go` - 워커 인터페이스와 기본 구현
  - `pool_strategy.go` - 풀 기반 워커 전략
  - `all_strategy.go` - 모든 작업 즉시 실행 전략

### `/examples/` - 사용 예제들
- `main.go` - 기본 HTTP 서버 예제
- 각 기능별 세부 예제들 (파일스토어, 메모리스토어, 멀티티어 등)

## 네이밍 규칙
- **인터페이스**: 대문자로 시작 (Store, Cache, Fetcher)
- **구현체**: 구체적인 이름 (MemStore, FileStore, DaramjweeCache)
- **옵션 함수**: `With` 접두사 (WithHotStore, WithCache)
- **에러 변수**: `Err` 접두사 (ErrNotFound, ErrNotModified)
- **테스트 파일**: `*_test.go` 패턴

## 의존성 방향
```
cmd/ → daramjwee (root) → pkg/ → internal/
examples/ → daramjwee (root) → pkg/
```

- 루트 패키지가 핵심 인터페이스 정의
- pkg/는 재사용 가능한 구현체들 제공
- internal/은 내부 구현 세부사항
- cmd/와 examples/는 사용자 진입점