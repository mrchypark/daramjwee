//go:build stress

// Filename: pkg/store/memstore/stress_test.go
package memstore

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/mrchypark/daramjwee" // Added import
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemStore_EvictionStress는 지속적인 쓰기 및 축출 상황에서
// MemStore의 메모리 사용량이 안정적으로 유지되는지 검증합니다.
func TestMemStore_EvictionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	ctx := context.Background()
	// 1MB의 작은 용량으로 설정하여 축출이 빈번하게 일어나도록 함
	capacity := int64(1 * 1024 * 1024)
	p := policy.NewLRUPolicy()
	store := New(capacity, p)

	// 테스트 전 메모리 상태 측정
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	// 10,000개의 객체를 추가하여 지속적인 축출을 유발
	iterations := 10000
	// 각 객체는 1KB ~ 10KB 사이의 랜덤 크기를 가짐
	dataChunk := make([]byte, 10*1024)
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("stress-key-%d", i)
		size := 1024 + rand.Intn(9*1024) // 1KB ~ 10KB

		writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)

		_, err = writer.Write(dataChunk[:size])
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)
	}

	// 스트레스 테스트 후, 저장소의 현재 크기가 설정된 용량을 초과하지 않는지 확인
	assert.LessOrEqual(t, store.currentSize, store.capacity, "Current size should not exceed capacity after stress test")

	// GC를 강제로 실행하여 미사용 메모리를 정리
	runtime.GC()

	// 테스트 후 메모리 상태 측정 및 로깅
	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	t.Logf("Memory Stats before test: Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB",
		bToMb(mBefore.Alloc), bToMb(mBefore.TotalAlloc), bToMb(mBefore.Sys))
	t.Logf("Memory Stats after test : Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB",
		bToMb(mAfter.Alloc), bToMb(mAfter.TotalAlloc), bToMb(mAfter.Sys))
	t.Logf("Allocated memory growth: %v MiB", bToMb(mAfter.TotalAlloc-mBefore.TotalAlloc))

	// 이 테스트의 핵심은 메모리 사용량이 반복 횟수에 비례하여 무한정 증가하지 않음을 확인하는 것입니다.
	// 실제 CI 환경에서는 특정 임계값을 설정하여 검증할 수 있습니다.
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
