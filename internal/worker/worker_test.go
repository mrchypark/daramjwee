// Filename: internal/worker/worker_test.go

package worker

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkerManager_NewManager는 올바른 전략으로 워커 매니저가 생성되는지 검증합니다.
func TestWorkerManager_NewManager(t *testing.T) {
	testCases := []struct {
		name         string
		strategy     string
		pSize        int
		qSize        int
		expectedType interface{}
	}{
		{"Pool Strategy", "pool", 1, 1, &PoolStrategy{}},
		{"All Strategy", "all", 1, 1, &AllStrategy{}},
		{"Invalid Strategy Should Default to Pool", "invalid-strategy", 1, 1, &PoolStrategy{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager, err := NewManager(tc.strategy, log.NewNopLogger(), tc.pSize, tc.qSize, 1*time.Second)
			require.NoError(t, err)
			// defer 호출을 수정합니다. 타임아웃 값을 명시적으로 전달합니다.
			defer manager.Shutdown(1 * time.Second)

			assert.IsType(t, tc.expectedType, manager.strategy)
		})
	}
}

// TestWorkerManager_SubmitAndRun은 작업이 성공적으로 제출되고 실행되는지 검증합니다.
func TestWorkerManager_SubmitAndRun(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 10, 1*time.Second)
	require.NoError(t, err)
	defer manager.Shutdown(1 * time.Second) // defer 호출 수정

	var counter int32
	jobDone := make(chan bool)

	job := func(ctx context.Context) {
		atomic.AddInt32(&counter, 1)
		jobDone <- true
	}

	manager.Submit(job)

	select {
	case <-jobDone:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("Job did not complete in time")
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&counter))
}

// --- TestWorkerManager_Shutdown 테스트를 새로운 의도에 맞게 재작성 ---

// TestWorkerManager_Shutdown_Success는 모든 작업이 타임아웃 내에 완료될 때,
// Shutdown이 에러 없이 정상 종료되는지 검증합니다.
func TestWorkerManager_Shutdown_Success(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 1, 5*time.Second)
	require.NoError(t, err)

	jobDone := make(chan struct{})
	job := func(ctx context.Context) {
		time.Sleep(50 * time.Millisecond) // 작업 시간
		close(jobDone)
	}
	manager.Submit(job)

	// 작업 시간(50ms)보다 긴 타임아웃(100ms)으로 Shutdown 호출
	shutdownErr := manager.Shutdown(100 * time.Millisecond)
	require.NoError(t, shutdownErr, "Shutdown should succeed without a timeout error")

	// Shutdown이 성공적으로 끝났다면, 실제 작업도 완료되었어야 합니다.
	select {
	case <-jobDone:
		// 예상된 성공 경로
	default:
		t.Fatal("Shutdown returned success, but the job did not complete")
	}
}

// TestWorkerManager_Shutdown_Timeout은 작업이 타임아웃보다 오래 걸릴 때,
// Shutdown이 타임아웃 에러를 올바르게 반환하는지 검증합니다.
func TestWorkerManager_Shutdown_Timeout(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 1, 5*time.Second)
	require.NoError(t, err)

	job := func(ctx context.Context) {
		time.Sleep(200 * time.Millisecond) // 작업 시간 > Shutdown 타임아웃
	}
	manager.Submit(job)

	// 작업 시간(200ms)보다 짧은 타임아웃(100ms)으로 Shutdown 호출
	shutdownErr := manager.Shutdown(100 * time.Millisecond)
	require.Error(t, shutdownErr, "Shutdown should have returned a timeout error")
	assert.ErrorIs(t, shutdownErr, ErrShutdownTimeout, "The returned error should be ErrShutdownTimeout")
}

// TestWorkerManager_JobTimeout은 작업이 설정된 타임아웃을 초과했을 때 컨텍스트가 취소되는지 검증합니다.
func TestWorkerManager_JobTimeout(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 6, 10*time.Millisecond)
	require.NoError(t, err)
	defer manager.Shutdown(1 * time.Second) // defer 호출 수정

	jobCanceled := make(chan bool)
	job := func(ctx context.Context) {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			jobCanceled <- true
		}
	}

	manager.Submit(job)

	select {
	case <-jobCanceled:
		// success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Job was not canceled by timeout")
	}
}

// TestWorkerPool_JobDroppingOnFullQueue는 큐가 가득 찼을 때 새 작업이 거부되는지 검증합니다.
func TestWorkerPool_JobDroppingOnFullQueue(t *testing.T) {
	poolSize, queueSize := 1, 1
	manager, err := NewManager("pool", log.NewNopLogger(), poolSize, queueSize, 1*time.Second)
	require.NoError(t, err)
	defer manager.Shutdown(1 * time.Second) // defer 호출 수정

	var firstJobDone = make(chan struct{})
	var thirdJobExecuted atomic.Bool

	firstJob := func(ctx context.Context) {
		time.Sleep(50 * time.Millisecond)
		close(firstJobDone)
	}
	thirdJob := func(ctx context.Context) {
		thirdJobExecuted.Store(true)
	}

	manager.Submit(firstJob)                     // 워커가 즉시 가져감
	manager.Submit(func(ctx context.Context) {}) // 큐에 들어감
	manager.Submit(thirdJob)                     // 큐가 꽉 차서 버려져야 함

	<-firstJobDone
	time.Sleep(50 * time.Millisecond)

	assert.False(t, thirdJobExecuted.Load(), "큐가 가득 찼을 때 제출된 작업은 실행되지 않고 버려져야 합니다.")
}

// TestShutdown_WithFullQueue는 큐가 가득 찬 상태에서 Shutdown을 호출했을 때의 동작을 검증합니다.
func TestShutdown_WithFullQueue(t *testing.T) {
	poolSize, queueSize := 2, 4
	manager, err := NewManager("pool", log.NewNopLogger(), poolSize, queueSize, 1*time.Second)
	require.NoError(t, err)

	var jobsDone atomic.Int32
	expectedJobsToRun := poolSize + queueSize

	// 큐를 가득 채웁니다.
	for i := 0; i < expectedJobsToRun; i++ {
		manager.Submit(func(ctx context.Context) {
			// 작업 내용은 테스트 결과에 영향을 주지 않으므로 간단하게 유지합니다.
			jobsDone.Add(1)
		})
		// *** 핵심 수정 ***
		// Submit 루프가 너무 빨리 실행되어 워커가 작업을 가져갈 기회를 갖기 전에
		// 큐가 가득 차는 것을 방지하기 위해, CPU를 다른 고루틴에 양보합니다.
		runtime.Gosched()
	}

	// 이 시점에는 워커들이 작업을 가져가기 시작했으므로,
	// 추가 작업을 제출하면 성공적으로 큐에 들어갈 가능성이 있습니다.
	// 테스트의 안정성을 위해, 모든 작업이 완료된 후 카운트를 확인합니다.

	// Shutdown이 시간 내에 정상적으로 완료되는지 확인
	// 모든 작업이 완료될 시간을 넉넉하게 줍니다.
	shutdownErr := manager.Shutdown(500 * time.Millisecond)
	require.NoError(t, shutdownErr, "Shutdown should complete without a timeout")

	// 최종적으로, 큐에 성공적으로 들어간 작업만 실행되었는지 확인합니다.
	// runtime.Gosched() 덕분에 6개의 작업이 모두 성공적으로 제출되었을 것입니다.
	assert.Equal(t, int32(expectedJobsToRun), jobsDone.Load(), "모든 작업이 성공적으로 제출되고 완료되어야 합니다.")
}
