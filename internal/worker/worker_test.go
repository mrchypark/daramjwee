// Filename: internal/worker/worker_test.go

package worker

import (
	"bytes"
	"context"
	"sync"
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
	defer manager.Shutdown(1 * time.Second)

	var firstJobStarted = make(chan struct{})
	var firstJobDone = make(chan struct{})
	var thirdJobExecuted atomic.Bool

	// 첫 번째 작업은 시작과 끝을 채널로 알립니다.
	firstJob := func(ctx context.Context) {
		close(firstJobStarted)
		// 작업을 모방하는 약간의 시간
		time.Sleep(10 * time.Millisecond)
		close(firstJobDone)
	}

	// 세 번째 작업
	thirdJob := func(ctx context.Context) {
		thirdJobExecuted.Store(true)
	}

	manager.Submit(firstJob) // 워커가 즉시 가져감
	<-firstJobStarted        // 첫 번째 작업이 시작될 때까지 대기

	manager.Submit(func(ctx context.Context) {}) // 큐에 들어감
	manager.Submit(thirdJob)                     // 큐가 꽉 차서 버려져야 함

	<-firstJobDone // 첫 번째 작업이 끝날 때까지 대기
	// 워커가 두 번째 작업을 처리할 시간을 약간 줍니다.
	time.Sleep(50 * time.Millisecond)

	assert.False(t, thirdJobExecuted.Load(), "큐가 가득 찼을 때 제출된 작업은 실행되지 않고 버려져야 합니다.")
}

// TestShutdown_WithFullQueue는 큐가 가득 찬 상태에서 Shutdown을 호출했을 때의 동작을 검증합니다.
func TestShutdown_WithFullQueue(t *testing.T) {
	poolSize, queueSize := 2, 4
	manager, err := NewManager("pool", log.NewNopLogger(), poolSize, queueSize, 1*time.Second)
	require.NoError(t, err)

	var jobsDone atomic.Int32
	var wg sync.WaitGroup
	manager, err = NewManager("pool", log.NewNopLogger(), 2, 4, 1*time.Second)
	require.NoError(t, err)

	expectedJobsToSubmit := poolSize + queueSize + 2 // 큐 용량 + 워커 수 + 추가로 드롭될 작업
	var submittedJobs int32

	wg.Add(int(expectedJobsToSubmit))

	for i := 0; i < int(expectedJobsToSubmit); i++ {
		if manager.Submit(func(ctx context.Context) {
			jobsDone.Add(1)
			wg.Done()
		}) {
			submittedJobs++
		} else {
			wg.Done() // 드롭된 작업에 대해서도 Done 호출
		}
	}

	wg.Wait() // 모든 작업 제출 시도 (성공 또는 실패)가 완료될 때까지 대기

	// Shutdown이 시간 내에 정상적으로 완료되는지 확인
	shutdownErr := manager.Shutdown(500 * time.Millisecond)
	require.NoError(t, shutdownErr, "Shutdown should complete without a timeout")

	// 실제로 실행된 작업 수와 제출된 작업 수를 비교합니다.
	// 큐가 가득 차서 드롭된 작업이 있을 수 있으므로, submittedJobs와 jobsDone.Load()를 비교합니다.
	assert.Equal(t, jobsDone.Load(), submittedJobs, "실제로 실행된 작업 수는 제출된 작업 수와 일치해야 합니다.")
}

// TestPoolStrategy_DropsJob_WhenQueueIsFull_And_LogsIt는 워커 풀의 큐가 가득 찼을 때
// 새로운 작업이 버려지고, 이 사실이 경고 로그로 기록되는지를 검증합니다.
func TestPoolStrategy_DropsJob_WhenQueueIsFull_And_LogsIt(t *testing.T) {
	// 1. 로그 출력을 캡처하기 위한 설정
	var logBuf bytes.Buffer
	// 테스트용 로거는 bytes.Buffer에 로그를 씁니다.
	logger := log.NewLogfmtLogger(&logBuf)

	// 2. 큐 크기가 1인 워커 풀 생성
	// 워커 1개, 큐 크기 1로 설정하여 시나리오를 단순화합니다.
	poolSize, queueSize := 1, 1
	strategy := NewPoolStrategy(logger, poolSize, queueSize, 1*time.Second)
	defer strategy.Shutdown(1 * time.Second) // 테스트 종료 시 자원 정리

	// 3. 작업 유실 시나리오 구성
	firstJobStarted := make(chan struct{})
	firstJobBlocker := make(chan struct{}) // 첫 번째 작업을 계속 점유 상태로 둘 블로커

	// 첫 번째 작업: 시작을 알리고, 블로커가 닫힐 때까지 대기
	firstJob := func(ctx context.Context) {
		close(firstJobStarted)
		<-firstJobBlocker
	}

	// 두 번째 및 세 번째 작업은 내용이 중요하지 않음
	secondJob := func(ctx context.Context) {}
	thirdJob := func(ctx context.Context) {}

	// 4. 작업 제출
	strategy.Submit(firstJob) // -> 워커가 즉시 가져가서 실행 (블로킹 상태)
	<-firstJobStarted         // 첫 번째 작업이 실행될 때까지 대기

	strategy.Submit(secondJob) // -> 큐(크기 1)에 들어감

	strategy.Submit(thirdJob) // -> 큐가 꽉 찼으므로 이 작업은 버려져야 함 (로그 발생)

	// 5. 로그 내용 검증
	// 버려진 작업에 대한 로그가 기록될 시간을 잠시 줍니다.
	time.Sleep(50 * time.Millisecond)

	assert.Contains(t, logBuf.String(), "level=warn msg=\"worker queue is full, dropping job\"", "작업 유실 시 경고 로그가 기록되어야 합니다.")

	// 6. 테스트 정리
	close(firstJobBlocker) // 블로킹을 해제하여 워커가 정상 종료되도록 함
}
