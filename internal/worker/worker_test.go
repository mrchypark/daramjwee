// Filename: internal/worker/worker_test.go
package worker

import (
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
		{
			name:         "Pool Strategy",
			strategy:     "pool",
			pSize:        1,
			qSize:        1,
			expectedType: &PoolStrategy{},
		},
		{
			name:         "All Strategy",
			strategy:     "all",
			pSize:        1,
			qSize:        1,
			expectedType: &AllStrategy{},
		},
		{
			name:         "Invalid Strategy Should Default to Pool",
			strategy:     "invalid-strategy",
			pSize:        1,
			qSize:        1,
			expectedType: &PoolStrategy{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager, err := NewManager(tc.strategy, log.NewNopLogger(), tc.pSize, tc.qSize, 1*time.Second)
			require.NoError(t, err)
			defer manager.Shutdown()

			assert.IsType(t, tc.expectedType, manager.strategy)
		})
	}
}

// TestWorkerManager_SubmitAndRun은 작업이 성공적으로 제출되고 실행되는지 검증합니다.
func TestWorkerManager_SubmitAndRun(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 10, 1*time.Second)
	require.NoError(t, err)
	defer manager.Shutdown()

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

// TestWorkerManager_Shutdown은 워커가 정상적으로 종료되는지 검증합니다.
func TestWorkerManager_Shutdown(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 10, 5*time.Second)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	jobStarted := make(chan bool)
	job := func(ctx context.Context) {
		jobStarted <- true
		time.Sleep(50 * time.Millisecond) // 작업을 시뮬레이션
		wg.Done()
	}

	manager.Submit(job)
	<-jobStarted // 작업이 시작된 것을 확인

	shutdownDone := make(chan bool)
	go func() {
		manager.Shutdown() // 이 함수는 wg.Wait() 때문에 작업이 끝날 때까지 블록되어야 함
		shutdownDone <- true
	}()

	// Shutdown이 즉시 리턴되지 않는지 확인 (블록되어야 함)
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown should have blocked until the job was finished")
	case <-time.After(10 * time.Millisecond):
		// expected behavior
	}

	// 작업이 완료되고 Shutdown이 정상적으로 끝나는지 확인
	wg.Wait()
	select {
	case <-shutdownDone:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Shutdown did not complete after job finished")
	}
}

// TestWorkerManager_JobTimeout은 작업이 설정된 타임아웃을 초과했을 때
// 컨텍스트가 취소되는지 검증합니다.
func TestWorkerManager_JobTimeout(t *testing.T) {
	// 10ms의 매우 짧은 타임아웃 설정
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 6, 10*time.Millisecond)
	require.NoError(t, err)
	defer manager.Shutdown()

	jobCanceled := make(chan bool)
	job := func(ctx context.Context) {
		// 작업은 타임아웃(10ms)보다 긴 100ms 동안 대기
		select {
		case <-time.After(100 * time.Millisecond):
			// 컨텍스트가 취소되지 않았다면 이쪽으로 빠짐 (테스트 실패)
		case <-ctx.Done():
			// 컨텍스트가 타임아웃으로 취소되면 이쪽으로 빠짐 (테스트 성공)
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
