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

// TestWorkerManager_NewManager verifies that the worker manager is created with the correct strategy.
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
			defer manager.Shutdown(1 * time.Second)

			assert.IsType(t, tc.expectedType, manager.strategy)
		})
	}
}

// TestWorkerManager_SubmitAndRun verifies that a job is successfully submitted and executed.
func TestWorkerManager_SubmitAndRun(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 10, 1*time.Second)
	require.NoError(t, err)
	defer manager.Shutdown(1 * time.Second)

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

// TestWorkerManager_Shutdown_Success verifies that Shutdown completes successfully
// when all jobs finish within the timeout.
func TestWorkerManager_Shutdown_Success(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 1, 5*time.Second)
	require.NoError(t, err)

	jobDone := make(chan struct{})
	job := func(ctx context.Context) {
		time.Sleep(50 * time.Millisecond)
		close(jobDone)
	}
	manager.Submit(job)

	shutdownErr := manager.Shutdown(100 * time.Millisecond)
	require.NoError(t, shutdownErr, "Shutdown should succeed without a timeout error")

	select {
	case <-jobDone:
		// Expected success path
	default:
		t.Fatal("Shutdown returned success, but the job did not complete")
	}
}

// TestWorkerManager_Shutdown_Timeout verifies that Shutdown correctly returns a timeout error
// when a job takes longer than the specified timeout.
func TestWorkerManager_Shutdown_Timeout(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 1, 5*time.Second)
	require.NoError(t, err)

	job := func(ctx context.Context) {
		time.Sleep(200 * time.Millisecond)
	}
	manager.Submit(job)

	shutdownErr := manager.Shutdown(100 * time.Millisecond)
	require.Error(t, shutdownErr, "Shutdown should have returned a timeout error")
	assert.ErrorIs(t, shutdownErr, ErrShutdownTimeout, "The returned error should be ErrShutdownTimeout")
}

// TestWorkerManager_JobTimeout verifies that the job's context is canceled
// when the job exceeds its configured timeout.
func TestWorkerManager_JobTimeout(t *testing.T) {
	manager, err := NewManager("pool", log.NewNopLogger(), 1, 6, 10*time.Millisecond)
	require.NoError(t, err)
	defer manager.Shutdown(1 * time.Second)

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

// TestWorkerPool_JobDroppingOnFullQueue verifies that new jobs are rejected
// when the queue is full.
func TestWorkerPool_JobDroppingOnFullQueue(t *testing.T) {
	poolSize, queueSize := 1, 1
	manager, err := NewManager("pool", log.NewNopLogger(), poolSize, queueSize, 1*time.Second)
	require.NoError(t, err)
	defer manager.Shutdown(1 * time.Second)

	var firstJobStarted = make(chan struct{})
	var firstJobDone = make(chan struct{})
	var thirdJobExecuted atomic.Bool

	firstJob := func(ctx context.Context) {
		close(firstJobStarted)
		time.Sleep(10 * time.Millisecond)
		close(firstJobDone)
	}

	thirdJob := func(ctx context.Context) {
		thirdJobExecuted.Store(true)
	}

	manager.Submit(firstJob)
	<-firstJobStarted

	manager.Submit(func(ctx context.Context) {}) // Enters the queue

	manager.Submit(thirdJob) // Should be dropped as queue is full

	<-firstJobDone
	time.Sleep(50 * time.Millisecond)

	assert.False(t, thirdJobExecuted.Load(), "Job submitted when queue is full should not be executed.")
}

// TestShutdown_WithFullQueue verifies the behavior of Shutdown when the queue is full.
func TestShutdown_WithFullQueue(t *testing.T) {
	poolSize, queueSize := 2, 4
	manager, err := NewManager("pool", log.NewNopLogger(), poolSize, queueSize, 1*time.Second)
	require.NoError(t, err)

	var jobsDone atomic.Int32
	var wg sync.WaitGroup
	manager, err = NewManager("pool", log.NewNopLogger(), 2, 4, 1*time.Second)
	require.NoError(t, err)

	expectedJobsToSubmit := poolSize + queueSize + 2
	var submittedJobs int32

	wg.Add(int(expectedJobsToSubmit))

	for i := 0; i < int(expectedJobsToSubmit); i++ {
		if manager.Submit(func(ctx context.Context) {
			jobsDone.Add(1)
			wg.Done()
		}) {
			submittedJobs++
		} else {
			wg.Done()
		}
	}

	wg.Wait()

	shutdownErr := manager.Shutdown(500 * time.Millisecond)
	require.NoError(t, shutdownErr, "Shutdown should complete without a timeout")

	assert.Equal(t, jobsDone.Load(), submittedJobs, "The number of executed jobs should match the number of submitted jobs.")
}

// TestPoolStrategy_DropsJob_WhenQueueIsFull_And_LogsIt verifies that when the worker pool's queue is full,
// new jobs are dropped and a warning is logged.
func TestPoolStrategy_DropsJob_WhenQueueIsFull_And_LogsIt(t *testing.T) {
	var logBuf bytes.Buffer
	logger := log.NewLogfmtLogger(&logBuf)

	poolSize, queueSize := 1, 1
	strategy := NewPoolStrategy(logger, poolSize, queueSize, 1*time.Second)
	defer strategy.Shutdown(1 * time.Second)

	firstJobStarted := make(chan struct{})
	firstJobBlocker := make(chan struct{})

	firstJob := func(ctx context.Context) {
		close(firstJobStarted)
		<-firstJobBlocker
	}

	secondJob := func(ctx context.Context) {}
	thirdJob := func(ctx context.Context) {}

	strategy.Submit(firstJob)
	<-firstJobStarted

	strategy.Submit(secondJob)

	strategy.Submit(thirdJob)

	time.Sleep(50 * time.Millisecond)

	assert.Contains(t, logBuf.String(), "level=warn msg=\"worker queue is full, dropping job\"", "A warning log should be recorded when a job is dropped.")

	close(firstJobBlocker)
}