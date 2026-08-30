package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestGroup_RemoveCache_AdjustsNextIndex(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.Register("cache-b", Config{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.Register("cache-c", Config{Weight: 1, QueueLimit: 4}))

	group := rt.(*Group)

	group.mu.Lock()
	group.nextIdx = 2
	group.mu.Unlock()

	rt.RemoveCache("cache-a")

	group.mu.Lock()
	defer group.mu.Unlock()
	require.Equal(t, []string{"cache-b", "cache-c"}, group.order)
	require.Equal(t, 1, group.nextIdx)
}

func TestGroup_Register_DuplicateCacheName(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))
	err := rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate cache name")
}

func TestGroup_Register_EmptyCacheName(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	err := rt.Register("", Config{Weight: 1, QueueLimit: 4})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cache name cannot be empty")
}

func TestGroup_Register_AfterShutdown(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.Shutdown(time.Second))

	err := rt.Register("cache-b", Config{Weight: 1, QueueLimit: 4})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cache group is closed")
}

func TestGroup_Submit(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))

	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})

	err := rt.Submit("cache-a", JobKindRefresh, Job{
		Run: func(ctx context.Context) {
			close(jobStarted)
			<-releaseJob
		},
	})
	require.NoError(t, err)
	<-jobStarted

	close(releaseJob)
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroup_Submit_DiscardOnReject(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.CloseCache("cache-a", time.Second))

	discardCalled := false
	err := rt.Submit("cache-a", JobKindRefresh, Job{
		Run: func(ctx context.Context) {
			t.Fatal("job should not be executed")
		},
		Discard: func(reason DropReason) {
			discardCalled = true
			require.Equal(t, DropReasonRejected, reason)
		},
	})
	require.ErrorIs(t, err, ErrRejected)
	require.True(t, discardCalled, "Discard should be called when job is rejected")

	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroup_CloseCache_DiscardsQueuedJobs(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))

	// Submit a blocking job to occupy the worker
	blocker := make(chan struct{})
	jobStarted := make(chan struct{})
	err := rt.Submit("cache-a", JobKindRefresh, Job{
		Run: func(ctx context.Context) {
			close(jobStarted)
			<-blocker
		},
	})
	require.NoError(t, err)
	<-jobStarted

	// Submit a job that will stay in the queue
	discarded := make(chan DropReason, 1)
	err = rt.Submit("cache-a", JobKindRefresh, Job{
		Run: func(ctx context.Context) {
			t.Fatal("job should not be executed")
		},
		Discard: func(reason DropReason) {
			discarded <- reason
		},
	})
	require.NoError(t, err)

	closed := make(chan error, 1)
	go func() { closed <- rt.CloseCache("cache-a", time.Second) }()
	require.Equal(t, DropReasonShutdown, <-discarded)
	close(blocker)
	require.NoError(t, <-closed)
}

func TestGroup_Shutdown_DiscardsAllJobs(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))

	// Submit a blocking job to occupy the worker
	blocker := make(chan struct{})
	jobStarted := make(chan struct{})
	err := rt.Submit("cache-a", JobKindRefresh, Job{
		Run: func(ctx context.Context) {
			close(jobStarted)
			<-blocker
		},
	})
	require.NoError(t, err)
	<-jobStarted

	// Submit a job that will stay in the queue
	discardCalled := false
	err = rt.Submit("cache-a", JobKindRefresh, Job{
		Run: func(ctx context.Context) {
			t.Fatal("job should not be executed")
		},
		Discard: func(reason DropReason) {
			discardCalled = true
			require.Equal(t, DropReasonShutdown, reason)
		},
	})
	require.NoError(t, err)

	// Release the blocking job so Shutdown can complete
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(blocker)
	}()

	require.NoError(t, rt.Shutdown(time.Second))
	require.True(t, discardCalled, "Discard should be called for dropped jobs")
}

func TestGroup_NilReceiver(t *testing.T) {
	var rt *Group
	err := rt.Submit("cache-a", JobKindRefresh, Job{
		Run:     func(ctx context.Context) {},
		Discard: func(reason DropReason) {},
	})
	require.ErrorIs(t, err, ErrRejected)
	require.NoError(t, rt.CloseCache("cache-a", time.Second))
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroup_AdaptiveWeight(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)
	group := rt.(*Group)

	base := &groupCacheState{weight: 2, queueLimit: 8}
	require.Equal(t, 2, group.adaptiveWeightLocked(base))

	halfFull := &groupCacheState{weight: 2, queueLimit: 8, queue: make(chan queuedJob, 8)}
	halfFull.queue <- queuedJob{}
	halfFull.queue <- queuedJob{}
	halfFull.queue <- queuedJob{}
	halfFull.queue <- queuedJob{}
	require.Equal(t, 6, group.adaptiveWeightLocked(halfFull))

	full := &groupCacheState{weight: 2, queueLimit: 8, queue: make(chan queuedJob, 8)}
	for i := 0; i < 8; i++ {
		full.queue <- queuedJob{}
	}
	require.Equal(t, 10, group.adaptiveWeightLocked(full))
}
