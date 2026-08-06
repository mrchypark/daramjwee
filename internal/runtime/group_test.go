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

	require.True(t, rt.Submit("cache-a", JobKindRefresh, func(ctx context.Context) {
		close(jobStarted)
		<-releaseJob
	}))
	<-jobStarted

	close(releaseJob)
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroup_SubmitWithDropCleanup_Success(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))

	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})
	onDropCalled := false

	require.True(t, rt.SubmitWithDropCleanup("cache-a", JobKindRefresh, func(ctx context.Context) {
		close(jobStarted)
		<-releaseJob
	}, func() {
		onDropCalled = true
	}))
	<-jobStarted

	close(releaseJob)
	require.NoError(t, rt.Shutdown(time.Second))
	require.False(t, onDropCalled, "onDrop should not be called on successful job completion")
}

func TestGroup_SubmitWithDropCleanup_ClosedCache(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.CloseCache("cache-a", time.Second))

	require.False(t, rt.SubmitWithDropCleanup("cache-a", JobKindRefresh, func(ctx context.Context) {
		t.Fatal("job should not be executed")
	}, nil))

	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroup_CloseCache_DropsQueuedJobs(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))

	onDropCalled := false
	require.True(t, rt.SubmitWithDropCleanup("cache-a", JobKindRefresh, func(ctx context.Context) {
		time.Sleep(10 * time.Second)
	}, func() {
		onDropCalled = true
	}))

	require.NoError(t, rt.CloseCache("cache-a", time.Second))
	require.True(t, onDropCalled, "onDrop should be called for dropped jobs")
}

func TestGroup_Shutdown_DropsAllJobs(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))

	onDropCalled := false
	require.True(t, rt.SubmitWithDropCleanup("cache-a", JobKindRefresh, func(ctx context.Context) {
		time.Sleep(10 * time.Second)
	}, func() {
		onDropCalled = true
	}))

	require.NoError(t, rt.Shutdown(time.Second))
	require.True(t, onDropCalled, "onDrop should be called for dropped jobs")
}

func TestGroup_NilReceiver(t *testing.T) {
	var rt *Group
	require.False(t, rt.Submit("cache-a", JobKindRefresh, func(ctx context.Context) {}))
	require.False(t, rt.SubmitWithDropCleanup("cache-a", JobKindRefresh, func(ctx context.Context) {}, nil))
	require.NoError(t, rt.CloseCache("cache-a", time.Second))
	require.NoError(t, rt.Shutdown(time.Second))
}
