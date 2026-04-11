package daramjwee

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestStandaloneRuntime_CloseCacheWaitsForJobCompletion(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := newStandaloneRuntime(manager)
	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})

	require.True(t, rt.Submit("cache", JobKindRefresh, func(ctx context.Context) {
		close(jobStarted)
		<-releaseJob
	}))
	<-jobStarted

	done := make(chan error, 1)
	go func() {
		done <- rt.CloseCache("cache", 2*time.Second)
	}()

	close(releaseJob)
	require.NoError(t, <-done)
}

func TestGroupRuntime_QueueIsolationAndLimitEnforcement(t *testing.T) {
	rt := newGroupRuntime(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", CacheRuntimeConfig{Weight: 1, QueueLimit: 1}))
	require.NoError(t, rt.Register("cache-b", CacheRuntimeConfig{Weight: 1, QueueLimit: 1}))

	blockA := make(chan struct{})
	releaseA := make(chan struct{})
	require.True(t, rt.Submit("cache-a", JobKindPersist, func(ctx context.Context) {
		close(blockA)
		<-releaseA
	}))
	<-blockA
	require.True(t, rt.Submit("cache-a", JobKindPersist, func(ctx context.Context) {}))
	require.False(t, rt.Submit("cache-a", JobKindPersist, func(ctx context.Context) {}))

	require.True(t, rt.Submit("cache-b", JobKindPersist, func(ctx context.Context) {}))
	close(releaseA)

	require.NoError(t, rt.CloseCache("cache-a", time.Second))
	require.False(t, rt.Submit("cache-a", JobKindPersist, func(ctx context.Context) {}))
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroupRuntime_WeightedDequeueProgress(t *testing.T) {
	rt := newGroupRuntime(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", CacheRuntimeConfig{Weight: 2, QueueLimit: 8}))
	require.NoError(t, rt.Register("cache-b", CacheRuntimeConfig{Weight: 1, QueueLimit: 8}))

	var mu sync.Mutex
	order := make([]string, 0, 3)
	done := make(chan struct{})
	record := func(name string) worker.Job {
		return func(ctx context.Context) {
			mu.Lock()
			order = append(order, name)
			if len(order) == 3 {
				close(done)
			}
			mu.Unlock()
		}
	}

	require.True(t, rt.Submit("cache-a", JobKindPersist, record("A")))
	require.True(t, rt.Submit("cache-a", JobKindPersist, record("A")))
	require.True(t, rt.Submit("cache-b", JobKindPersist, record("B")))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for weighted dequeue order")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"A", "A", "B"}, order)
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroupRuntime_CloseCacheWaitsForDequeuedJobReservation(t *testing.T) {
	rt := newGroupRuntime(log.NewNopLogger(), 1, time.Second)

	const cacheID = "cache-race"
	require.NoError(t, rt.Register(cacheID, CacheRuntimeConfig{Weight: 1, QueueLimit: 4}))

	jobReady := make(chan struct{})
	releaseJob := make(chan struct{})
	rt.beforeJobStart = func(id string, kind JobKind) {
		if id == cacheID && kind == JobKindRefresh {
			select {
			case <-jobReady:
			default:
				close(jobReady)
			}
			<-releaseJob
		}
	}

	require.True(t, rt.Submit(cacheID, JobKindRefresh, func(ctx context.Context) {}))
	<-jobReady

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- rt.CloseCache(cacheID, time.Second)
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("cache close returned before reserved job was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseJob)
	require.NoError(t, <-closeDone)

	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroupRuntime_CloseCache_IdempotentWhileJobActive(t *testing.T) {
	rt := newGroupRuntime(log.NewNopLogger(), 1, time.Second)

	const cacheID = "cache-repeat-close"
	require.NoError(t, rt.Register(cacheID, CacheRuntimeConfig{Weight: 1, QueueLimit: 4}))

	jobReady := make(chan struct{})
	releaseJob := make(chan struct{})
	rt.beforeJobStart = func(id string, kind JobKind) {
		if id == cacheID && kind == JobKindRefresh {
			select {
			case <-jobReady:
			default:
				close(jobReady)
			}
			<-releaseJob
		}
	}

	require.True(t, rt.Submit(cacheID, JobKindRefresh, func(ctx context.Context) {}))
	<-jobReady

	var firstReturned atomic.Bool
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- rt.CloseCache(cacheID, time.Second)
		firstReturned.Store(true)
	}()

	require.Never(t, firstReturned.Load, 100*time.Millisecond, 10*time.Millisecond)

	var secondReturned atomic.Bool
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- rt.CloseCache(cacheID, time.Second)
		secondReturned.Store(true)
	}()

	require.Never(t, secondReturned.Load, 100*time.Millisecond, 10*time.Millisecond)

	close(releaseJob)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)

	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroupRuntime_RecoversPanickingJobAndContinues(t *testing.T) {
	rt := newGroupRuntime(log.NewNopLogger(), 1, time.Second)

	const cacheID = "cache-panic"
	require.NoError(t, rt.Register(cacheID, CacheRuntimeConfig{Weight: 1, QueueLimit: 4}))

	require.True(t, rt.Submit(cacheID, JobKindRefresh, func(ctx context.Context) {
		panic("boom")
	}))

	secondDone := make(chan struct{})
	require.True(t, rt.Submit(cacheID, JobKindRefresh, func(ctx context.Context) {
		close(secondDone)
	}))

	require.Eventually(t, func() bool {
		select {
		case <-secondDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rt.CloseCache(cacheID, time.Second))
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestGroupRuntime_RemoveCache_AdjustsNextIndex(t *testing.T) {
	rt := newGroupRuntime(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", CacheRuntimeConfig{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.Register("cache-b", CacheRuntimeConfig{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.Register("cache-c", CacheRuntimeConfig{Weight: 1, QueueLimit: 4}))

	rt.mu.Lock()
	rt.nextIdx = 2
	rt.mu.Unlock()

	rt.RemoveCache("cache-a")

	rt.mu.Lock()
	defer rt.mu.Unlock()
	require.Equal(t, []string{"cache-b", "cache-c"}, rt.order)
	require.Equal(t, 1, rt.nextIdx)
}
