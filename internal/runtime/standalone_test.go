package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee/internal/worker"
)

func TestStandalone_Submit(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})

	err = rt.Submit("cache", JobKindRefresh, Job{
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

func TestStandalone_Submit_DiscardOnReject(t *testing.T) {
	rt := NewStandalone(nil)
	discardCalled := false

	err := rt.Submit("cache", JobKindRefresh, Job{
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
}

func TestStandalone_Submit_NoDiscardOnSuccess(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})
	discardCalled := false

	err = rt.Submit("cache", JobKindRefresh, Job{
		Run: func(ctx context.Context) {
			close(jobStarted)
			<-releaseJob
		},
		Discard: func(reason DropReason) {
			discardCalled = true
		},
	})
	require.NoError(t, err)
	<-jobStarted

	close(releaseJob)
	require.NoError(t, rt.Shutdown(time.Second))
	require.False(t, discardCalled, "Discard should not be called on successful job completion")
}

func TestStandalone_CloseCache(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	require.NoError(t, rt.CloseCache("cache", time.Second))
}

func TestStandalone_RemoveCache(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	rt.RemoveCache("cache")
}

func TestStandalone_Shutdown(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestStandalone_NilManager(t *testing.T) {
	rt := NewStandalone(nil)
	err := rt.Submit("cache", JobKindRefresh, Job{
		Run:     func(ctx context.Context) {},
		Discard: func(reason DropReason) {},
	})
	require.ErrorIs(t, err, ErrRejected)
	require.NoError(t, rt.CloseCache("cache", time.Second))
	require.NoError(t, rt.Shutdown(time.Second))
}
