package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestStandalone_Submit(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})

	require.True(t, rt.Submit("cache", JobKindRefresh, func(ctx context.Context) {
		close(jobStarted)
		<-releaseJob
	}))
	<-jobStarted

	close(releaseJob)
	require.NoError(t, rt.Shutdown(time.Second))
}

func TestStandalone_SubmitWithDropCleanup_Success(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})
	onDropCalled := false

	require.True(t, rt.SubmitWithDropCleanup("cache", JobKindRefresh, func(ctx context.Context) {
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

func TestStandalone_SubmitWithDropCleanup_Rejected(t *testing.T) {
	rt := NewStandalone(nil)
	onDropCalled := false

	require.False(t, rt.SubmitWithDropCleanup("cache", JobKindRefresh, func(ctx context.Context) {
		t.Fatal("job should not be executed")
	}, func() {
		onDropCalled = true
	}))
	require.True(t, onDropCalled, "onDrop should be called when job is rejected")
}

func TestStandalone_SubmitWithDropCleanup_Panic(t *testing.T) {
	manager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, time.Second)
	require.NoError(t, err)

	rt := NewStandalone(manager)
	jobStarted := make(chan struct{})
	releaseJob := make(chan struct{})
	onDropDone := make(chan struct{})
	onDropCalled := false

	require.True(t, rt.SubmitWithDropCleanup("cache", JobKindRefresh, func(ctx context.Context) {
		close(jobStarted)
		<-releaseJob
		panic("test panic")
	}, func() {
		onDropCalled = true
		close(onDropDone)
	}))
	<-jobStarted

	releaseJob <- struct{}{}
	<-onDropDone
	require.True(t, onDropCalled, "onDrop should be called on panic")
	require.NoError(t, rt.Shutdown(time.Second))
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
	require.False(t, rt.Submit("cache", JobKindRefresh, func(ctx context.Context) {}))
	require.False(t, rt.SubmitWithDropCleanup("cache", JobKindRefresh, func(ctx context.Context) {}, nil))
	require.NoError(t, rt.CloseCache("cache", time.Second))
	require.NoError(t, rt.Shutdown(time.Second))
}
