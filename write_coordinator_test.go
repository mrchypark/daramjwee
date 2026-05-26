package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetStreamToStoreWithTopGenerationRejectsStaleWriterBeforeBeginSet(t *testing.T) {
	store := &destructiveReservationStore{
		data: []byte("live-body"),
		meta: Metadata{CacheTag: "live"},
	}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}
	cache.noteTopWriteGeneration("key")

	expectedGeneration := uint64(0)
	writer, err := cache.setStreamToTopStoreWithGeneration(context.Background(), "key", &Metadata{CacheTag: "stale"}, &expectedGeneration)
	if !errors.Is(err, ErrTopWriteInvalidated) {
		t.Fatalf("expected invalidated error, got writer=%v err=%v", writer, err)
	}
	if store.beginSetCalls != 0 {
		t.Fatalf("expected BeginSet not to be called for stale writer, got %d", store.beginSetCalls)
	}
	if got := string(store.data); got != "live-body" {
		t.Fatalf("expected live body to remain intact, got %q", got)
	}
}

func TestSetStreamToStoreWithTopGenerationRestoresGenerationOnBeginSetFailure(t *testing.T) {
	store := &failingBeginSetStore{err: errors.New("boom")}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	expectedGeneration := uint64(0)
	writer, err := cache.setStreamToTopStoreWithGeneration(context.Background(), "key", &Metadata{CacheTag: "v1"}, &expectedGeneration)
	if writer != nil {
		t.Fatalf("expected no writer on BeginSet failure, got %T", writer)
	}
	if !errors.Is(err, store.err) {
		t.Fatalf("expected BeginSet error, got %v", err)
	}
	if got := cache.currentTopWriteGeneration("key"); got != 0 {
		t.Fatalf("expected generation to be restored after BeginSet failure, got %d", got)
	}
}

func TestTopFillPreemptDoesNotDeadlockWithCloseWaitingOnDelete(t *testing.T) {
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	coord.writeMu.Lock()
	coord.stateMu.Lock()
	coord.activeDeletes = 1
	coord.stateMu.Unlock()

	fill := newPendingTopFillSink(coord)
	topWriter := &coordinatedTopWriteSink{
		WriteSink:  &stubWriteSink{},
		coord:      coord,
		key:        "key",
		generation: 0,
	}
	if !fill.attach(topWriter) {
		t.Fatal("expected fill attach to succeed")
	}
	coord.stateMu.Lock()
	fill.registered = true
	coord.activeFill = fill
	coord.stateMu.Unlock()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- fill.Close()
	}()
	time.Sleep(50 * time.Millisecond)

	preemptDone := make(chan error, 1)
	go func() {
		preemptDone <- fill.Preempt()
	}()

	select {
	case err := <-preemptDone:
		if err != nil {
			t.Fatalf("preempt returned unexpected error: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("preempt blocked on topFillSink.mu while close waited on active delete")
	}

	coord.finishDelete(true)
	select {
	case err := <-closeDone:
		if !errors.Is(err, ErrTopWriteInvalidated) {
			t.Fatalf("expected invalidated close after delete, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("fill close did not resume after delete finished")
	}
}

func TestDeleteWaitsForConcurrentPublisherCleanupBeforeReturning(t *testing.T) {
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	coord.writeMu.Lock()
	publishSink := newBlockingCloseSink()
	cleanupDone := make(chan struct{})
	topWriter := &coordinatedTopWriteSink{
		WriteSink:  publishSink,
		coord:      coord,
		key:        "key",
		generation: 0,
		onInvalidated: func() error {
			close(cleanupDone)
			return nil
		},
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- topWriter.Close()
	}()

	select {
	case <-publishSink.closeStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("writer close did not enter underlying publish")
	}

	deleteDone := make(chan struct{})
	go func() {
		if err := coord.beginDelete(context.Background()); err != nil {
			t.Errorf("begin delete: %v", err)
			return
		}
		coord.finishDelete(true)
		close(deleteDone)
	}()

	select {
	case <-deleteDone:
		t.Fatal("delete returned while a concurrent publisher was still visible")
	case <-time.After(50 * time.Millisecond):
	}

	close(publishSink.releaseClose)

	select {
	case <-cleanupDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("invalidated publisher did not run cleanup")
	}
	select {
	case <-deleteDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("delete did not return after publisher cleanup completed")
	}
	select {
	case err := <-closeDone:
		if !errors.Is(err, ErrTopWriteInvalidated) {
			t.Fatalf("expected invalidated close, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("writer close did not return")
	}
}

func TestDeleteWaitsForConditionalPublisherCleanupBeforeReturning(t *testing.T) {
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	publishSink := newBlockingCloseSink()
	cleanupDone := make(chan struct{})
	sink := newConditionalGenerationWriteSink(publishSink, coord, 0, func() error {
		close(cleanupDone)
		return nil
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- sink.Close()
	}()

	select {
	case <-publishSink.closeStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("conditional writer close did not enter underlying publish")
	}

	deleteDone := make(chan struct{})
	go func() {
		if err := coord.beginDelete(context.Background()); err != nil {
			t.Errorf("begin delete: %v", err)
			return
		}
		coord.finishDelete(true)
		close(deleteDone)
	}()

	select {
	case <-deleteDone:
		t.Fatal("delete returned while a concurrent conditional publisher was still visible")
	case <-time.After(50 * time.Millisecond):
	}

	close(publishSink.releaseClose)

	select {
	case <-cleanupDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("invalidated conditional publisher did not run cleanup")
	}
	select {
	case <-deleteDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("delete did not return after conditional publisher cleanup completed")
	}
	select {
	case err := <-closeDone:
		if !errors.Is(err, ErrTopWriteInvalidated) {
			t.Fatalf("expected invalidated close, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("conditional writer close did not return")
	}
}

func TestCurrentTopWriteGenerationDoesNotCreateCoordinatorForMissingKey(t *testing.T) {
	cache := &DaramjweeCache{}

	if got := cache.currentTopWriteGeneration("missing"); got != 0 {
		t.Fatalf("expected zero generation for missing key, got %d", got)
	}
	if _, ok := cache.topWrites.coords.Load("missing"); ok {
		t.Fatal("expected read-only generation lookup not to create coordinator")
	}
}

func TestSetStreamToTopStoreWithGenerationHonorsCanceledContextWhileDeleteInProgress(t *testing.T) {
	store := &failingBeginSetStore{}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	coord := cache.topWrites.coordinator("key")
	if err := coord.beginDelete(context.Background()); err != nil {
		t.Fatalf("begin delete: %v", err)
	}
	defer coord.finishDelete(false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := cache.setStreamToTopStoreWithGeneration(ctx, "key", &Metadata{CacheTag: "v1"}, nil)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("setStreamToTopStoreWithGeneration did not return after context cancellation")
	}
}

func TestFanoutWriteManagerSerializesSameDestinationKey(t *testing.T) {
	var manager fanoutWriteManager

	unlockFirst := manager.lock(1, "same-key")

	startedSecond := make(chan struct{})
	acquiredSecond := make(chan struct{})
	releaseSecond := make(chan struct{})
	doneSecond := make(chan struct{})
	go func() {
		close(startedSecond)
		unlockSecond := manager.lock(1, "same-key")
		close(acquiredSecond)
		<-releaseSecond
		unlockSecond()
		close(doneSecond)
	}()

	<-startedSecond

	select {
	case <-acquiredSecond:
		t.Fatal("second same-destination fanout acquired the lock before the first release")
	case <-time.After(50 * time.Millisecond):
	}

	unlockFirst()

	select {
	case <-acquiredSecond:
	case <-time.After(2 * time.Second):
		t.Fatal("second same-destination fanout did not acquire the lock after release")
	}

	close(releaseSecond)

	select {
	case <-doneSecond:
	case <-time.After(2 * time.Second):
		t.Fatal("second same-destination fanout did not finish")
	}
}

func TestFanoutWriteManagerReleasesIdleLocks(t *testing.T) {
	var manager fanoutWriteManager

	unlock := manager.lock(2, "cleanup-key")
	lockKey := fanoutLockKey{destTierIndex: 2, key: "cleanup-key"}
	if _, ok := manager.locks.Load(lockKey); !ok {
		t.Fatal("fanout lock was not registered")
	}

	unlock()

	if _, ok := manager.locks.Load(lockKey); ok {
		t.Fatal("idle fanout lock was not released")
	}
}

func TestFanoutWriteManagerReleasesIdleLocksAfterConcurrentUse(t *testing.T) {
	var manager fanoutWriteManager
	const goroutines = 8

	var wg sync.WaitGroup
	var start sync.WaitGroup
	var concurrent int32
	var maxConcurrent int32
	wg.Add(goroutines)
	start.Add(1)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			start.Wait()
			unlock := manager.lock(3, "concurrent-cleanup-key")
			current := atomic.AddInt32(&concurrent, 1)
			for {
				recorded := atomic.LoadInt32(&maxConcurrent)
				if current <= recorded || atomic.CompareAndSwapInt32(&maxConcurrent, recorded, current) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
			unlock()
		}()
	}
	start.Done()
	wg.Wait()

	if got := atomic.LoadInt32(&maxConcurrent); got != 1 {
		t.Fatalf("fanout lock allowed %d concurrent critical sections", got)
	}

	lockKey := fanoutLockKey{destTierIndex: 3, key: "concurrent-cleanup-key"}
	if _, ok := manager.locks.Load(lockKey); ok {
		t.Fatal("fanout lock leaked after concurrent use")
	}
}

func TestFanoutWriteManagerOrdersStaleCleanupBeforeNewerWrite(t *testing.T) {
	var manager fanoutWriteManager
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	coord.committedGeneration = 1

	firstCloseStarted := make(chan struct{})
	releaseFirstClose := make(chan struct{})
	firstCleanupDone := make(chan struct{})
	secondWriteDone := make(chan struct{})
	secondStarted := make(chan struct{})

	firstSinkDone := make(chan error, 1)
	go func() {
		unlock := manager.lock(1, "key")
		defer unlock()

		sink := newConditionalGenerationWriteSink(&testWriteSink{
			closeFn: func() error {
				close(firstCloseStarted)
				<-releaseFirstClose
				return nil
			},
		}, coord, 1, func() error {
			close(firstCleanupDone)
			return nil
		})
		firstSinkDone <- sink.Close()
	}()

	<-firstCloseStarted

	coord.stateMu.Lock()
	coord.committedGeneration = 2
	coord.stateMu.Unlock()

	secondSinkDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		unlock := manager.lock(1, "key")
		defer unlock()

		sink := newConditionalGenerationWriteSink(&testWriteSink{
			closeFn: func() error {
				close(secondWriteDone)
				return nil
			},
		}, coord, 2, nil)
		secondSinkDone <- sink.Close()
	}()

	<-secondStarted

	select {
	case <-secondWriteDone:
		t.Fatal("newer fanout write completed before stale cleanup finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirstClose)

	select {
	case <-firstCleanupDone:
	case <-time.After(2 * time.Second):
		t.Fatal("stale fanout cleanup did not run")
	}

	if err := <-firstSinkDone; !errors.Is(err, ErrTopWriteInvalidated) {
		t.Fatalf("expected stale fanout to be invalidated, got %v", err)
	}

	select {
	case <-secondWriteDone:
	case <-time.After(2 * time.Second):
		t.Fatal("newer fanout write did not complete")
	}

	if err := <-secondSinkDone; err != nil {
		t.Fatalf("expected newer fanout write to succeed, got %v", err)
	}
}

func TestCoordinatedTopWriteSinkInvalidatedAfterPublishRunsCleanupWithoutStateMu(t *testing.T) {
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	coord.committedGeneration = 1
	coord.writeMu.Lock()

	sink := &coordinatedTopWriteSink{
		WriteSink: &testWriteSink{
			closeFn: func() error {
				coord.stateMu.Lock()
				coord.committedGeneration = 2
				coord.stateChanged.Broadcast()
				coord.stateMu.Unlock()
				return nil
			},
		},
		coord:      coord,
		generation: 1,
		onInvalidated: func() error {
			if got := coord.current(); got != 2 {
				t.Errorf("expected cleanup to observe generation 2, got %d", got)
			}
			return nil
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- sink.Close()
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrTopWriteInvalidated) {
			t.Fatalf("expected invalidated error, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cleanup blocked while stateMu was held")
	}
}

func TestConditionalGenerationWriteSinkInvalidatedAfterPublishRunsCleanupWithoutStateMu(t *testing.T) {
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	coord.committedGeneration = 1

	sink := newConditionalGenerationWriteSink(&testWriteSink{
		closeFn: func() error {
			coord.stateMu.Lock()
			coord.committedGeneration = 2
			coord.stateChanged.Broadcast()
			coord.stateMu.Unlock()
			return nil
		},
	}, coord, 1, func() error {
		if got := coord.current(); got != 2 {
			t.Errorf("expected cleanup to observe generation 2, got %d", got)
		}
		return nil
	})

	done := make(chan error, 1)
	go func() {
		done <- sink.Close()
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrTopWriteInvalidated) {
			t.Fatalf("expected invalidated error, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cleanup blocked while stateMu was held")
	}
}

type destructiveReservationStore struct {
	beginSetCalls int
	data          []byte
	meta          Metadata
}

func (s *destructiveReservationStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	meta := s.meta
	return io.NopCloser(bytes.NewReader(bytes.Clone(s.data))), &meta, nil
}

func (s *destructiveReservationStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	s.beginSetCalls++
	s.data = nil
	return &stubWriteSink{}, nil
}

func (s *destructiveReservationStore) Delete(ctx context.Context, key string) error {
	s.data = nil
	s.meta = Metadata{}
	return nil
}

func (s *destructiveReservationStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	meta := s.meta
	return &meta, nil
}

type failingBeginSetStore struct {
	err error
}

func (s *failingBeginSetStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *failingBeginSetStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return nil, s.err
}

func (s *failingBeginSetStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *failingBeginSetStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type stubWriteSink struct{}

func (s *stubWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *stubWriteSink) Close() error                { return nil }
func (s *stubWriteSink) Abort() error                { return nil }

type blockingCloseSink struct {
	closeStarted chan struct{}
	releaseClose chan struct{}
	once         sync.Once
}

func newBlockingCloseSink() *blockingCloseSink {
	return &blockingCloseSink{
		closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
}

func (s *blockingCloseSink) Write(p []byte) (int, error) { return len(p), nil }

func (s *blockingCloseSink) Close() error {
	s.once.Do(func() {
		close(s.closeStarted)
	})
	<-s.releaseClose
	return nil
}

func (s *blockingCloseSink) Abort() error { return nil }

type testWriteSink struct {
	closeFn func() error
	abortFn func() error
}

func (s *testWriteSink) Write(p []byte) (int, error) { return len(p), nil }

func (s *testWriteSink) Close() error {
	if s.closeFn == nil {
		return nil
	}
	return s.closeFn()
}

func (s *testWriteSink) Abort() error {
	if s.abortFn == nil {
		return nil
	}
	return s.abortFn()
}
