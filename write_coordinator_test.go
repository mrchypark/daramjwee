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

func TestSetStreamToTopStoreDoesNotExposeStagedCommitBypass(t *testing.T) {
	store := &stubStagingStore{}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	writer, err := cache.setStreamToTopStoreWithGeneration(context.Background(), "key", &Metadata{CacheTag: "v1"}, nil)
	if err != nil {
		t.Fatalf("expected staged writer, got %v", err)
	}
	defer writer.Abort()

	if _, ok := writer.(StagedWriteSink); ok {
		t.Fatal("top-write coordinator exposed StagedWriteSink and allowed direct Commit bypass")
	}
}

func TestLaterStagedBeginFailureDoesNotInvalidateOlderWriter(t *testing.T) {
	beginErr := errors.New("begin staged failed")
	store := &blockingSecondBeginStagingStore{
		secondBeginStarted: make(chan struct{}),
		releaseSecondBegin: make(chan struct{}),
		secondBeginErr:     beginErr,
	}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	older, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "older"})
	if err != nil {
		t.Fatalf("older Set failed: %v", err)
	}
	defer older.Abort()

	secondDone := make(chan error, 1)
	go func() {
		second, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "newer"})
		if second != nil {
			_ = second.Abort()
		}
		secondDone <- err
	}()

	select {
	case <-store.secondBeginStarted:
	case <-time.After(time.Second):
		t.Fatal("second staged begin did not start")
	}

	olderDone := make(chan error, 1)
	go func() {
		olderDone <- older.Close()
	}()

	select {
	case err := <-olderDone:
		if err != nil {
			t.Fatalf("older Close should publish while later BeginStagedSet is still pending, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("older Close blocked behind later BeginStagedSet")
	}

	close(store.releaseSecondBegin)
	if err := <-secondDone; !errors.Is(err, beginErr) {
		t.Fatalf("expected second BeginStagedSet error, got %v", err)
	}
}

func TestLaterStagedAbortCleanupDoesNotInvalidateOlderWriter(t *testing.T) {
	store := &blockingSecondAbortStagingStore{
		abortStarted: make(chan struct{}),
		releaseAbort: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	older, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "older"})
	if err != nil {
		t.Fatalf("older Set failed: %v", err)
	}
	defer older.Abort()

	newer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "newer"})
	if err != nil {
		t.Fatalf("newer Set failed: %v", err)
	}

	abortDone := make(chan error, 1)
	go func() {
		abortDone <- newer.Abort()
	}()

	select {
	case <-store.abortStarted:
	case <-time.After(time.Second):
		t.Fatal("newer Abort did not reach store cleanup")
	}

	olderDone := make(chan error, 1)
	go func() {
		olderDone <- older.Close()
	}()

	select {
	case err := <-olderDone:
		if err != nil {
			t.Fatalf("older Close should not be invalidated by aborting later writer, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("older Close blocked behind later abort cleanup")
	}

	close(store.releaseAbort)
	if err := <-abortDone; err != nil {
		t.Fatalf("newer Abort failed: %v", err)
	}
}

func TestStaleStagedCloseAbortCleanupDoesNotBlockNewerCommit(t *testing.T) {
	store := &blockingFirstAbortStagingStore{
		abortStarted: make(chan struct{}),
		releaseAbort: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	older, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "older"})
	if err != nil {
		t.Fatalf("older Set failed: %v", err)
	}
	defer older.Abort()

	newer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "newer"})
	if err != nil {
		t.Fatalf("newer Set failed: %v", err)
	}
	if err := newer.Close(); err != nil {
		t.Fatalf("newer Close failed: %v", err)
	}

	olderDone := make(chan error, 1)
	go func() {
		olderDone <- older.Close()
	}()

	select {
	case <-store.abortStarted:
	case <-time.After(time.Second):
		t.Fatal("stale older Close did not reach store abort cleanup")
	}

	third, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "third"})
	if err != nil {
		t.Fatalf("third Set failed: %v", err)
	}

	thirdDone := make(chan error, 1)
	go func() {
		thirdDone <- third.Close()
	}()

	select {
	case err := <-thirdDone:
		if err != nil {
			t.Fatalf("third Close should not be blocked by stale abort cleanup, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("third Close blocked behind stale abort cleanup")
	}

	close(store.releaseAbort)
	if err := <-olderDone; !errors.Is(err, ErrTopWriteInvalidated) {
		t.Fatalf("expected older Close to be invalidated, got %v", err)
	}
}

func TestStagedCloseWaitingForDeleteTimesOutAndReleasesReservation(t *testing.T) {
	store := &stubStagingStore{}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: 25 * time.Millisecond,
	}

	writer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v1"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	defer writer.Abort()

	coord := cache.topWrites.coordinator("key")
	if err := coord.beginDelete(context.Background()); err != nil {
		t.Fatalf("beginDelete failed: %v", err)
	}

	closeErr := writer.Close()
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("expected close wait timeout, got %v", closeErr)
	}

	coord.finishDelete(false)
	next, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v2"})
	if err != nil {
		t.Fatalf("next Set should not be poisoned by timed-out close, got %v", err)
	}
	if err := next.Close(); err != nil {
		t.Fatalf("next Close should succeed, got %v", err)
	}
}

func TestStagedCloseWaitingForCommitLeaseTimesOutAndReleasesReservation(t *testing.T) {
	store := &blockingFirstCommitStagingStore{
		commitStarted: make(chan struct{}),
		releaseCommit: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: 25 * time.Millisecond,
	}

	first, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v1"})
	if err != nil {
		t.Fatalf("first Set failed: %v", err)
	}
	second, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v2"})
	if err != nil {
		t.Fatalf("second Set failed: %v", err)
	}
	defer second.Abort()

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.Close()
	}()

	select {
	case <-store.commitStarted:
	case <-time.After(time.Second):
		t.Fatal("first Commit did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- second.Close()
	}()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected second Close to time out waiting for commit lease, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(store.releaseCommit)
		<-firstDone
		t.Fatal("second Close blocked past close timeout while commit lease was held")
	}

	close(store.releaseCommit)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Close should finish after release, got %v", err)
	}

	third, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v3"})
	if err != nil {
		t.Fatalf("third Set should not be poisoned by timed-out second close, got %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("third Close should succeed, got %v", err)
	}
}

func TestCommittedStagedWritePrunesOlderAbandonedReservations(t *testing.T) {
	store := &stubStagingStore{}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	abandoned, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v1"})
	if err != nil {
		t.Fatalf("abandoned Set failed: %v", err)
	}
	defer abandoned.Abort()

	newer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v2"})
	if err != nil {
		t.Fatalf("newer Set failed: %v", err)
	}
	if err := newer.Close(); err != nil {
		t.Fatalf("newer Close failed: %v", err)
	}

	coord := cache.topWrites.coordinator("key")
	coord.stateMu.Lock()
	defer coord.stateMu.Unlock()
	if _, ok := coord.activeReservations[1]; ok {
		t.Fatal("older abandoned reservation remained after a newer generation committed")
	}
}

func TestStagedCloseAbortsUnderlyingSinkAfterCommitFailure(t *testing.T) {
	commitErr := errors.New("commit failed")
	store := &failingCommitStagingStore{commitErr: commitErr}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	writer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v1"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	closeErr := writer.Close()
	if !errors.Is(closeErr, commitErr) {
		t.Fatalf("expected commit failure, got %v", closeErr)
	}
	if !store.sink.aborted {
		t.Fatal("expected failed staged commit to abort underlying sink")
	}
}

func TestFailedStagedCommitAbortCleanupDoesNotHoldCommitLease(t *testing.T) {
	commitErr := errors.New("commit failed")
	store := &blockingAbortAfterCommitFailureStagingStore{
		commitErr:    commitErr,
		abortStarted: make(chan struct{}),
		releaseAbort: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: 25 * time.Millisecond,
	}

	first, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v1"})
	if err != nil {
		t.Fatalf("first Set failed: %v", err)
	}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.Close()
	}()

	select {
	case <-store.abortStarted:
	case <-time.After(time.Second):
		t.Fatal("failed commit did not start abort cleanup")
	}

	second, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "v2"})
	if err != nil {
		close(store.releaseAbort)
		<-firstDone
		t.Fatalf("second Set failed: %v", err)
	}
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- second.Close()
	}()

	select {
	case err := <-secondDone:
		if err != nil {
			close(store.releaseAbort)
			<-firstDone
			t.Fatalf("second Close should not wait for failed commit abort cleanup, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(store.releaseAbort)
		<-firstDone
		t.Fatal("second Close blocked behind failed commit abort cleanup")
	}

	close(store.releaseAbort)
	if err := <-firstDone; !errors.Is(err, commitErr) {
		t.Fatalf("expected first Close commit failure, got %v", err)
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
		t.Fatalf("beginDelete failed: %v", err)
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

func TestSetWithAbandonedTopWriteSinkReturnsWhenContextExpires(t *testing.T) {
	store := &countingBeginSetStore{}
	cache := &DaramjweeCache{
		tiers:        []Store{store},
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}

	first, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "first"})
	if err != nil {
		t.Fatalf("first Set failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		second, err := cache.Set(ctx, "key", &Metadata{CacheTag: "second"})
		if second != nil {
			_ = second.Abort()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation from second Set, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		_ = first.Abort()
		err := <-done
		t.Fatalf("second Set blocked past its context deadline; eventually returned %v", err)
	}

	if err := first.Abort(); err != nil {
		t.Fatalf("first Abort failed: %v", err)
	}
}

func TestInvalidatedCleanupDoesNotHoldStateMu(t *testing.T) {
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	coord.committedGeneration = 1

	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})

	sink := newConditionalGenerationWriteSink(&testWriteSink{
		closeFn: func() error {
			close(closeStarted)
			<-releaseClose
			return nil
		},
	}, coord, 1, func() error {
		close(cleanupStarted)
		<-releaseCleanup
		return nil
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- sink.Close()
	}()

	<-closeStarted
	coord.stateMu.Lock()
	coord.committedGeneration = 2
	coord.stateMu.Unlock()
	close(releaseClose)

	select {
	case <-cleanupStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("invalidated cleanup did not start")
	}

	stateMuAcquired := make(chan struct{})
	go func() {
		coord.stateMu.Lock()
		close(stateMuAcquired)
		coord.stateMu.Unlock()
	}()

	select {
	case <-stateMuAcquired:
	case <-time.After(100 * time.Millisecond):
		close(releaseCleanup)
		if err := <-closeDone; !errors.Is(err, ErrTopWriteInvalidated) {
			t.Fatalf("expected invalidated close after blocked stateMu acquisition, got %v", err)
		}
		t.Fatal("cleanup held coordinator stateMu while it was blocked")
	}

	close(releaseCleanup)
	if err := <-closeDone; !errors.Is(err, ErrTopWriteInvalidated) {
		t.Fatalf("expected invalidated close, got %v", err)
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

type countingBeginSetStore struct {
	mu    sync.Mutex
	calls int
}

func (s *countingBeginSetStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *countingBeginSetStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return &stubWriteSink{}, nil
}

func (s *countingBeginSetStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *countingBeginSetStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type stubWriteSink struct{}

func (s *stubWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *stubWriteSink) Close() error                { return nil }
func (s *stubWriteSink) Abort() error                { return nil }

type stubStagingStore struct{}

func (s *stubStagingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *stubStagingStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &stubWriteSink{}, nil
}

func (s *stubStagingStore) BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error) {
	return &stubStagedWriteSink{}, nil
}

func (s *stubStagingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *stubStagingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type stubStagedWriteSink struct{}

func (s *stubStagedWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *stubStagedWriteSink) Commit(ctx context.Context) error {
	return nil
}
func (s *stubStagedWriteSink) Abort() error { return nil }

type blockingSecondBeginStagingStore struct {
	mu                 sync.Mutex
	calls              int
	secondBeginStarted chan struct{}
	releaseSecondBegin chan struct{}
	secondBeginErr     error
}

func (s *blockingSecondBeginStagingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *blockingSecondBeginStagingStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &stubWriteSink{}, nil
}

func (s *blockingSecondBeginStagingStore) BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 2 {
		close(s.secondBeginStarted)
		<-s.releaseSecondBegin
		return nil, s.secondBeginErr
	}
	return &stubStagedWriteSink{}, nil
}

func (s *blockingSecondBeginStagingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingSecondBeginStagingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type blockingSecondAbortStagingStore struct {
	mu           sync.Mutex
	calls        int
	abortStarted chan struct{}
	releaseAbort chan struct{}
}

func (s *blockingSecondAbortStagingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *blockingSecondAbortStagingStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &stubWriteSink{}, nil
}

func (s *blockingSecondAbortStagingStore) BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 2 {
		return &blockingAbortStagedWriteSink{
			abortStarted: s.abortStarted,
			releaseAbort: s.releaseAbort,
		}, nil
	}
	return &stubStagedWriteSink{}, nil
}

func (s *blockingSecondAbortStagingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingSecondAbortStagingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type blockingFirstAbortStagingStore struct {
	mu           sync.Mutex
	calls        int
	abortStarted chan struct{}
	releaseAbort chan struct{}
}

func (s *blockingFirstAbortStagingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *blockingFirstAbortStagingStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &stubWriteSink{}, nil
}

func (s *blockingFirstAbortStagingStore) BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		return &blockingAbortStagedWriteSink{
			abortStarted: s.abortStarted,
			releaseAbort: s.releaseAbort,
		}, nil
	}
	return &stubStagedWriteSink{}, nil
}

func (s *blockingFirstAbortStagingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingFirstAbortStagingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type blockingAbortStagedWriteSink struct {
	abortStarted chan struct{}
	releaseAbort chan struct{}
}

func (s *blockingAbortStagedWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *blockingAbortStagedWriteSink) Commit(ctx context.Context) error {
	return nil
}
func (s *blockingAbortStagedWriteSink) Abort() error {
	close(s.abortStarted)
	<-s.releaseAbort
	return nil
}

type blockingFirstCommitStagingStore struct {
	mu            sync.Mutex
	calls         int
	commitStarted chan struct{}
	releaseCommit chan struct{}
}

func (s *blockingFirstCommitStagingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *blockingFirstCommitStagingStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &stubWriteSink{}, nil
}

func (s *blockingFirstCommitStagingStore) BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		return &blockingCommitStagedWriteSink{
			commitStarted: s.commitStarted,
			releaseCommit: s.releaseCommit,
		}, nil
	}
	return &stubStagedWriteSink{}, nil
}

func (s *blockingFirstCommitStagingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingFirstCommitStagingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type blockingCommitStagedWriteSink struct {
	commitStarted chan struct{}
	releaseCommit chan struct{}
}

func (s *blockingCommitStagedWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *blockingCommitStagedWriteSink) Commit(ctx context.Context) error {
	close(s.commitStarted)
	<-s.releaseCommit
	return nil
}
func (s *blockingCommitStagedWriteSink) Abort() error { return nil }

type failingCommitStagingStore struct {
	commitErr error
	sink      *failingCommitStagedWriteSink
}

func (s *failingCommitStagingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *failingCommitStagingStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &stubWriteSink{}, nil
}

func (s *failingCommitStagingStore) BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error) {
	s.sink = &failingCommitStagedWriteSink{commitErr: s.commitErr}
	return s.sink, nil
}

func (s *failingCommitStagingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *failingCommitStagingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type failingCommitStagedWriteSink struct {
	commitErr error
	aborted   bool
}

func (s *failingCommitStagedWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *failingCommitStagedWriteSink) Commit(ctx context.Context) error {
	return s.commitErr
}
func (s *failingCommitStagedWriteSink) Abort() error {
	s.aborted = true
	return nil
}

type blockingAbortAfterCommitFailureStagingStore struct {
	mu           sync.Mutex
	calls        int
	commitErr    error
	abortStarted chan struct{}
	releaseAbort chan struct{}
}

func (s *blockingAbortAfterCommitFailureStagingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *blockingAbortAfterCommitFailureStagingStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &stubWriteSink{}, nil
}

func (s *blockingAbortAfterCommitFailureStagingStore) BeginStagedSet(ctx context.Context, key string, metadata *Metadata) (StagedWriteSink, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		return &blockingAbortAfterCommitFailureSink{
			commitErr:    s.commitErr,
			abortStarted: s.abortStarted,
			releaseAbort: s.releaseAbort,
		}, nil
	}
	return &stubStagedWriteSink{}, nil
}

func (s *blockingAbortAfterCommitFailureStagingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingAbortAfterCommitFailureStagingStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type blockingAbortAfterCommitFailureSink struct {
	commitErr    error
	abortStarted chan struct{}
	releaseAbort chan struct{}
}

func (s *blockingAbortAfterCommitFailureSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *blockingAbortAfterCommitFailureSink) Commit(ctx context.Context) error {
	return s.commitErr
}
func (s *blockingAbortAfterCommitFailureSink) Abort() error {
	close(s.abortStarted)
	<-s.releaseAbort
	return nil
}

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
