package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTopWriteManagerRetiresCompletedHighCardinalityWrites(t *testing.T) {
	store := &stubStagingStore{}
	cache := &DaramjweeCache{
		tiers:  []Store{store},
		config: cacheConfig{opTimeout: time.Second, closeTimeout: time.Second},
	}

	const keyCount = 128
	for i := range keyCount {
		writer, err := cache.Set(context.Background(), fmt.Sprintf("key-%d", i), &Metadata{})
		if err != nil {
			t.Fatalf("Set key %d: %v", i, err)
		}
		if i%2 == 0 {
			err = writer.Close()
		} else {
			err = writer.Abort()
		}
		if err != nil {
			t.Fatalf("finish key %d: %v", i, err)
		}
	}

	retained := 0
	cache.topWrites.coords.Range(func(_, _ any) bool {
		retained++
		return true
	})
	if retained != 0 {
		t.Fatalf("expected completed coordinators to retire, retained %d", retained)
	}
}

func TestTopWriteManagerConcurrentAcquiresShareCoordinator(t *testing.T) {
	manager := &topWriteManager{}
	const callers = 64

	start := make(chan struct{})
	release := make(chan struct{})
	results := make(chan *writeCoordinator, callers)
	var workers sync.WaitGroup
	workers.Add(callers)
	for range callers {
		go func() {
			defer workers.Done()
			<-start
			coord := manager.coordinator("key")
			results <- coord
			<-release
			coord.releaseReference()
		}()
	}

	close(start)
	first := <-results
	for range callers - 1 {
		if got := <-results; got != first {
			t.Fatalf("concurrent callers split across coordinators: first=%p got=%p", first, got)
		}
	}
	close(release)
	workers.Wait()
	if _, ok := manager.coords.Load("key"); ok {
		t.Fatal("expected shared coordinator to retire after the last caller")
	}
}

func TestTopWriteManagerRejectsReleasedObservationAfterReplacement(t *testing.T) {
	manager := &topWriteManager{}
	stale := manager.currentGeneration("key")
	retired := stale.coord
	stale.release()

	fresh := manager.coordinator("key")
	defer fresh.releaseReference()
	if fresh == retired {
		t.Fatal("expected a fresh coordinator after retirement")
	}

	if coord, _, err := manager.coordinatorForWrite("key", stale); !errors.Is(err, ErrTopWriteInvalidated) || coord != nil {
		t.Fatalf("expected released observation to be rejected, coord=%p err=%v", coord, err)
	}
	if current, ok := manager.coords.Load("key"); !ok || current != fresh {
		t.Fatal("stale observation changed or removed the fresh coordinator")
	}
}

func TestTopWriteManagerUnrelatedKeyDoesNotInvalidateObservation(t *testing.T) {
	cache := &DaramjweeCache{}
	observed := cache.currentTopWriteGeneration("key-a")
	defer observed.release()

	cache.noteTopWriteGeneration("key-b")
	if !cache.canAttemptExpectedTopWrite("key-a", observed) {
		t.Fatal("unrelated key write invalidated key-a observation")
	}
	if got := observed.coord.current(); got != observed.generation {
		t.Fatalf("unrelated key changed visible generation: got %d want %d", got, observed.generation)
	}
}

func TestCloseCorePanicReleasesCoordinatorReferenceAndWriteLease(t *testing.T) {
	tests := []struct {
		name           string
		conditional    bool
		panicFromAbort bool
	}{
		{name: "non-staging commit"},
		{name: "conditional commit", conditional: true},
		{name: "abort", panicFromAbort: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &topWriteManager{}
			coord := manager.coordinator("key")
			var generation uint64
			var err error
			if tt.conditional {
				generation, err = coord.reserve(context.Background(), nil)
			} else {
				generation, err = coord.begin(context.Background(), nil)
			}
			require.NoError(t, err)

			if tt.panicFromAbort {
				coord.stateMu.Lock()
				coord.committedGeneration.Store(generation + 1)
				coord.stateMu.Unlock()
			}

			var recovered any
			var releaseLease func()
			if !tt.conditional {
				releaseLease = coord.releaseWrite
			}
			func() {
				defer func() { recovered = recover() }()
				_ = closeCore(context.Background(), closeCoreParams{
					generation: generation,
					coord:      coord,
				}, func(context.Context) error {
					panic("commit panic")
				}, func() error {
					if tt.panicFromAbort {
						panic("abort panic")
					}
					return nil
				}, releaseLease)
			}()
			require.NotNil(t, recovered)
			if tt.panicFromAbort {
				require.Equal(t, "abort panic", recovered)
			} else {
				require.Equal(t, "commit panic", recovered)
			}
			if _, ok := manager.coords.Load("key"); ok {
				t.Fatal("expected coordinator reference to be released after panic")
			}

			if !tt.conditional {
				leaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, coord.acquireWrite(leaseCtx))
				coord.releaseWrite()
			}
		})
	}
}

func TestStagedClosePreservesCommitPanicWhenAbortPanics(t *testing.T) {
	manager := &topWriteManager{}
	coord := manager.coordinator("key")
	generation, err := coord.reserve(context.Background(), nil)
	require.NoError(t, err)

	commitPanic := errors.New("commit panic")
	abortPanic := errors.New("abort panic")
	underlying := &panicStagedWriteSink{
		commitPanic: commitPanic,
		abortPanic:  abortPanic,
	}
	sink := &coordinatedStagedTopWriteSink{
		sink:       underlying,
		coord:      coord,
		generation: generation,
	}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = sink.Close()
	}()
	require.Same(t, commitPanic, recovered)
	require.Equal(t, 1, underlying.abortCalls)
	if _, ok := manager.coords.Load("key"); ok {
		t.Fatal("expected coordinator reference to be released after panic")
	}
	coord.stateMu.Lock()
	_, reserved := coord.activeReservations[generation]
	coord.stateMu.Unlock()
	require.False(t, reserved)

	leaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, coord.acquireCommit(leaseCtx))
	coord.releaseCommit()
}

func TestSetStreamToStoreWithTopGenerationRejectsStaleWriterBeforeBeginSet(t *testing.T) {
	store := &destructiveReservationStore{
		data: []byte("live-body"),
		meta: Metadata{CacheTag: "live"},
	}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
	}
	expectedGeneration := cache.currentTopWriteGeneration("key")
	defer expectedGeneration.release()
	cache.noteTopWriteGeneration("key")

	writer, err := cache.setStreamToTopStoreWithGeneration(context.Background(), "key", &Metadata{CacheTag: "stale"}, expectedGeneration)
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
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
	}

	expectedGeneration := cache.currentTopWriteGeneration("key")
	defer expectedGeneration.release()
	writer, err := cache.setStreamToTopStoreWithGeneration(context.Background(), "key", &Metadata{CacheTag: "v1"}, expectedGeneration)
	if writer != nil {
		t.Fatalf("expected no writer on BeginSet failure, got %T", writer)
	}
	if !errors.Is(err, store.err) {
		t.Fatalf("expected BeginSet error, got %v", err)
	}
	current := cache.currentTopWriteGeneration("key")
	defer current.release()
	if got := current.generation; got != 0 {
		t.Fatalf("expected generation to be restored after BeginSet failure, got %d", got)
	}
}

func TestRolledBackReservationDoesNotInvalidateConditionalWrite(t *testing.T) {
	coord := &writeCoordinator{}
	generation, err := coord.reserve(context.Background(), nil)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	coord.unregisterReservation(generation)

	expected := uint64(0)
	next, err := coord.reserve(context.Background(), &expected)
	if err != nil {
		t.Fatalf("rolled-back unpublished generation should not invalidate conditional reserve: %v", err)
	}
	if next <= generation {
		t.Fatalf("next generation should keep monotonic assignment, got %d after %d", next, generation)
	}
	coord.unregisterReservation(next)
}

func TestSetStreamToTopStoreDoesNotExposeStagedCommitBypass(t *testing.T) {
	store := &stubStagingStore{}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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

func TestStagedFillPreemptDoesNotWaitForAbortCleanup(t *testing.T) {
	store := &blockingFirstAbortStagingStore{
		abortStarted: make(chan struct{}),
		releaseAbort: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
	}

	observed := cache.currentTopWriteGeneration("key")
	defer observed.release()
	fill, err := cache.setStreamToTopStoreForFill(context.Background(), "key", &Metadata{CacheTag: "fill"}, observed)
	if err != nil {
		t.Fatalf("fill writer failed: %v", err)
	}
	if _, err := fill.Write([]byte("partial")); err != nil {
		t.Fatalf("fill write failed: %v", err)
	}

	setDone := make(chan error, 1)
	go func() {
		writer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "user"})
		if err == nil {
			err = writer.Abort()
		}
		setDone <- err
	}()

	select {
	case <-store.abortStarted:
	case <-time.After(time.Second):
		t.Fatal("preempted staged fill abort cleanup did not start")
	}

	select {
	case err := <-setDone:
		if err != nil {
			t.Fatalf("same-key Set should not wait for staged fill abort cleanup, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(store.releaseAbort)
		t.Fatal("same-key Set waited for staged fill abort cleanup")
	}

	close(store.releaseAbort)
}

func TestStagedFillDeletePreemptDoesNotWaitForAbortCleanup(t *testing.T) {
	store := &blockingFirstAbortStagingStore{
		abortStarted: make(chan struct{}),
		releaseAbort: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
	}

	observed := cache.currentTopWriteGeneration("key")
	defer observed.release()
	fill, err := cache.setStreamToTopStoreForFill(context.Background(), "key", &Metadata{CacheTag: "fill"}, observed)
	if err != nil {
		t.Fatalf("fill writer failed: %v", err)
	}
	if _, err := fill.Write([]byte("partial")); err != nil {
		t.Fatalf("fill write failed: %v", err)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- cache.Delete(context.Background(), "key")
	}()

	select {
	case <-store.abortStarted:
	case <-time.After(time.Second):
		t.Fatal("preempted staged fill abort cleanup did not start")
	}

	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("Delete should not wait for staged fill abort cleanup, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(store.releaseAbort)
		t.Fatal("Delete waited for staged fill abort cleanup")
	}

	close(store.releaseAbort)
}

func TestLegacyFillContextCancelPreemptsPendingBeginSet(t *testing.T) {
	store := &blockingFirstBeginSetStore{
		beginStarted: make(chan struct{}),
		releaseBegin: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:        time.Second,
			closeTimeout:     time.Second,
			fillLeaseTimeout: time.Hour,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	observed := cache.currentTopWriteGeneration("key")
	defer observed.release()
	fillDone := make(chan error, 1)
	go func() {
		writer, err := cache.setStreamToTopStoreForFill(ctx, "key", &Metadata{CacheTag: "fill"}, observed)
		if writer != nil {
			_ = writer.Abort()
		}
		fillDone <- err
	}()

	select {
	case <-store.beginStarted:
	case <-time.After(time.Second):
		t.Fatal("fill BeginSet did not start")
	}

	cancel()
	select {
	case err := <-fillDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled fill setup, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled fill setup did not return")
	}

	setDone := make(chan error, 1)
	go func() {
		writer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "user"})
		if err == nil {
			err = writer.Abort()
		}
		setDone <- err
	}()

	select {
	case err := <-setDone:
		t.Fatalf("same-key Set completed before pending BeginSet returned: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.releaseBegin)
	select {
	case err := <-setDone:
		if err != nil {
			t.Fatalf("same-key Set should proceed after canceled fill BeginSet returns, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("same-key Set stayed blocked until fill lease expiry")
	}
}

func TestLegacyFillPreemptCancelsPendingBeginSet(t *testing.T) {
	store := &contextBlockingFirstBeginSetStore{
		beginStarted: make(chan struct{}),
		releaseBegin: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:        time.Second,
			closeTimeout:     time.Second,
			fillLeaseTimeout: time.Hour,
		},
	}

	observed := cache.currentTopWriteGeneration("key")
	defer observed.release()
	fillDone := make(chan error, 1)
	go func() {
		writer, err := cache.setStreamToTopStoreForFill(context.Background(), "key", &Metadata{CacheTag: "fill"}, observed)
		if writer != nil {
			_ = writer.Abort()
		}
		fillDone <- err
	}()

	select {
	case <-store.beginStarted:
	case <-time.After(time.Second):
		t.Fatal("fill BeginSet did not start")
	}

	setDone := make(chan error, 1)
	go func() {
		writer, err := cache.Set(context.Background(), "key", &Metadata{CacheTag: "user"})
		if err == nil {
			err = writer.Abort()
		}
		setDone <- err
	}()

	select {
	case err := <-setDone:
		if err != nil {
			t.Fatalf("same-key Set should proceed after preempting pending BeginSet, got %v", err)
		}
	case <-time.After(time.Second):
		close(store.releaseBegin)
		t.Fatal("same-key Set stayed blocked behind preempted BeginSet")
	}

	select {
	case err := <-fillDone:
		if err != nil {
			t.Fatalf("preempted fill should return without setup error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("preempted fill setup did not return")
	}
}

func TestStaleStagedCloseAbortCleanupDoesNotBlockNewerCommit(t *testing.T) {
	store := &blockingFirstAbortStagingStore{
		abortStarted: make(chan struct{}),
		releaseAbort: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: 25 * time.Millisecond,
		},
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

func TestLegacyTopWriteCloseWaitingForDeleteTimesOut(t *testing.T) {
	store := &countingBeginSetStore{}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: 25 * time.Millisecond,
		},
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

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- writer.Close()
	}()

	select {
	case err := <-closeDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected close wait timeout, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		coord.finishDelete(false)
		t.Fatal("legacy top write Close blocked past close timeout while delete was active")
	}

	coord.finishDelete(false)
}

func TestLegacyTopWriteCloseDoesNotReturnTimeoutAfterSuccessfulCommit(t *testing.T) {
	coord := &writeCoordinator{}
	if err := coord.acquireWrite(context.Background()); err != nil {
		t.Fatalf("acquireWrite failed: %v", err)
	}

	deleteStarted := false
	sink := &coordinatedTopWriteSink{
		WriteSink: &testWriteSink{
			closeFn: func() error {
				if err := coord.beginDelete(context.Background()); err != nil {
					return err
				}
				deleteStarted = true
				return nil
			},
		},
		coord:       coord,
		generation:  1,
		waitTimeout: 25 * time.Millisecond,
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("successful legacy top write should not be reported as a timeout, got %v", err)
	}
	if !deleteStarted {
		t.Fatal("test did not start the racing delete")
	}
	coord.finishDelete(false)
}

func TestStagedCloseWaitingForCommitLeaseTimesOutAndReleasesReservation(t *testing.T) {
	store := &blockingFirstCommitStagingStore{
		commitStarted: make(chan struct{}),
		releaseCommit: make(chan struct{}),
	}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: 25 * time.Millisecond,
		},
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
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: 25 * time.Millisecond,
		},
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

	observed := cache.currentTopWriteGeneration("missing")
	if got := observed.generation; got != 0 {
		t.Fatalf("expected zero generation for missing key, got %d", got)
	}
	observed.release()
	if _, ok := cache.topWrites.coords.Load("missing"); ok {
		t.Fatal("expected completed generation observation not to retain a coordinator")
	}
}

func TestSetStreamToTopStoreWithGenerationHonorsCanceledContextWhileDeleteInProgress(t *testing.T) {
	store := &failingBeginSetStore{}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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

func TestConditionalGenerationWriteSinkWaitingForDeleteTimesOut(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()
	if err := coord.beginDelete(context.Background()); err != nil {
		t.Fatalf("beginDelete failed: %v", err)
	}
	defer coord.finishDelete(false)

	sink := newConditionalGenerationWriteSink(&testWriteSink{}, coord, 1, 25*time.Millisecond, nil)

	done := make(chan error, 1)
	go func() {
		done <- sink.Close()
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected conditional close wait timeout, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("conditional generation Close blocked past close timeout while delete was active")
	}
}

func TestConditionalGenerationWriteSinkDoesNotReturnTimeoutAfterSuccessfulClose(t *testing.T) {
	coord := &writeCoordinator{}

	deleteStarted := false
	sink := newConditionalGenerationWriteSink(&testWriteSink{
		closeFn: func() error {
			if err := coord.beginDelete(context.Background()); err != nil {
				return err
			}
			deleteStarted = true
			return nil
		},
	}, coord, 1, 25*time.Millisecond, nil)

	if err := sink.Close(); err != nil {
		t.Fatalf("successful conditional close should not be reported as a timeout, got %v", err)
	}
	if !deleteStarted {
		t.Fatal("test did not start the racing delete")
	}
	coord.finishDelete(false)
}

func TestSetWithAbandonedTopWriteSinkReturnsWhenContextExpires(t *testing.T) {
	store := &countingBeginSetStore{}
	cache := &DaramjweeCache{
		tiers: []Store{store},
		config: cacheConfig{
			opTimeout:    time.Second,
			closeTimeout: time.Second,
		},
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
	coord.committedGeneration.Store(1)

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
	}, coord, 1, time.Second, func() error {
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
	coord.committedGeneration.Store(2)
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
	coord.committedGeneration.Store(1)

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
		}, coord, 1, time.Second, func() error {
			close(firstCleanupDone)
			return nil
		})
		firstSinkDone <- sink.Close()
	}()

	<-firstCloseStarted

	coord.stateMu.Lock()
	coord.committedGeneration.Store(2)
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
		}, coord, 2, time.Second, nil)
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

func TestWaitForNoActiveDeletesUnblocksAfterDeleteCompletes(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	require.NoError(t, coord.beginDelete(context.Background()))

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- coord.waitForNoActiveDeletes(context.Background())
	}()

	select {
	case <-waitDone:
		t.Fatal("waitForNoActiveDeletes returned before delete finished")
	case <-time.After(50 * time.Millisecond):
	}

	coord.finishDelete(false)

	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("waitForNoActiveDeletes did not unblock after finishDelete")
	}
}

func TestWaitForNoActiveDeletesRespectsContextCancellation(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()
	require.NoError(t, coord.beginDelete(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	err := coord.waitForNoActiveDeletes(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	coord.finishDelete(false)
}

func TestWaitForNoActiveDeletesReturnsImmediatelyWhenNoDeletes(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	err := coord.waitForNoActiveDeletes(context.Background())
	require.NoError(t, err)
}

func TestWaitForNoActiveDeletesHandlesSequentialDeletes(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	// First delete: activeDeletes goes 0→1.
	require.NoError(t, coord.beginDelete(context.Background()))

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- coord.waitForNoActiveDeletes(context.Background())
	}()

	select {
	case <-waitDone:
		t.Fatal("waitForNoActiveDeletes returned with active delete in progress")
	case <-time.After(50 * time.Millisecond):
	}

	// Finish first delete: activeDeletes goes 1→0, unblocks waiter.
	coord.finishDelete(false)

	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("waitForNoActiveDeletes did not unblock after first delete finished")
	}

	// Second delete after the first completes.
	require.NoError(t, coord.beginDelete(context.Background()))

	waitDone2 := make(chan error, 1)
	go func() {
		waitDone2 <- coord.waitForNoActiveDeletes(context.Background())
	}()

	select {
	case <-waitDone2:
		t.Fatal("waitForNoActiveDeletes returned with second active delete")
	case <-time.After(50 * time.Millisecond):
	}

	coord.finishDelete(false)

	select {
	case err := <-waitDone2:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("waitForNoActiveDeletes did not unblock after second delete finished")
	}
}

func TestLockCommitWhenNoActiveDeletesAcquiresCommitLease(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	err := coord.lockCommitWhenNoActiveDeletes(context.Background())
	require.NoError(t, err)
	coord.releaseCommit()
}

func TestLockCommitWhenNoActiveDeletesRetriesAfterDeleteStartsAndFinishes(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	// Simulate the race: beginDelete holds the commit lease.
	require.NoError(t, coord.beginDelete(context.Background()))

	lockDone := make(chan error, 1)
	go func() {
		lockDone <- coord.lockCommitWhenNoActiveDeletes(context.Background())
	}()

	// lockCommitWhenNoActiveDeletes should be blocked: first on waitForNoActiveDeletes,
	// then on acquireCommit after the delete finishes.
	select {
	case <-lockDone:
		t.Fatal("lockCommitWhenNoActiveDeletes returned while delete was active")
	case <-time.After(50 * time.Millisecond):
	}

	// Finish the delete: releases the commit lease and closes the deletes-done channel.
	coord.finishDelete(false)

	select {
	case err := <-lockDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("lockCommitWhenNoActiveDeletes did not unblock after delete finished")
	}
}

func TestLockCommitWhenNoActiveDeletesRespectsContextCancellation(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	// Hold the commit lease by starting a delete (which acquires it internally).
	require.NoError(t, coord.beginDelete(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	err := coord.lockCommitWhenNoActiveDeletes(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	coord.finishDelete(false)
}

func TestReserveBestEffortFailsWhenActiveFillIsSet(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	coord.stateMu.Lock()
	coord.activeFill = &topFillSink{}
	coord.stateMu.Unlock()

	_, err := coord.reserveBestEffort(context.Background(), nil)
	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestReserveBestEffortFailsWhenActiveDeletesGreaterThanZero(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	require.NoError(t, coord.beginDelete(context.Background()))

	_, err := coord.reserveBestEffort(context.Background(), nil)
	require.ErrorIs(t, err, ErrTopWriteInvalidated)

	coord.finishDelete(false)
}

func TestReserveBestEffortFailsWhenFillPreemptionsPositive(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	coord.stateMu.Lock()
	coord.fillPreemptions = 1
	coord.stateMu.Unlock()

	_, err := coord.reserveBestEffort(context.Background(), nil)
	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestReserveBestEffortSucceedsWhenIdle(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	gen, err := coord.reserveBestEffort(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), gen)
	coord.unregisterReservation(gen)
}

func TestReserveBestEffortFailsOnStaleExpectedGeneration(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	coord.stateMu.Lock()
	coord.committedGeneration.Store(2)
	coord.stateMu.Unlock()

	stale := uint64(1)
	_, err := coord.reserveBestEffort(context.Background(), &stale)
	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestReserveBestEffortFailsOnCancelledContext(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := coord.reserveBestEffort(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReserveWithFillRegistersFillOnCoordinator(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	fill := &topFillSink{}
	gen, err := coord.reserveWithFill(context.Background(), 0, fill)
	require.NoError(t, err)
	require.Equal(t, uint64(1), gen)

	coord.stateMu.Lock()
	registered := coord.activeFill
	coord.stateMu.Unlock()

	require.Equal(t, fill, registered)
	require.True(t, fill.registered)

	coord.unregisterReservation(gen)
	coord.unregisterActiveFill(fill)
}

func TestReserveWithFillFailsWhenActiveFillAlreadySet(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	coord.stateMu.Lock()
	coord.activeFill = &topFillSink{}
	coord.stateMu.Unlock()

	_, err := coord.reserveWithFill(context.Background(), 0, &topFillSink{})
	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestReserveWithFillFailsWhenActiveDeletesPositive(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	require.NoError(t, coord.beginDelete(context.Background()))

	_, err := coord.reserveWithFill(context.Background(), 0, &topFillSink{})
	require.ErrorIs(t, err, ErrTopWriteInvalidated)

	coord.finishDelete(false)
}

func TestReserveWithFillFailsWhenFillPreemptionsPositive(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	coord.stateMu.Lock()
	coord.fillPreemptions = 1
	coord.stateMu.Unlock()

	_, err := coord.reserveWithFill(context.Background(), 0, &topFillSink{})
	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestReserveWithFillNilFillDoesNotSetActiveFill(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	gen, err := coord.reserveWithFill(context.Background(), 0, nil)
	require.NoError(t, err)

	coord.stateMu.Lock()
	activeFill := coord.activeFill
	coord.stateMu.Unlock()

	require.Nil(t, activeFill)
	coord.unregisterReservation(gen)
}

func TestReserveWithFillFailsOnStaleExpectedGeneration(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	_, err := coord.reserveWithFill(context.Background(), 1, &topFillSink{})
	require.ErrorIs(t, err, ErrTopWriteInvalidated)
}

func TestBeginDeleteIncrementsActiveDeletesAndCreatesChannel(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	require.NoError(t, coord.beginDelete(context.Background()))

	coord.stateMu.Lock()
	require.Equal(t, 1, coord.activeDeletes)
	doneCh := coord.activeDeletesDone
	coord.stateMu.Unlock()
	require.NotNil(t, doneCh)

	coord.finishDelete(false)

	coord.stateMu.Lock()
	require.Equal(t, 0, coord.activeDeletes)
	// The done channel should have been replaced and closed.
	select {
	case <-coord.activeDeletesDone:
	default:
		t.Fatal("activeDeletesDone channel should be closed when no deletes active")
	}
	coord.stateMu.Unlock()
}

func TestBeginDeleteBlocksCommitLeaseAndFinishDeleteReleasesIt(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	require.NoError(t, coord.beginDelete(context.Background()))

	// A second beginDelete should block because the commit lease is held.
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- coord.beginDelete(context.Background())
	}()

	select {
	case <-secondDone:
		t.Fatal("second beginDelete should be blocked on commit lease")
	case <-time.After(50 * time.Millisecond):
	}

	// Finish the first delete: decrements activeDeletes and releases commit lease.
	coord.finishDelete(false)

	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second beginDelete did not proceed after commit lease was released")
	}
	coord.finishDelete(false)

	// After both deletes finish, commit lease should be available.
	err := coord.acquireCommit(context.Background())
	require.NoError(t, err)
	coord.releaseCommit()
}

func TestFinishDeleteWithSuccessAdvancesCommittedGeneration(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	require.NoError(t, coord.beginDelete(context.Background()))
	coord.finishDelete(true)

	gen := coord.committedGeneration.Load()
	require.NotZero(t, gen, "committedGeneration should advance on successful delete")
}

func TestFinishDeleteWithoutSuccessDoesNotAdvanceGeneration(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	require.NoError(t, coord.beginDelete(context.Background()))
	coord.finishDelete(false)

	gen := coord.committedGeneration.Load()
	require.Zero(t, gen, "committedGeneration should remain zero on unsuccessful delete")
}

func TestBeginDeleteRespectsContextCancellation(t *testing.T) {
	coord := &writeCoordinator{}
	coord.initLease()

	// Acquire the commit lease via a beginDelete so the next beginDelete blocks.
	require.NoError(t, coord.beginDelete(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	err := coord.beginDelete(ctx)
	require.Error(t, err)

	coord.finishDelete(false)
}

func TestFanoutWriteManagerAcquireReleaseRefCounting(t *testing.T) {
	var manager fanoutWriteManager

	unlock1 := manager.lock(0, "ref-key")
	lockKey := fanoutLockKey{destTierIndex: 0, key: "ref-key"}

	stored, ok := manager.locks.Load(lockKey)
	require.True(t, ok)
	lock := stored.(*fanoutWriteLock)
	require.Equal(t, 1, lock.refs)

	acquired2 := make(chan struct{})
	goroutineDone := make(chan struct{})
	go func() {
		unlock2 := manager.lock(0, "ref-key")
		close(acquired2)
		<-goroutineDone
		unlock2()
	}()

	// The second lock blocks on lock.mu because the first holds it.
	select {
	case <-acquired2:
		t.Fatal("second lock should be blocked until first is released")
	case <-time.After(50 * time.Millisecond):
	}

	// Release the first lock; the second should proceed.
	unlock1()

	select {
	case <-acquired2:
	case <-time.After(time.Second):
		t.Fatal("second lock did not acquire after first release")
	}

	// After the second lock is acquired, it's the only reference.
	stored, _ = manager.locks.Load(lockKey)
	lock = stored.(*fanoutWriteLock)
	require.Equal(t, 1, lock.refs)

	close(goroutineDone)

	// Wait for the goroutine to fully complete, including release().
	time.Sleep(50 * time.Millisecond)

	// After the second lock releases, the entry should be cleaned up.
	_, ok = manager.locks.Load(lockKey)
	require.False(t, ok, "lock should be removed after all references released")
}

func TestFanoutWriteManagerDifferentKeysAreIndependent(t *testing.T) {
	var manager fanoutWriteManager

	unlockA := manager.lock(0, "key-a")
	unlockB := manager.lock(0, "key-b")

	_, okA := manager.locks.Load(fanoutLockKey{destTierIndex: 0, key: "key-a"})
	_, okB := manager.locks.Load(fanoutLockKey{destTierIndex: 0, key: "key-b"})
	require.True(t, okA)
	require.True(t, okB)

	unlockA()

	_, okA = manager.locks.Load(fanoutLockKey{destTierIndex: 0, key: "key-a"})
	_, okB = manager.locks.Load(fanoutLockKey{destTierIndex: 0, key: "key-b"})
	require.False(t, okA, "key-a lock should be removed")
	require.True(t, okB, "key-b lock should still exist")

	unlockB()
}

func TestFanoutWriteManagerDifferentTiersAreIndependent(t *testing.T) {
	var manager fanoutWriteManager

	unlock0 := manager.lock(0, "same-key")
	unlock1 := manager.lock(1, "same-key")

	_, ok0 := manager.locks.Load(fanoutLockKey{destTierIndex: 0, key: "same-key"})
	_, ok1 := manager.locks.Load(fanoutLockKey{destTierIndex: 1, key: "same-key"})
	require.True(t, ok0)
	require.True(t, ok1)

	unlock0()

	_, ok0 = manager.locks.Load(fanoutLockKey{destTierIndex: 0, key: "same-key"})
	require.False(t, ok0)
	_, ok1 = manager.locks.Load(fanoutLockKey{destTierIndex: 1, key: "same-key"})
	require.True(t, ok1)

	unlock1()
}

func TestWriteCoordinatorInitIdempotent(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()
	ch1 := coord.activeDeletesDone
	coord.init()
	ch2 := coord.activeDeletesDone
	require.Equal(t, ch1, ch2, "init should be idempotent via sync.Once")
}

func TestWriteCoordinatorReserveMonotonicGenerations(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	g1, err := coord.reserve(context.Background(), nil)
	require.NoError(t, err)

	g2, err := coord.reserve(context.Background(), nil)
	require.NoError(t, err)

	g3, err := coord.reserve(context.Background(), nil)
	require.NoError(t, err)

	require.Greater(t, g2, g1)
	require.Greater(t, g3, g2)

	coord.unregisterReservation(g1)
	coord.unregisterReservation(g2)
	coord.unregisterReservation(g3)
}

func TestWriteCoordinatorCanAttemptExpectedTopWrite(t *testing.T) {
	coord := &writeCoordinator{}
	coord.init()

	require.True(t, coord.canAttemptExpectedTopWrite(0))

	coord.stateMu.Lock()
	coord.committedGeneration.Store(5)
	coord.stateMu.Unlock()

	require.True(t, coord.canAttemptExpectedTopWrite(5))
	require.False(t, coord.canAttemptExpectedTopWrite(4))

	require.NoError(t, coord.beginDelete(context.Background()))
	require.False(t, coord.canAttemptExpectedTopWrite(5))
	coord.finishDelete(false)

	require.True(t, coord.canAttemptExpectedTopWrite(5))
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

type panicStagedWriteSink struct {
	commitPanic any
	abortPanic  any
	abortCalls  int
}

func (s *panicStagedWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *panicStagedWriteSink) Commit(context.Context) error {
	panic(s.commitPanic)
}
func (s *panicStagedWriteSink) Abort() error {
	s.abortCalls++
	panic(s.abortPanic)
}

type blockingFirstBeginSetStore struct {
	beginStarted chan struct{}
	releaseBegin chan struct{}
}

func (s *blockingFirstBeginSetStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *blockingFirstBeginSetStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	select {
	case <-s.beginStarted:
	default:
		close(s.beginStarted)
	}
	<-s.releaseBegin
	return &stubWriteSink{}, nil
}

func (s *blockingFirstBeginSetStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingFirstBeginSetStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type contextBlockingFirstBeginSetStore struct {
	mu           sync.Mutex
	calls        int
	beginStarted chan struct{}
	releaseBegin chan struct{}
}

func (s *contextBlockingFirstBeginSetStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *contextBlockingFirstBeginSetStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	s.mu.Unlock()
	if !first {
		return &stubWriteSink{}, nil
	}
	close(s.beginStarted)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.releaseBegin:
		return &stubWriteSink{}, nil
	}
}

func (s *contextBlockingFirstBeginSetStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *contextBlockingFirstBeginSetStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

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
