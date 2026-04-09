package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestPruneIdleTopWriteStatesRemovesExpiredEntries(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	state := cache.topWriteStateForKey("expired")
	state.lastTouched.Store(time.Now().Add(-10 * time.Second).UnixNano())

	cache.pruneIdleTopWriteStates(time.Now())

	if _, ok := cache.topWriteStates.Load("expired"); ok {
		t.Fatal("expected expired top write state to be pruned")
	}
}

func TestPruneIdleTopWriteStatesKeepsRecentEntries(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	cache.topWriteStateForKey("recent")

	cache.pruneIdleTopWriteStates(time.Now())

	if _, ok := cache.topWriteStates.Load("recent"); !ok {
		t.Fatal("expected recent top write state to remain")
	}
}

func TestPruneIdleTopWriteStatesSkipsLockedEntries(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	state := cache.topWriteStateForKey("locked")
	state.lastTouched.Store(time.Now().Add(-10 * time.Second).UnixNano())
	state.commitMu.Lock()
	defer state.commitMu.Unlock()

	cache.pruneIdleTopWriteStates(time.Now())

	if _, ok := cache.topWriteStates.Load("locked"); !ok {
		t.Fatal("expected locked top write state to remain")
	}
}

func TestPruneIdleTopWriteStatesSkipsBeginLockedEntries(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	state := cache.topWriteStateForKey("begin-locked")
	state.lastTouched.Store(time.Now().Add(-10 * time.Second).UnixNano())
	state.beginMu.Lock()
	defer state.beginMu.Unlock()

	cache.pruneIdleTopWriteStates(time.Now())

	if _, ok := cache.topWriteStates.Load("begin-locked"); !ok {
		t.Fatal("expected begin-locked top write state to remain")
	}
}

func TestSetStreamToStoreWithTopGenerationRejectsStaleWriterBeforeBeginSet(t *testing.T) {
	store := &destructiveReservationStore{
		data: []byte("live-body"),
		meta: Metadata{CacheTag: "live"},
	}
	cache := &DaramjweeCache{
		Tiers:         []Store{store},
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}
	cache.noteTopWriteGeneration("key")

	expectedGeneration := uint64(0)
	writer, err := cache.setStreamToStoreWithTopGeneration(context.Background(), store, "key", &Metadata{CacheTag: "stale"}, &expectedGeneration)
	if !errors.Is(err, errTopWriteInvalidated) {
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
		Tiers:         []Store{store},
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	expectedGeneration := uint64(0)
	writer, err := cache.setStreamToStoreWithTopGeneration(context.Background(), store, "key", &Metadata{CacheTag: "v1"}, &expectedGeneration)
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

func TestCancelTopWriteReservationDoesNotUndoConcurrentInvalidation(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	state, generation, ok := cache.beginTopWriteReservation("key", nil)
	if !ok {
		t.Fatal("expected reservation to succeed")
	}

	cache.noteTopWriteGeneration("key")
	cache.cancelTopWriteReservation(state, generation)

	if got := cache.currentTopWriteGeneration("key"); got != generation+1 {
		t.Fatalf("expected concurrent invalidation to survive rollback, got %d", got)
	}
}

func TestBeginTopWriteReservationRejectsStaleWriterAfterConcurrentInvalidation(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	expectedGeneration := uint64(0)
	var once sync.Once
	testHookAfterTopWriteReservationValidation = func() {
		once.Do(func() {
			cache.noteTopWriteGeneration("key")
		})
	}
	defer func() {
		testHookAfterTopWriteReservationValidation = nil
	}()

	state, generation, ok := cache.beginTopWriteReservation("key", &expectedGeneration)
	if ok || state != nil || generation != 0 {
		t.Fatalf("expected stale reservation to be rejected, got state=%v generation=%d ok=%v", state, generation, ok)
	}
	if got := cache.currentTopWriteGeneration("key"); got != 1 {
		t.Fatalf("expected delete invalidation generation to survive, got %d", got)
	}
}

func TestBeginTopWriteReservationRetriesPublicWriterAfterConcurrentInvalidation(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	var once sync.Once
	testHookAfterTopWriteReservationValidation = func() {
		once.Do(func() {
			cache.noteTopWriteGeneration("key")
		})
	}
	defer func() {
		testHookAfterTopWriteReservationValidation = nil
	}()

	state, generation, ok := cache.beginTopWriteReservation("key", nil)
	if !ok || state == nil {
		t.Fatalf("expected public writer reservation to retry and succeed, got state=%v generation=%d ok=%v", state, generation, ok)
	}
	cache.finishTopWriteReservation(state)
	if generation != 2 {
		t.Fatalf("expected reservation to advance past concurrent invalidation, got %d", generation)
	}
	if got := cache.currentTopWriteGeneration("key"); got != 2 {
		t.Fatalf("expected current generation to include invalidation and reservation, got %d", got)
	}
}

func TestTopWriteSinkCloseReturnsInvalidationErrorWhenAbortSucceeds(t *testing.T) {
	state := &topWriteState{}
	state.generation.Store(2)

	sink := &topWriteSink{
		WriteSink:  &stubWriteSink{},
		cache:      &DaramjweeCache{},
		key:        "key",
		store:      nil,
		state:      state,
		generation: 1,
	}
	err := sink.Close()
	if !errors.Is(err, errTopWriteInvalidated) {
		t.Fatalf("expected invalidated error, got %v", err)
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
