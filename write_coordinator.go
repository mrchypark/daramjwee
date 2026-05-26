package daramjwee

import (
	"context"
	"errors"
	"sync"
)

var ErrTopWriteInvalidated = errors.New("daramjwee: top-tier write invalidated")

type topWriteManager struct {
	coords sync.Map
}

type fanoutWriteManager struct {
	locks sync.Map
}

type writeCoordinator struct {
	writeMu             sync.Mutex
	stateMu             sync.Mutex
	stateChanged        *sync.Cond
	committedGeneration uint64
	activeDeletes       int
	activePublishers    int
	pendingWriters      int
	activeFill          *topFillSink
}

type coordinatedTopWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	key           string
	generation    uint64
	onInvalidated func() error
	trace         func(event string, generation uint64, keyvals ...any)
	once          sync.Once
	err           error
}

type conditionalGenerationWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	generation    uint64
	onInvalidated func() error
	once          sync.Once
	err           error
}

type fanoutWriteLock struct {
	mu    sync.Mutex
	refMu sync.Mutex
	refs  int
}

type fanoutLockKey struct {
	destTierIndex int
	key           string
}

func (m *topWriteManager) coordinator(key string) *writeCoordinator {
	if coord, ok := m.coords.Load(key); ok {
		return coord.(*writeCoordinator)
	}
	coord := &writeCoordinator{}
	coord.stateChanged = sync.NewCond(&coord.stateMu)
	actual, _ := m.coords.LoadOrStore(key, coord)
	return actual.(*writeCoordinator)
}

func (m *topWriteManager) coordinatorIfPresent(key string) *writeCoordinator {
	coord, ok := m.coords.Load(key)
	if !ok {
		return nil
	}
	return coord.(*writeCoordinator)
}

func (m *fanoutWriteManager) lock(destTierIndex int, key string) func() {
	lockKey := fanoutLockKey{destTierIndex: destTierIndex, key: key}
	lock := m.acquire(lockKey)
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.release(lockKey, lock)
	}
}

func (m *fanoutWriteManager) acquire(lockKey fanoutLockKey) *fanoutWriteLock {
	for {
		if existing, ok := m.locks.Load(lockKey); ok {
			lock := existing.(*fanoutWriteLock)
			lock.refMu.Lock()
			current, stillPresent := m.locks.Load(lockKey)
			if !stillPresent || current != existing {
				lock.refMu.Unlock()
				continue
			}
			lock.refs++
			lock.refMu.Unlock()
			return lock
		}

		lock := &fanoutWriteLock{refs: 1}
		actual, loaded := m.locks.LoadOrStore(lockKey, lock)
		if !loaded {
			return lock
		}
		resolved := actual.(*fanoutWriteLock)
		resolved.refMu.Lock()
		current, stillPresent := m.locks.Load(lockKey)
		if !stillPresent || current != actual {
			resolved.refMu.Unlock()
			continue
		}
		resolved.refs++
		resolved.refMu.Unlock()
		return resolved
	}
}

func (m *fanoutWriteManager) release(lockKey fanoutLockKey, lock *fanoutWriteLock) {
	lock.refMu.Lock()
	defer lock.refMu.Unlock()
	lock.refs--
	if lock.refs == 0 {
		m.locks.CompareAndDelete(lockKey, lock)
	}
}

func (m *topWriteManager) currentGeneration(key string) uint64 {
	coord := m.coordinatorIfPresent(key)
	if coord == nil {
		return 0
	}
	return coord.current()
}

func (c *writeCoordinator) current() uint64 {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.committedGeneration
}

func (c *writeCoordinator) canAttemptExpectedTopWrite(expectedGeneration uint64) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.committedGeneration == expectedGeneration &&
		c.activeDeletes == 0 &&
		c.activePublishers == 0 &&
		c.pendingWriters == 0
}

// begin serializes same-key top writes and snapshots the committed generation
// visible to readers. Successful publish advances the committed generation only
// after the underlying sink has been closed.
func (c *writeCoordinator) begin(ctx context.Context, expected *uint64) (uint64, error) {
	return c.beginWithFill(ctx, expected, nil)
}

func (c *writeCoordinator) beginBestEffort(ctx context.Context, expected *uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.stateMu.Lock()
	if c.activeFill != nil || c.activeDeletes > 0 || c.activePublishers > 0 || c.pendingWriters > 0 {
		c.stateMu.Unlock()
		return 0, ErrTopWriteInvalidated
	}
	c.stateMu.Unlock()
	if !c.writeMu.TryLock() {
		return 0, ErrTopWriteInvalidated
	}
	c.stateMu.Lock()
	if c.activeFill != nil || c.activeDeletes > 0 || c.activePublishers > 0 || c.pendingWriters > 0 {
		c.stateMu.Unlock()
		c.writeMu.Unlock()
		return 0, ErrTopWriteInvalidated
	}
	if expected != nil && c.committedGeneration != *expected {
		c.stateMu.Unlock()
		c.writeMu.Unlock()
		return 0, ErrTopWriteInvalidated
	}
	generation := c.committedGeneration
	c.stateMu.Unlock()
	return generation, nil
}

func (c *writeCoordinator) beginWithFill(ctx context.Context, expected *uint64, fill *topFillSink) (uint64, error) {
	writeLocked := false
	if expected != nil && fill != nil {
		c.stateMu.Lock()
		if c.activeFill != nil || c.activeDeletes > 0 || c.pendingWriters > 0 {
			c.stateMu.Unlock()
			return 0, ErrTopWriteInvalidated
		}
		c.stateMu.Unlock()
		if !c.writeMu.TryLock() {
			return 0, ErrTopWriteInvalidated
		}
		writeLocked = true
	}
	if expected == nil {
		c.stateMu.Lock()
		c.pendingWriters++
		c.stateMu.Unlock()
		c.preemptActiveFill()
	}
	if !writeLocked {
		c.writeMu.Lock()
	}
	c.stateMu.Lock()
	if expected == nil {
		c.pendingWriters--
		c.stateChanged.Broadcast()
		if c.activeDeletes > 0 {
			done := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					c.stateMu.Lock()
					c.stateChanged.Broadcast()
					c.stateMu.Unlock()
				case <-done:
				}
			}()
			defer close(done)
		}
		for c.activeDeletes > 0 {
			if err := ctx.Err(); err != nil {
				c.stateMu.Unlock()
				c.writeMu.Unlock()
				return 0, err
			}
			c.stateChanged.Wait()
		}
	} else if c.activeDeletes > 0 || c.pendingWriters > 0 {
		c.stateMu.Unlock()
		c.writeMu.Unlock()
		return 0, ErrTopWriteInvalidated
	}
	if expected != nil && c.committedGeneration != *expected {
		c.stateMu.Unlock()
		c.writeMu.Unlock()
		return 0, ErrTopWriteInvalidated
	}
	generation := c.committedGeneration
	if fill != nil {
		fill.registered = true
		c.activeFill = fill
	}
	c.stateMu.Unlock()
	return generation, nil
}

func (c *writeCoordinator) rollbackAndUnlock(_ uint64) {
	c.writeMu.Unlock()
}

func (c *writeCoordinator) registerActiveFill(fill *topFillSink) {
	c.stateMu.Lock()
	c.activeFill = fill
	c.stateMu.Unlock()
}

func (c *writeCoordinator) unregisterActiveFill(fill *topFillSink) {
	c.stateMu.Lock()
	if c.activeFill == fill {
		c.activeFill = nil
	}
	c.stateMu.Unlock()
}

func (c *writeCoordinator) preemptActiveFill() {
	c.stateMu.Lock()
	fill := c.activeFill
	c.stateMu.Unlock()
	if fill != nil {
		_ = fill.Preempt()
	}
}

func (c *writeCoordinator) beginDelete(ctx context.Context) error {
	c.stateMu.Lock()
	c.activeDeletes++
	c.stateMu.Unlock()
	c.preemptActiveFill()
	c.stateMu.Lock()
	if c.activePublishers > 0 {
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				c.stateMu.Lock()
				c.stateChanged.Broadcast()
				c.stateMu.Unlock()
			case <-done:
			}
		}()
		defer close(done)
	}
	for c.activePublishers > 0 {
		if err := ctx.Err(); err != nil {
			if c.activeDeletes > 0 {
				c.activeDeletes--
			}
			c.stateChanged.Broadcast()
			c.stateMu.Unlock()
			return err
		}
		c.stateChanged.Wait()
	}
	c.stateMu.Unlock()
	return nil
}

func (c *writeCoordinator) finishDelete(success bool) {
	c.stateMu.Lock()
	if success {
		c.committedGeneration++
	}
	if c.activeDeletes > 0 {
		c.activeDeletes--
	}
	c.stateChanged.Broadcast()
	c.stateMu.Unlock()
}

func (c *DaramjweeCache) currentTopWriteGeneration(key string) uint64 {
	return c.topWrites.currentGeneration(key)
}

func (c *DaramjweeCache) noteTopWriteGeneration(key string) {
	coord := c.topWrites.coordinator(key)
	coord.stateMu.Lock()
	coord.committedGeneration++
	coord.stateChanged.Broadcast()
	coord.stateMu.Unlock()
}

func (c *DaramjweeCache) setStreamToTopStoreWithGeneration(ctx context.Context, key string, metadata *Metadata, expectedGeneration *uint64) (WriteSink, error) {
	store := c.topWriteStore()
	coord := c.topWrites.coordinator(key)
	expectedGenerationValue := diagnosticGenerationValue(expectedGeneration)
	c.diagnosticLog("top_write_begin", key, 0, "expected_generation", expectedGenerationValue)
	generation, err := coord.begin(ctx, expectedGeneration)
	if err != nil {
		c.diagnosticLog("top_write_begin_error", key, 0, "expected_generation", expectedGenerationValue, "err", err)
		return nil, err
	}
	c.diagnosticLog("top_write_begin_acquired", key, generation, "expected_generation", expectedGenerationValue)

	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		c.diagnosticLog("top_write_store_begin_error", key, generation, "err", err)
		coord.rollbackAndUnlock(generation)
		return nil, err
	}
	c.diagnosticLog("top_write_store_begin_ok", key, generation)

	return &coordinatedTopWriteSink{
		WriteSink:     sink,
		coord:         coord,
		key:           key,
		generation:    generation,
		onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		trace: func(event string, generation uint64, keyvals ...any) {
			c.diagnosticLog(event, key, generation, keyvals...)
		},
	}, nil
}

func (c *DaramjweeCache) setStreamToTopStoreBestEffortWithGeneration(ctx context.Context, key string, metadata *Metadata, expectedGeneration *uint64) (WriteSink, error) {
	store := c.topWriteStore()
	coord := c.topWrites.coordinator(key)
	expectedGenerationValue := diagnosticGenerationValue(expectedGeneration)
	c.diagnosticLog("top_write_begin_best_effort", key, 0, "expected_generation", expectedGenerationValue)
	generation, err := coord.beginBestEffort(ctx, expectedGeneration)
	if err != nil {
		c.diagnosticLog("top_write_begin_best_effort_error", key, 0, "expected_generation", expectedGenerationValue, "err", err)
		return nil, err
	}
	c.diagnosticLog("top_write_begin_best_effort_acquired", key, generation, "expected_generation", expectedGenerationValue)

	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		c.diagnosticLog("top_write_store_begin_error", key, generation, "err", err)
		coord.rollbackAndUnlock(generation)
		return nil, err
	}
	c.diagnosticLog("top_write_store_begin_ok", key, generation)

	return &coordinatedTopWriteSink{
		WriteSink:     sink,
		coord:         coord,
		key:           key,
		generation:    generation,
		onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		trace: func(event string, generation uint64, keyvals ...any) {
			c.diagnosticLog(event, key, generation, keyvals...)
		},
	}, nil
}

func (c *DaramjweeCache) setStreamToTopStoreForFill(ctx context.Context, key string, metadata *Metadata, expectedGeneration uint64) (WriteSink, error) {
	store := c.topWriteStore()
	coord := c.topWrites.coordinator(key)
	expectedGenerationValue := diagnosticGenerationValue(&expectedGeneration)
	c.diagnosticLog("top_write_begin", key, 0, "expected_generation", expectedGenerationValue)
	fill := newPendingTopFillSink(coord)
	generation, err := coord.beginWithFill(ctx, &expectedGeneration, fill)
	if err != nil {
		c.diagnosticLog("top_write_begin_error", key, 0, "expected_generation", expectedGenerationValue, "err", err)
		return nil, err
	}
	c.diagnosticLog("top_write_begin_acquired", key, generation, "expected_generation", expectedGenerationValue)

	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		c.diagnosticLog("top_write_store_begin_error", key, generation, "err", err)
		fill.failBeginSet(err)
		return nil, err
	}
	c.diagnosticLog("top_write_store_begin_ok", key, generation)

	topWriter := &coordinatedTopWriteSink{
		WriteSink:     sink,
		coord:         coord,
		key:           key,
		generation:    generation,
		onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		trace: func(event string, generation uint64, keyvals ...any) {
			c.diagnosticLog(event, key, generation, keyvals...)
		},
	}
	if fill.attach(topWriter) {
		fill.startLease(c.fillLeaseTimeout)
	} else {
		_ = fill.finishPreemptedAttach(topWriter)
	}
	return fill, nil
}

func (s *coordinatedTopWriteSink) Close() error {
	s.once.Do(func() {
		s.traceEvent("top_write_close_start")
		defer func() {
			s.coord.writeMu.Unlock()
			s.traceEvent("top_write_close_unlock", "err", s.err)
		}()
		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.traceEvent("top_write_close_wait_delete")
			s.coord.stateChanged.Wait()
		}

		if s.coord.committedGeneration != s.generation {
			committedGeneration := s.coord.committedGeneration
			s.coord.stateMu.Unlock()
			s.traceEvent("top_write_close_invalidated_before_publish", "committed_generation", committedGeneration)
			abortErr := s.WriteSink.Abort()
			s.err = ErrTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			s.traceEvent("top_write_close_abort_after_invalidation", "err", s.err)
			return
		}
		s.coord.activePublishers++
		s.coord.stateMu.Unlock()

		s.traceEvent("top_write_underlying_close_start")
		closeErr := s.WriteSink.Close()
		s.traceEvent("top_write_underlying_close_done", "err", closeErr)

		s.coord.stateMu.Lock()
		if closeErr != nil {
			s.coord.activePublishers--
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			s.err = closeErr
			return
		}
		if s.coord.committedGeneration != s.generation || s.coord.activeDeletes > 0 {
			committedGeneration := s.coord.committedGeneration
			s.coord.stateMu.Unlock()
			s.err = ErrTopWriteInvalidated
			s.traceEvent("top_write_close_invalidated_after_publish", "committed_generation", committedGeneration)
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			s.coord.stateMu.Lock()
			s.coord.activePublishers--
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			return
		}
		s.coord.committedGeneration++
		s.traceEvent("top_write_close_committed", "committed_generation", s.coord.committedGeneration)
		s.coord.activePublishers--
		s.coord.stateChanged.Broadcast()
		s.coord.stateMu.Unlock()
	})
	return s.err
}

func (c *DaramjweeCache) deleteTopStoreKey(key string) error {
	store := c.topWriteStore()
	if !hasRealStore(store) {
		return nil
	}
	ctx, cancel := c.newCtxWithTimeout(context.Background())
	defer cancel()
	err := c.deleteFromStore(ctx, store, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (s *coordinatedTopWriteSink) Abort() error {
	s.once.Do(func() {
		s.traceEvent("top_write_abort_start")
		defer func() {
			s.coord.writeMu.Unlock()
			s.traceEvent("top_write_abort_unlock", "err", s.err)
		}()
		s.err = s.WriteSink.Abort()
		s.traceEvent("top_write_abort_done", "err", s.err)
	})
	return s.err
}

func (s *coordinatedTopWriteSink) traceEvent(event string, keyvals ...any) {
	if s.trace == nil {
		return
	}
	s.trace(event, s.generation, keyvals...)
}

func diagnosticGenerationValue(generation *uint64) any {
	if generation == nil {
		return nil
	}
	return *generation
}

func newConditionalGenerationWriteSink(sink WriteSink, coord *writeCoordinator, generation uint64, onInvalidated func() error) WriteSink {
	return &conditionalGenerationWriteSink{
		WriteSink:     sink,
		coord:         coord,
		generation:    generation,
		onInvalidated: onInvalidated,
	}
}

func (s *conditionalGenerationWriteSink) Close() error {
	s.once.Do(func() {
		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if s.coord.committedGeneration != s.generation {
			s.coord.stateMu.Unlock()
			abortErr := s.WriteSink.Abort()
			s.err = ErrTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}
		s.coord.activePublishers++
		s.coord.stateMu.Unlock()

		closeErr := s.WriteSink.Close()

		s.coord.stateMu.Lock()
		if closeErr != nil {
			s.coord.activePublishers--
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			s.err = closeErr
			return
		}
		if s.coord.committedGeneration != s.generation || s.coord.activeDeletes > 0 {
			s.coord.stateMu.Unlock()
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			s.coord.stateMu.Lock()
			s.coord.activePublishers--
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			return
		}
		s.err = nil
		s.coord.activePublishers--
		s.coord.stateChanged.Broadcast()
		s.coord.stateMu.Unlock()
	})
	return s.err
}

func (s *conditionalGenerationWriteSink) Abort() error {
	s.once.Do(func() {
		s.err = s.WriteSink.Abort()
	})
	return s.err
}
