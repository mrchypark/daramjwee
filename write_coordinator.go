package daramjwee

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrTopWriteInvalidated = errors.New("daramjwee: top-tier write invalidated")

type topWriteManager struct {
	coords sync.Map
}

type fanoutWriteManager struct {
	locks sync.Map
}

type writeCoordinator struct {
	initOnce    sync.Once
	leaseOnce   sync.Once
	writeLease  chan struct{}
	commitLease chan struct{}
	stateMu     sync.Mutex
	// committedGeneration is the latest generation visible in the top store.
	// activeReservations tracks in-flight writers so conditional fills can be
	// invalidated without holding a same-key lock for the writer lifetime.
	committedGeneration uint64
	nextGeneration      uint64
	activeReservations  map[uint64]struct{}
	activeDeletes       int
	activeDeletesDone   chan struct{}
}

type coordinatedTopWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	generation    uint64
	waitTimeout   time.Duration
	onInvalidated func() error
	once          sync.Once
	err           error
}

type coordinatedStagedTopWriteSink struct {
	sink          StagedWriteSink
	coord         *writeCoordinator
	generation    uint64
	waitTimeout   time.Duration
	onInvalidated func() error
	once          sync.Once
	err           error
}

type conditionalGenerationWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	generation    uint64
	waitTimeout   time.Duration
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
		resolved := coord.(*writeCoordinator)
		resolved.init()
		return resolved
	}
	coord := &writeCoordinator{}
	coord.init()
	actual, _ := m.coords.LoadOrStore(key, coord)
	resolved := actual.(*writeCoordinator)
	resolved.init()
	return resolved
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
	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.committedGeneration
}

func (c *writeCoordinator) ensureReservationsLocked() {
	if c.activeReservations == nil {
		c.activeReservations = make(map[uint64]struct{})
	}
}

func (c *writeCoordinator) latestGenerationLocked() uint64 {
	latest := c.committedGeneration
	if len(c.activeReservations) == 0 {
		return latest
	}
	// nextGeneration is only an assignment high-water mark. Rolled-back
	// reservations must not invalidate conditional writes, so compare only
	// generations that are committed or still active.
	for generation := range c.activeReservations {
		if generation > latest {
			latest = generation
		}
	}
	return latest
}

func (c *writeCoordinator) reserveGenerationLocked() uint64 {
	c.ensureReservationsLocked()
	latest := c.latestGenerationLocked()
	if c.nextGeneration < latest {
		c.nextGeneration = latest
	}
	c.nextGeneration++
	generation := c.nextGeneration
	c.activeReservations[generation] = struct{}{}
	return generation
}

func (c *writeCoordinator) advanceCommittedLocked() {
	generation := c.reserveGenerationLocked()
	c.committedGeneration = generation
	c.pruneReservationsThroughLocked(generation)
}

func (c *writeCoordinator) removeReservationLocked(generation uint64) {
	c.ensureReservationsLocked()
	delete(c.activeReservations, generation)
}

func (c *writeCoordinator) pruneReservationsThroughLocked(generation uint64) {
	c.ensureReservationsLocked()
	for reserved := range c.activeReservations {
		if reserved <= generation {
			delete(c.activeReservations, reserved)
		}
	}
}

func (c *writeCoordinator) reserve(ctx context.Context, expected *uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if expected == nil {
		if err := c.waitForNoActiveDeletesLocked(ctx); err != nil {
			return 0, err
		}
	} else if c.activeDeletes > 0 {
		return 0, ErrTopWriteInvalidated
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if expected != nil && c.latestGenerationLocked() != *expected {
		return 0, ErrTopWriteInvalidated
	}
	generation := c.reserveGenerationLocked()
	return generation, nil
}

func (c *writeCoordinator) waitForNoActiveDeletes(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.waitForNoActiveDeletesLocked(ctx)
}

func (c *writeCoordinator) waitForNoActiveDeletesLocked(ctx context.Context) error {
	for c.activeDeletes > 0 {
		done := c.activeDeletesDone
		c.stateMu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			c.stateMu.Lock()
			return ctx.Err()
		}
		c.stateMu.Lock()
	}
	return ctx.Err()
}

func (c *writeCoordinator) lockCommitWhenNoActiveDeletes(ctx context.Context) error {
	for {
		if err := c.waitForNoActiveDeletes(ctx); err != nil {
			return err
		}
		if err := c.acquireCommit(ctx); err != nil {
			return err
		}
		c.stateMu.Lock()
		if c.activeDeletes == 0 {
			c.stateMu.Unlock()
			return nil
		}
		c.stateMu.Unlock()
		c.releaseCommit()
	}
}

func newCoordinatorWaitContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (c *writeCoordinator) unregisterReservation(generation uint64) {
	c.init()
	c.stateMu.Lock()
	c.removeReservationLocked(generation)
	c.stateMu.Unlock()
}

func (c *writeCoordinator) init() {
	c.initOnce.Do(func() {
		if c.activeDeletesDone == nil {
			c.activeDeletesDone = make(chan struct{})
			if c.activeDeletes == 0 {
				close(c.activeDeletesDone)
			}
		}
		if c.activeReservations == nil {
			c.activeReservations = make(map[uint64]struct{})
		}
	})
}

func (c *writeCoordinator) initLease() {
	c.init()
	c.leaseOnce.Do(func() {
		c.writeLease = make(chan struct{}, 1)
		c.writeLease <- struct{}{}
		c.commitLease = make(chan struct{}, 1)
		c.commitLease <- struct{}{}
	})
}

func (c *writeCoordinator) acquireWrite(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.initLease()
	select {
	case <-c.writeLease:
		if err := ctx.Err(); err != nil {
			c.releaseWrite()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *writeCoordinator) acquireCommit(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.initLease()
	select {
	case <-c.commitLease:
		if err := ctx.Err(); err != nil {
			c.releaseCommit()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *writeCoordinator) releaseCommit() {
	c.commitLease <- struct{}{}
}

func (c *writeCoordinator) releaseWrite() {
	c.writeLease <- struct{}{}
}

// begin serializes same-key top writes for stores that cannot stage separately.
// It reserves a generation while holding the write lease for the writer
// lifetime, preserving the legacy compatibility path for external stores.
func (c *writeCoordinator) begin(ctx context.Context, expected *uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.acquireWrite(ctx); err != nil {
		return 0, err
	}
	c.stateMu.Lock()
	if expected == nil {
		if err := c.waitForNoActiveDeletesLocked(ctx); err != nil {
			c.stateMu.Unlock()
			c.releaseWrite()
			return 0, err
		}
	} else if c.activeDeletes > 0 {
		c.stateMu.Unlock()
		c.releaseWrite()
		return 0, ErrTopWriteInvalidated
	}
	if err := ctx.Err(); err != nil {
		c.stateMu.Unlock()
		c.releaseWrite()
		return 0, err
	}
	if expected != nil && c.latestGenerationLocked() != *expected {
		c.stateMu.Unlock()
		c.releaseWrite()
		return 0, ErrTopWriteInvalidated
	}
	generation := c.reserveGenerationLocked()
	c.stateMu.Unlock()
	return generation, nil
}

func (c *writeCoordinator) rollbackAndUnlock(generation uint64) {
	c.unregisterReservation(generation)
	c.releaseWrite()
}

func (c *writeCoordinator) beginDelete(ctx context.Context) error {
	if err := c.acquireCommit(ctx); err != nil {
		return err
	}
	c.init()
	c.stateMu.Lock()
	if c.activeDeletes == 0 {
		c.activeDeletesDone = make(chan struct{})
	}
	c.activeDeletes++
	c.stateMu.Unlock()
	return nil
}

func (c *writeCoordinator) finishDelete(success bool) {
	c.init()
	c.stateMu.Lock()
	if success {
		c.advanceCommittedLocked()
	}
	if c.activeDeletes > 0 {
		c.activeDeletes--
		if c.activeDeletes == 0 {
			close(c.activeDeletesDone)
		}
	}
	c.stateMu.Unlock()
	c.releaseCommit()
}

func (c *DaramjweeCache) currentTopWriteGeneration(key string) uint64 {
	return c.topWrites.currentGeneration(key)
}

func (c *DaramjweeCache) noteTopWriteGeneration(key string) {
	coord := c.topWrites.coordinator(key)
	coord.stateMu.Lock()
	coord.advanceCommittedLocked()
	coord.stateMu.Unlock()
}

func (c *DaramjweeCache) setStreamToTopStoreWithGeneration(ctx context.Context, key string, metadata *Metadata, expectedGeneration *uint64) (WriteSink, error) {
	store := c.topWriteStore()
	coord := c.topWrites.coordinator(key)
	if staging, ok := store.(StagingStore); ok {
		generation, err := coord.reserve(ctx, expectedGeneration)
		if err != nil {
			return nil, err
		}
		sink, err := staging.BeginStagedSet(ctx, key, metadata)
		if err != nil {
			coord.unregisterReservation(generation)
			return nil, err
		}
		return &coordinatedStagedTopWriteSink{
			sink:          sink,
			coord:         coord,
			generation:    generation,
			waitTimeout:   c.closeTimeout,
			onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		}, nil
	}
	generation, err := coord.begin(ctx, expectedGeneration)
	if err != nil {
		return nil, err
	}

	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		coord.rollbackAndUnlock(generation)
		return nil, err
	}

	return &coordinatedTopWriteSink{
		WriteSink:     sink,
		coord:         coord,
		generation:    generation,
		waitTimeout:   c.closeTimeout,
		onInvalidated: func() error { return c.deleteTopStoreKey(key) },
	}, nil
}

func (s *coordinatedStagedTopWriteSink) Close() error {
	s.once.Do(func() {
		commitCtx, cancelCommit := newCoordinatorWaitContext(s.waitTimeout)
		defer cancelCommit()
		waitCtx, cancelWait := newCoordinatorWaitContext(s.waitTimeout)
		defer cancelWait()

		if err := s.coord.lockCommitWhenNoActiveDeletes(waitCtx); err != nil {
			s.coord.unregisterReservation(s.generation)
			abortErr := s.sink.Abort()
			s.err = err
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}
		commitLocked := true
		defer func() {
			if commitLocked {
				s.coord.releaseCommit()
			}
		}()

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration > s.generation {
			s.coord.removeReservationLocked(s.generation)
			s.coord.stateMu.Unlock()
			s.coord.releaseCommit()
			commitLocked = false
			abortErr := s.sink.Abort()
			s.err = ErrTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}
		s.coord.stateMu.Unlock()

		closeErr := s.sink.Commit(commitCtx)
		if closeErr != nil {
			s.coord.stateMu.Lock()
			s.coord.removeReservationLocked(s.generation)
			s.coord.stateMu.Unlock()
			s.err = closeErr
			s.coord.releaseCommit()
			commitLocked = false
			if abortErr := s.sink.Abort(); abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration < s.generation {
			s.coord.committedGeneration = s.generation
		}
		s.coord.pruneReservationsThroughLocked(s.coord.committedGeneration)
		s.coord.stateMu.Unlock()

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration > s.generation {
			s.coord.stateMu.Unlock()
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			return
		}
		s.coord.stateMu.Unlock()
	})
	return s.err
}

func (s *coordinatedStagedTopWriteSink) Write(p []byte) (int, error) {
	return s.sink.Write(p)
}

func (s *coordinatedStagedTopWriteSink) Abort() error {
	s.once.Do(func() {
		s.coord.unregisterReservation(s.generation)
		s.err = s.sink.Abort()
	})
	return s.err
}

func (s *coordinatedTopWriteSink) Close() error {
	s.once.Do(func() {
		defer s.coord.releaseWrite()
		waitCtx, cancelWait := newCoordinatorWaitContext(s.waitTimeout)
		defer cancelWait()

		if err := s.coord.waitForNoActiveDeletes(waitCtx); err != nil {
			s.coord.unregisterReservation(s.generation)
			abortErr := s.WriteSink.Abort()
			s.err = err
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration > s.generation {
			s.coord.removeReservationLocked(s.generation)
			s.coord.stateMu.Unlock()
			abortErr := s.WriteSink.Abort()
			s.err = ErrTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}
		s.coord.stateMu.Unlock()

		closeErr := s.WriteSink.Close()

		if closeErr != nil {
			s.coord.stateMu.Lock()
			s.coord.removeReservationLocked(s.generation)
			s.coord.stateMu.Unlock()
			s.err = closeErr
			return
		}

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration < s.generation {
			s.coord.committedGeneration = s.generation
		}
		s.coord.pruneReservationsThroughLocked(s.coord.committedGeneration)
		s.coord.stateMu.Unlock()

		postCloseWaitCtx, cancelPostCloseWait := newCoordinatorWaitContext(s.waitTimeout)
		defer cancelPostCloseWait()
		_ = s.coord.waitForNoActiveDeletes(postCloseWaitCtx)

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration > s.generation {
			s.coord.stateMu.Unlock()
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			return
		}
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
		defer s.coord.releaseWrite()
		s.err = s.WriteSink.Abort()
		s.coord.unregisterReservation(s.generation)
	})
	return s.err
}

func newConditionalGenerationWriteSink(sink WriteSink, coord *writeCoordinator, generation uint64, waitTimeout time.Duration, onInvalidated func() error) WriteSink {
	return &conditionalGenerationWriteSink{
		WriteSink:     sink,
		coord:         coord,
		generation:    generation,
		waitTimeout:   waitTimeout,
		onInvalidated: onInvalidated,
	}
}

func (s *conditionalGenerationWriteSink) Close() error {
	s.once.Do(func() {
		waitCtx, cancelWait := newCoordinatorWaitContext(s.waitTimeout)
		defer cancelWait()

		if err := s.coord.waitForNoActiveDeletes(waitCtx); err != nil {
			abortErr := s.WriteSink.Abort()
			s.err = err
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration > s.generation {
			s.coord.stateMu.Unlock()
			abortErr := s.WriteSink.Abort()
			s.err = ErrTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}
		s.coord.stateMu.Unlock()

		closeErr := s.WriteSink.Close()

		if closeErr != nil {
			s.err = closeErr
			return
		}

		postCloseWaitCtx, cancelPostCloseWait := newCoordinatorWaitContext(s.waitTimeout)
		defer cancelPostCloseWait()
		_ = s.coord.waitForNoActiveDeletes(postCloseWaitCtx)

		s.coord.stateMu.Lock()
		if s.coord.committedGeneration > s.generation {
			s.coord.stateMu.Unlock()
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			return
		}
		s.err = nil
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
