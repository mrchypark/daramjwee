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
	initOnce     sync.Once
	leaseOnce    sync.Once
	writeLease   chan struct{}
	commitMu     sync.Mutex
	stateMu      sync.Mutex
	stateChanged *sync.Cond
	// committedGeneration is the latest generation visible in the top store.
	// activeReservations tracks in-flight writers so conditional fills can be
	// invalidated without holding a same-key lock for the writer lifetime.
	committedGeneration uint64
	nextGeneration      uint64
	activeReservations  map[uint64]struct{}
	activeDeletes       int
}

type coordinatedTopWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	generation    uint64
	onInvalidated func() error
	once          sync.Once
	err           error
}

type coordinatedStagedTopWriteSink struct {
	sink          StagedWriteSink
	coord         *writeCoordinator
	generation    uint64
	conditional   bool
	onInvalidated func() error
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
	delete(c.activeReservations, generation)
	c.committedGeneration = generation
}

func (c *writeCoordinator) reserve(ctx context.Context, expected *uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if expected == nil {
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
				return 0, err
			}
			c.stateChanged.Wait()
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
	c.stateChanged.Broadcast()
	return generation, nil
}

func (c *writeCoordinator) unregisterReservation(generation uint64) {
	c.init()
	c.stateMu.Lock()
	if _, ok := c.activeReservations[generation]; ok {
		delete(c.activeReservations, generation)
		c.stateChanged.Broadcast()
	}
	c.stateMu.Unlock()
}

func (c *writeCoordinator) init() {
	c.initOnce.Do(func() {
		if c.stateChanged == nil {
			c.stateChanged = sync.NewCond(&c.stateMu)
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
				c.releaseWrite()
				return 0, err
			}
			c.stateChanged.Wait()
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
	c.stateChanged.Broadcast()
	c.stateMu.Unlock()
	return generation, nil
}

func (c *writeCoordinator) rollbackAndUnlock(generation uint64) {
	c.unregisterReservation(generation)
	c.releaseWrite()
}

func (c *writeCoordinator) beginDelete() {
	c.init()
	c.stateMu.Lock()
	c.activeDeletes++
	c.stateMu.Unlock()
}

func (c *writeCoordinator) finishDelete(success bool) {
	c.init()
	c.stateMu.Lock()
	if success {
		c.advanceCommittedLocked()
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
	coord.advanceCommittedLocked()
	coord.stateChanged.Broadcast()
	coord.stateMu.Unlock()
}

func (c *DaramjweeCache) setStreamToTopStoreWithGeneration(ctx context.Context, key string, metadata *Metadata, expectedGeneration *uint64) (WriteSink, error) {
	store := c.topWriteStore()
	coord := c.topWrites.coordinator(key)
	if staging, ok := store.(StagingStore); ok {
		coord.commitMu.Lock()
		generation, err := coord.reserve(ctx, expectedGeneration)
		if err != nil {
			coord.commitMu.Unlock()
			return nil, err
		}
		sink, err := staging.BeginStagedSet(ctx, key, metadata)
		if err != nil {
			coord.unregisterReservation(generation)
			coord.commitMu.Unlock()
			return nil, err
		}
		coord.commitMu.Unlock()
		return &coordinatedStagedTopWriteSink{
			sink:          sink,
			coord:         coord,
			generation:    generation,
			conditional:   expectedGeneration != nil,
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
		onInvalidated: func() error { return c.deleteTopStoreKey(key) },
	}, nil
}

func (s *coordinatedStagedTopWriteSink) Close() error {
	s.once.Do(func() {
		ctx := context.Background()
		s.coord.commitMu.Lock()
		commitMuLocked := true
		defer func() {
			if commitMuLocked {
				s.coord.commitMu.Unlock()
			}
		}()

		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if s.coord.latestGenerationLocked() != s.generation {
			s.coord.ensureReservationsLocked()
			delete(s.coord.activeReservations, s.generation)
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			s.coord.commitMu.Unlock()
			commitMuLocked = false
			abortErr := s.sink.Abort()
			s.err = ErrTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}
		s.coord.stateMu.Unlock()

		closeErr := s.sink.Commit(ctx)

		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if closeErr != nil {
			s.coord.ensureReservationsLocked()
			delete(s.coord.activeReservations, s.generation)
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			s.err = closeErr
			return
		}
		if s.coord.latestGenerationLocked() != s.generation {
			s.coord.ensureReservationsLocked()
			delete(s.coord.activeReservations, s.generation)
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			return
		}
		s.coord.ensureReservationsLocked()
		delete(s.coord.activeReservations, s.generation)
		s.coord.committedGeneration = s.generation
		s.coord.stateChanged.Broadcast()
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
		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if s.coord.latestGenerationLocked() != s.generation {
			s.coord.ensureReservationsLocked()
			delete(s.coord.activeReservations, s.generation)
			s.coord.stateChanged.Broadcast()
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

		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if closeErr != nil {
			s.coord.ensureReservationsLocked()
			delete(s.coord.activeReservations, s.generation)
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			s.err = closeErr
			return
		}
		if s.coord.latestGenerationLocked() != s.generation {
			s.coord.ensureReservationsLocked()
			delete(s.coord.activeReservations, s.generation)
			s.coord.stateChanged.Broadcast()
			s.coord.stateMu.Unlock()
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			return
		}
		s.coord.ensureReservationsLocked()
		delete(s.coord.activeReservations, s.generation)
		s.coord.committedGeneration = s.generation
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
		defer s.coord.releaseWrite()
		s.err = s.WriteSink.Abort()
		s.coord.unregisterReservation(s.generation)
	})
	return s.err
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
		s.coord.init()
		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if s.coord.latestGenerationLocked() != s.generation {
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

		s.coord.stateMu.Lock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if closeErr != nil {
			s.coord.stateMu.Unlock()
			s.err = closeErr
			return
		}
		if s.coord.latestGenerationLocked() != s.generation {
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
