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
}

type coordinatedTopWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	generation    uint64
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

// begin serializes same-key top writes and snapshots the committed generation
// visible to readers. Successful publish advances the committed generation only
// after the underlying sink has been closed.
func (c *writeCoordinator) begin(ctx context.Context, expected *uint64) (uint64, error) {
	c.writeMu.Lock()
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
				c.writeMu.Unlock()
				return 0, err
			}
			c.stateChanged.Wait()
		}
	} else if c.activeDeletes > 0 {
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

func (c *writeCoordinator) rollbackAndUnlock(_ uint64) {
	c.writeMu.Unlock()
}

func (c *writeCoordinator) beginDelete() {
	c.stateMu.Lock()
	c.activeDeletes++
	c.stateMu.Unlock()
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

func (s *coordinatedTopWriteSink) Close() error {
	s.once.Do(func() {
		defer s.coord.writeMu.Unlock()
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
		s.coord.stateMu.Unlock()

		closeErr := s.WriteSink.Close()

		s.coord.stateMu.Lock()
		defer s.coord.stateMu.Unlock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if closeErr != nil {
			s.err = closeErr
			return
		}
		if s.coord.committedGeneration != s.generation {
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			return
		}
		s.coord.committedGeneration++
		s.coord.stateChanged.Broadcast()
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
		defer s.coord.writeMu.Unlock()
		s.err = s.WriteSink.Abort()
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
		s.coord.stateMu.Unlock()

		closeErr := s.WriteSink.Close()

		s.coord.stateMu.Lock()
		defer s.coord.stateMu.Unlock()
		for s.coord.activeDeletes > 0 {
			s.coord.stateChanged.Wait()
		}

		if closeErr != nil {
			s.err = closeErr
			return
		}
		if s.coord.committedGeneration != s.generation {
			s.err = ErrTopWriteInvalidated
			if s.onInvalidated != nil {
				if cleanupErr := s.onInvalidated(); cleanupErr != nil {
					s.err = errors.Join(s.err, cleanupErr)
				}
			}
			return
		}
		s.err = nil
	})
	return s.err
}

func (s *conditionalGenerationWriteSink) Abort() error {
	s.once.Do(func() {
		s.err = s.WriteSink.Abort()
	})
	return s.err
}
