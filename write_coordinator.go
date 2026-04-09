package daramjwee

import (
	"context"
	"errors"
	"sync"
)

var errTopWriteInvalidated = errors.New("daramjwee: top-tier write invalidated")

type topWriteManager struct {
	coords sync.Map
}

type writeCoordinator struct {
	writeMu    sync.Mutex
	stateMu    sync.Mutex
	generation uint64
}

type coordinatedTopWriteSink struct {
	WriteSink
	coord      *writeCoordinator
	generation uint64
	once       sync.Once
	err        error
}

type conditionalGenerationWriteSink struct {
	WriteSink
	coord      *writeCoordinator
	generation uint64
	once       sync.Once
	err        error
}

func (m *topWriteManager) coordinator(key string) *writeCoordinator {
	if coord, ok := m.coords.Load(key); ok {
		return coord.(*writeCoordinator)
	}
	coord := &writeCoordinator{}
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
	return c.generation
}

// begin serializes same-key top writes while reserving a generation that lets
// concurrent deletes and stale conditional writers invalidate later commit.
func (c *writeCoordinator) begin(expected *uint64) (uint64, bool) {
	c.writeMu.Lock()
	c.stateMu.Lock()
	if expected != nil && c.generation != *expected {
		c.stateMu.Unlock()
		c.writeMu.Unlock()
		return 0, false
	}
	c.generation++
	generation := c.generation
	c.stateMu.Unlock()
	return generation, true
}

func (c *writeCoordinator) rollbackAndUnlock(reserved uint64) {
	c.stateMu.Lock()
	if c.generation == reserved {
		c.generation--
	}
	c.stateMu.Unlock()
	c.writeMu.Unlock()
}

func (c *DaramjweeCache) currentTopWriteGeneration(key string) uint64 {
	return c.topWrites.currentGeneration(key)
}

func (c *DaramjweeCache) noteTopWriteGeneration(key string) {
	coord := c.topWrites.coordinator(key)
	coord.stateMu.Lock()
	coord.generation++
	coord.stateMu.Unlock()
}

func (c *DaramjweeCache) setStreamToStoreWithTopGeneration(ctx context.Context, store Store, key string, metadata *Metadata, expectedGeneration *uint64) (WriteSink, error) {
	if !sameStoreInstance(store, c.topWriteStore()) {
		return store.BeginSet(ctx, key, metadata)
	}

	coord := c.topWrites.coordinator(key)
	generation, ok := coord.begin(expectedGeneration)
	if !ok {
		return nil, errTopWriteInvalidated
	}

	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		coord.rollbackAndUnlock(generation)
		return nil, err
	}

	return &coordinatedTopWriteSink{
		WriteSink:  sink,
		coord:      coord,
		generation: generation,
	}, nil
}

func (s *coordinatedTopWriteSink) Close() error {
	s.once.Do(func() {
		defer s.coord.writeMu.Unlock()
		s.coord.stateMu.Lock()
		defer s.coord.stateMu.Unlock()

		if s.coord.generation != s.generation {
			abortErr := s.WriteSink.Abort()
			s.err = errTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}

		s.err = s.WriteSink.Close()
	})
	return s.err
}

func (s *coordinatedTopWriteSink) Abort() error {
	s.once.Do(func() {
		defer s.coord.writeMu.Unlock()
		s.err = s.WriteSink.Abort()
	})
	return s.err
}

func newConditionalGenerationWriteSink(sink WriteSink, coord *writeCoordinator, generation uint64) WriteSink {
	return &conditionalGenerationWriteSink{
		WriteSink:  sink,
		coord:      coord,
		generation: generation,
	}
}

func (s *conditionalGenerationWriteSink) Close() error {
	s.once.Do(func() {
		s.coord.stateMu.Lock()
		defer s.coord.stateMu.Unlock()

		if s.coord.generation != s.generation {
			abortErr := s.WriteSink.Abort()
			s.err = errTopWriteInvalidated
			if abortErr != nil {
				s.err = errors.Join(s.err, abortErr)
			}
			return
		}

		s.err = s.WriteSink.Close()
	})
	return s.err
}

func (s *conditionalGenerationWriteSink) Abort() error {
	s.once.Do(func() {
		s.err = s.WriteSink.Abort()
	})
	return s.err
}
