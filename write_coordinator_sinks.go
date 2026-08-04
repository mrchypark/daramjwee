package daramjwee

import (
	"errors"
	"sync"
	"time"
)

// coordinatedTopWriteSink holds a write lease for the writer lifetime.
// Used for stores that cannot stage separately.
type coordinatedTopWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	generation    uint64
	waitTimeout   time.Duration
	onInvalidated func() error
	once          sync.Once
	err           error
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

func (s *coordinatedTopWriteSink) Abort() error {
	s.once.Do(func() {
		defer s.coord.releaseWrite()
		s.err = s.WriteSink.Abort()
		s.coord.unregisterReservation(s.generation)
	})
	return s.err
}

func (s *coordinatedTopWriteSink) detachForFillPreempt() func() error {
	var cleanup func() error
	s.once.Do(func() {
		s.err = ErrTopWriteInvalidated
		s.coord.unregisterReservation(s.generation)
		s.coord.releaseWrite()
		cleanup = s.WriteSink.Abort
	})
	return cleanup
}

// coordinatedStagedTopWriteSink uses a staging model with a short commit phase.
// The write lease is not held for the writer lifetime.
type coordinatedStagedTopWriteSink struct {
	sink          StagedWriteSink
	coord         *writeCoordinator
	generation    uint64
	waitTimeout   time.Duration
	onInvalidated func() error
	once          sync.Once
	err           error
}

func (s *coordinatedStagedTopWriteSink) Write(p []byte) (int, error) {
	return s.sink.Write(p)
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

func (s *coordinatedStagedTopWriteSink) Abort() error {
	s.once.Do(func() {
		s.coord.unregisterReservation(s.generation)
		s.err = s.sink.Abort()
	})
	return s.err
}

func (s *coordinatedStagedTopWriteSink) detachForFillPreempt() func() error {
	var cleanup func() error
	s.once.Do(func() {
		s.coord.unregisterReservation(s.generation)
		s.err = ErrTopWriteInvalidated
		cleanup = s.sink.Abort
	})
	return cleanup
}

// conditionalGenerationWriteSink is used for persist-to-lower-tier fanout writes.
// It does not hold any lease and only validates generation before committing.
type conditionalGenerationWriteSink struct {
	WriteSink
	coord         *writeCoordinator
	generation    uint64
	waitTimeout   time.Duration
	onInvalidated func() error
	once          sync.Once
	err           error
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
