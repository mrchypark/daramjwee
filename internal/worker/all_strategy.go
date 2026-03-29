package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// AllStrategy creates a new goroutine for every submitted job.
// It implements the Strategy interface.
type AllStrategy struct {
	logger       log.Logger
	timeout      time.Duration
	jobs         chan Job
	wg           sync.WaitGroup
	dispatcherWg sync.WaitGroup
	submitMu     sync.Mutex
	shutdownOnce sync.Once
	isShutdown   bool
}

// NewAllStrategy creates a new AllStrategy with the specified logger and job timeout.
func NewAllStrategy(logger log.Logger, timeout time.Duration) *AllStrategy {
	s := &AllStrategy{
		logger:  logger,
		timeout: timeout,
		jobs:    make(chan Job),
	}
	s.start()
	return s
}

// start begins the goroutine that listens for new jobs and dispatches them.
func (s *AllStrategy) start() {
	s.dispatcherWg.Add(1)
	go func() {
		defer s.dispatcherWg.Done()
		for job := range s.jobs {
			s.wg.Add(1)
			go func(j Job) {
				defer s.wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
				defer cancel()
				runJobSafely(s.logger, j, ctx)
			}(job)
		}
	}()
}

// Submit sends a job to the strategy. It always returns true as it creates a new goroutine for each job.
func (s *AllStrategy) Submit(job Job) bool {
	s.submitMu.Lock()
	defer s.submitMu.Unlock()

	if s.isShutdown {
		level.Warn(s.logger).Log("msg", "worker is shutdown, dropping job")
		return false
	}

	s.jobs <- job
	return true
}

// Shutdown waits for all currently running jobs to complete or until the specified timeout is reached.
// It returns an error if the shutdown times out.
func (s *AllStrategy) Shutdown(timeout time.Duration) error {
	s.shutdownOnce.Do(func() {
		s.submitMu.Lock()
		defer s.submitMu.Unlock()

		s.isShutdown = true
		close(s.jobs)
	})

	s.dispatcherWg.Wait()

	doneCh := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		return nil
	case <-time.After(timeout):
		level.Error(s.logger).Log("msg", "AllStrategy shutdown timed out", "timeout", timeout)
		return ErrShutdownTimeout
	}
}
