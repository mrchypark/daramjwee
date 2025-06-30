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
	logger  log.Logger
	timeout time.Duration
	jobs    chan Job
	wg      sync.WaitGroup
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
	go func() {
		for job := range s.jobs {
			s.wg.Add(1)
			go func(j Job) {
				defer s.wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
				defer cancel()
				j(ctx)
			}(job)
		}
	}()
}

// Submit sends a job to the strategy. It always returns true as it creates a new goroutine for each job.
func (s *AllStrategy) Submit(job Job) bool {
	s.jobs <- job
	return true
}

// Shutdown waits for all currently running jobs to complete or until the specified timeout is reached.
// It returns an error if the shutdown times out.
func (s *AllStrategy) Shutdown(timeout time.Duration) error {
	close(s.jobs)
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