// Package worker provides different strategies for managing background tasks.
// This file implements the 'all' strategy, where each submitted job is executed
// in its own goroutine, without a fixed-size pool.
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
)

// AllStrategy implements a worker strategy where every submitted job
// is executed in a new, separate goroutine. This strategy is simple and allows
// for maximum concurrency but offers no control over the number of active goroutines,
// which could lead to resource exhaustion under high load.
type AllStrategy struct {
	logger  log.Logger    // logger is used for logging messages.
	timeout time.Duration // timeout specifies the maximum duration for each job.
	wg      sync.WaitGroup // wg is used to wait for all active jobs to complete during shutdown.
}

// NewAllStrategy creates a new AllStrategy instance.
// Parameters:
//   logger: A log.Logger for logging.
//   timeout: The default timeout duration for each job execution.
// Returns:
//   A pointer to the newly created AllStrategy.
func NewAllStrategy(logger log.Logger, timeout time.Duration) *AllStrategy {
	return &AllStrategy{
		logger:  logger,
		timeout: timeout,
	}
}

// Submit takes a Job and executes it in a new goroutine.
// A context with the configured timeout is created for each job.
// The strategy's WaitGroup is incremented before starting the goroutine
// and decremented when the goroutine finishes.
// Parameters:
//   job: The Job function to be executed.
func (s *AllStrategy) Submit(job Job) {
	s.wg.Add(1) // Increment WaitGroup counter for the new job.
	go func() {
		defer s.wg.Done() // Decrement WaitGroup counter when the job goroutine finishes.

		// Create a new context with timeout for the job.
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel() // Ensure the context is cancelled to free resources.

		// Execute the provided job function with the created context.
		job(ctx)
	}()
}

// Shutdown waits for all currently running jobs (goroutines) to complete.
// It blocks until the internal WaitGroup counter becomes zero.
// This ensures a graceful shutdown where all submitted tasks are allowed to finish.
func (s *AllStrategy) Shutdown() {
	s.wg.Wait() // Wait for all job goroutines to complete.
}
