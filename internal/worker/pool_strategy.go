// Package worker provides different strategies for managing background tasks.
// This file implements the 'pool' strategy, which uses a fixed number of
// worker goroutines and a buffered job queue to process tasks.
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// PoolStrategy implements a worker strategy that uses a fixed-size pool of goroutines
// to process jobs from a shared queue. This approach provides control over the
// maximum number of concurrently executing jobs and can help manage resource usage.
type PoolStrategy struct {
	logger   log.Logger    // logger is used for logging messages.
	timeout  time.Duration // timeout specifies the maximum duration for each job.
	poolSize int           // poolSize is the number of worker goroutines in the pool.
	jobs     chan Job      // jobs is a buffered channel that serves as the job queue.
	wg       sync.WaitGroup // wg is used to wait for all worker goroutines to complete during shutdown.
	quit     chan struct{} // quit is a channel used to signal worker goroutines to stop.
}

// NewPoolStrategy creates a new PoolStrategy instance and starts its worker goroutines.
// Parameters:
//   logger: A log.Logger for logging.
//   poolSize: The number of worker goroutines to create. If <= 0, a default of 10 is used.
//   queueSize: The buffer size of the job queue. If <= 0, a default of 100 is used.
//   timeout: The default timeout duration for each job execution.
// Returns:
//   A pointer to the newly created and started PoolStrategy.
func NewPoolStrategy(logger log.Logger, poolSize int, queueSize int, timeout time.Duration) *PoolStrategy {
	if poolSize <= 0 {
		poolSize = 10 // Default pool size if an invalid value is provided.
	}

	// Set a reasonable default queue size if an invalid value is provided.
	if queueSize <= 0 {
		queueSize = 100
	}
	p := &PoolStrategy{
		logger:   logger,
		poolSize: poolSize,
		timeout:  timeout,
		// The job channel is buffered with queueSize.
		jobs: make(chan Job, queueSize),
		quit: make(chan struct{}),
	}
	p.start() // Initialize and start the worker goroutines.
	return p
}

// start initializes and launches the worker goroutines for the pool.
// It creates `poolSize` number of goroutines, each running the workerLoop logic.
func (p *PoolStrategy) start() {
	p.wg.Add(p.poolSize) // Increment WaitGroup counter for each worker to be started.
	for i := 0; i < p.poolSize; i++ {
		// Launch a new worker goroutine.
		go func(workerID int) {
			defer p.wg.Done() // Decrement WaitGroup counter when this worker goroutine exits.
			logger := log.With(p.logger, "worker_id", workerID)
			level.Info(logger).Log("msg", "worker started")

			// This is the main worker loop.
			// It continuously waits for jobs from the 'jobs' channel or a quit signal.
			for {
				select {
				case job := <-p.jobs: // Wait for a job from the queue.
					ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
					job(ctx) // Execute the job with a timeout context.
					// Explicitly call cancel as defer is not suitable in a loop for immediate resource release.
					cancel()
				case <-p.quit: // Wait for a quit signal.
					level.Info(logger).Log("msg", "worker stopped")
					return // Exit the workerLoop and the goroutine.
				}
			}
		}(i)
	}
}

// Submit adds a Job to the job queue for processing by a worker goroutine.
// This method is blocking: if the job queue (channel) is full, it will wait
// until space becomes available or the strategy is shut down. This behavior
// provides back-pressure, preventing the system from being overwhelmed with jobs.
// If submitting jobs in a non-blocking manner (e.g., dropping jobs when the queue is full)
// is required, a select statement with a default case could be used here.
// Parameters:
//   job: The Job function to be queued and executed.
func (p *PoolStrategy) Submit(job Job) {
	select {
	case p.jobs <- job:
		// Job successfully submitted to the queue.
		level.Debug(p.logger).Log("msg", "job submitted to queue")
	case <-p.quit:
		// If the quit channel is closed, it means the pool is shutting down.
		level.Warn(p.logger).Log("msg", "worker pool is shutting down, job submission ignored")
	}
}

// Shutdown initiates a graceful shutdown of the worker pool.
// It first closes the `quit` channel, signaling all worker goroutines to stop
// processing new jobs and exit their loops.
// Then, it waits for all worker goroutines to complete their current tasks and
// shut down using the `sync.WaitGroup`.
func (p *PoolStrategy) Shutdown() {
	level.Info(p.logger).Log("msg", "shutting down worker pool")
	close(p.quit) // Signal all workers to stop by closing the quit channel.
	p.wg.Wait()   // Wait for all worker goroutines to finish.
	level.Info(p.logger).Log("msg", "worker pool shutdown complete")
}
