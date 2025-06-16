// Package worker defines common interfaces and a manager for various background
// task execution strategies. It allows for different approaches (e.g., a pool of workers,
// or spawning a new goroutine for each task) to be used interchangeably.
package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// Job defines a unit of work to be executed asynchronously by a worker.
// The function should encapsulate all necessary information for its execution,
// often through a closure. It receives a context which may have a timeout
// or cancellation signal.
type Job func(ctx context.Context)

// Strategy defines the interface for different policies of executing jobs.
// Each strategy dictates how and when jobs are run (e.g., in a pool, per goroutine).
type Strategy interface {
	// Submit enqueues or executes the given job according to the strategy's policy.
	Submit(job Job)
	// Shutdown gracefully stops the strategy, ensuring all active jobs are processed
	// or terminated according to the policy.
	Shutdown()
}

// Manager orchestrates job execution by delegating to a specific worker Strategy.
// It acts as a unified interface for submitting jobs and managing the lifecycle
// of the chosen worker strategy.
type Manager struct {
	strategy Strategy   // strategyImpl holds the concrete implementation of the worker strategy.
	logger   log.Logger // logger is used for logging messages from the manager.
}

// NewManager creates a new worker Manager with a specified strategy.
// Parameters:
//   strategyType: A string indicating the desired worker strategy ("all" or "pool").
//                 If an unknown type is provided, it defaults to "pool".
//   logger: A log.Logger for logging.
//   poolSize: The number of worker goroutines for the "pool" strategy. Ignored by "all".
//   queueSize: The size of the job queue for the "pool" strategy. Ignored by "all".
//   jobTimeout: The maximum execution time for each individual job. If <= 0,
//               a default timeout of 30 seconds is used.
// Returns:
//   A pointer to the newly created Manager and an error if initialization fails (though
//   current implementations always return nil error).
func NewManager(strategyType string, logger log.Logger, poolSize int, queueSize int, jobTimeout time.Duration) (*Manager, error) {
	if jobTimeout <= 0 {
		jobTimeout = 30 * time.Second // Default timeout for jobs if not specified or invalid.
	}

	var strategyImpl Strategy
	switch strategyType {
	case "all":
		level.Info(logger).Log("msg", "initializing 'all' worker strategy", "job_timeout", jobTimeout)
		strategyImpl = NewAllStrategy(logger, jobTimeout)
	case "pool":
		level.Info(logger).Log("msg", "initializing 'pool' worker strategy", "pool_size", poolSize, "queue_size", queueSize, "job_timeout", jobTimeout)
		strategyImpl = NewPoolStrategy(logger, poolSize, queueSize, jobTimeout)
	default:
		level.Warn(logger).Log("msg", "unknown worker strategy specified, defaulting to 'pool'", "unknown_strategy", strategyType, "pool_size", poolSize, "queue_size", queueSize, "job_timeout", jobTimeout)
		strategyImpl = NewPoolStrategy(logger, poolSize, queueSize, jobTimeout) // Default to PoolStrategy
	}

	if strategyImpl == nil {
		// This case should ideally not be reached if default is handled properly.
		return nil, fmt.Errorf("failed to initialize worker strategy: %s", strategyType)
	}

	return &Manager{
		strategy: strategyImpl,
		logger:   logger,
	}, nil
}

// Submit delegates the job to the configured worker strategy for execution.
// Parameters:
//   job: The Job to be executed.
func (m *Manager) Submit(job Job) {
	if m.strategy == nil {
		level.Error(m.logger).Log("msg", "worker manager strategy not initialized, cannot submit job")
		return
	}
	m.strategy.Submit(job)
}

// Shutdown gracefully shuts down the underlying worker strategy.
// It ensures that the strategy stops accepting new jobs and attempts to complete
// or terminate existing jobs as per its policy.
func (m *Manager) Shutdown() {
	level.Info(m.logger).Log("msg", "shutting down worker manager and its strategy")
	if m.strategy == nil {
		level.Error(m.logger).Log("msg", "worker manager strategy not initialized, nothing to shut down")
		return
	}
	m.strategy.Shutdown()
	level.Info(m.logger).Log("msg", "worker manager shutdown complete")
}
