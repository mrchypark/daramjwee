package worker

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// Job defines a unit of work to be executed asynchronously by a worker.
// This function should capture all necessary information for its execution as a closure.
type Job func(ctx context.Context)

// Strategy defines the interface for a worker strategy.
type Strategy interface {
	// Submit attempts to submit a job for execution. It returns true if the job was accepted, false otherwise.
	Submit(job Job) bool
	// Shutdown gracefully shuts down the worker strategy within the given timeout.
	// It returns an error if the shutdown times out or encounters other issues.
	Shutdown(timeout time.Duration) error
}

// Manager manages worker strategies and dispatches jobs.
type Manager struct {
	strategy Strategy
	logger   log.Logger
}

// NewManager creates a new worker manager with the specified strategy.
// jobTimeout is the maximum execution time applied to each background job.
func NewManager(strategyType string, logger log.Logger, poolSize int, queueSize int, jobTimeout time.Duration) (*Manager, error) {
	if jobTimeout <= 0 {
		jobTimeout = 30 * time.Second // Default timeout
	}

	var strategy Strategy
	switch strategyType {
	case "all":
		strategy = NewAllStrategy(logger, jobTimeout)
	case "pool":
		strategy = NewPoolStrategy(logger, poolSize, queueSize, jobTimeout)
	default:
		level.Info(logger).Log("msg", "unknown strategy, defaulting to 'pool'", "strategy", strategyType)
		strategy = NewPoolStrategy(logger, poolSize, queueSize, jobTimeout)
	}

	return &Manager{
		strategy: strategy,
		logger:   logger,
	}, nil
}

// Submit dispatches a job to the currently configured strategy.
func (m *Manager) Submit(job Job) bool {
	return m.strategy.Submit(job)
}

// Shutdown safely shuts down the worker manager.
// It calls the Shutdown method of the underlying strategy.
func (m *Manager) Shutdown(timeout time.Duration) error {
	level.Info(m.logger).Log("msg", "shutting down worker manager")
	err := m.strategy.Shutdown(timeout)
	if err != nil {
		level.Error(m.logger).Log("msg", "error during shutdown", "err", err)
		return err
	}
	level.Info(m.logger).Log("msg", "worker manager shutdown complete")
	return nil
}