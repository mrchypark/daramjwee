package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// ErrShutdownTimeout is returned when a shutdown operation times out.
var ErrShutdownTimeout = errors.New("worker: shutdown timed out")

// PoolStrategy processes jobs using a fixed-size pool of workers.
// It implements the Strategy interface.
type PoolStrategy struct {
	logger       log.Logger
	timeout      time.Duration
	poolSize     int
	jobs         chan Job
	wg           sync.WaitGroup
	shutdownOnce sync.Once

	submitMu   sync.Mutex // Protects isShutdown and jobs channel operations.
	isShutdown bool       // Flag to indicate if the pool is shutting down.
}

// NewPoolStrategy creates a new PoolStrategy with the specified logger, pool size, queue size, and job timeout.
// If poolSize or queueSize are non-positive, default values are used.
func NewPoolStrategy(logger log.Logger, poolSize int, queueSize int, timeout time.Duration) *PoolStrategy {
	if poolSize <= 0 {
		poolSize = 10 // Default pool size
	}
	if queueSize <= 0 {
		queueSize = 100 // Default queue size
	}
	p := &PoolStrategy{
		logger:   logger,
		poolSize: poolSize,
		timeout:  timeout,
		jobs:     make(chan Job, queueSize),
	}
	p.start()
	return p
}

// start initializes and runs the worker goroutines.
func (p *PoolStrategy) start() {
	p.wg.Add(p.poolSize)
	for i := 0; i < p.poolSize; i++ {
		go func(workerID int) {
			defer p.wg.Done()
			logger := log.With(p.logger, "worker_id", workerID)
			level.Info(logger).Log("msg", "worker started")

			for job := range p.jobs {
				ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
				job(ctx)
				cancel()
				p.wg.Done() // Decrement WaitGroup after job is processed
			}
			level.Info(logger).Log("msg", "worker stopped")
		}(i)
	}
}

// Submit attempts to submit a job to the worker pool.
// It returns true if the job was successfully submitted, false if the queue is full or the pool is shutting down.
func (s *PoolStrategy) Submit(job Job) bool {
	s.submitMu.Lock()
	defer s.submitMu.Unlock()

	if s.isShutdown {
		level.Warn(s.logger).Log("msg", "worker is shutdown, dropping job")
		return false
	}

	select {
	case s.jobs <- job:
		s.wg.Add(1) // Increment WaitGroup when job is successfully submitted
		return true
	default:
		level.Warn(s.logger).Log("msg", "worker queue is full, dropping job")
		return false
	}
}

// Shutdown gracefully shuts down the worker pool.
// It closes the job channel and waits for all active workers to finish their current jobs
// or until the specified timeout is reached.
func (p *PoolStrategy) Shutdown(timeout time.Duration) error {
	p.shutdownOnce.Do(func() {
		p.submitMu.Lock()
		defer p.submitMu.Unlock()

		p.isShutdown = true
		close(p.jobs)
	})

	doneCh := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		return nil
	case <-time.After(timeout):
		level.Error(p.logger).Log("msg", "shutdown timed out", "timeout", timeout)
		return ErrShutdownTimeout
	}
}