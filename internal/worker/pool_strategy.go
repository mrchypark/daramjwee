// Filename: internal/worker/pool_strategy.go

package worker

import (
	"context"
	"errors" // 에러 정의를 위해 추가
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// ErrShutdownTimeout은 종료 작업이 타임아웃되었을 때 반환되는 에러입니다.
var ErrShutdownTimeout = errors.New("worker: shutdown timed out")

// PoolStrategy는 정해진 개수의 워커 풀을 사용하여 작업을 처리합니다.
type PoolStrategy struct {
	logger       log.Logger
	timeout      time.Duration
	poolSize     int
	jobs         chan Job
	wg           sync.WaitGroup
	shutdownOnce sync.Once
}

func NewPoolStrategy(logger log.Logger, poolSize int, queueSize int, timeout time.Duration) *PoolStrategy {
	if poolSize <= 0 {
		poolSize = 10
	}
	if queueSize <= 0 {
		queueSize = 100
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

// start는 for-range 패턴을 사용하여 워커를 실행합니다.
func (p *PoolStrategy) start() {
	p.wg.Add(p.poolSize)
	for i := 0; i < p.poolSize; i++ {
		go func(workerID int) {
			defer p.wg.Done()
			logger := log.With(p.logger, "worker_id", workerID)
			level.Info(logger).Log("msg", "worker started")

			// **핵심 수정**: for-range는 'jobs' 채널이 닫힐 때까지 큐의 모든 작업을 처리하고,
			// 채널이 닫히면 루프가 자동으로 안전하게 종료됩니다.
			for job := range p.jobs {
				ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
				job(ctx)
				cancel()
			}
			level.Info(logger).Log("msg", "worker stopped")
		}(i)
	}
}

// Submit은 큐가 찼을 때 작업을 버립니다.
func (s *PoolStrategy) Submit(job Job) bool {
	select {
	case s.jobs <- job:
		return true
	default:
		level.Warn(s.logger).Log("msg", "worker queue is full, dropping job")
		return false
	}
}

// Shutdown은 타임아웃과 함께 우아한 종료를 수행합니다.
func (p *PoolStrategy) Shutdown(timeout time.Duration) error {
	p.shutdownOnce.Do(func() {
		// 1. **(가장 중요)** 'jobs' 채널을 닫습니다.
		//    이것이 "더 이상 새 일을 받지 말고, 남은 일만 처리하고 퇴근하라"는 신호입니다.
		close(p.jobs)
	})

	doneCh := make(chan struct{})
	go func() {
		// 모든 워커가 for-range 루프를 마치고 종료되면 Wait()가 리턴됩니다.
		p.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// 시간 내에 정상 종료
		return nil
	case <-time.After(timeout):
		// 타임아웃 발생
		level.Error(p.logger).Log("msg", "shutdown timed out", "timeout", timeout)
		return ErrShutdownTimeout
	}
}
