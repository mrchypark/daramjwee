// Filename: internal/worker/pool_strategy.go

package worker

import (
	"context"
	"errors"
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

	// [핵심 수정] 제출과 종료의 동시성 문제를 해결하기 위한 뮤텍스와 플래그
	submitMu   sync.Mutex
	isShutdown bool
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
	// [핵심 수정] 락을 사용하여 종료 플래그 확인과 채널 전송을 원자적으로 보호합니다.
	s.submitMu.Lock()
	defer s.submitMu.Unlock()

	// 락 안에서 종료 여부를 다시 한번 확인합니다.
	if s.isShutdown {
		level.Warn(s.logger).Log("msg", "worker is shutdown, dropping job")
		return false
	}

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
		// [핵심 수정] 락을 사용하여 isShutdown 플래그 설정과 채널 닫기를 동기화합니다.
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
