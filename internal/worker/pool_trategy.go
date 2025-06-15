package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// PoolStrategy는 정해진 개수의 워커 풀을 사용하여 작업을 처리합니다.
type PoolStrategy struct {
	logger   log.Logger
	timeout  time.Duration
	poolSize int
	jobs     chan Job
	wg       sync.WaitGroup
	quit     chan struct{}
}

func NewPoolStrategy(logger log.Logger, poolSize int, timeout time.Duration) *PoolStrategy {
	if poolSize <= 0 {
		poolSize = 10 // 기본 풀 사이즈
	}
	p := &PoolStrategy{
		logger:   logger,
		poolSize: poolSize,
		timeout:  timeout,
		jobs:     make(chan Job, poolSize*2), // 작업 큐 버퍼
		quit:     make(chan struct{}),
	}
	p.start()
	return p
}

func (p *PoolStrategy) start() {
	p.wg.Add(p.poolSize)
	for i := 0; i < p.poolSize; i++ {
		go func(workerID int) {
			defer p.wg.Done()
			logger := log.With(p.logger, "worker_id", workerID)
			level.Info(logger).Log("msg", "worker started")

			for {
				select {
				case job := <-p.jobs:
					ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
					job(ctx)
					cancel() // 루프 내에서는 defer를 사용할 수 없으므로 명시적으로 호출
				case <-p.quit:
					level.Info(logger).Log("msg", "worker stopped")
					return
				}
			}
		}(i)
	}
}

func (p *PoolStrategy) Submit(job Job) {
	select {
	case p.jobs <- job:
		// Job 전송 성공
	case <-p.quit:
		level.Warn(p.logger).Log("msg", "worker is shutting down, job submission ignored")
	}
}

func (p *PoolStrategy) Shutdown() {
	close(p.quit) // 모든 워커에게 종료 신호 전송
	p.wg.Wait()   // 모든 워커가 종료될 때까지 대기
}
