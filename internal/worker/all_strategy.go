package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// AllStrategy는 제출된 모든 작업에 대해 새로운 고루틴을 생성합니다.
type AllStrategy struct {
	logger  log.Logger
	timeout time.Duration
	jobs    chan Job
	wg      sync.WaitGroup
}

// 컴파일 타임에 Strategy 인터페이스를 만족하는지 확인합니다.
var _ Strategy = (*AllStrategy)(nil)

func NewAllStrategy(logger log.Logger, timeout time.Duration) *AllStrategy {
	s := &AllStrategy{
		logger:  logger,
		timeout: timeout,
		jobs:    make(chan Job),
	}
	s.start()
	return s
}

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

// Submit a job to all workers.
func (s *AllStrategy) Submit(job Job) bool {
	s.jobs <- job
	return true
}

// Shutdown은 실행 중인 모든 작업이 완료될 때까지 기다리되, 지정된 타임아웃을 초과하면 에러를 반환합니다.
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
