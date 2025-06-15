package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
)

// AllStrategy는 제출된 모든 작업에 대해 새로운 고루틴을 생성합니다.
type AllStrategy struct {
	logger  log.Logger
	timeout time.Duration
	wg      sync.WaitGroup
}

func NewAllStrategy(logger log.Logger, timeout time.Duration) *AllStrategy {
	return &AllStrategy{
		logger:  logger,
		timeout: timeout,
	}
}

func (s *AllStrategy) Submit(job Job) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel()

		// 주입된 job 함수를 직접 실행
		job(ctx)
	}()
}

func (s *AllStrategy) Shutdown() {
	s.wg.Wait() // 모든 고루틴이 끝날 때까지 기다립니다.
}
