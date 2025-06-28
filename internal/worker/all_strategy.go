// Filename: daramjwee/internal/worker/all_strategy.go

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
	wg      sync.WaitGroup
}

// 컴파일 타임에 Strategy 인터페이스를 만족하는지 확인합니다.
// (worker.go의 Strategy 인터페이스도 Shutdown(timeout time.Duration) error로 수정되어야 합니다)
var _ Strategy = (*AllStrategy)(nil)

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

// --- 여기가 수정된 부분입니다 (Shutdown 로직) ---

// Shutdown은 실행 중인 모든 작업이 완료될 때까지 기다리되, 지정된 타임아웃을 초과하면 에러를 반환합니다.
func (s *AllStrategy) Shutdown(timeout time.Duration) error {
	doneCh := make(chan struct{})
	go func() {
		// 모든 작업 고루틴이 종료되면 wg.Wait()가 리턴되고, doneCh 채널이 닫힙니다.
		s.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// 모든 작업이 시간 내에 정상적으로 종료되었습니다.
		return nil
	case <-time.After(timeout):
		// 지정된 시간을 초과했습니다. 타임아웃 에러를 반환합니다.
		level.Error(s.logger).Log("msg", "AllStrategy shutdown timed out", "timeout", timeout)
		return ErrShutdownTimeout // worker.go에 정의된 공통 에러 사용
	}
}
