package worker

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// Job은 워커에 의해 비동기적으로 실행될 작업 단위를 정의합니다.
// 이 함수는 실행에 필요한 모든 정보를 클로저(closure) 형태로 캡처해야 합니다.
type Job func(ctx context.Context)

// Strategy는 작업을 어떻게 실행할지에 대한 정책을 정의하는 인터페이스입니다.
type Strategy interface {
	Submit(job Job)
	Shutdown()
}

// Manager는 워커 전략을 관리하고 작업을 전달하는 역할을 합니다.
type Manager struct {
	strategy Strategy
	logger   log.Logger
}

// NewManager는 주어진 전략으로 새 워커 매니저를 생성합니다.
// jobTimeout은 각 백그라운드 작업에 적용될 최대 실행 시간입니다.
func NewManager(strategyType string, logger log.Logger, poolSize int, queueSize int, jobTimeout time.Duration) (*Manager, error) {
	if jobTimeout <= 0 {
		jobTimeout = 30 * time.Second // 기본 타임아웃
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

// Submit은 작업을 현재 설정된 전략으로 전달합니다.
func (m *Manager) Submit(job Job) {
	m.strategy.Submit(job)
}

// Shutdown은 워커 매니저를 안전하게 종료합니다.
func (m *Manager) Shutdown() {
	level.Info(m.logger).Log("msg", "shutting down worker manager")
	m.strategy.Shutdown()
}
