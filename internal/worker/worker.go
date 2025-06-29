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

// Strategy 인터페이스에 타임아웃이 있는 종료 메서드를 추가합니다.
type Strategy interface {
	Submit(job Job) bool
	Shutdown(timeout time.Duration) error // 기존 Shutdown을 대체하거나, 오버로딩합니다.
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
func (m *Manager) Submit(job Job) bool {
	return m.strategy.Submit(job)
}

// Shutdown은 워커 매니저를 안전하게 종료합니다.
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
