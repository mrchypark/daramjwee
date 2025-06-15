package daramjwee

import (
	"fmt"
	"time"
)

// ConfigError는 설정 과정에서 발생하는 에러를 나타냅니다.
type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("daramjwee: configuration error: %s", e.Message)
}

// Config는 daramjwee 캐시의 모든 설정을 담는 구조체입니다.
// Option 함수들은 이 구조체의 필드를 변경합니다.
type Config struct {
	HotStore         Store
	ColdStore        Store
	WorkerStrategy   string
	WorkerPoolSize   int
	WorkerJobTimeout time.Duration
	DefaultTimeout   time.Duration
	NegativeCacheTTL time.Duration
}

// Option은 Config를 수정하는 함수 타입입니다.
type Option func(cfg *Config) error

// WithHotStore는 Hot Tier로 사용할 Store를 설정합니다.
// 이 옵션은 필수적으로 제공되어야 합니다.
func WithHotStore(store Store) Option {
	return func(cfg *Config) error {
		if store == nil {
			return &ConfigError{"hot store cannot be nil"}
		}
		cfg.HotStore = store
		return nil
	}
}

// WithColdStore는 Cold Tier로 사용할 Store를 설정합니다.
// 이 옵션은 선택 사항입니다.
func WithColdStore(store Store) Option {
	return func(cfg *Config) error {
		cfg.ColdStore = store
		return nil
	}
}

// WithWorker는 백그라운드 작업을 위한 워커의 전략과 상세 설정을 지정합니다.
// 설정하지 않으면 합리적인 기본값("pool", size 10)이 사용됩니다.
func WithWorker(strategyType string, poolSize int, jobTimeout time.Duration) Option {
	return func(cfg *Config) error {
		if strategyType == "" {
			return &ConfigError{"worker strategy type cannot be empty"}
		}
		if poolSize <= 0 {
			return &ConfigError{"worker pool size must be positive"}
		}
		if jobTimeout <= 0 {
			return &ConfigError{"worker job timeout must be positive"}
		}
		cfg.WorkerStrategy = strategyType
		cfg.WorkerPoolSize = poolSize
		cfg.WorkerJobTimeout = jobTimeout
		return nil
	}
}

// WithDefaultTimeout은 Get, Set 등 캐시 작업에 적용될 기본 타임아웃을 설정합니다.
func WithDefaultTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return &ConfigError{"default timeout must be positive"}
		}
		cfg.DefaultTimeout = timeout
		return nil
	}
}

// WithNegativeCaching은 부정 캐시를 활성화하고, 그 유효 기간(TTL)을 설정합니다.
// TTL을 0보다 큰 값으로 설정하면 기능이 활성화됩니다.
func WithNegativeCaching(ttl time.Duration) Option {
	return func(cfg *Config) error {
		if ttl < 0 {
			return &ConfigError{"negative cache TTL cannot be negative"}
		}
		cfg.NegativeCacheTTL = ttl
		return nil
	}
}
