package daramjwee

import (
	"fmt"
	"time"
)

// ConfigError represents an error that occurs during the configuration process.
type ConfigError struct {
	Message string
}

// Error returns the error message for ConfigError.
func (e *ConfigError) Error() string {
	return fmt.Sprintf("daramjwee: configuration error: %s", e.Message)
}

// Config holds all the configurable settings for the daramjwee cache.
// Option functions modify fields within this struct.
type Config struct {
	HotStore  Store
	ColdStore Store

	WorkerStrategy   string
	WorkerPoolSize   int
	WorkerQueueSize  int
	WorkerJobTimeout time.Duration

	DefaultTimeout  time.Duration
	ShutdownTimeout time.Duration

	PositiveFreshFor time.Duration
	NegativeFreshFor time.Duration

	ColdStorePositiveFreshFor time.Duration
	ColdStoreNegativeFreshFor time.Duration
}

// Option is a function type that modifies the Config.
type Option func(cfg *Config) error

// WithHotStore sets the Store to be used as the Hot Tier.
// This option is mandatory.
func WithHotStore(store Store) Option {
	return func(cfg *Config) error {
		if store == nil {
			return &ConfigError{"hot store cannot be nil"}
		}
		cfg.HotStore = store
		return nil
	}
}

// WithColdStore sets the Store to be used as the Cold Tier.
// This option is optional.
func WithColdStore(store Store) Option {
	return func(cfg *Config) error {
		cfg.ColdStore = store
		return nil
	}
}

// WithWorker specifies the worker strategy and detailed settings for background tasks.
// If not set, reasonable defaults ("pool", size 10) are used.
func WithWorker(strategyType string, poolSize int, queueSize int, jobTimeout time.Duration) Option {
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
		cfg.WorkerQueueSize = queueSize
		cfg.WorkerJobTimeout = jobTimeout
		return nil
	}
}

// WithDefaultTimeout sets the default timeout for cache operations like Get and Set.
func WithDefaultTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return &ConfigError{"default timeout must be positive"}
		}
		cfg.DefaultTimeout = timeout
		return nil
	}
}

// WithShutdownTimeout sets the timeout for graceful shutdown of the cache.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return &ConfigError{"Shutdown timeout must be positive"}
		}
		cfg.ShutdownTimeout = timeout
		return nil
	}
}

// WithCache sets the freshness duration for positive cache entries.
// If freshFor is 0, the cache entry is considered stale immediately and a background refresh will be triggered on access.
func WithCache(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"positive cache TTL cannot be a negative value"}
		}
		cfg.PositiveFreshFor = freshFor
		return nil
	}
}

// WithNegativeCache sets the freshness duration for negative cache entries (e.g., for ErrNotFound).
// If freshFor is 0, the negative cache entry is considered stale immediately and a background refresh will be triggered on access.
func WithNegativeCache(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"negative cache TTL cannot be a negative value"}
		}
		cfg.NegativeFreshFor = freshFor
		return nil
	}
}

func WithColdStorePositiveFreshFor(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"cold store positive cache TTL cannot be a negative value"}
		}
		cfg.ColdStorePositiveFreshFor = freshFor
		return nil
	}
}

func WithColdStoreNegativeFreshFor(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"cold store negative cache TTL cannot be a negative value"}
		}
		cfg.ColdStoreNegativeFreshFor = freshFor
		return nil
	}
}
