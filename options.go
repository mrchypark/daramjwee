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
type Config struct {
	Tiers []Store

	WorkerStrategy   string
	WorkerPoolSize   int
	WorkerQueueSize  int
	WorkerJobTimeout time.Duration

	DefaultTimeout  time.Duration
	ShutdownTimeout time.Duration

	TierPositiveFreshFor time.Duration
	TierNegativeFreshFor time.Duration
}

// Option is a function type that modifies the Config.
type Option func(cfg *Config) error

// WithTiers sets the regular cache tiers in top-to-bottom order.
func WithTiers(stores ...Store) Option {
	return func(cfg *Config) error {
		cfg.Tiers = append([]Store(nil), stores...)
		return nil
	}
}

// WithWorker specifies the worker strategy and detailed settings for background tasks.
// If not set, the defaults are strategy "pool", size 1, queue size 500.
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

// WithTierFreshness sets the freshness duration for positive and negative
// cache entries across the whole ordered tier chain.
func WithTierFreshness(positive, negative time.Duration) Option {
	return func(cfg *Config) error {
		if positive < 0 {
			return &ConfigError{"tier positive cache TTL cannot be a negative value"}
		}
		if negative < 0 {
			return &ConfigError{"tier negative cache TTL cannot be a negative value"}
		}
		cfg.TierPositiveFreshFor = positive
		cfg.TierNegativeFreshFor = negative
		return nil
	}
}

// WithCache sets the positive freshness duration across the whole tier chain.
// If freshFor is 0, the cache entry is considered stale immediately.
func WithCache(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"positive cache TTL cannot be a negative value"}
		}
		cfg.TierPositiveFreshFor = freshFor
		return nil
	}
}

// WithNegativeCache sets the negative freshness duration across the whole tier chain.
// If freshFor is 0, the cache entry is considered stale immediately.
func WithNegativeCache(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"negative cache TTL cannot be a negative value"}
		}
		cfg.TierNegativeFreshFor = freshFor
		return nil
	}
}
