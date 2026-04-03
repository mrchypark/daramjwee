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

	WorkerStrategy string
	Workers        int
	WorkerQueue    int
	WorkerTimeout  time.Duration

	OpTimeout    time.Duration
	CloseTimeout time.Duration

	PositiveFreshness      time.Duration
	NegativeFreshness      time.Duration
	TierFreshnessOverrides map[int]TierFreshnessOverride
}

type TierFreshnessOverride struct {
	Positive time.Duration
	Negative time.Duration
}

// Option is a function type that modifies the Config.
type Option func(cfg *Config) error

// WithTiers sets the regular cache tiers in top-to-bottom order.
// It replaces the entire tier chain, so any WithTierFreshness override indices
// are validated against the final order supplied here.
func WithTiers(stores ...Store) Option {
	return func(cfg *Config) error {
		cfg.Tiers = append([]Store(nil), stores...)
		return nil
	}
}

// WithWorkerStrategy sets the background worker strategy.
func WithWorkerStrategy(strategy string) Option {
	return func(cfg *Config) error {
		if strategy == "" {
			return &ConfigError{"worker strategy cannot be empty"}
		}
		cfg.WorkerStrategy = strategy
		return nil
	}
}

// WithWorkers sets the number of background workers used for async jobs.
func WithWorkers(count int) Option {
	return func(cfg *Config) error {
		if count <= 0 {
			return &ConfigError{"worker count must be positive"}
		}
		cfg.Workers = count
		return nil
	}
}

// WithWorkerQueue sets the queue capacity used for async background jobs.
func WithWorkerQueue(size int) Option {
	return func(cfg *Config) error {
		if size <= 0 {
			return &ConfigError{"worker queue size must be positive"}
		}
		cfg.WorkerQueue = size
		return nil
	}
}

// WithWorkerTimeout sets the timeout applied to each background job.
func WithWorkerTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return &ConfigError{"worker job timeout must be positive"}
		}
		cfg.WorkerTimeout = timeout
		return nil
	}
}

// WithOpTimeout sets the setup-stage timeout for cache operations like Get and Set.
func WithOpTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return &ConfigError{"operation timeout must be positive"}
		}
		cfg.OpTimeout = timeout
		return nil
	}
}

// WithCloseTimeout sets the timeout for graceful shutdown of the cache.
func WithCloseTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return &ConfigError{"close timeout must be positive"}
		}
		cfg.CloseTimeout = timeout
		return nil
	}
}

// WithFreshness sets the chain-wide default freshness duration for positive and
// negative cache entries across the ordered tier chain.
func WithFreshness(positive, negative time.Duration) Option {
	return func(cfg *Config) error {
		if positive < 0 {
			return &ConfigError{"positive freshness cannot be a negative value"}
		}
		if negative < 0 {
			return &ConfigError{"negative freshness cannot be a negative value"}
		}
		cfg.PositiveFreshness = positive
		cfg.NegativeFreshness = negative
		return nil
	}
}

// WithTierFreshness overrides the positive and negative freshness durations for
// a specific tier index. The configured values override the chain-wide default.
func WithTierFreshness(index int, positive, negative time.Duration) Option {
	return func(cfg *Config) error {
		if index < 0 {
			return &ConfigError{"tier index cannot be negative"}
		}
		if positive < 0 {
			return &ConfigError{"positive freshness cannot be a negative value"}
		}
		if negative < 0 {
			return &ConfigError{"negative freshness cannot be a negative value"}
		}
		if cfg.TierFreshnessOverrides == nil {
			cfg.TierFreshnessOverrides = make(map[int]TierFreshnessOverride)
		}
		cfg.TierFreshnessOverrides[index] = TierFreshnessOverride{
			Positive: positive,
			Negative: negative,
		}
		return nil
	}
}
