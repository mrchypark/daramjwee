// Package daramjwee provides core cache components and configuration options.
// This file defines the Config struct and Option functions used to configure
// a DaramjweeCache instance using the functional options pattern.
package daramjwee

import (
	"fmt"
	"time"
)

// ConfigError represents an error that occurs during the configuration process
// of the DaramjweeCache.
type ConfigError struct {
	Message string // Message describes the specific configuration error.
}

// Error implements the error interface for ConfigError.
// It returns a formatted string detailing the configuration error.
func (e *ConfigError) Error() string {
	return fmt.Sprintf("daramjwee: configuration error: %s", e.Message)
}

// Config holds all configurable parameters for a DaramjweeCache instance.
// Option functions are used to modify instances of this struct.
// The New function in daramjwee.go initializes this struct with defaults and then applies provided Option functions.
type Config struct {
	// HotStore is the primary, high-speed cache storage. This is a required field.
	HotStore Store
	// ColdStore is the secondary, slower but potentially larger cache storage. This is optional.
	// If not set, a nullStore (no-op store) will be used.
	ColdStore Store
	// WorkerStrategy specifies the strategy for managing background worker tasks (e.g., "pool", "all").
	// Defaults are handled in the New function if not specified via options.
	WorkerStrategy string
	// WorkerPoolSize is the number of worker goroutines if a pool-based strategy is used.
	// Relevant only if WorkerStrategy is "pool".
	WorkerPoolSize int
	// WorkerQueueSize is the buffer size of the job queue if a pool-based strategy is used.
	// Relevant only if WorkerStrategy is "pool".
	WorkerQueueSize int
	// WorkerJobTimeout is the maximum duration for each background job executed by a worker.
	WorkerJobTimeout time.Duration
	// DefaultTimeout specifies the default timeout for cache operations like Get and Set,
	// if no timeout is specified in the operation's context.
	DefaultTimeout time.Duration
}

// Option is a function type used to modify a Config struct.
// It follows the functional options pattern, allowing for flexible and clear configuration.
// Each Option function takes a pointer to a Config and returns an error if the option is invalid.
type Option func(cfg *Config) error

// WithHotStore sets the Store implementation to be used as the hot tier of the cache.
// This is a mandatory option; the cache cannot operate without a HotStore.
// Parameters:
//   store: The Store implementation for the hot tier. Must not be nil.
// Returns:
//   An Option function that applies this configuration.
func WithHotStore(store Store) Option {
	return func(cfg *Config) error {
		if store == nil {
			return &ConfigError{"hot store cannot be nil"}
		}
		cfg.HotStore = store
		return nil
	}
}

// WithColdStore sets the Store implementation to be used as the cold tier of the cache.
// This option is optional. If not provided, a nullStore (no-op) will be used for the cold tier.
// Parameters:
//   store: The Store implementation for the cold tier. Can be nil (which will result in nullStore).
// Returns:
//   An Option function that applies this configuration.
func WithColdStore(store Store) Option {
	return func(cfg *Config) error {
		// If store is nil, the New() function will default to a nullStore.
		// So, no explicit nil check erroring out here.
		cfg.ColdStore = store
		return nil
	}
}

// WithWorker configures the strategy and parameters for background worker tasks.
// This function sets the worker strategy type, pool size (if applicable),
// queue size (if applicable), and job timeout.
// Note: Default values for worker strategy and its parameters are typically applied in the New()
// function if this option is not used or if specific fields are not covered.
// The prompt mentioned more granular worker options like WithWorkerStrategyAll,
// WithWorkerStrategyPool, WithWorkerPoolConfig. These are not explicitly implemented here;
// this WithWorker function serves as a more general configurator for worker properties.
// Parameters:
//   strategyType: The type of worker strategy (e.g., "pool", "all").
//   poolSize: The number of workers in the pool (for "pool" strategy). Must be positive.
//   queueSize: The size of the job queue (for "pool" strategy).
//   jobTimeout: The timeout for individual worker jobs. Must be positive.
// Returns:
//   An Option function that applies this worker configuration.
func WithWorker(strategyType string, poolSize int, queueSize int, jobTimeout time.Duration) Option {
	return func(cfg *Config) error {
		if strategyType == "" {
			return &ConfigError{"worker strategy type cannot be empty"}
		}
		// poolSize and queueSize are only strictly validated if strategyType is "pool",
		// but positive jobTimeout is always good.
		// More specific validation might occur in the NewManager function for workers.
		if jobTimeout <= 0 {
			return &ConfigError{"worker job timeout must be positive"}
		}
		// A poolSize of 0 or negative might be problematic for "pool" strategy.
		// The worker.NewManager or individual strategy constructors should handle defaults or errors for these.
		// For now, we'll ensure poolSize is at least not negative if set.
		if poolSize < 0 {
			return &ConfigError{"worker pool size cannot be negative"}
		}
		if queueSize < 0 {
			return &ConfigError{"worker queue size cannot be negative"}
		}

		cfg.WorkerStrategy = strategyType
		cfg.WorkerPoolSize = poolSize
		cfg.WorkerQueueSize = queueSize
		cfg.WorkerJobTimeout = jobTimeout
		return nil
	}
}

// WithDefaultTimeout sets the default timeout duration for cache operations (e.g., Get, Set).
// This timeout is used if the context passed to an operation does not have its own deadline.
// Parameters:
//   timeout: The default timeout duration. Must be positive.
// Returns:
//   An Option function that applies this default timeout configuration.
func WithDefaultTimeout(timeout time.Duration) Option {
	return func(cfg *Config) error {
		if timeout <= 0 {
			return &ConfigError{"default timeout must be positive"}
		}
		cfg.DefaultTimeout = timeout
		return nil
	}
}
