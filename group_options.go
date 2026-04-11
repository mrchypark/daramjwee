package daramjwee

import "time"

// GroupConfig holds the shared runtime defaults used by caches created from a CacheGroup.
type GroupConfig struct {
	Workers            int
	WorkerTimeout      time.Duration
	WorkerQueueDefault int
	CloseTimeout       time.Duration
}

// GroupOption configures a CacheGroup.
type GroupOption func(*GroupConfig) error

// WithGroupWorkers sets the number of shared runtime workers used by caches in a group.
func WithGroupWorkers(count int) GroupOption {
	return func(cfg *GroupConfig) error {
		if count <= 0 {
			return &ConfigError{"group worker count must be positive"}
		}
		cfg.Workers = count
		return nil
	}
}

// WithGroupWorkerTimeout sets the timeout applied to each shared runtime worker job.
func WithGroupWorkerTimeout(timeout time.Duration) GroupOption {
	return func(cfg *GroupConfig) error {
		if timeout <= 0 {
			return &ConfigError{"group worker job timeout must be positive"}
		}
		cfg.WorkerTimeout = timeout
		return nil
	}
}

// WithGroupWorkerQueueDefault sets the default queue capacity used for group-attached caches.
func WithGroupWorkerQueueDefault(size int) GroupOption {
	return func(cfg *GroupConfig) error {
		if size <= 0 {
			return &ConfigError{"group worker queue size must be positive"}
		}
		cfg.WorkerQueueDefault = size
		return nil
	}
}

// WithGroupCloseTimeout sets the timeout for graceful shutdown of the shared cache group.
func WithGroupCloseTimeout(timeout time.Duration) GroupOption {
	return func(cfg *GroupConfig) error {
		if timeout <= 0 {
			return &ConfigError{"group close timeout must be positive"}
		}
		cfg.CloseTimeout = timeout
		return nil
	}
}
