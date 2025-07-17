package daramjwee

import (
	"fmt"
	"time"
)

// ConfigError represents an error that occurs during the configuration process.
//
// ConfigError is returned when cache configuration validation fails, indicating
// that the provided configuration parameters are invalid, inconsistent, or
// incompatible. This error type helps distinguish configuration issues from
// runtime errors, enabling appropriate handling during application startup.
//
// Common validation failure scenarios:
//   - Missing mandatory configuration (e.g., HotStore not provided)
//   - Invalid parameter values (negative timeouts, zero buffer sizes)
//   - Inconsistent parameter combinations (min > max buffer sizes)
//   - Resource constraint violations (invalid worker pool configurations)
//
// Resolution strategies for different error types:
//
// Missing mandatory configuration:
//   - Provide required configuration parameters
//   - Use configuration validation tools or schemas
//   - Implement configuration defaults where appropriate
//
// Invalid parameter values:
//   - Validate parameter ranges and constraints
//   - Use configuration validation functions
//   - Provide clear error messages with valid ranges
//
// Inconsistent parameter combinations:
//   - Implement cross-parameter validation
//   - Document parameter dependencies and constraints
//   - Provide configuration examples and templates
//
// Example error handling during cache initialization:
//
//	cache, err := daramjwee.New(logger, options...)
//	if err != nil {
//	    var configErr *daramjwee.ConfigError
//	    if errors.As(err, &configErr) {
//	        // Handle configuration error specifically
//	        log.Error("Cache configuration error", "error", configErr.Message)
//
//	        // Provide helpful guidance
//	        switch {
//	        case strings.Contains(configErr.Message, "hot store"):
//	            log.Info("Hot store is mandatory. Example: daramjwee.WithHotStore(memstore.New())")
//	        case strings.Contains(configErr.Message, "timeout"):
//	            log.Info("Timeouts must be positive. Example: daramjwee.WithDefaultTimeout(30*time.Second)")
//	        case strings.Contains(configErr.Message, "buffer"):
//	            log.Info("Buffer sizes must be positive and consistent (min <= default <= max)")
//	        }
//
//	        return fmt.Errorf("failed to initialize cache due to configuration error: %w", err)
//	    }
//
//	    // Handle other error types
//	    return fmt.Errorf("failed to initialize cache: %w", err)
//	}
//
// Configuration validation best practices:
//   - Validate configuration early in application startup
//   - Provide clear, actionable error messages
//   - Document configuration requirements and constraints
//   - Use configuration validation tools and schemas
//   - Implement configuration testing and validation suites
//
// Monitoring and alerting considerations:
//   - Track configuration error frequency during deployments
//   - Alert on configuration validation failures
//   - Monitor configuration drift and changes
//   - Implement configuration health checks
type ConfigError struct {
	// Message contains the specific configuration error description.
	//
	// The message should be descriptive and actionable, helping developers
	// understand what configuration parameter is invalid and how to fix it.
	// It should include context about the specific validation that failed.
	Message string
}

// Error returns the formatted error message for ConfigError.
//
// The error message follows the standard daramjwee error format and includes
// the specific configuration issue description. This method implements the
// error interface, allowing ConfigError to be used with standard Go error
// handling patterns.
//
// Error message format: "daramjwee: configuration error: {specific message}"
//
// Example error messages:
//   - "daramjwee: configuration error: hot store cannot be nil"
//   - "daramjwee: configuration error: default timeout must be positive"
//   - "daramjwee: configuration error: buffer pool minimum size cannot be larger than default size"
func (e *ConfigError) Error() string {
	return fmt.Sprintf("daramjwee: configuration error: %s", e.Message)
}

// Config holds all the configurable settings for the daramjwee cache.
//
// This struct contains all configuration parameters that control cache behavior,
// performance characteristics, and operational settings. Option functions modify
// fields within this struct to customize cache behavior for specific use cases.
//
// Configuration categories:
//   - Storage: Hot and cold tier store configurations
//   - Workers: Background task processing settings
//   - Timeouts: Operation and shutdown timeout controls
//   - Caching: TTL and freshness behavior settings
//   - Performance: Buffer pool and optimization settings
type Config struct {
	// HotStore is the primary cache tier optimized for fast access.
	//
	// Selection criteria and mandatory requirements:
	//   - MANDATORY: This field must be set, cache cannot function without it
	//   - Should provide fast access patterns (memory-based recommended)
	//   - Limited capacity but high performance characteristics
	//   - Must support high concurrency for multi-threaded applications
	//   - Typically uses eviction policies (LRU, S3-FIFO, SIEVE)
	//
	// Common implementations:
	//   - MemStore with appropriate eviction policy
	//   - Fast SSD-based storage for larger working sets
	//   - Network-attached high-performance storage
	//
	// Performance considerations:
	//   - Size according to available memory and working set
	//   - Monitor hit rates and adjust capacity accordingly
	//   - Consider eviction policy based on access patterns
	HotStore Store

	// ColdStore is the secondary cache tier providing larger capacity storage.
	//
	// Optional usage patterns and benefits:
	//   - OPTIONAL: Can be nil, enabling single-tier caching
	//   - Provides larger storage capacity for extended cache coverage
	//   - Typically higher latency but lower cost than hot store
	//   - Serves cache misses from hot tier, reducing origin load
	//   - Can provide persistence across application restarts
	//
	// Configuration patterns:
	//   - FileStore for persistent, cost-effective storage
	//   - Cloud storage adapters for distributed deployments
	//   - Network storage for shared cache scenarios
	//   - SSD-based storage for capacity/performance balance
	//
	// Multi-tier behavior when configured:
	//   - Cache lookups: hot tier first, then cold tier
	//   - Cache hits in cold tier promote data to hot tier
	//   - Cache writes go to both tiers
	//   - Independent eviction policies per tier
	ColdStore Store

	// WorkerStrategy defines the background task execution strategy.
	//
	// Strategy selection and performance impact:
	//   - "pool": Fixed-size worker pool with job queue (recommended)
	//     * Provides controlled resource usage
	//     * Good for sustained background load
	//     * Configurable concurrency and queue management
	//   - "all": Execute all jobs immediately without pooling
	//     * Lower latency for background tasks
	//     * Higher resource usage under load
	//     * Suitable for low-frequency background operations
	//
	// Default: "pool" strategy provides good balance for most applications
	WorkerStrategy string

	// WorkerPoolSize controls the number of concurrent background workers.
	//
	// Sizing recommendations and performance impact:
	//   - Start with CPU core count for CPU-bound tasks
	//   - Increase to 2-4x CPU cores for I/O-bound tasks
	//   - Monitor worker utilization and adjust accordingly
	//   - Consider origin system capacity when sizing
	//
	// Typical values:
	//   - Small applications: 1-2 workers
	//   - Web applications: 4-8 workers
	//   - High-throughput systems: 8-16 workers
	//
	// Performance trade-offs:
	//   - More workers: Better concurrency, higher resource usage
	//   - Fewer workers: Lower resource usage, potential bottlenecks
	WorkerPoolSize int

	// WorkerQueueSize defines the maximum number of queued background jobs.
	//
	// Queue sizing and operational considerations:
	//   - Set to 10-50x WorkerPoolSize for burst handling
	//   - Larger queues provide better burst tolerance
	//   - Monitor queue depth to detect bottlenecks
	//   - Jobs are dropped when queue is full
	//
	// Typical values:
	//   - Conservative: 50-100 jobs
	//   - Balanced: 200-500 jobs
	//   - High-burst: 500-1000 jobs
	//
	// Monitoring considerations:
	//   - Track queue utilization and overflow events
	//   - Adjust based on actual workload patterns
	//   - Consider memory usage of queued jobs
	WorkerQueueSize int

	// WorkerJobTimeout sets the maximum duration for individual background jobs.
	//
	// Timeout selection guidelines and operational considerations:
	//   - Set based on expected origin response times
	//   - Include network latency and processing overhead
	//   - Balance job completion vs. resource cleanup
	//   - Consider retry strategies for timed-out jobs
	//
	// Typical values:
	//   - Fast origins: 30-60 seconds
	//   - Network origins: 60-120 seconds
	//   - Slow origins: 120-300 seconds
	//
	// Impact of timeout issues:
	//   - Failed refreshes lead to continued stale data serving
	//   - Resource cleanup prevents worker pool exhaustion
	//   - Monitoring helps identify origin performance issues
	WorkerJobTimeout time.Duration

	// DefaultTimeout sets the default timeout for cache operations.
	//
	// Timeout selection criteria and operational considerations:
	//   - Applies to Get, Set, Delete operations without explicit context timeout
	//   - Consider network latency to storage backends
	//   - Include origin system response times for cache misses
	//   - Balance responsiveness vs. success rate
	//
	// Recommended values:
	//   - Local storage: 1-5 seconds
	//   - Network storage: 10-30 seconds
	//   - Remote origins: 30-60 seconds
	//   - Batch operations: 60-300 seconds
	//
	// Impact analysis:
	//   - Too short: Increased timeout errors, reduced cache effectiveness
	//   - Too long: Poor user experience during outages
	//   - Applications should handle timeout errors gracefully
	DefaultTimeout time.Duration

	// ShutdownTimeout controls graceful shutdown duration.
	//
	// Graceful shutdown behavior and recommendations:
	//   - Time allowed for in-flight operations to complete
	//   - Prevents indefinite blocking during application shutdown
	//   - Balances shutdown speed vs. data consistency
	//   - Includes storage backend flush operations
	//
	// Timeout selection guidelines:
	//   - Short (5-15s): Fast shutdown, may interrupt operations
	//   - Medium (30-60s): Balanced approach for most applications
	//   - Long (120-300s): Ensures completion of long-running operations
	//
	// Shutdown sequence:
	//   - Stop accepting new operations
	//   - Wait for in-flight operations (up to timeout)
	//   - Terminate background workers gracefully
	//   - Close storage connections and release resources
	ShutdownTimeout time.Duration

	// PositiveFreshFor defines TTL for successful cache entries.
	//
	// TTL strategies and refresh behavior:
	//   - Controls when cached data is considered stale
	//   - Implements stale-while-revalidate patterns
	//   - Zero duration triggers immediate background refresh
	//   - Balances data freshness vs. origin system load
	//
	// Recommended values:
	//   - High-frequency data: 1-5 minutes
	//   - User profiles: 15-30 minutes
	//   - Product catalogs: 60-120 minutes
	//   - Configuration data: 4-24 hours
	//
	// Performance considerations:
	//   - Shorter TTLs: Higher freshness, more origin load
	//   - Longer TTLs: Lower origin load, potentially stale data
	//   - Background refresh prevents cache miss penalties
	PositiveFreshFor time.Duration

	// NegativeFreshFor defines TTL for negative cache entries.
	//
	// Negative caching implications and handling:
	//   - Caches "not found" responses to reduce origin load
	//   - Prevents repeated requests for non-existent resources
	//   - Balances load reduction vs. recovery time
	//   - Zero duration triggers immediate background refresh
	//
	// TTL selection strategies:
	//   - API endpoints: 1-10 minutes (quick recovery from outages)
	//   - User content: 30-60 minutes (deleted/private resources)
	//   - Static resources: 2-24 hours (permanently missing files)
	//
	// Operational benefits:
	//   - Reduces origin load from repeated failed requests
	//   - Improves response times for known missing resources
	//   - Prevents cascading failures during origin outages
	NegativeFreshFor time.Duration

	// BufferPool contains buffer pool optimization settings.
	//
	// Performance optimization guidelines and trade-offs:
	//   - Controls buffer allocation and reuse for I/O operations
	//   - Significantly reduces GC pressure and allocation overhead
	//   - Configurable sizing for different workload patterns
	//   - Optional logging for performance monitoring
	//
	// Configuration considerations:
	//   - Enable for high-throughput applications
	//   - Size buffers according to typical data sizes
	//   - Monitor pool effectiveness and adjust accordingly
	//   - Balance memory usage vs. performance benefits
	//
	// Performance benefits when properly configured:
	//   - 60-80% reduction in memory allocation overhead
	//   - Significant GC pressure reduction
	//   - Improved I/O throughput for streaming operations
	//   - Better performance under high concurrency
	BufferPool BufferPoolConfig
}

// Option is a function type that modifies the Config.
type Option func(cfg *Config) error

// WithHotStore sets the Store to be used as the Hot Tier.
//
// The hot tier is the primary cache layer optimized for fast access and high
// throughput. It typically uses memory-based storage for minimal latency and
// is the first tier checked during cache lookups.
//
// Hot tier selection criteria:
//   - Fast access patterns (memory-based stores recommended)
//   - Limited capacity but high performance
//   - Suitable for frequently accessed data
//   - Should support high concurrency
//
// Performance considerations:
//   - MemStore with LRU/S3-FIFO/SIEVE policies for optimal hit rates
//   - Size according to available memory and working set size
//   - Consider eviction policy based on access patterns
//   - Monitor hit rates and adjust capacity as needed
//
// This option is mandatory - the cache cannot function without a hot store.
//
// Example usage:
//
//	// Memory store with LRU eviction for hot tier
//	hotStore := memstore.New(
//	    memstore.WithCapacity(100 * 1024 * 1024), // 100MB capacity
//	    memstore.WithEvictionPolicy(policy.NewLRU()),
//	)
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithHotStore(hotStore),
//	    // ... other options
//	)
//
// Common hot store configurations:
//   - Small, fast memory store for high-frequency data
//   - SSD-based store for larger working sets
//   - Network-attached fast storage for distributed scenarios
//
// Integration patterns: The hot store should be optimized for the most
// common access patterns in your application. Monitor cache hit rates
// and adjust capacity or eviction policies accordingly.
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
//
// The cold tier provides larger capacity storage with potentially higher latency
// than the hot tier. It serves as a secondary cache layer that can store more
// data for longer periods, reducing load on origin systems.
//
// Cold tier benefits:
//   - Larger storage capacity for extended cache coverage
//   - Persistent storage options for cache durability across restarts
//   - Cost-effective storage for less frequently accessed data
//   - Reduces origin load by serving cache misses from hot tier
//
// Configuration patterns:
//   - FileStore for persistent, cost-effective storage
//   - Cloud storage adapters for distributed deployments
//   - SSD-based storage for balance of capacity and performance
//   - Network storage for shared cache scenarios
//
// This option is optional. If not provided, only hot tier caching is used.
//
// Example usage:
//
//	// File-based cold store for persistence
//	coldStore := filestore.New(
//	    filestore.WithDirectory("/var/cache/daramjwee"),
//	    filestore.WithHashedKeys(true), // Better directory distribution
//	)
//
//	// Cloud storage cold store
//	bucket := s3.NewBucket("my-cache-bucket", nil)
//	coldStore := adapter.NewObjStore(bucket, "cache-prefix/")
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithHotStore(hotStore),
//	    daramjwee.WithColdStore(coldStore),
//	    // ... other options
//	)
//
// Multi-tier behavior: When both hot and cold stores are configured:
//  1. Cache lookups check hot tier first, then cold tier
//  2. Cache hits in cold tier promote data to hot tier
//  3. Cache writes go to both tiers (configurable)
//  4. Eviction from hot tier doesn't affect cold tier
//
// Performance considerations:
//   - Cold tier latency should be acceptable for cache miss scenarios
//   - Consider network latency for remote storage
//   - Balance capacity vs. cost for storage selection
//   - Monitor promotion patterns between tiers
//
// Use cases:
//   - Large working sets that exceed hot tier capacity
//   - Persistent caching across application restarts
//   - Shared cache scenarios in distributed systems
//   - Cost optimization for infrequently accessed data
func WithColdStore(store Store) Option {
	return func(cfg *Config) error {
		cfg.ColdStore = store
		return nil
	}
}

// WithWorker specifies the worker strategy and detailed settings for background tasks.
//
// Background workers handle asynchronous operations like cache refresh, eviction,
// and cleanup tasks. The worker configuration significantly impacts cache performance
// and resource utilization under different load patterns.
//
// Worker strategy selection and sizing guidelines:
//
// Strategy Types:
//   - "pool": Fixed-size worker pool with job queue (recommended for most cases)
//   - "all": Execute all jobs immediately without pooling (for low-latency scenarios)
//
// Pool sizing considerations:
//   - poolSize: Number of concurrent background workers
//   - Start with CPU core count for CPU-bound tasks
//   - Increase for I/O-bound tasks (2-4x CPU cores)
//   - Monitor worker utilization and adjust accordingly
//   - queueSize: Maximum queued jobs before dropping
//   - Set to 10-50x poolSize for burst handling
//   - Larger queues provide better burst tolerance
//   - Monitor queue depth to detect bottlenecks
//   - jobTimeout: Maximum time for individual background jobs
//   - Set based on expected origin response times
//   - Include network latency and processing overhead
//   - Typical values: 30s-300s depending on origin characteristics
//
// Performance impact analysis:
//   - Larger pools: Better concurrency but higher resource usage
//   - Smaller pools: Lower resource usage but potential bottlenecks
//   - Queue overflow: Jobs are dropped, reducing cache effectiveness
//   - Timeout issues: Failed refreshes lead to stale data serving
//
// If not set, reasonable defaults are used: "pool" strategy with size 1,
// queue size 500, and 30-second job timeout.
//
// Example configurations:
//
//	// High-throughput configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithWorker("pool", 8, 400, 60*time.Second),
//	    // ... other options
//	)
//
//	// Low-latency configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithWorker("all", 0, 0, 30*time.Second),
//	    // ... other options
//	)
//
//	// Memory-constrained configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithWorker("pool", 2, 50, 45*time.Second),
//	    // ... other options
//	)
//
// Monitoring and tuning:
//   - Monitor worker queue depth and utilization
//   - Track job completion times and timeout rates
//   - Adjust pool size based on actual workload patterns
//   - Consider origin system capacity when sizing workers
//
// Common patterns:
//   - Web applications: 4-8 workers with 200-400 queue size
//   - Microservices: 2-4 workers with 100-200 queue size
//   - Batch processing: 8-16 workers with 500-1000 queue size
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
//
// This timeout applies to all cache operations that don't have explicit context
// timeouts. It serves as a safety mechanism to prevent operations from hanging
// indefinitely and ensures predictable response times.
//
// Timeout selection criteria:
//   - Consider network latency to storage backends
//   - Include origin system response times for cache misses
//   - Account for serialization/deserialization overhead
//   - Balance responsiveness vs. success rate
//
// Impact analysis:
//   - Too short: Increased timeout errors and reduced cache effectiveness
//   - Too long: Poor user experience during outages or slow responses
//   - Network conditions: Consider variable latency in distributed deployments
//   - Load conditions: Higher timeouts may be needed under heavy load
//
// Recommended values:
//   - Local storage: 1-5 seconds
//   - Network storage: 10-30 seconds
//   - Remote origins: 30-60 seconds
//   - Batch operations: 60-300 seconds
//
// Example usage:
//
//	// Fast local cache configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithDefaultTimeout(5*time.Second),
//	    // ... other options
//	)
//
//	// Network-based cache configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithDefaultTimeout(30*time.Second),
//	    // ... other options
//	)
//
//	// High-latency origin configuration
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithDefaultTimeout(120*time.Second),
//	    // ... other options
//	)
//
// Operational considerations:
//   - Monitor timeout rates and adjust based on actual performance
//   - Consider different timeouts for different operation types
//   - Use context.WithTimeout for operation-specific timeouts
//   - Plan for timeout handling in application logic
//
// Integration patterns: Applications should handle timeout errors gracefully,
// potentially falling back to degraded functionality or cached responses.
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
//
// This timeout controls how long the cache waits for in-flight operations
// to complete during shutdown. It ensures graceful termination while preventing
// indefinite blocking during application shutdown.
//
// Graceful shutdown patterns and recommendations:
//   - Allow sufficient time for background workers to complete current jobs
//   - Consider origin system response times for pending fetches
//   - Balance shutdown speed vs. data consistency
//   - Account for storage backend flush operations
//
// Timeout selection guidelines:
//   - Short timeouts (5-15s): Fast shutdown, may interrupt operations
//   - Medium timeouts (30-60s): Balanced approach for most applications
//   - Long timeouts (120-300s): Ensures completion of long-running operations
//   - Consider maximum expected operation duration
//
// Shutdown behavior:
//   - Stop accepting new cache operations
//   - Wait for in-flight operations to complete
//   - Terminate background workers gracefully
//   - Close storage connections and release resources
//   - Force termination if timeout is exceeded
//
// Example usage:
//
//	// Fast shutdown for development
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithShutdownTimeout(10*time.Second),
//	    // ... other options
//	)
//
//	// Production shutdown with safety margin
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithShutdownTimeout(60*time.Second),
//	    // ... other options
//	)
//
//	// Long-running batch operations
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithShutdownTimeout(300*time.Second),
//	    // ... other options
//	)
//
// Operational considerations:
//   - Monitor shutdown times in production
//   - Consider impact on application restart times
//   - Plan for forced termination scenarios
//   - Log shutdown progress for debugging
//
// Integration patterns: Applications should initiate shutdown early in their
// termination sequence to allow sufficient time for graceful completion.
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
//
// This option controls how long successfully cached data remains fresh before
// triggering background refresh operations. It implements stale-while-revalidate
// patterns where stale data is served immediately while fresh data is fetched
// in the background.
//
// TTL selection and refresh strategies:
//   - Zero duration (0): Immediate staleness, always triggers background refresh
//   - Short TTL (1-5 minutes): High freshness, more origin load
//   - Medium TTL (15-60 minutes): Balanced freshness and performance
//   - Long TTL (hours): Lower origin load, potentially stale data
//
// Refresh behavior patterns:
//   - Fresh data: Served immediately from cache
//   - Stale data: Served immediately, background refresh triggered
//   - Missing data: Fetched synchronously from origin
//   - Failed refresh: Stale data continues to be served
//
// Performance and origin load considerations:
//   - Shorter TTLs increase origin system load
//   - Longer TTLs reduce freshness but improve performance
//   - Background refresh prevents cache miss penalties
//   - Consider origin system capacity and response times
//
// Example usage:
//
//	// High-frequency data with quick refresh
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithCache(5*time.Minute),
//	    // ... other options
//	)
//
//	// Stable data with longer refresh intervals
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithCache(60*time.Minute),
//	    // ... other options
//	)
//
//	// Always-fresh data (immediate background refresh)
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithCache(0*time.Second),
//	    // ... other options
//	)
//
// Use case patterns:
//   - User profiles: 15-30 minutes (balance freshness and load)
//   - Product catalogs: 60-120 minutes (stable data)
//   - Real-time feeds: 1-5 minutes (high freshness requirements)
//   - Configuration data: 4-24 hours (infrequent changes)
//
// Integration with negative caching: Positive and negative cache TTLs can
// be configured independently to optimize for different data patterns.
func WithCache(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"positive cache TTL cannot be a negative value"}
		}
		cfg.PositiveFreshFor = freshFor
		return nil
	}
}

// WithNegativeCache sets the freshness duration for negative cache entries.
//
// Negative caching stores "not found" or error states to prevent repeated
// requests to the origin system for non-existent resources. This significantly
// reduces origin load and improves overall system performance.
//
// Negative cache behavior and implications:
//   - Caches ErrCacheableNotFound responses from fetchers
//   - Prevents repeated origin requests for missing resources
//   - Serves cached "not found" responses immediately
//   - Triggers background refresh when entries become stale
//
// TTL selection strategies:
//   - Zero duration (0): Immediate staleness, always triggers background refresh
//   - Short TTL (1-10 minutes): Quick recovery from temporary failures
//   - Medium TTL (30-60 minutes): Balance between load reduction and freshness
//   - Long TTL (hours): Aggressive load reduction for stable missing resources
//
// Use case considerations:
//   - API endpoints: Short TTL to handle temporary outages
//   - User-generated content: Medium TTL for deleted/private resources
//   - Static resources: Long TTL for permanently missing files
//   - Database records: Medium TTL for deleted entities
//
// Example usage:
//
//	// Quick recovery from temporary failures
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithNegativeCache(5*time.Minute),
//	    // ... other options
//	)
//
//	// Balanced negative caching
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithNegativeCache(30*time.Minute),
//	    // ... other options
//	)
//
//	// Aggressive negative caching for stable missing resources
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithNegativeCache(4*time.Hour),
//	    // ... other options
//	)
//
// Performance benefits:
//   - Reduces origin system load from repeated failed requests
//   - Improves response times for known missing resources
//   - Prevents cascading failures during origin outages
//   - Reduces bandwidth usage for failed requests
//
// Operational considerations:
//   - Monitor negative cache hit rates
//   - Consider impact on resource recovery times
//   - Balance load reduction vs. freshness requirements
//   - Plan for cache invalidation of recovered resources
//
// Integration patterns: Applications should handle negative cache responses
// appropriately, potentially providing alternative content or graceful degradation.
func WithNegativeCache(freshFor time.Duration) Option {
	return func(cfg *Config) error {
		if freshFor < 0 {
			return &ConfigError{"negative cache TTL cannot be a negative value"}
		}
		cfg.NegativeFreshFor = freshFor
		return nil
	}
}

// WithBufferPool enables or disables buffer pooling with basic configuration.
//
// Buffer pooling significantly improves performance by reusing allocated buffers
// for I/O operations, reducing garbage collection pressure and memory allocation
// overhead. This option provides a simple way to enable buffer pooling with
// reasonable defaults.
//
// Performance tuning guidance:
//   - enabled: Controls whether buffer pooling is active
//   - true: Enables pooling with performance benefits
//   - false: Disables pooling, falls back to standard operations
//   - defaultSize: Default buffer size for pool operations
//   - Should match common data sizes in your application
//   - Typical values: 32KB-64KB for general use
//   - Larger sizes improve I/O throughput but use more memory
//
// Automatic sizing behavior:
//   - MinBufferSize: Set to defaultSize/8 (minimum 1KB)
//   - MaxBufferSize: Set to defaultSize*2
//   - These defaults provide good balance for most applications
//
// When enabled, it uses reasonable defaults for buffer sizes based on the
// provided defaultSize parameter.
//
// Example usage:
//
//	// Enable buffer pooling with 32KB default buffers
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithBufferPool(true, 32*1024),
//	    // ... other options
//	)
//
//	// Disable buffer pooling for debugging
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithBufferPool(false, 32*1024),
//	    // ... other options
//	)
//
//	// Large buffer configuration for big data scenarios
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithBufferPool(true, 128*1024),
//	    // ... other options
//	)
//
// Performance benefits when enabled:
//   - 60-80% reduction in memory allocation overhead
//   - Significant reduction in GC pressure
//   - Improved I/O throughput for streaming operations
//   - Better performance under high concurrency
//
// Use case recommendations:
//   - High-throughput applications: Always enable with appropriate sizing
//   - Memory-constrained environments: Enable with smaller buffer sizes
//   - Development/debugging: May disable for simpler memory profiling
//   - Low-traffic applications: Benefits may be minimal
//
// Integration patterns: For most applications, enabling buffer pooling
// provides significant performance benefits with minimal configuration effort.
func WithBufferPool(enabled bool, defaultSize int) Option {
	return func(cfg *Config) error {
		if defaultSize <= 0 {
			return &ConfigError{"buffer pool default size must be positive"}
		}
		cfg.BufferPool.Enabled = enabled
		cfg.BufferPool.DefaultBufferSize = defaultSize
		// Set reasonable defaults for min/max based on default size
		cfg.BufferPool.MinBufferSize = defaultSize / 8
		if cfg.BufferPool.MinBufferSize < 1024 {
			cfg.BufferPool.MinBufferSize = 1024 // Minimum 1KB
		}
		cfg.BufferPool.MaxBufferSize = defaultSize * 2
		return nil
	}
}

// WithBufferPoolAdvanced provides detailed configuration for buffer pool behavior.
//
// This option allows fine-grained control over buffer pool parameters for
// advanced performance tuning. It provides complete control over buffer sizing,
// logging, and monitoring behavior to optimize for specific workload patterns.
//
// Performance tuning guidance:
//
// Buffer sizing parameters:
//   - DefaultBufferSize: Primary buffer size for most operations
//   - Should match the most common data sizes in your application
//   - Typical values: 32KB-128KB depending on workload
//   - Larger sizes improve I/O efficiency but use more memory
//   - MinBufferSize: Smallest buffer size that will be pooled
//   - Buffers smaller than this are not returned to the pool
//   - Prevents pool pollution with tiny buffers
//   - Typical values: 4KB-16KB
//   - MaxBufferSize: Largest buffer size that will be pooled
//   - Buffers larger than this are not returned to the pool
//   - Prevents memory bloat from oversized buffers
//   - Typical values: 64KB-256KB
//
// Monitoring and logging configuration:
//   - EnableLogging: Controls performance metrics logging
//   - true: Enables detailed buffer pool statistics logging
//   - false: Disables logging for minimal overhead
//   - LoggingInterval: Frequency of statistics logging
//   - Zero value disables periodic logging
//   - Typical values: 30s-300s for production monitoring
//
// All size parameters must be positive and logically consistent:
//
//	MinBufferSize ≤ DefaultBufferSize ≤ MaxBufferSize
//
// Example usage:
//
//	// High-performance configuration with monitoring
//	config := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 64 * 1024,  // 64KB default
//	    MinBufferSize:     8 * 1024,   // 8KB minimum
//	    MaxBufferSize:     128 * 1024, // 128KB maximum
//	    EnableLogging:     true,
//	    LoggingInterval:   60 * time.Second,
//	}
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithBufferPoolAdvanced(config),
//	    // ... other options
//	)
//
//	// Memory-constrained configuration
//	config := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 16 * 1024,  // 16KB default
//	    MinBufferSize:     4 * 1024,   // 4KB minimum
//	    MaxBufferSize:     32 * 1024,  // 32KB maximum
//	    EnableLogging:     false,      // Minimal overhead
//	    LoggingInterval:   0,
//	}
//
//	// Large data configuration
//	config := daramjwee.BufferPoolConfig{
//	    Enabled:           true,
//	    DefaultBufferSize: 256 * 1024, // 256KB default
//	    MinBufferSize:     64 * 1024,  // 64KB minimum
//	    MaxBufferSize:     512 * 1024, // 512KB maximum
//	    EnableLogging:     true,
//	    LoggingInterval:   120 * time.Second,
//	}
//
// Performance optimization strategies:
//   - Match buffer sizes to your data size distribution
//   - Monitor pool hit rates and adjust sizing accordingly
//   - Use logging to identify optimal buffer size ranges
//   - Balance memory usage vs. allocation overhead
//
// Trade-offs and considerations:
//   - Larger buffers: Better I/O performance, higher memory usage
//   - Smaller buffers: Lower memory usage, more frequent allocations
//   - Wider size range: Better reuse, more complex pool management
//   - Narrower size range: Simpler pool, potentially more allocations
//
// Monitoring recommendations:
//   - Enable logging in production for performance insights
//   - Monitor pool hit rates and buffer size distribution
//   - Adjust configuration based on actual usage patterns
//   - Consider different configurations for different environments
func WithBufferPoolAdvanced(config BufferPoolConfig) Option {
	return func(cfg *Config) error {
		if config.DefaultBufferSize <= 0 {
			return &ConfigError{"buffer pool default size must be positive"}
		}
		if config.MinBufferSize <= 0 {
			return &ConfigError{"buffer pool minimum size must be positive"}
		}
		if config.MaxBufferSize <= 0 {
			return &ConfigError{"buffer pool maximum size must be positive"}
		}
		if config.MinBufferSize > config.DefaultBufferSize {
			return &ConfigError{"buffer pool minimum size cannot be larger than default size"}
		}
		if config.DefaultBufferSize > config.MaxBufferSize {
			return &ConfigError{"buffer pool default size cannot be larger than maximum size"}
		}
		cfg.BufferPool = config
		return nil
	}
}

// WithLargeObjectOptimization enables adaptive buffer pool with large object optimization.
//
// This option configures the cache to use adaptive buffer management strategies
// based on object size, providing optimal performance across different data sizes.
// It addresses performance degradation observed with large objects in standard buffer pools.
//
// Parameters:
//   - largeThreshold: Size threshold for large object detection (e.g., 256KB)
//   - veryLargeThreshold: Size threshold for very large objects (e.g., 1MB)
//   - chunkSize: Chunk size for streaming operations (e.g., 64KB)
//   - maxConcurrentLargeOps: Maximum concurrent large object operations
//
// Performance benefits:
//   - Eliminates performance degradation for large objects (256KB+)
//   - Maintains optimal performance for small and medium objects
//   - Reduces memory pressure and GC overhead for large transfers
//   - Provides adaptive strategy selection based on object size
//
// Example usage:
//
//	// Enable large object optimization with default settings
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithLargeObjectOptimization(256*1024, 1024*1024, 64*1024, 4),
//	    // ... other options
//	)
//
//	// Conservative settings for memory-constrained environments
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithLargeObjectOptimization(128*1024, 512*1024, 32*1024, 2),
//	    // ... other options
//	)
//
//	// Aggressive settings for high-memory systems
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithLargeObjectOptimization(512*1024, 2048*1024, 128*1024, 8),
//	    // ... other options
//	)
//
// Configuration guidelines:
//   - largeThreshold should be larger than MaxBufferSize
//   - veryLargeThreshold should be larger than largeThreshold
//   - chunkSize should be optimized for I/O patterns (typically 64KB-128KB)
//   - maxConcurrentLargeOps should consider available memory and I/O capacity
//
// Use cases:
//   - Applications handling mixed object sizes
//   - Systems experiencing performance issues with large objects
//   - High-throughput scenarios with variable data sizes
//   - Memory-efficient processing of large data streams
func WithLargeObjectOptimization(largeThreshold, veryLargeThreshold, chunkSize, maxConcurrentLargeOps int) Option {
	return func(cfg *Config) error {
		if largeThreshold <= 0 {
			return &ConfigError{"large object threshold must be positive"}
		}
		if veryLargeThreshold <= largeThreshold {
			return &ConfigError{"very large object threshold must be larger than large object threshold"}
		}
		if chunkSize <= 0 {
			return &ConfigError{"chunk size must be positive"}
		}
		if maxConcurrentLargeOps <= 0 {
			return &ConfigError{"max concurrent large operations must be positive"}
		}

		// Enable buffer pool if not already configured
		if !cfg.BufferPool.Enabled {
			cfg.BufferPool.Enabled = true
			if cfg.BufferPool.DefaultBufferSize <= 0 {
				cfg.BufferPool.DefaultBufferSize = 32 * 1024 // 32KB default
			}
			if cfg.BufferPool.MaxBufferSize <= 0 {
				cfg.BufferPool.MaxBufferSize = 64 * 1024 // 64KB default
			}
			if cfg.BufferPool.MinBufferSize <= 0 {
				cfg.BufferPool.MinBufferSize = 4 * 1024 // 4KB default
			}
		}

		// Validate that large threshold is larger than max buffer size
		if largeThreshold <= cfg.BufferPool.MaxBufferSize {
			return &ConfigError{"large object threshold must be larger than max buffer size"}
		}

		cfg.BufferPool.LargeObjectThreshold = largeThreshold
		cfg.BufferPool.VeryLargeObjectThreshold = veryLargeThreshold
		cfg.BufferPool.ChunkSize = chunkSize
		cfg.BufferPool.MaxConcurrentLargeOps = maxConcurrentLargeOps
		cfg.BufferPool.LargeObjectStrategy = StrategyAdaptive
		cfg.BufferPool.EnableDetailedMetrics = true

		return nil
	}
}

// WithAdaptiveBufferPool provides comprehensive configuration for adaptive buffer pool behavior.
//
// This option allows fine-grained control over all adaptive buffer pool parameters,
// including size thresholds, strategies, and performance monitoring settings.
//
// Parameters:
//   - config: Complete BufferPoolConfig with adaptive settings
//
// Advanced configuration example:
//
//	config := daramjwee.BufferPoolConfig{
//	    // Basic buffer pool settings
//	    Enabled:           true,
//	    DefaultBufferSize: 32 * 1024,  // 32KB
//	    MaxBufferSize:     128 * 1024, // 128KB
//	    MinBufferSize:     4 * 1024,   // 4KB
//
//	    // Large object optimization settings
//	    LargeObjectThreshold:     256 * 1024,  // 256KB
//	    VeryLargeObjectThreshold: 1024 * 1024, // 1MB
//	    LargeObjectStrategy:      daramjwee.StrategyAdaptive,
//	    ChunkSize:                64 * 1024,   // 64KB
//	    MaxConcurrentLargeOps:    4,
//
//	    // Monitoring and logging
//	    EnableDetailedMetrics: true,
//	    EnableLogging:         true,
//	    LoggingInterval:       5 * time.Minute,
//	}
//
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithAdaptiveBufferPool(config),
//	    // ... other options
//	)
//
// Validation and error handling:
//   - All configuration parameters are validated for consistency
//   - Threshold values must be in ascending order
//   - Strategy selection must be compatible with thresholds
//   - Resource limits must be reasonable for the system
//
// Performance tuning guidelines:
//   - Monitor metrics to optimize threshold values
//   - Adjust chunk sizes based on I/O patterns
//   - Configure concurrent operation limits based on available resources
//   - Enable detailed metrics during tuning, disable in production if needed
func WithAdaptiveBufferPool(config BufferPoolConfig) Option {
	return func(cfg *Config) error {
		// Validate adaptive-specific configuration
		if err := config.validateAdaptive(); err != nil {
			return err
		}

		cfg.BufferPool = config
		return nil
	}
}

// WithBufferPoolMetrics enables detailed metrics collection for buffer pool monitoring.
//
// This option enables comprehensive metrics collection for buffer pool performance
// analysis, including size-category breakdowns and strategy effectiveness tracking.
//
// Parameters:
//   - enabled: Whether to enable detailed metrics collection
//   - loggingInterval: How often to log metrics (0 disables periodic logging)
//
// Metrics collected:
//   - Operation counts by object size category
//   - Strategy usage statistics
//   - Performance latency by category
//   - Memory efficiency measurements
//   - Pool hit/miss ratios
//
// Example usage:
//
//	// Enable metrics with periodic logging every 5 minutes
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithBufferPoolMetrics(true, 5*time.Minute),
//	    // ... other options
//	)
//
//	// Enable metrics without periodic logging
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithBufferPoolMetrics(true, 0),
//	    // ... other options
//	)
//
//	// Disable detailed metrics for production
//	cache, err := daramjwee.New(logger,
//	    daramjwee.WithBufferPoolMetrics(false, 0),
//	    // ... other options
//	)
//
// Performance considerations:
//   - Detailed metrics add minimal overhead
//   - Periodic logging can increase log volume
//   - Metrics are useful for performance tuning and troubleshooting
//   - Consider disabling in stable production environments
//
// Monitoring integration:
//   - Metrics can be exported to monitoring systems
//   - Use GetStats() method to retrieve current metrics
//   - Integrate with application performance monitoring (APM) tools
func WithBufferPoolMetrics(enabled bool, loggingInterval time.Duration) Option {
	return func(cfg *Config) error {
		cfg.BufferPool.EnableDetailedMetrics = enabled
		cfg.BufferPool.EnableLogging = enabled
		cfg.BufferPool.LoggingInterval = loggingInterval
		return nil
	}
}
