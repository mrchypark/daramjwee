# Observability

This document describes the observability features in daramjwee.

## Logging

daramjwee uses `go-kit/log` for structured logging.

### Log Levels

- **Debug**: Detailed information for debugging
- **Info**: General operational information
- **Warn**: Warning conditions
- **Error**: Error conditions

### Configuration

```go
import (
    "github.com/go-kit/log"
    "github.com/go-kit/log/level"
)

logger := log.NewLogfmtLogger(os.Stderr)
logger = level.NewFilter(logger, level.AllowDebug())

cache, err := daramjwee.New(logger, ...)
```

### Log Format

All logs are structured with key-value pairs:

```
level=info msg="daramjwee cache initialized" op_timeout=5s
level=debug msg="top tier hit" key=my-key
level=warn msg="failed to schedule stale refresh" key=my-key err="worker queue full"
level=error msg="background fetch failed" key=my-key err="connection timeout"
```

### Diagnostic Logging

For detailed debugging, enable diagnostic logging via environment variables:

```bash
export DJ_CACHE_DIAGNOSTICS=1
# or
export DJ_REPRO_CACHE_STUCK=1
```

Diagnostic logs include generation information:

```go
c.diagnosticLog("event", key, generation, keyvals...)
```

## Metrics

### Metrics Abstraction

daramjwee provides a metrics abstraction layer for integration with various metrics systems:

```go
// MetricsCollector defines the interface for collecting cache metrics.
type MetricsCollector interface {
    // Cache operations
    IncrCacheHit(tier int)
    IncrCacheMiss()
    IncrCacheError(operation string)
    
    // Timing
    ObserveGetLatency(duration time.Duration, tier int)
    ObserveSetLatency(duration time.Duration)
    ObserveDeleteLatency(duration time.Duration)
    
    // Background operations
    IncrRefreshScheduled()
    IncrRefreshCompleted()
    IncrRefreshFailed()
    IncrPersistScheduled()
    IncrPersistCompleted()
    IncrPersistFailed()
    
    // Worker pool
    SetActiveWorkers(count int)
    SetQueueDepth(count int)
}
```

### Built-in Metrics

daramjwee exposes metrics through the logging system. Key metrics include:

#### Cache Operations

- **Get**: Cache read operations
  - Hit/miss ratio
  - Tier hit distribution
  - Stale hit count

- **Set**: Cache write operations
  - Success/failure count
  - Write latency

- **Delete**: Cache delete operations
  - Success/failure count
  - Multi-tier delete latency

#### Background Operations

- **Refresh**: Background refresh jobs
  - Scheduled count
  - Completed count
  - Failed count

- **Persist**: Background persistence jobs
  - Scheduled count
  - Completed count
  - Failed count

#### Worker Pool

- **Active workers**: Number of currently active workers
- **Queue depth**: Number of pending jobs
- **Job latency**: Time spent in queue + execution

### Custom Metrics Integration

To integrate with metrics systems (Prometheus, StatsD, etc.):

```go
// Example: Prometheus metrics
var (
    cacheHits = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "daramjwee_cache_hits_total",
            Help: "Total number of cache hits",
        },
        []string{"tier"},
    )
    cacheMisses = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "daramjwee_cache_misses_total",
            Help: "Total number of cache misses",
        },
    )
)

// Custom logger that also records metrics
type metricsLogger struct {
    next log.Logger
}

func (l *metricsLogger) Log(keyvals ...interface{}) error {
    // Extract metrics from keyvals
    // Record to Prometheus
    // Forward to underlying logger
    return l.next.Log(keyvals...)
}
```

## Tracing

### Built-in Tracing

daramjwee includes built-in trace events for debugging:

```go
r.traceEvent("stream_through_write_to_eof", "bytes", written)
r.traceEvent("stream_through_write_to_sink_error", "bytes", written, "err", err)
r.traceEvent("stream_through_write_to_read_error", "bytes", written, "err", err)
```

### OpenTelemetry Integration

To integrate with OpenTelemetry:

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/trace"
)

// Create a custom logger that creates spans
type otelLogger struct {
    tracer trace.Tracer
    next   log.Logger
}

func (l *otelLogger) Log(keyvals ...interface{}) error {
    // Create span for significant events
    ctx, span := l.tracer.Start(context.Background(), "cache.operation")
    defer span.End()
    
    // Add attributes from keyvals
    // ...
    
    return l.next.Log(keyvals...)
}
```

## Debugging

### Common Issues

#### Cache Misses

If you see unexpected cache misses:

1. Check freshness configuration
2. Verify tier ordering
3. Check for concurrent deletes

#### Background Refresh Failures

If background refreshes are failing:

1. Check worker pool configuration
2. Verify origin connectivity
3. Check queue depth for backpressure

#### Memory Leaks

If memory usage is growing:

1. Check for unclosed responses
2. Verify pool buffer sizes
3. Monitor goroutine count

### Debug Environment Variables

```bash
# Enable cache diagnostics
export DJ_CACHE_DIAGNOSTICS=1

# Enable stuck detection
export DJ_REPRO_CACHE_STUCK=1

# Enable race detection (compile time)
go build -race
```

### Health Checks

Implement health checks by verifying:

1. Cache is not closed
2. Worker pool is accepting jobs
3. Tiers are accessible

```go
func healthCheck(cache daramjwee.Cache) error {
    // Simple health check
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    
    _, err := cache.Get(ctx, "health-check", daramjwee.GetRequest{}, nil)
    if err == daramjwee.ErrNilFetcher {
        return nil // Cache is responsive
    }
    return err
}
```

## Best Practices

1. **Use structured logging**: Always use key-value pairs for log messages.

2. **Set appropriate log level**: Use Debug for development, Info for production.

3. **Monitor worker pool**: Watch for queue saturation and job failures.

4. **Implement health checks**: Verify cache responsiveness in load balancer health probes.

5. **Use diagnostic logging**: Enable for debugging specific issues, disable in production.
