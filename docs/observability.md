# Observability

daramjwee emits structured key/value logs through `go-kit/log`. Pass a filtered
logger to `New` or `NewGroup` to control verbosity:

```go
logger := log.NewLogfmtLogger(os.Stderr)
logger = level.NewFilter(logger, level.AllowDebug())
cache, err := daramjwee.New(logger, /* stores and options */)
```

Operational logs cover tier hits and misses, background refresh/persist
scheduling and completion, queue rejection, and generation-invalidated writes.
Set `DJ_CACHE_DIAGNOSTICS=1` (or `DJ_REPRO_CACHE_STUCK=1`) for additional
generation and stream-lifecycle diagnostics while investigating a problem.

There is currently no public `MetricsCollector` interface and no built-in
Prometheus/StatsD exporter. Derive metrics from structured logs or instrument
the calling application until a measured need justifies a stable metrics API.
