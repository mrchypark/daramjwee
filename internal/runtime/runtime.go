package runtime

import (
	"time"
)

type JobKind int

const (
	JobKindRefresh JobKind = iota
	JobKindPersist
)

func (k JobKind) String() string {
	switch k {
	case JobKindRefresh:
		return "refresh"
	case JobKindPersist:
		return "persist"
	default:
		return "unknown"
	}
}

type Config struct {
	Weight     int
	QueueLimit int
}

// Runtime manages background jobs for one or more caches.
//
// Submit enqueues a job. On success (nil error returned), the runtime guarantees
// that exactly one of job.Run or job.Discard will be called. On error, the caller
// must handle cleanup themselves (typically by calling job.Discard(reason)).
type Runtime interface {
	Register(cacheID string, cfg Config) error
	Submit(cacheID string, kind JobKind, job Job) error
	CloseCache(cacheID string, timeout time.Duration) error
	RemoveCache(cacheID string)
	Shutdown(timeout time.Duration) error
}
