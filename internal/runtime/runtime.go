package runtime

import (
	"time"

	"github.com/mrchypark/daramjwee/internal/worker"
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

type Runtime interface {
	Register(cacheID string, cfg Config) error
	Submit(cacheID string, kind JobKind, job worker.Job) bool
	SubmitWithDropCleanup(cacheID string, kind JobKind, job worker.Job, onDrop func()) bool
	CloseCache(cacheID string, timeout time.Duration) error
	RemoveCache(cacheID string)
	Shutdown(timeout time.Duration) error
}
