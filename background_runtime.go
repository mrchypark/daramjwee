package daramjwee

import (
	"time"

	"github.com/mrchypark/daramjwee/internal/worker"
)

type cacheConstructionMode int

const (
	cacheConstructionStandalone cacheConstructionMode = iota
	cacheConstructionGroup
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

type CacheRuntimeConfig struct {
	Weight     int
	QueueLimit int
}

type backgroundRuntime interface {
	Register(cacheID string, cfg CacheRuntimeConfig) error
	Submit(cacheID string, kind JobKind, job worker.Job) bool
	CloseCache(cacheID string, timeout time.Duration) error
	RemoveCache(cacheID string)
	Shutdown(timeout time.Duration) error
}
