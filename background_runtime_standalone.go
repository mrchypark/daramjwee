package daramjwee

import (
	"sync"
	"time"

	"github.com/mrchypark/daramjwee/internal/worker"
)

type standaloneRuntime struct {
	manager *worker.Manager
	once    sync.Once
}

func newStandaloneRuntime(manager *worker.Manager) backgroundRuntime {
	return &standaloneRuntime{manager: manager}
}

func (r *standaloneRuntime) Register(string, CacheRuntimeConfig) error {
	return nil
}

func (r *standaloneRuntime) Submit(_ string, _ JobKind, job worker.Job) bool {
	if r == nil || r.manager == nil {
		return false
	}
	return r.manager.Submit(job)
}

func (r *standaloneRuntime) CloseCache(_ string, timeout time.Duration) error {
	return r.Shutdown(timeout)
}

func (r *standaloneRuntime) RemoveCache(_ string) {}

func (r *standaloneRuntime) Shutdown(timeout time.Duration) error {
	if r == nil || r.manager == nil {
		return nil
	}
	var shutdownErr error
	r.once.Do(func() {
		shutdownErr = r.manager.Shutdown(timeout)
	})
	return shutdownErr
}
