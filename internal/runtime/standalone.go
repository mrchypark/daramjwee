package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/mrchypark/daramjwee/internal/worker"
)

type Standalone struct {
	manager *worker.Manager
	once    sync.Once
}

func NewStandalone(manager *worker.Manager) Runtime {
	return &Standalone{manager: manager}
}

func (r *Standalone) Register(string, Config) error {
	return nil
}

func (r *Standalone) Submit(_ string, _ JobKind, job Job) error {
	if r == nil || r.manager == nil {
		if job.Discard != nil {
			job.Discard(DropReasonRejected)
		}
		return ErrRejected
	}

	wrappedJob := func(ctx context.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				// On panic, the job is considered incomplete.
				// We re-panic to let the worker recover, but do NOT call Discard
				// to maintain exact-once semantics (Run was called, so Discard must not be).
				panic(rec)
			}
		}()
		job.Run(ctx)
	}

	if !r.manager.Submit(wrappedJob) {
		if job.Discard != nil {
			job.Discard(DropReasonRejected)
		}
		return ErrRejected
	}
	return nil
}

func (r *Standalone) CloseCache(_ string, timeout time.Duration) error {
	return r.Shutdown(timeout)
}

func (r *Standalone) RemoveCache(_ string) {}

func (r *Standalone) Shutdown(timeout time.Duration) error {
	if r == nil || r.manager == nil {
		return nil
	}
	var shutdownErr error
	r.once.Do(func() {
		shutdownErr = r.manager.Shutdown(timeout)
	})
	return shutdownErr
}
