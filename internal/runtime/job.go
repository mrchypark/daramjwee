package runtime

import (
	"context"
	"errors"
)

// ErrRejected is returned by Runtime.Submit when the job was not accepted.
// The caller must handle cleanup by calling job.Discard(DropReasonRejected).
var ErrRejected = errors.New("runtime: job rejected")

// ErrShutdownTimeout is returned when a cache close or shutdown times out.
var ErrShutdownTimeout = errors.New("runtime: shutdown timed out")

// DropReason describes why a submitted job was discarded instead of executed.
type DropReason int

const (
	// DropReasonRejected indicates the runtime rejected the job (queue full, cache closed, etc.)
	DropReasonRejected DropReason = iota
	// DropReasonShutdown indicates the runtime was shutting down when the job was submitted or dequeued.
	DropReasonShutdown
)

// Job represents a unit of background work with explicit terminal semantics.
//
// Contract: after Runtime.Submit returns nil, exactly one of Run or Discard
// will be called by the runtime. The caller must NOT call Run or Discard
// directly — they are invoked exclusively by the runtime.
//
// Run receives a context that is cancelled when the job's deadline expires.
// Discard is called exactly once when the job cannot be executed.
type Job struct {
	// Run executes the job. The context is cancelled on timeout or shutdown.
	Run func(ctx context.Context)

	// Discard is called exactly once when the job is rejected or dropped
	// during shutdown. The reason indicates why the job was not executed.
	Discard func(DropReason)
}
