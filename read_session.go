package daramjwee

import (
	"io"
	"sync"
)

// Outcome describes how a read session terminated.
type Outcome int

const (
	// OutcomeEOF indicates the source was read to completion.
	OutcomeEOF Outcome = iota
	// OutcomeEarlyClose indicates the caller closed the body before reading to EOF.
	OutcomeEarlyClose
	// OutcomeReadError indicates a read error occurred before EOF.
	OutcomeReadError
	// OutcomeCancelled indicates the context was cancelled.
	OutcomeCancelled
	// OutcomePreempted indicates the fill was preempted by a write.
	OutcomePreempted
)

// ReadSession owns all request-scoped resources for a single Get operation.
// It consolidates cancellation, generation references, and miss leadership
// into a single ownership object.
//
// The session is created at the start of a Get call and must be finished
// exactly once, either by the Get method (on error or non-body response)
// or by the response body wrapper (on body close).
type ReadSession struct {
	// source is the original body read closer from the store or fetcher.
	source io.ReadCloser

	// setupCancel cancels the setup context used for lower-tier lookups.
	setupCancel func()

	// fence holds the generation reference for this request.
	fence *topWriteGeneration

	// missTicket holds the miss leadership, if this request is the leader.
	missTicket *missLead

	// missRelease is called to release the miss leader registration.
	missRelease func()

	// publishDone is closed when a streaming fill completes (EOF or error).
	publishDone chan struct{}

	// finishOnce ensures Finish is called exactly once.
	finishOnce sync.Once

	// outcome records how the session terminated.
	outcome Outcome

	// err captures the first error encountered during finish.
	err error
}

// newReadSession creates a new ReadSession with the given resources.
func newReadSession(source io.ReadCloser, setupCancel func(), fence *topWriteGeneration) *ReadSession {
	return &ReadSession{
		source:      source,
		setupCancel: setupCancel,
		fence:       fence,
		publishDone: make(chan struct{}),
	}
}

// SetMissLeader attaches miss leadership to this session.
// When the session finishes, the miss leader will be released.
func (s *ReadSession) SetMissLeader(lead *missLead, release func()) {
	s.missTicket = lead
	s.missRelease = release
}

// Finish terminates the session exactly once. It releases all owned resources.
// The outcome indicates how the session ended.
//
// Finish must be called exactly once:
//   - By the Get method on error or non-body response
//   - By the response body wrapper on body close
func (s *ReadSession) Finish(outcome Outcome) error {
	s.finishOnce.Do(func() {
		s.outcome = outcome
		s.release()
	})
	return s.err
}

// release performs the actual cleanup.
func (s *ReadSession) release() {
	// Close the source if it hasn't been consumed to EOF.
	if s.source != nil && s.outcome != OutcomeEOF {
		_ = s.source.Close()
	}

	// Release miss leadership.
	if s.missRelease != nil {
		s.missRelease()
	}

	// Release generation reference.
	if s.fence != nil {
		s.fence.release()
	}

	// Cancel the setup context.
	if s.setupCancel != nil {
		s.setupCancel()
	}

	// Signal publish completion for waiters.
	if s.publishDone != nil {
		close(s.publishDone)
	}
}

// WaitForPublish blocks until the session's publish is complete.
// This is used by miss waiters to wait for the leader's fill to become visible.
func (s *ReadSession) WaitForPublish() {
	if s.publishDone != nil {
		<-s.publishDone
	}
}

// outcomeReader wraps the session's source and signals publish completion
// on close. This is used for streaming responses.
type outcomeReader struct {
	io.ReadCloser
	session *ReadSession
}

// newOutcomeReader creates a reader that signals the session on close.
func newOutcomeReader(source io.ReadCloser, session *ReadSession) io.ReadCloser {
	return &outcomeReader{
		ReadCloser: source,
		session:    session,
	}
}

// Close closes the underlying reader and finishes the session with OutcomeEOF.
func (r *outcomeReader) Close() error {
	err := r.ReadCloser.Close()
	r.session.Finish(OutcomeEOF)
	return err
}
