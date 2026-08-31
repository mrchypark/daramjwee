package daramjwee

import (
	"io"
	"sync"
)

// Outcome describes how a read session ended.
type Outcome int

const (
	OutcomeEOF Outcome = iota
	OutcomeEarlyClose
	OutcomeReadError
	OutcomeCancelled
	OutcomePreempted
)

// ReadSession releases resources owned by one Get call exactly once.
type ReadSession struct {
	source      io.ReadCloser
	setupCancel func()
	fence       *topWriteGeneration
	missRelease func()
	publishDone chan struct{}
	finishOnce  sync.Once
}

func newReadSession(source io.ReadCloser, setupCancel func(), fence *topWriteGeneration) *ReadSession {
	return &ReadSession{
		source:      source,
		setupCancel: setupCancel,
		fence:       fence,
		publishDone: make(chan struct{}),
	}
}

// SetMissLeader arranges for miss leadership to be released with the session.
func (s *ReadSession) SetMissLeader(_ *missLead, release func()) {
	s.missRelease = release
}

// Finish releases all resources. Repeated calls are harmless.
func (s *ReadSession) Finish(outcome Outcome) error {
	s.finishOnce.Do(func() {
		if s.source != nil && outcome != OutcomeEOF {
			_ = s.source.Close()
		}
		if s.missRelease != nil {
			s.missRelease()
		}
		if s.fence != nil {
			s.fence.release()
		}
		if s.setupCancel != nil {
			s.setupCancel()
		}
		if s.publishDone != nil {
			close(s.publishDone)
		}
	})
	return nil
}

// WaitForPublish blocks until Finish is called.
func (s *ReadSession) WaitForPublish() {
	if s.publishDone != nil {
		<-s.publishDone
	}
}

type outcomeReader struct {
	io.ReadCloser
	session *ReadSession
}

func newOutcomeReader(source io.ReadCloser, session *ReadSession) io.ReadCloser {
	return &outcomeReader{ReadCloser: source, session: session}
}

func (r *outcomeReader) Close() error {
	err := r.ReadCloser.Close()
	_ = r.session.Finish(OutcomeEOF)
	return err
}
