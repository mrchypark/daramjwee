package daramjwee

import (
	"errors"
	"io"
	"sync"
	"time"
)

var streamCopyBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 32*1024)
	},
}

func writeAll(w io.Writer, p []byte) error {
	_, err := writeAllCount(w, p)
	return err
}

func writeAllCount(w io.Writer, p []byte) (int, error) {
	written := 0
	for written < len(p) {
		n, err := w.Write(p[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

type fillReadCloser struct {
	src       io.ReadCloser
	sink      WriteSink
	onPublish func()
	cancel    func()
	trace     func(event string, keyvals ...any)

	mu        sync.Mutex
	sawEOF    bool
	readErr   error
	sinkErr   error
	closeErr  error
	closed    bool
	closeDone chan struct{}
}

type topFillSink struct {
	sink        WriteSink
	coord       *writeCoordinator
	timer       *time.Timer
	release     func()
	cancelBegin func()
	preemptCh   chan struct{}
	preemptOnce sync.Once

	ioMu                 sync.Mutex
	mu                   sync.Mutex
	done                 bool
	preempted            bool
	registered           bool
	reportPreemptOnClose bool
	wrote                bool
	err                  error
}

type fillPreemptDetachedSink interface {
	detachForFillPreempt() func() error
}

type streamTeeWriter struct {
	dst     io.Writer
	sink    io.Writer
	sinkErr error
}

func (w *streamTeeWriter) Write(p []byte) (int, error) {
	n, err := writeAllCount(w.dst, p)
	if err != nil {
		return n, err
	}
	if _, err := writeAllCount(w.sink, p); err != nil {
		w.sinkErr = err
		return len(p), err
	}
	return len(p), nil
}

func streamThrough(src io.ReadCloser, sink WriteSink, cancel func(), onPublish func()) io.ReadCloser {
	return newStreamingReadCloser(src, sink, cancel, onPublish, nil)
}

func streamThroughWithTrace(src io.ReadCloser, sink WriteSink, cancel func(), onPublish func(), trace func(event string, keyvals ...any)) io.ReadCloser {
	return newStreamingReadCloser(src, sink, cancel, onPublish, trace)
}

func newStreamingReadCloser(src io.ReadCloser, sink WriteSink, cancel func(), onPublish func(), trace func(event string, keyvals ...any)) io.ReadCloser {
	return &fillReadCloser{
		src:       src,
		sink:      sink,
		cancel:    cancel,
		onPublish: onPublish,
		trace:     trace,
	}
}

func newPendingTopFillSink(coord *writeCoordinator, release func(), cancelBegin func()) *topFillSink {
	return &topFillSink{coord: coord, release: release, cancelBegin: cancelBegin, preemptCh: make(chan struct{})}
}

func (s *topFillSink) startLease(timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.preempted {
		return
	}
	if timeout > 0 {
		s.timer = time.AfterFunc(timeout, func() {
			_ = s.Preempt()
		})
	}
}

func (s *topFillSink) attach(sink WriteSink) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.preempted {
		return false
	}
	s.sink = sink
	return true
}

func (s *topFillSink) failBeginSet(err error) {
	s.mu.Lock()
	if s.err != nil {
		s.err = errors.Join(s.err, err)
	} else {
		s.err = err
	}
	s.done = true
	s.stopTimerLocked()
	cancelBegin := s.unregisterLocked()
	s.mu.Unlock()
	if cancelBegin != nil {
		cancelBegin()
	}
	if s.release != nil {
		s.release()
	}
}

func (s *topFillSink) isPreempted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.preempted
}

func (s *topFillSink) Write(p []byte) (int, error) {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	s.mu.Lock()
	if s.done || s.preempted {
		s.mu.Unlock()
		return len(p), nil
	}
	if s.sink == nil {
		s.mu.Unlock()
		return len(p), nil
	}
	sink := s.sink
	s.mu.Unlock()

	n, err := writeAllCount(sink, p)

	s.mu.Lock()
	defer s.mu.Unlock()
	if n > 0 {
		s.wrote = true
	}
	if s.done || s.preempted {
		return len(p), nil
	}
	if err != nil {
		s.err = err
		return n, err
	}
	return len(p), nil
}

func (s *topFillSink) Close() error {
	s.mu.Lock()
	if s.done {
		if s.preempted {
			if s.reportPreemptOnClose && s.wrote {
				s.mu.Unlock()
				return ErrTopWriteInvalidated
			}
			s.mu.Unlock()
			return nil
		}
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	s.mu.Lock()
	if s.done {
		if s.preempted {
			if s.reportPreemptOnClose && s.wrote {
				s.mu.Unlock()
				return ErrTopWriteInvalidated
			}
			s.mu.Unlock()
			return nil
		}
		err := s.err
		s.mu.Unlock()
		return err
	}
	if s.preempted {
		s.done = true
		if s.reportPreemptOnClose && s.wrote {
			s.err = errors.Join(s.err, ErrTopWriteInvalidated)
			err := s.err
			s.mu.Unlock()
			return err
		}
		s.mu.Unlock()
		return nil
	}
	s.done = true
	s.stopTimerLocked()
	cancelBegin := s.unregisterLocked()
	if s.sink == nil {
		err := s.err
		s.mu.Unlock()
		if cancelBegin != nil {
			cancelBegin()
		}
		return err
	}
	sink := s.sink
	s.mu.Unlock()

	err := sink.Close()
	if cancelBegin != nil {
		cancelBegin()
	}
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	return err
}

func (s *topFillSink) Abort() error {
	s.mu.Lock()
	if s.done {
		if s.preempted {
			s.mu.Unlock()
			return nil
		}
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	s.mu.Lock()
	if s.done {
		if s.preempted {
			s.mu.Unlock()
			return nil
		}
		err := s.err
		s.mu.Unlock()
		return err
	}
	if s.preempted {
		s.done = true
		s.mu.Unlock()
		return nil
	}
	s.done = true
	s.stopTimerLocked()
	cancelBegin := s.unregisterLocked()
	if s.sink == nil {
		err := s.err
		s.mu.Unlock()
		if cancelBegin != nil {
			cancelBegin()
		}
		return err
	}
	sink := s.sink
	s.mu.Unlock()

	err := sink.Abort()
	if cancelBegin != nil {
		cancelBegin()
	}
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	return err
}

func (s *topFillSink) Preempt() error {
	s.mu.Lock()
	if s.done {
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.preempted = true
	s.preemptOnce.Do(func() { close(s.preemptCh) })
	s.stopTimerLocked()
	cancelBegin := s.unregisterLocked()
	if s.sink == nil {
		err := s.err
		s.mu.Unlock()
		if cancelBegin != nil {
			cancelBegin()
		}
		return err
	}
	s.done = true
	sink := s.sink
	s.mu.Unlock()

	if cancelBegin != nil {
		cancelBegin()
	}
	cleanup := s.preemptCleanup(sink)
	if cleanup != nil {
		// Detach same-key coordination immediately so Set/Delete can make
		// progress even if the fill Write is stalled. The actual sink cleanup
		// stays serialized with Write because WriteSink does not promise that
		// terminal methods are safe to call concurrently with Write.
		go s.abortPreemptedSink(cleanup)
	}
	return nil
}

func (s *topFillSink) preemptCleanup(sink WriteSink) func() error {
	if detached, ok := sink.(fillPreemptDetachedSink); ok {
		return detached.detachForFillPreempt()
	}
	return sink.Abort
}

func (s *topFillSink) abortPreemptedSink(cleanup func() error) error {
	s.ioMu.Lock()
	err := cleanup()
	s.ioMu.Unlock()

	s.mu.Lock()
	if err != nil {
		s.err = errors.Join(s.err, err)
	}
	s.mu.Unlock()
	return err
}

func (s *topFillSink) preemptedSignal() <-chan struct{} {
	return s.preemptCh
}

func (s *topFillSink) finishPreemptedAttach(sink WriteSink) error {
	cleanup := s.preemptCleanup(sink)
	var err error
	if cleanup != nil {
		err = cleanup()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return s.err
	}
	s.done = true
	s.err = errors.Join(s.err, err)
	return s.err
}

func (s *topFillSink) Preempted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.preempted
}

func (s *topFillSink) stopTimerLocked() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

func (s *topFillSink) unregisterLocked() func() {
	if s.registered {
		s.registered = false
		s.coord.unregisterActiveFill(s)
	}
	cancelBegin := s.cancelBegin
	s.cancelBegin = nil
	return cancelBegin
}

func (r *fillReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.closed {
		closeDone := r.closeDone
		if closeDone != nil {
			r.mu.Unlock()
			<-closeDone
			r.mu.Lock()
		}
		err := r.readAfterClosedErrorLocked()
		r.mu.Unlock()
		return 0, err
	}
	r.mu.Unlock()

	n, err := r.src.Read(p)
	if n > 0 {
		if writeErr := writeAll(r.sink, p[:n]); writeErr != nil {
			r.mu.Lock()
			r.sinkErr = writeErr
			var readErr error
			if err != nil && !errors.Is(err, io.EOF) {
				r.readErr = err
				readErr = err
				r.traceEvent("stream_through_read_error", "err", err)
			}
			r.mu.Unlock()
			closeErr := r.Close()
			return n, errors.Join(readErr, writeErr, closeErr)
		}
	}

	r.mu.Lock()
	if errors.Is(err, io.EOF) {
		r.sawEOF = true
		r.traceEvent("stream_through_read_eof")
		r.mu.Unlock()
		if reportErr := autoFinalizeError(r.Close()); reportErr != nil {
			return n, reportErr
		}
		return n, err
	}
	if err != nil {
		r.readErr = err
		r.traceEvent("stream_through_read_error", "err", err)
		r.mu.Unlock()
		closeErr := r.Close()
		return n, errors.Join(err, closeErr)
	}
	r.mu.Unlock()
	return n, err
}

func (r *fillReadCloser) WriteTo(dst io.Writer) (int64, error) {
	r.mu.Lock()
	if r.closed {
		closeDone := r.closeDone
		if closeDone != nil {
			r.mu.Unlock()
			<-closeDone
			r.mu.Lock()
		}
		err := r.readAfterClosedErrorLocked()
		r.mu.Unlock()
		return 0, err
	}
	r.mu.Unlock()

	tee := &streamTeeWriter{dst: dst, sink: r.sink}

	var (
		written int64
		err     error
	)

	if wt, ok := r.src.(io.WriterTo); ok {
		written, err = wt.WriteTo(tee)
	} else {
		buf := streamCopyBufferPool.Get().([]byte)
		defer streamCopyBufferPool.Put(buf)

		for {
			nr, readErr := r.src.Read(buf)
			if nr > 0 {
				nw, writeErr := tee.Write(buf[:nr])
				written += int64(nw)
				if writeErr != nil {
					if readErr != nil && !errors.Is(readErr, io.EOF) {
						err = errors.Join(readErr, writeErr)
					} else {
						err = writeErr
					}
					break
				}
			}
			if errors.Is(readErr, io.EOF) {
				err = nil
				break
			}
			if readErr != nil {
				err = readErr
				break
			}
		}
	}
	if tee.sinkErr != nil {
		if err != nil && !errors.Is(err, tee.sinkErr) {
			err = errors.Join(err, tee.sinkErr)
		} else if err == nil {
			err = tee.sinkErr
		}
	}

	r.mu.Lock()
	if err == nil {
		r.sawEOF = true
		r.traceEvent("stream_through_write_to_eof", "bytes", written)
		r.mu.Unlock()
		return written, autoFinalizeError(r.Close())
	}
	if tee.sinkErr != nil {
		r.sinkErr = tee.sinkErr
		r.traceEvent("stream_through_write_to_sink_error", "bytes", written, "err", tee.sinkErr)
		if readErr := withoutError(err, tee.sinkErr); readErr != nil {
			r.readErr = readErr
		}
	} else {
		r.readErr = err
		r.traceEvent("stream_through_write_to_read_error", "bytes", written, "err", err)
	}
	r.mu.Unlock()
	closeErr := r.Close()
	return written, errors.Join(err, closeErr)
}

func (r *fillReadCloser) Close() error {
	r.mu.Lock()
	if r.closed {
		closeDone := r.closeDone
		if closeDone == nil {
			err := r.closeErr
			r.mu.Unlock()
			return err
		}
		r.mu.Unlock()
		<-closeDone
		r.mu.Lock()
		err := r.closeErr
		r.mu.Unlock()
		return err
	}
	r.closed = true
	closeDone := make(chan struct{})
	r.closeDone = closeDone
	publish := r.sawEOF && r.readErr == nil && r.sinkErr == nil
	r.traceEvent("stream_through_close_start", "publish", publish, "saw_eof", r.sawEOF, "read_err", r.readErr, "sink_err", r.sinkErr)
	r.mu.Unlock()

	var err error
	if publish {
		srcErr := r.src.Close()
		if srcErr != nil {
			r.traceEvent("stream_through_sink_abort_start")
			abortErr := r.sink.Abort()
			r.traceEvent("stream_through_sink_abort_done", "err", abortErr)
			err = errors.Join(abortErr, srcErr)
		} else {
			r.traceEvent("stream_through_sink_close_start")
			err = r.sink.Close()
			r.traceEvent("stream_through_sink_close_done", "err", err)
			if err == nil && r.onPublish != nil && !sinkPreempted(r.sink) {
				r.traceEvent("stream_through_on_publish_start")
				r.onPublish()
				r.traceEvent("stream_through_on_publish_done")
			}
		}
	} else {
		r.traceEvent("stream_through_sink_abort_start")
		err = r.sink.Abort()
		r.traceEvent("stream_through_sink_abort_done", "err", err)
		err = errors.Join(err, r.src.Close())
	}
	if r.cancel != nil {
		r.cancel()
	}

	r.mu.Lock()
	r.closeErr = err
	close(closeDone)
	r.mu.Unlock()
	r.traceEvent("stream_through_close_done", "err", err)
	return err
}

func (r *fillReadCloser) readAfterClosedErrorLocked() error {
	if r.sawEOF && r.readErr == nil {
		return io.EOF
	}
	if r.readErr != nil {
		return errors.Join(r.readErr, r.closeErr)
	}
	if r.closeErr != nil {
		return r.closeErr
	}
	return io.ErrClosedPipe
}

func sinkPreempted(sink WriteSink) bool {
	preempted, ok := sink.(interface{ Preempted() bool })
	return ok && preempted.Preempted()
}

func (r *fillReadCloser) traceEvent(event string, keyvals ...any) {
	if r.trace == nil {
		return
	}
	r.trace(event, keyvals...)
}

func autoFinalizeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrTopWriteInvalidated) {
		return withoutError(err, ErrTopWriteInvalidated)
	}
	return err
}

// withoutError recursively unwraps err and removes target from joined errors.
// Custom wrapper context around target may be discarded when only child errors remain.
func withoutError(err, target error) error {
	if err == nil {
		return nil
	}
	if !errors.Is(err, target) {
		return err
	}
	switch unwrapped := err.(type) {
	case interface{ Unwrap() []error }:
		remaining := make([]error, 0, len(unwrapped.Unwrap()))
		for _, child := range unwrapped.Unwrap() {
			if childErr := withoutError(child, target); childErr != nil {
				remaining = append(remaining, childErr)
			}
		}
		return errors.Join(remaining...)
	case interface{ Unwrap() error }:
		if childErr := withoutError(unwrapped.Unwrap(), target); childErr != nil {
			return childErr
		}
		return nil
	default:
		if errors.Is(err, target) {
			return nil
		}
		return err
	}
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	cancel    func()
	closeOnce sync.Once
	closeErr  error
}

func newCancelOnCloseReadCloser(rc io.ReadCloser, cancel func()) io.ReadCloser {
	return &cancelOnCloseReadCloser{ReadCloser: rc, cancel: cancel}
}

func (c *cancelOnCloseReadCloser) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.ReadCloser.Close()
		if c.cancel != nil {
			c.cancel()
		}
	})
	return c.closeErr
}

type cancelWriteSink struct {
	WriteSink
	cancel func()
	once   sync.Once
	err    error
}

func newCancelWriteSink(sink WriteSink, cancel func()) WriteSink {
	return &cancelWriteSink{WriteSink: sink, cancel: cancel}
}

func (c *cancelWriteSink) Close() error {
	c.once.Do(func() {
		c.err = c.WriteSink.Close()
		if c.cancel != nil {
			c.cancel()
		}
	})
	return c.err
}

func (c *cancelWriteSink) Abort() error {
	c.once.Do(func() {
		c.err = c.WriteSink.Abort()
		if c.cancel != nil {
			c.cancel()
		}
	})
	return c.err
}
