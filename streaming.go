package daramjwee

import (
	"errors"
	"io"
	"sync"
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

	mu       sync.Mutex
	sawEOF   bool
	readErr  error
	sinkErr  error
	closeErr error
	closed   bool
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
	return newStreamingReadCloser(src, sink, cancel, onPublish)
}

func newStreamingReadCloser(src io.ReadCloser, sink WriteSink, cancel func(), onPublish func()) io.ReadCloser {
	return &fillReadCloser{
		src:       src,
		sink:      sink,
		cancel:    cancel,
		onPublish: onPublish,
	}
}

func (r *fillReadCloser) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		if writeErr := writeAll(r.sink, p[:n]); writeErr != nil {
			r.mu.Lock()
			r.sinkErr = writeErr
			r.mu.Unlock()
			return n, writeErr
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if errors.Is(err, io.EOF) {
		r.sawEOF = true
		return n, err
	}
	if err != nil {
		r.readErr = err
	}
	return n, err
}

func (r *fillReadCloser) WriteTo(dst io.Writer) (int64, error) {
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
					err = writeErr
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

	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.sawEOF = true
		return written, nil
	}
	if tee.sinkErr != nil {
		r.sinkErr = tee.sinkErr
	} else {
		r.readErr = err
	}
	return written, err
}

func (r *fillReadCloser) Close() error {
	r.mu.Lock()
	if r.closed {
		err := r.closeErr
		r.mu.Unlock()
		return err
	}
	r.closed = true
	publish := r.sawEOF && r.readErr == nil && r.sinkErr == nil
	r.mu.Unlock()

	var err error
	if publish {
		err = r.sink.Close()
		if err == nil && r.onPublish != nil {
			r.onPublish()
		}
	} else {
		err = r.sink.Abort()
	}
	err = errors.Join(err, r.src.Close())
	if r.cancel != nil {
		r.cancel()
	}

	r.mu.Lock()
	r.closeErr = err
	r.mu.Unlock()
	return err
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
